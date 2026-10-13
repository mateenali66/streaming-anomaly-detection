"""Temporal contiguity of sliding-window sequences: audit and corrected builder.

training.py:469 builds sequences with a dense sliding window over frame order:

    for i in range(len(X) - seq_length + 1):
        sequences.append(X[i:i + seq_length])

It never checks that consecutive rows are adjacent in time. Two things break that
assumption in this dataset:

  1. Cooldown windows are labelled EXCLUDED and dropped before sequencing, so kept rows
     either side of a cooldown are ~20 minutes apart.
  2. A fold's test set is one repetition's runs, which are spread across the whole
     campaign in wall-clock time, so consecutive test rows can be hours or a day apart.

The result is that a share of sequences splice telemetry from unrelated points in the
campaign. That share grows with seq_length, and the paper's HPO searches seq_length in
[5, 20], so longer contexts are penalised by an artifact rather than by the data. This
module quantifies the effect and provides a contiguity-respecting builder.

Nothing here mutates training.py. The original create_sequences stays as-is so the
published numbers remain reproducible; the corrected builder is opt-in.

Run directly to produce the audit table:
    python analysis/cross_workload/sequence_contiguity.py
"""
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent

WORKLOADS = {
    "ecommerce": ROOT / "data" / "features_v3" / "features_{ws}s.parquet",
    "iot": ROOT / "data" / "v3" / "iot" / "features" / "features_{ws}s.parquet",
}
WINDOW_SIZES = [15, 30, 60, 120]
SEQ_LENGTHS = [5, 8, 10, 15, 20]


def infer_stride(window_start):
    """Typical spacing between consecutive windows, inferred from the data.

    Do not assume stride == window_seconds. The 15s configuration is built on 10s
    Prometheus scrapes and its spacing alternates (modal gap 20s, not 15s), so a hardcoded
    stride flags every adjacency as a break. The 30/60/120s configurations are regular.
    """
    t = np.asarray(window_start, dtype=np.float64)
    d = np.diff(t)
    d = d[d > 0]
    if d.size == 0:
        return float("nan")
    return float(np.median(d))


def contiguity_breaks(window_start, stride, factor=2.5):
    """Boolean array of length n-1: True where row i+1 does NOT follow row i in time.

    A break is a gap exceeding `factor` times the typical stride. The real breaks in this
    dataset are cooldown exclusions (~1230s) and cross-run jumps (hours), both orders of
    magnitude above any legitimate spacing, so the factor is not delicate.
    """
    t = np.asarray(window_start, dtype=np.float64)
    return np.diff(t) > stride * factor


def create_sequences_contiguous(X, seq_length, window_start, stride=None):
    """Sliding-window sequences that never span a temporal break.

    Returns (sequences, end_idx) where end_idx[k] is the row index of the LAST timestep
    of sequences[k]. Labels align as y[end_idx], replacing the dense-window assumption
    y[seq_length - 1:] used by the original builder.
    """
    X = np.asarray(X)
    n = len(X)
    if n < seq_length:
        return np.empty((0, seq_length, X.shape[1]), dtype=X.dtype), np.empty(0, dtype=int)

    if stride is None:
        stride = infer_stride(window_start)
    brk = contiguity_breaks(window_start, stride)
    # prefix[i] = number of breaks strictly before adjacency i
    prefix = np.concatenate([[0], np.cumsum(brk.astype(np.int64))])

    starts = []
    for i in range(n - seq_length + 1):
        # sequence covers rows i .. i+seq_length-1, i.e. adjacencies i .. i+seq_length-2
        if prefix[i + seq_length - 1] - prefix[i] == 0:
            starts.append(i)
    if not starts:
        return np.empty((0, seq_length, X.shape[1]), dtype=X.dtype), np.empty(0, dtype=int)

    starts = np.asarray(starts, dtype=int)
    seqs = np.stack([X[i:i + seq_length] for i in starts])
    return seqs, starts + seq_length - 1


def audit_split(df, stride, seq_lengths=SEQ_LENGTHS):
    t = df["window_start"].to_numpy(dtype=np.float64)
    n = len(t)
    if n < 2:
        return []
    brk = contiguity_breaks(t, stride)
    prefix = np.concatenate([[0], np.cumsum(brk.astype(np.int64))])
    gaps = np.diff(t)[brk]
    rows = []
    for L in seq_lengths:
        n_seq = n - L + 1
        if n_seq <= 0:
            continue
        kept = int(sum(1 for i in range(n_seq)
                       if prefix[i + L - 1] - prefix[i] == 0))
        rows.append(dict(
            n_rows=n, seq_length=L, n_sequences_dense=n_seq,
            n_sequences_contiguous=kept,
            n_straddling=n_seq - kept,
            frac_straddling=(n_seq - kept) / n_seq,
            n_breaks=int(brk.sum()),
            median_gap_s=float(np.median(gaps)) if gaps.size else 0.0,
            max_gap_s=float(gaps.max()) if gaps.size else 0.0,
        ))
    return rows


def main():
    records = []
    for wl, tmpl in WORKLOADS.items():
        for ws in WINDOW_SIZES:
            path = Path(str(tmpl).format(ws=ws))
            if not path.exists():
                print(f"skip missing {path}")
                continue
            cols = ["window_start", "label"] + [f"fold_{i}_split" for i in range(1, 11)]
            d = pd.read_parquet(path, columns=cols)
            stride = infer_stride(d["window_start"])
            for fold in range(1, 11):
                col = f"fold_{fold}_split"
                splits = {
                    "train_normal": (d[col] == "train") & (d["label"] == "NORMAL"),
                    "test": (d[col] == "test") & (d["label"] != "EXCLUDED"),
                }
                for split_name, mask in splits.items():
                    for r in audit_split(d.loc[mask], stride):
                        records.append(dict(workload=wl, window_seconds=ws, fold=fold,
                                            split=split_name, stride_s=stride, **r))
    df = pd.DataFrame(records)
    df.to_csv(OUT / "sequence_contiguity_audit.csv", index=False)

    lines = ["SEQUENCE TEMPORAL-CONTIGUITY AUDIT", "",
             "Fraction of sliding-window sequences that span a temporal break,",
             "under the dense builder used by training.py:469.",
             "Mean across 10 folds.", ""]
    for split in ("train_normal", "test"):
        sub = df[df["split"] == split]
        piv = sub.pivot_table(index=["workload", "window_seconds"],
                              columns="seq_length", values="frac_straddling")
        lines += [f"=== {split}: fraction of sequences straddling a break ===",
                  piv.round(3).to_string(), ""]
    gaps = df[df["split"] == "test"].groupby("workload")[["median_gap_s", "max_gap_s"]].max()
    lines += ["=== test-split break sizes (seconds) ===", gaps.round(0).to_string(), ""]

    text = "\n".join(lines)
    (OUT / "sequence_contiguity_audit.txt").write_text(text)
    print(text)
    print(f"Saved: {OUT}/sequence_contiguity_audit.csv")


if __name__ == "__main__":
    main()
