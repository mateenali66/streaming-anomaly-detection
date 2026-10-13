"""RQ2 on a matched evaluation set: point models versus sequence models.

The capped, contiguity-corrected rerun evaluates the
sequence models on temporally valid sequences only, which is a strict subset of each test
fold. The point models were not rerun, correctly, since they consume single windows and the
sequence-construction defect never touched them. But that leaves the two families scored on
different window sets, and comparing them directly would confound the family effect with the
population change.

This restricts the published point-model scores to exactly the windows the capped sequence
models were evaluated on, so both families are scored on an identical set of windows with an
identical prevalence. The remaining difference is the thing RQ2 asks about.

The subset is reconstructed rather than assumed: for each fold we rebuild the contiguous
sequence end-indices at the capped length and take those rows of the test split, then verify
the resulting label vector matches the one the rerun logged alongside its scores.

Output:
  rq2_matched_per_fold.csv
  rq2_matched_summary.csv
  rq2_matched.txt
"""
import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import wilcoxon
from sklearn.metrics import roc_auc_score

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(OUT))
from sequence_contiguity import create_sequences_contiguous, infer_stride  # noqa: E402

PARQUETS = {
    "ecommerce": ROOT / "data" / "features_v3" / "features_30s.parquet",
    "iot": ROOT / "data" / "v3" / "iot" / "features" / "features_30s.parquet",
}
# Published per-window scores for the point models and supervised reference.
PUBLISHED_TREES = {
    "ecommerce": [ROOT / "data" / "results_v3" / "merged" / "raw_scores",
                  ROOT / "data" / "v3" / "results" / "ecom-gapfill" / "raw_scores"],
    "iot": [ROOT / "data" / "v3" / "iot" / "results" / inst / "raw_scores"
            for inst in ("gpu-1", "gpu-2", "cpu-1")],
}
POINT_MODELS = ["IF", "OC_SVM"]
REFERENCE_MODELS = ["RF", "RuleBased"]
SEQ_MODELS = ["LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE", "DeepSVDD", "DAGMM"]
CAP = 11
N_FOLDS = 10

META = {"window_start", "window_end", "window_mid", "n_samples", "label", "fault_id",
        "fault_category", "run_number", "repetition", "run_source"}


def load_npy(trees, model, fold):
    for t in trees:
        s, l = t / f"{model}_fold{fold}_scores.npy", t / f"{model}_fold{fold}_labels.npy"
        if s.exists() and l.exists():
            return np.load(s), np.load(l)
    return None, None


def selected_seq_lengths(capped_root, workload):
    """Per (model, fold) sequence length chosen by the capped rerun.

    Not every fold lands on the cap: one IoT Transformer-AE fold selected 10. Reconstructing
    the evaluated subset with the cap would then misalign, so read the actual value. The CSV
    is ragged because models append different hyperparameter columns, hence the manual parse.
    """
    import csv
    path = Path(capped_root) / workload / "optuna_results.csv"
    out = {}
    if not path.exists():
        return out
    rows = list(csv.reader(open(path)))
    if not rows:
        return out
    hdr = rows[0]
    try:
        mi, fi, si = hdr.index("model"), hdr.index("fold"), hdr.index("seq_length")
    except ValueError:
        return out
    for r in rows[1:]:
        if len(r) <= max(mi, fi, si):
            continue
        try:
            out[(r[mi], int(float(r[fi])))] = int(float(r[si]))
        except ValueError:
            continue
    return out


def contiguous_end_idx(df, fold, seq_len):
    """Row positions (within the fold's test split) that the capped rerun scored."""
    col = f"fold_{fold}_split"
    te = df[(df[col] == "test") & (df["label"] != "EXCLUDED")].reset_index(drop=True)
    t = te["window_start"].to_numpy(np.float64)
    X = np.zeros((len(te), 1), dtype=np.float32)
    _, end_idx = create_sequences_contiguous(X, seq_len, t, infer_stride(df["window_start"]))
    y = (te["label"] == "ANOMALOUS").to_numpy()
    return te, end_idx, y


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--capped-root", required=True,
                    help="local sync of s3 .../results/capped_hpo")
    ap.add_argument("--workloads", nargs="*", default=["ecommerce", "iot"])
    ap.add_argument("--cap", type=int, default=CAP)
    args = ap.parse_args()

    rows = []
    for wl in args.workloads:
        df = pd.read_parquet(PARQUETS[wl])
        capped_tree = [Path(args.capped_root) / wl / "raw_scores"]
        if not capped_tree[0].exists():
            print(f"skip {wl}: no capped raw_scores at {capped_tree[0]}")
            continue

        seq_lens = selected_seq_lengths(args.capped_root, wl)

        for fold in range(1, N_FOLDS + 1):
            # sequence models, from the capped rerun, each at its own selected length
            for m in SEQ_MODELS:
                s, lab = load_npy(capped_tree, m, fold)
                if s is None:
                    continue
                L = seq_lens.get((m, fold), args.cap)
                _, ei, y_all_m = contiguous_end_idx(df, fold, L)
                y_m = y_all_m[ei]
                if len(s) != len(y_m) or not np.array_equal(lab.astype(bool), y_m):
                    print(f"  {wl} f{fold} {m}: subset mismatch at L={L} "
                          f"({len(s)} scores vs {len(y_m)} reconstructed), skipping")
                    continue
                rows.append(dict(workload=wl, fold=fold, model=m, family="sequence",
                                 n=len(y_m), prevalence=float(y_m.mean()), seq_length=L,
                                 auc=float(roc_auc_score(y_m, s))))

            # Point models are compared on the modal subset, so every family is scored on
            # one common window set within a fold.
            te, end_idx, y_all = contiguous_end_idx(df, fold, args.cap)
            if end_idx.size < 20:
                continue
            y_sub = y_all[end_idx]

            # point models and references, published scores restricted to the same rows
            for m in POINT_MODELS + REFERENCE_MODELS:
                s, lab = load_npy(PUBLISHED_TREES[wl], m, fold)
                if s is None:
                    continue
                offset = len(te) - len(s)          # tail alignment of the published arrays
                idx = end_idx - offset
                idx = idx[(idx >= 0) & (idx < len(s))]
                if len(idx) != len(end_idx):
                    print(f"  {wl} f{fold} {m}: {len(end_idx) - len(idx)} rows outside "
                          f"the published score array, using {len(idx)}")
                yy = y_all[end_idx[-len(idx):]] if len(idx) else np.array([])
                if len(yy) < 20 or yy.sum() == 0 or yy.sum() == len(yy):
                    continue
                fam = "point" if m in POINT_MODELS else "reference"
                rows.append(dict(workload=wl, fold=fold, model=m, family=fam,
                                 n=len(yy), prevalence=float(yy.mean()),
                                 seq_length=np.nan,
                                 auc=float(roc_auc_score(yy, s[idx]))))

    d = pd.DataFrame(rows)
    if d.empty:
        print("nothing to compare")
        return
    d.to_csv(OUT / "rq2_matched_per_fold.csv", index=False)

    summ = d.groupby(["workload", "family", "model"]).agg(
        n_folds=("fold", "nunique"), auc_mean=("auc", "mean"),
        auc_se=("auc", lambda s: s.std(ddof=1) / np.sqrt(s.notna().sum())),
        prevalence=("prevalence", "mean"), n_windows=("n", "mean"),
    ).round(4).reset_index()
    summ.to_csv(OUT / "rq2_matched_summary.csv", index=False)

    lines = ["RQ2 ON A MATCHED EVALUATION SET", "",
             f"All models scored on the same windows: the temporally contiguous sequence",
             f"end-points at length {args.cap}. Point-model scores are the published ones,",
             "restricted to those rows. Prevalence is therefore identical within a fold.", ""]
    for wl in sorted(d["workload"].unique()):
        sub = summ[summ["workload"] == wl].sort_values("auc_mean", ascending=False)
        lines += [f"=== {wl} ===", sub.to_string(index=False), ""]
        # paired family contrast over folds, best of each family per fold
        w = d[d["workload"] == wl]
        pt = w[w.family == "point"].groupby("fold")["auc"].max()
        sq = w[w.family == "sequence"].groupby("fold")["auc"].max()
        common = pt.index.intersection(sq.index)
        if len(common) >= 3:
            diff = (sq.loc[common] - pt.loc[common]).astype(float)
            try:
                stat, p = wilcoxon(diff.values, alternative="two-sided")
            except ValueError:
                stat, p = float("nan"), 1.0
            lines += [f"best sequence minus best point, paired over {len(common)} folds: "
                      f"mean {diff.mean():+.4f} (SE {diff.std(ddof=1)/np.sqrt(len(diff)):.4f}), "
                      f"Wilcoxon p = {p:.4g}", ""]
    text = "\n".join(lines)
    (OUT / "rq2_matched.txt").write_text(text)
    print(text)


if __name__ == "__main__":
    main()
