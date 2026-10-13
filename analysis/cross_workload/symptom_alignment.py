"""Symptom onset versus injection onset, and symptom-aligned label sensitivity.

Checks whether injection-period labels coincide with verified
symptom manifestation, and reports the sensitivity of results to symptom-aligned labels.

Every window inside an injection interval is currently labelled ANOMALOUS regardless of
whether the fault had yet produced any measurable effect. A broker killed at t=0 does not
change consumer lag at t=0, so the first windows of an injection may be indistinguishable
from normal, and every model is penalised for not detecting something that has not happened.

Symptom onset is defined per run, self-calibrated, and model-free:

  run          one temporally contiguous block: pre-fault NORMAL, injection, recovery
  baseline     the run's own pre-fault NORMAL windows
  statistic    number of features more than SIGMA_K sigma from the baseline mean
  envelope     the largest value that statistic takes anywhere in the run's own baseline
  onset        the first injection window whose statistic exceeds the envelope

Using each run's own baseline avoids importing a global threshold, and using a count of
out-of-envelope features rather than a max z-score avoids a single noisy feature dominating.

Two relabelings are reported. Under "relabel", injection windows before symptom onset become
NORMAL, the conservative reading that nothing measurable had happened. Under "exclude" they
are dropped, on the view that their true state is ambiguous. Sensitivity re-scores the
published per-window scores under both, so no model is retrained and the comparison isolates
the labelling change.

Output:
  symptom_onset_per_run.csv       one row per fault run
  symptom_onset_by_fault.csv      per fault type
  symptom_alignment_sensitivity.csv  per model, published vs symptom-aligned labels
  symptom_alignment.txt
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import f1_score, roc_auc_score

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(OUT))
from sequence_contiguity import infer_stride, contiguity_breaks  # noqa: E402

PARQUET = ROOT / "data" / "features_v3" / "features_30s.parquet"
SCORE_TREES = [ROOT / "data" / "results_v3" / "merged" / "raw_scores",
               ROOT / "data" / "v3" / "results" / "ecom-gapfill" / "raw_scores"]
PANEL = OUT / "panel_overall.csv"

WINDOW_S = 30
SIGMA_K = 3.0
BASELINE_WINDOWS = 10   # protocol's 5-minute pre-fault window at 30s
SIGMA_FLOOR = 1e-8
N_FOLDS = 10

META = {"window_start", "window_end", "window_mid", "n_samples", "label", "fault_id",
        "fault_category", "run_number", "repetition", "run_source"}


def feature_columns(df):
    return [c for c in df.columns if c not in META and not c.startswith("fold_")]


def blocks_of(df):
    t = df["window_start"].to_numpy(np.float64)
    stride = infer_stride(df["window_start"])
    brk = contiguity_breaks(t, stride)
    edges = np.concatenate([[0], np.flatnonzero(brk) + 1, [len(t)]])
    return [(int(edges[i]), int(edges[i + 1])) for i in range(len(edges) - 1)]


def analyse_runs(df, feat_cols):
    """Symptom onset per single-fault run."""
    X = df[feat_cols].to_numpy(np.float64)
    labels = df["label"].to_numpy()
    faults = df["fault_id"].to_numpy()
    rows, onset_index = [], {}

    for bi, (a, b) in enumerate(blocks_of(df)):
        seg_lab = labels[a:b]
        anom = np.flatnonzero(seg_lab == "ANOMALOUS")
        if anom.size == 0:
            continue
        fids = sorted({f for f in faults[a:b] if f})
        if len(fids) != 1:
            continue
        fault = fids[0]

        inj_start = int(anom[0])
        # Baseline is the protocol's 5-minute pre-fault window, i.e. the BASELINE_WINDOWS
        # NORMAL windows immediately before injection. Do not use every preceding NORMAL
        # window: contiguous blocks sometimes run on into the 24-hour baseline collection,
        # and an envelope taken over that much data is so permissive that no injection ever
        # clears it.
        base_all = np.flatnonzero(seg_lab[:inj_start] == "NORMAL")
        if base_all.size < 4:
            continue
        base = base_all[-BASELINE_WINDOWS:]

        Xb = X[a:b][base]
        mu = np.nanmean(Xb, axis=0)
        sd = np.nanstd(Xb, axis=0)
        sd = np.where(~np.isfinite(sd) | (sd < SIGMA_FLOOR), np.nan, sd)
        usable = np.isfinite(mu) & np.isfinite(sd)

        Z = np.abs((X[a:b][:, usable] - mu[usable]) / sd[usable])
        stat = np.nansum(Z > SIGMA_K, axis=1)
        envelope = int(stat[base].max())

        rel = None
        for k in range(inj_start, len(seg_lab)):
            if seg_lab[k] != "ANOMALOUS":
                break
            if stat[k] > envelope:
                rel = k - inj_start
                break

        n_inj = int((seg_lab == "ANOMALOUS").sum())
        rows.append(dict(
            block=bi, fault=fault, n_windows=b - a, n_baseline=int(base.size), n_baseline_available=int(base_all.size),
            n_injection=n_inj, envelope=envelope,
            baseline_stat_median=float(np.median(stat[base])),
            injection_stat_max=int(stat[inj_start:inj_start + n_inj].max()),
            onset_offset_windows=rel,
            onset_lag_s=(rel * WINDOW_S) if rel is not None else np.nan,
            symptom_detected=rel is not None,
            frac_injection_pre_symptom=(rel / n_inj) if rel is not None else 1.0,
        ))
        onset_index[bi] = (a, b, inj_start, rel, n_inj)
    return pd.DataFrame(rows), onset_index


def build_symptom_labels(df, onset_index):
    """Return per-row arrays: symptom-aligned binary label, and an exclude mask."""
    n = len(df)
    y_pub = (df["label"] == "ANOMALOUS").to_numpy()
    y_sym = y_pub.copy()
    drop = np.zeros(n, dtype=bool)
    for (a, b, inj_start, rel, n_inj) in onset_index.values():
        pre_end = inj_start + (rel if rel is not None else n_inj)
        idx = np.arange(a + inj_start, a + pre_end)
        y_sym[idx] = False   # no measurable effect yet
        drop[idx] = True     # ambiguous under the exclude variant
    return y_pub, y_sym, drop


def find_scores(model, fold):
    for tree in SCORE_TREES:
        s, l = tree / f"{model}_fold{fold}_scores.npy", tree / f"{model}_fold{fold}_labels.npy"
        if s.exists() and l.exists():
            return np.load(s), np.load(l)
    return None, None


def sensitivity(df, y_pub, y_sym, drop):
    panel = pd.read_csv(PANEL)
    panel = panel[(panel["workload"] == "ecommerce") & (panel["window_s"] == WINDOW_S)
                  & (panel["category"] == "all")]
    thr = {(r["model"], int(r["fold"])): float(r["threshold"])
           for _, r in panel.iterrows() if pd.notna(r["threshold"])}
    models = sorted(panel["model"].unique())
    rows = []
    for fold in range(1, N_FOLDS + 1):
        col = f"fold_{fold}_split"
        mask = (df[col] == "test") & (df["label"] != "EXCLUDED")
        pos = np.flatnonzero(mask.to_numpy())
        for model in models:
            s, lab = find_scores(model, fold)
            if s is None or (model, fold) not in thr:
                continue
            sel = pos[len(pos) - len(s):]              # tail alignment
            if not np.array_equal(lab.astype(bool), y_pub[sel]):
                continue
            t = thr[(model, fold)]
            for variant, yy, keep in (
                    ("published", y_pub[sel], np.ones(len(sel), bool)),
                    ("symptom_relabel", y_sym[sel], np.ones(len(sel), bool)),
                    ("symptom_exclude", y_pub[sel], ~drop[sel])):
                yv, sv = yy[keep], s[keep]
                if yv.sum() == 0 or yv.sum() == len(yv):
                    continue
                pred = (sv >= t).astype(int)
                rows.append(dict(model=model, fold=fold, variant=variant,
                                 n=len(yv), prevalence=float(yv.mean()),
                                 f1=float(f1_score(yv, pred, zero_division=0)),
                                 auc=float(roc_auc_score(yv, sv))))
    return pd.DataFrame(rows)


def main():
    df = pd.read_parquet(PARQUET)
    df = df[df["label"] != "EXCLUDED"].reset_index(drop=True)
    feat_cols = feature_columns(df)
    print(f"{len(df)} windows, {len(feat_cols)} features")

    runs, onset_index = analyse_runs(df, feat_cols)
    runs.to_csv(OUT / "symptom_onset_per_run.csv", index=False)

    by_fault = runs.groupby("fault").agg(
        n_runs=("block", "count"),
        n_with_symptom=("symptom_detected", "sum"),
        median_lag_s=("onset_lag_s", "median"),
        max_lag_s=("onset_lag_s", "max"),
        median_frac_pre_symptom=("frac_injection_pre_symptom", "median"),
    ).reset_index()
    by_fault["pct_injection_before_symptom"] = 100 * by_fault["median_frac_pre_symptom"]
    by_fault.to_csv(OUT / "symptom_onset_by_fault.csv", index=False)

    y_pub, y_sym, drop = build_symptom_labels(df, onset_index)
    sens = sensitivity(df, y_pub, y_sym, drop)
    sens.to_csv(OUT / "symptom_alignment_sensitivity.csv", index=False)

    lines = ["SYMPTOM ONSET VERSUS INJECTION ONSET", "",
             f"Symptom statistic: number of features beyond {SIGMA_K} sigma of the run's own",
             "pre-fault baseline. Onset is the first injection window whose statistic exceeds",
             "the largest value that statistic reaches anywhere in that run's baseline.", "",
             by_fault.drop(columns=["median_frac_pre_symptom"]).round(2).to_string(index=False),
             "",
             f"Windows labelled ANOMALOUS with no measurable effect yet: "
             f"{int(drop.sum())} of {int(y_pub.sum())} "
             f"({100 * drop.sum() / max(y_pub.sum(), 1):.1f}%)."]

    if not sens.empty:
        piv = sens.pivot_table(index="model", columns="variant",
                               values=["auc", "f1", "prevalence"])
        lines += ["", "=== Sensitivity: published scores re-scored under each labelling ===",
                  "symptom_relabel keeps every window, moving pre-symptom injection windows to",
                  "normal. symptom_exclude drops them. Neither retrains anything.", "",
                  piv.round(3).to_string()]
    text = "\n".join(lines)
    (OUT / "symptom_alignment.txt").write_text(text)
    print(text)
    print(f"\nSaved: {OUT}/symptom_onset_by_fault.csv")


if __name__ == "__main__":
    main()
