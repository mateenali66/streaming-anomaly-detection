"""Per-fold feature selection versus the published global selection.

Quantifies the effect of fitting the variance and correlation
filters once over the whole dataset, before fold assignment, rather than inside each fold.

The filters run once, before fold assignment. preprocessing.py calls
select_features() and then assign_folds(), and select_features() fits on every NORMAL window
in the dataset, including windows that later become test folds. So test-fold data did inform
which of the 6,491 candidate features survived to the published 475.

Two things bound how much this matters, and both should be stated rather than asserted:

  1. The filter is label-agnostic. It uses only NORMAL windows and only variance and Pearson
     correlation. No anomaly label enters it, so it cannot leak the target.
  2. It is a filter, not a fit. Nothing learned from test data is carried into the model
     beyond the identity of the retained columns.

This script measures the residual effect directly. It rebuilds the 6,491-feature
pre-selection matrix from the raw campaign metrics, reruns the identical filter using only
each fold's training NORMAL windows, and reports (a) how much each fold's selection differs
from the published one and (b) whether refitting per fold changes model performance.

Only the cheap models are retrained. The deep models take sequences and their cost is not
justified for a filter that cannot leak labels; the point-model result bounds the effect.

Output:
  feature_selection_overlap.csv      per fold, selection overlap with the published set
  feature_selection_sensitivity.csv  per fold and model, global vs per-fold selection
  feature_selection_leakage.txt
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import f1_score, roc_auc_score
from sklearn.preprocessing import StandardScaler
from sklearn.svm import OneClassSVM

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "sagemaker"))

import preprocessing as pp  # noqa: E402

WINDOW_S = 30
N_FOLDS = 10
SEED = 42
PUBLISHED = ROOT / "data" / "features_v3" / f"features_{WINDOW_S}s.parquet"

META = {"window_start", "window_end", "window_mid", "n_samples", "label", "fault_id",
        "fault_category", "run_number", "repetition", "run_source"}


def build_preselection():
    """Rebuild the pre-selection feature matrix, stopping before select_features()."""
    data_dir = ROOT / "data"
    baseline = pp.normalize_dataframe(pd.read_parquet(data_dir / "baseline_24h.parquet"))
    v1 = pp.normalize_dataframe(pd.read_parquet(data_dir / "campaign" / "campaign_v1_metrics.parquet"))
    v2 = pp.normalize_dataframe(pd.read_parquet(data_dir / "campaign" / "campaign_v2_metrics.parquet"))
    canonical = ["timestamp"] + sorted(set(baseline.columns) - {"timestamp"})
    combined = pd.concat([baseline.reindex(columns=canonical),
                          v1.reindex(columns=canonical),
                          v2.reindex(columns=canonical)], ignore_index=True)
    combined = combined.sort_values("timestamp").reset_index(drop=True)
    runs = pp.load_manifests(data_dir, workload="ecommerce")

    w = pp.window_aggregate(combined, window_seconds=WINDOW_S)
    w = pp.compute_derived_features(w, window_seconds=WINDOW_S)
    w = pp.add_broker_reachable(w)
    w = pp.label_windows(w, runs)
    return w


def select_on(df_subset, all_cols):
    """Run the published filter on a subset, return the retained column list."""
    sub = df_subset.copy()
    keep_meta = [c for c in sub.columns if c in META]
    sub = sub[keep_meta + all_cols]
    selected, report = pp.select_features(sub)
    cols = [c for c in selected.columns if c not in META]
    return cols, report


def fit_eval(model_name, Xn, Xtr, ytr, Xte, yte, seed=SEED):
    if model_name == "IF":
        m = IsolationForest(n_estimators=200, contamination=0.05,
                            random_state=seed, n_jobs=-1).fit(Xn)
        s = -m.decision_function(Xte)
    elif model_name == "OC_SVM":
        m = OneClassSVM(nu=0.05, kernel="rbf", gamma="scale").fit(Xn)
        s = -m.decision_function(Xte)
    elif model_name == "RF":
        m = RandomForestClassifier(n_estimators=200, class_weight="balanced",
                                   random_state=seed, n_jobs=-1).fit(Xtr, ytr)
        s = m.predict_proba(Xte)[:, 1]
    else:
        raise ValueError(model_name)
    auc = roc_auc_score(yte, s) if len(np.unique(yte)) > 1 else np.nan
    # F1 at the score median split is not the paper's threshold; use the score quantile
    # matching the training-set anomaly rate so both arms are treated identically.
    q = 1 - float(np.mean(ytr)) if len(ytr) else 0.9
    pred = (s >= np.quantile(s, np.clip(q, 0.5, 0.99))).astype(int)
    return auc, f1_score(yte, pred, zero_division=0)


def main():
    print("Rebuilding pre-selection matrix from raw campaign metrics...")
    pre = build_preselection()
    all_cols = [c for c in pre.columns if c not in META]
    print(f"  pre-selection: {len(pre)} windows x {len(all_cols)} features")

    pub = pd.read_parquet(PUBLISHED)
    pub_cols = [c for c in pub.columns if c not in META and not c.startswith("fold_")]
    print(f"  published selection: {len(pub_cols)} features")

    global_cols, global_report = select_on(pre, all_cols)
    print(f"  reproduced global selection: {len(global_cols)} features "
          f"(report: {global_report['initial_features']} -> {global_report['final_features']})")
    print(f"  overlap with published: "
          f"{len(set(global_cols) & set(pub_cols))}/{len(pub_cols)}")

    # Fold assignment on the published frame so splits match exactly
    folds = pub[[c for c in pub.columns if c.startswith("fold_")] + ["window_start"]]
    pre = pre.merge(folds, on="window_start", how="inner")
    print(f"  aligned to fold structure: {len(pre)} windows")

    overlap_rows, sens_rows = [], []
    for fold in range(1, N_FOLDS + 1):
        col = f"fold_{fold}_split"
        tr_mask = pre[col] == "train"
        fold_cols, rep = select_on(pre[tr_mask], all_cols)

        inter = len(set(fold_cols) & set(global_cols))
        union = len(set(fold_cols) | set(global_cols))
        overlap_rows.append(dict(
            fold=fold, n_selected_perfold=len(fold_cols), n_selected_global=len(global_cols),
            intersection=inter, jaccard=inter / union if union else np.nan,
            only_global=len(set(global_cols) - set(fold_cols)),
            only_perfold=len(set(fold_cols) - set(global_cols))))
        print(f"  fold {fold:>2}: {len(fold_cols):>4} features, "
              f"Jaccard {inter / union:.3f} vs global")

        te_mask = (pre[col] == "test") & (pre["label"] != "EXCLUDED")
        n_mask = tr_mask & (pre["label"] == "NORMAL")
        y = (pre["label"] == "ANOMALOUS").to_numpy()

        for arm, cols in (("global_selection", global_cols),
                          ("per_fold_selection", fold_cols)):
            Xn_r = pre.loc[n_mask, cols].to_numpy(np.float32)
            Xtr_r = pre.loc[tr_mask, cols].to_numpy(np.float32)
            Xte_r = pre.loc[te_mask, cols].to_numpy(np.float32)
            imp = SimpleImputer(strategy="median").fit(Xn_r)
            Xn_i, Xtr_i, Xte_i = (np.nan_to_num(imp.transform(a), nan=0.0,
                                                posinf=0.0, neginf=0.0)
                                  for a in (Xn_r, Xtr_r, Xte_r))
            sc = StandardScaler().fit(Xn_i)
            Xn_s, Xtr_s, Xte_s = (sc.transform(a) for a in (Xn_i, Xtr_i, Xte_i))
            ytr, yte = y[tr_mask.to_numpy()], y[te_mask.to_numpy()]
            for model in ("IF", "OC_SVM", "RF"):
                auc, f1 = fit_eval(model, Xn_s, Xtr_s, ytr, Xte_s, yte)
                sens_rows.append(dict(fold=fold, arm=arm, model=model,
                                      n_features=len(cols), auc=auc, f1=f1))

    ov = pd.DataFrame(overlap_rows)
    se = pd.DataFrame(sens_rows)
    ov.to_csv(OUT / "feature_selection_overlap.csv", index=False)
    se.to_csv(OUT / "feature_selection_sensitivity.csv", index=False)

    piv = se.pivot_table(index="model", columns="arm", values=["auc", "f1"])
    delta = (se[se.arm == "per_fold_selection"].set_index(["fold", "model"])[["auc", "f1"]]
             - se[se.arm == "global_selection"].set_index(["fold", "model"])[["auc", "f1"]])
    dsum = delta.groupby("model").agg(["mean", "std"]).round(4)

    lines = [
        "FEATURE SELECTION: PER-FOLD REFIT VERSUS THE PUBLISHED GLOBAL SELECTION", "",
        "The published pipeline runs select_features() before assign_folds(), fitting the",
        "variance and correlation filters on every NORMAL window including future test",
        "folds. The filter is label-agnostic, so it cannot leak the target, but test-fold",
        "data did inform which features survived. This quantifies the residual effect.", "",
        f"Pre-selection candidates: {len(all_cols)}",
        f"Published selection: {len(pub_cols)}   reproduced global: {len(global_cols)}", "",
        "=== Selection overlap, per-fold refit vs global ===",
        ov.round(3).to_string(index=False), "",
        "=== Performance, same folds, only the selection differs ===",
        piv.round(4).to_string(), "",
        "=== Per-fold delta (per-fold selection minus global selection) ===",
        dsum.to_string(),
    ]
    text = "\n".join(lines)
    (OUT / "feature_selection_leakage.txt").write_text(text)
    print("\n" + text)


if __name__ == "__main__":
    main()
