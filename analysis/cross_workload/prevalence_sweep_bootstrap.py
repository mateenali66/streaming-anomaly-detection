"""Bootstrap CIs over folds for prevalence-adjusted F1.

Confidence intervals around the prevalence-adjusted F1 numbers. The point estimates use mean per-fold TPR/FPR; this
script resamples folds with replacement (10,000 iterations), recomputes mean
TPR/FPR per resample, and derives the F1(p) distribution at each target
prevalence p in {0.01, 0.05, 0.079, 0.319} for the e-commerce 30s anchor.

Output: prevalence_sweep_bootstrap.csv with point F1, 2.5%/97.5% bounds.
"""
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
PANEL = ROOT / "analysis/cross_workload/panel_overall.csv"
OUT = ROOT / "analysis/cross_workload/prevalence_sweep_bootstrap.csv"

PREVALENCES = [0.01, 0.05, 0.079, 0.319]
N_BOOT = 10_000
RNG = np.random.default_rng(42)


def derive(df):
    df = df.copy()
    df["TP"] = df["recall"] * df["true_anomalies"]
    df["FN"] = df["true_anomalies"] - df["TP"]
    df["FP"] = df["predicted_anomalies"] - df["TP"]
    df["TN"] = df["total_samples"] - df["TP"] - df["FP"] - df["FN"]
    df["TPR"] = df["recall"]
    df["FPR"] = df["FP"] / (df["FP"] + df["TN"]).replace(0, np.nan)
    return df


def f1_at(tpr, fpr, p):
    num = tpr * p
    den = tpr * p + fpr * (1 - p)
    if den <= 0:
        return 0.0
    pr = num / den
    if pr + tpr == 0:
        return 0.0
    return 2 * pr * tpr / (pr + tpr)


def bootstrap_model(model_df):
    """Resample folds with replacement, recompute mean TPR/FPR, derive F1(p)."""
    folds = model_df["fold"].unique()
    out = {p: [] for p in PREVALENCES}
    for _ in range(N_BOOT):
        idx = RNG.choice(folds, size=len(folds), replace=True)
        sub = pd.concat([model_df[model_df["fold"] == f] for f in idx])
        tpr = sub["TPR"].mean()
        fpr = sub["FPR"].mean()
        for p in PREVALENCES:
            out[p].append(f1_at(tpr, fpr, p))
    return out


def main():
    panel = pd.read_csv(PANEL)
    sub = panel[(panel["workload"] == "ecommerce") & (panel["window_s"] == 30)]
    sub = derive(sub)

    rows = []
    for model, mdf in sub.groupby("model"):
        if mdf["TPR"].isna().all() or mdf["FPR"].isna().all():
            continue
        boot = bootstrap_model(mdf)
        tpr_mean = mdf["TPR"].mean()
        fpr_mean = mdf["FPR"].mean()
        row = {"model": model, "tpr": round(tpr_mean, 3), "fpr": round(fpr_mean, 3)}
        for p in PREVALENCES:
            point = f1_at(tpr_mean, fpr_mean, p)
            arr = np.array(boot[p])
            lo, hi = np.quantile(arr, [0.025, 0.975])
            row[f"f1_p{int(p*1000):03d}"] = round(point, 3)
            row[f"f1_p{int(p*1000):03d}_lo"] = round(float(lo), 3)
            row[f"f1_p{int(p*1000):03d}_hi"] = round(float(hi), 3)
        rows.append(row)
    df = pd.DataFrame(rows).sort_values("f1_p079", ascending=False)
    df.to_csv(OUT, index=False)
    print(df.to_string(index=False))
    print(f"\nWritten {OUT}")


if __name__ == "__main__":
    main()
