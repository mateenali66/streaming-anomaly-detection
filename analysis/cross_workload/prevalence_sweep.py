"""
Prevalence-adjusted F1 / precision sweep.

The per-fold test prevalence is ~31.9% because each fold's test set is one
repetition's eight campaign runs (~80 pre-fault + 72 anomalous + 74 recovery
= ~226 windows) with the 24-hour baseline kept in training. Real production
prevalence is much lower.

Because TPR (= recall) and FPR are prevalence-invariant under fixed-threshold
detection, we can derive expected precision at any target prevalence p:
    precision(p) = TPR * p / (TPR * p + FPR * (1 - p))
    F1(p)        = 2 * precision(p) * TPR / (precision(p) + TPR)

We compute these from per-fold mean TPR and FPR for each model on each
workload at the 30s anchor.
"""
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
PANEL = ROOT / "analysis/cross_workload/panel_overall.csv"
OUT = ROOT / "analysis/cross_workload/prevalence_sweep.csv"

TARGET_PREVALENCES = [0.01, 0.05, 0.079, 0.10, 0.20, 0.319]


def derive_tpr_fpr(df):
    df = df.copy()
    df["TP"] = df["recall"] * df["true_anomalies"]
    df["FN"] = df["true_anomalies"] - df["TP"]
    df["FP"] = df["predicted_anomalies"] - df["TP"]
    df["TN"] = df["total_samples"] - df["TP"] - df["FP"] - df["FN"]
    df["TPR"] = df["recall"]
    df["FPR"] = df["FP"] / (df["FP"] + df["TN"]).replace(0, np.nan)
    return df


def precision_at(tpr, fpr, p):
    num = tpr * p
    den = tpr * p + fpr * (1 - p)
    return num / den if den > 0 else 0.0


def f1_at(tpr, fpr, p):
    pr = precision_at(tpr, fpr, p)
    if pr + tpr == 0:
        return 0.0
    return 2 * pr * tpr / (pr + tpr)


def main():
    panel = pd.read_csv(PANEL)
    rows = []
    for wl in ["ecommerce", "iot"]:
        sub = panel[(panel["workload"] == wl) & (panel["window_s"] == 30)].copy()
        sub = derive_tpr_fpr(sub)
        agg = sub.groupby("model").agg(tpr=("TPR", "mean"), fpr=("FPR", "mean")).reset_index()
        for _, r in agg.iterrows():
            row = {"workload": wl, "model": r["model"],
                   "tpr": round(r["tpr"], 3), "fpr": round(r["fpr"], 3)}
            for p in TARGET_PREVALENCES:
                row[f"f1_p{int(p*1000):03d}"] = round(f1_at(r["tpr"], r["fpr"], p), 3)
                row[f"prec_p{int(p*1000):03d}"] = round(precision_at(r["tpr"], r["fpr"], p), 3)
            rows.append(row)
    out = pd.DataFrame(rows)
    out.to_csv(OUT, index=False)
    print(out.to_string(index=False))
    print(f"\nWritten {OUT}")

    # Summary table for the paper: F1 by model at four prevalence levels
    summary = out[["workload", "model", "tpr", "fpr", "f1_p079", "f1_p050", "f1_p010", "prec_p079", "prec_p010"]]
    print("\n=== F1 across prevalence levels (e-commerce, sorted by F1 at 7.9%) ===")
    print(summary[summary["workload"]=="ecommerce"].sort_values("f1_p079", ascending=False).to_string(index=False))
    print("\n=== F1 across prevalence levels (IoT, sorted by F1 at 7.9%) ===")
    print(summary[summary["workload"]=="iot"].sort_values("f1_p079", ascending=False).to_string(index=False))


if __name__ == "__main__":
    main()
