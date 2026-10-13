"""Tuned variant of the curated rule baseline.

A tuned operational rule baseline that uses the same calibration budget as
the ML models. The fixed-threshold v2 baseline uses
production-guide thresholds (Burrow, Xinfra, Ververica) without per-fold
calibration. This variant keeps the seven rules and their continuous features, but
tunes the integer rule-score cutoff on each fold's cal split and applies it to the
test split. That matches the F1-optimal-on-cal protocol used by every other detector.

Output: curated_rules_tuned_summary.csv with per-workload mean TPR/FPR/F1 across folds,
used in tab:prevalence.
"""
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.metrics import f1_score, precision_score, recall_score

import curated_rules_v2 as crv2

ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = Path(__file__).parent


def find_optimal_score_cutoff(scores, y_true):
    """Find F1-optimal integer score cutoff over a calibration set."""
    if y_true.sum() == 0 or y_true.sum() == len(y_true):
        return 0
    best_t, best_f1 = 0, -1.0
    for t in range(0, int(scores.max()) + 1):
        preds = (scores > t).astype(int)
        f1 = f1_score(y_true, preds, zero_division=0)
        if f1 > best_f1:
            best_f1 = f1
            best_t = t
    return best_t


def evaluate_tuned(raw_df, feat_df):
    """Per-fold protocol mirroring sagemaker/training.py:
    test = one rep, cal = next rep (mod 10), train+hpo_val = the rest.
    Cal is used to pick the F1-optimal score cutoff; test uses that cutoff.
    """
    agg = crv2._agg_30s(raw_df.copy())
    feat_df = feat_df.copy()
    feat_df["ws"] = (pd.to_datetime(feat_df["window_start"]).astype("int64") // 1_000_000_000 // crv2.WINDOW_SEC) * crv2.WINDOW_SEC
    merged = feat_df.merge(agg, left_on="ws", right_on="window_start", how="left", suffixes=("", "_raw"))

    reps = sorted(r for r in merged["repetition"].unique() if r >= 1)
    rows = []
    for fold_idx, test_rep in enumerate(reps, start=1):
        cal_rep = reps[(fold_idx) % len(reps)]  # next rep modulo, matches CV layout
        if cal_rep == test_rep:
            continue
        m_test = (merged["repetition"] == test_rep) & (merged["label"].isin(["NORMAL", "ANOMALOUS", "RECOVERY"]))
        m_cal = (merged["repetition"] == cal_rep) & (merged["label"].isin(["NORMAL", "ANOMALOUS", "RECOVERY"]))

        sub_test = merged[m_test].sort_values("ws").reset_index(drop=True)
        sub_cal = merged[m_cal].sort_values("ws").reset_index(drop=True)
        if len(sub_test) == 0 or len(sub_cal) == 0:
            continue

        sig_test = crv2.build_signals_raw(sub_test)
        sig_cal = crv2.build_signals_raw(sub_cal)
        _, score_test, _ = crv2.fire_rules(sig_test)
        _, score_cal, _ = crv2.fire_rules(sig_cal)

        y_test = (sub_test["label"] == "ANOMALOUS").astype(int).values
        y_cal = (sub_cal["label"] == "ANOMALOUS").astype(int).values
        if y_test.sum() == 0 or y_cal.sum() == 0:
            continue

        cutoff = find_optimal_score_cutoff(score_cal.values, y_cal)
        pred = (score_test.values > cutoff).astype(int)

        # Build TP/FP/TN/FN
        tp = int(((pred == 1) & (y_test == 1)).sum())
        fp = int(((pred == 1) & (y_test == 0)).sum())
        fn = int(((pred == 0) & (y_test == 1)).sum())
        tn = int(((pred == 0) & (y_test == 0)).sum())
        tpr = tp / max(tp + fn, 1)
        fpr = fp / max(fp + tn, 1)

        rows.append({
            "fold": fold_idx, "test_rep": int(test_rep), "cal_rep": int(cal_rep),
            "cutoff": cutoff,
            "tp": tp, "fp": fp, "fn": fn, "tn": tn,
            "tpr": tpr, "fpr": fpr,
            "precision": precision_score(y_test, pred, zero_division=0),
            "recall": recall_score(y_test, pred, zero_division=0),
            "f1": f1_score(y_test, pred, zero_division=0),
        })
    return pd.DataFrame(rows)


def main():
    print("loading e-commerce raw campaign...")
    ec_raw = pd.concat([pd.read_parquet(crv2.EC_RAW_V1), pd.read_parquet(crv2.EC_RAW_V2)], ignore_index=True)
    ec_feat = pd.read_parquet(crv2.EC_FEATS)
    pf_ec = evaluate_tuned(ec_raw, ec_feat)

    print("loading IoT raw campaign...")
    iot_raw = pd.read_parquet(crv2.IOT_RAW)
    iot_feat = pd.read_parquet(crv2.IOT_FEATS)
    pf_iot = evaluate_tuned(iot_raw, iot_feat)

    pf_ec.to_csv(OUT_DIR / "curated_rules_tuned_ecommerce_per_fold.csv", index=False)
    pf_iot.to_csv(OUT_DIR / "curated_rules_tuned_iot_per_fold.csv", index=False)

    summary_rows = []
    for label, df in [("ecommerce", pf_ec), ("iot", pf_iot)]:
        if len(df) == 0:
            continue
        summary_rows.append({
            "workload": label,
            "n_folds": len(df),
            "tp_mean": df["tp"].mean(), "fp_mean": df["fp"].mean(),
            "fn_mean": df["fn"].mean(), "tn_mean": df["tn"].mean(),
            "tpr_mean": df["tpr"].mean(), "fpr_mean": df["fpr"].mean(),
            "precision_mean": df["precision"].mean(),
            "recall_mean": df["recall"].mean(),
            "f1_mean": df["f1"].mean(), "f1_std": df["f1"].std(),
            "cutoff_mode": int(df["cutoff"].mode().iloc[0]),
        })
    summary = pd.DataFrame(summary_rows)
    summary.to_csv(OUT_DIR / "curated_rules_tuned_summary.csv", index=False)
    print("\n=== TUNED RULE BASELINE SUMMARY ===")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
