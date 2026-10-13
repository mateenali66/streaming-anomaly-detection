"""Per-rule threshold tuning for the curated rule baseline.

Tests whether tuning the *internal* rule thresholds (rather than only the
rule-score cutoff) closes the gap with the ML detectors. For each workload and fold, the
cal split is used to grid-search the three continuous-threshold rules (R1 consumer_lag,
R4 backpressure_ms_per_s, R7 heap utilisation ratio). The four binary rules
(R2 under_replicated, R3 failed_ckpt delta, R5 broker_offline, R6 rebalance_burst) keep
their fixed triggers because they have no continuous threshold to tune. The OR of all seven rules is then evaluated on
the test fold. We report mean per-fold TPR/FPR/F1 alongside the fixed-rule
v2 numbers.

Output: curated_rules_per_rule_tuned_summary.csv.
"""
from pathlib import Path
import itertools
import numpy as np
import pandas as pd
from sklearn.metrics import f1_score, precision_score, recall_score

import curated_rules_v2 as crv2

ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = Path(__file__).parent

# Tunable threshold grids
LAG_GRID  = [1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000]    # records_lag_max
BP_GRID   = [25, 50, 100, 200, 400, 600, 800]                          # backPressuredTime ms/s
HEAP_GRID = [0.70, 0.80, 0.85, 0.90, 0.95]                              # heap used / committed


def fire_rules_with_thresholds(signals, lag_t, bp_t, heap_t):
    fired = pd.DataFrame(index=signals.index)
    fired["R1"] = (signals["consumer_lag"] > lag_t).astype(int)
    fired["R2"] = (signals["under_repl"] > 0).astype(int)
    ckpt_delta = signals["failed_ckpt"].diff().fillna(0).clip(lower=0)
    fired["R3"] = (ckpt_delta > 0).astype(int)
    fired["R4"] = (signals["backpressure"] > bp_t).astype(int)
    fired["R5"] = ((signals["active_ctrl"] < 1) | (signals["offline_part"] > 0)).astype(int)
    fired["R6"] = (signals["rebal_failed"] > 0).astype(int)
    ratio = np.where(signals["heap_committed"] > 0,
                     signals["heap_used"] / signals["heap_committed"], 0.0)
    fired["R7"] = (ratio > heap_t).astype(int)
    # 2-window persistence on R1 and R4
    for r in ["R1", "R4"]:
        fired[r] = ((fired[r] == 1) & (fired[r].shift(1).fillna(0).astype(int) == 1)).astype(int)
    score = fired.sum(axis=1)
    pred = (score > 0).astype(int)
    return pred, score


def evaluate_per_rule_tuned(raw_df, feat_df):
    agg = crv2._agg_30s(raw_df.copy())
    feat_df = feat_df.copy()
    feat_df["ws"] = (pd.to_datetime(feat_df["window_start"]).astype("int64") // 1_000_000_000 // crv2.WINDOW_SEC) * crv2.WINDOW_SEC
    merged = feat_df.merge(agg, left_on="ws", right_on="window_start", how="left", suffixes=("", "_raw"))

    reps = sorted(r for r in merged["repetition"].unique() if r >= 1)
    rows = []
    for fold_idx, test_rep in enumerate(reps, start=1):
        cal_rep = reps[fold_idx % len(reps)]
        if cal_rep == test_rep:
            continue
        sub_test = merged[(merged["repetition"] == test_rep) & merged["label"].isin(["NORMAL", "ANOMALOUS", "RECOVERY"])].sort_values("ws").reset_index(drop=True)
        sub_cal = merged[(merged["repetition"] == cal_rep) & merged["label"].isin(["NORMAL", "ANOMALOUS", "RECOVERY"])].sort_values("ws").reset_index(drop=True)
        if len(sub_test) == 0 or len(sub_cal) == 0:
            continue
        sig_test = crv2.build_signals_raw(sub_test)
        sig_cal = crv2.build_signals_raw(sub_cal)
        y_test = (sub_test["label"] == "ANOMALOUS").astype(int).values
        y_cal = (sub_cal["label"] == "ANOMALOUS").astype(int).values
        if y_cal.sum() == 0 or y_test.sum() == 0:
            continue

        # Grid-search over (lag_t, bp_t, heap_t) on cal
        best_f1, best_combo = -1.0, (LAG_GRID[3], BP_GRID[2], HEAP_GRID[3])  # default = production guide
        for lag_t, bp_t, heap_t in itertools.product(LAG_GRID, BP_GRID, HEAP_GRID):
            pred_cal, _ = fire_rules_with_thresholds(sig_cal, lag_t, bp_t, heap_t)
            f1_cal = f1_score(y_cal, pred_cal, zero_division=0)
            if f1_cal > best_f1:
                best_f1 = f1_cal
                best_combo = (lag_t, bp_t, heap_t)

        lag_t, bp_t, heap_t = best_combo
        pred_test, _ = fire_rules_with_thresholds(sig_test, lag_t, bp_t, heap_t)

        tp = int(((pred_test == 1) & (y_test == 1)).sum())
        fp = int(((pred_test == 1) & (y_test == 0)).sum())
        fn = int(((pred_test == 0) & (y_test == 1)).sum())
        tn = int(((pred_test == 0) & (y_test == 0)).sum())

        rows.append({
            "fold": fold_idx, "test_rep": int(test_rep), "cal_rep": int(cal_rep),
            "best_lag_t": lag_t, "best_bp_t": bp_t, "best_heap_t": heap_t,
            "cal_f1": best_f1,
            "tp": tp, "fp": fp, "fn": fn, "tn": tn,
            "tpr": tp / max(tp + fn, 1),
            "fpr": fp / max(fp + tn, 1),
            "precision": precision_score(y_test, pred_test, zero_division=0),
            "recall":    recall_score(y_test, pred_test, zero_division=0),
            "f1":        f1_score(y_test, pred_test, zero_division=0),
        })
    return pd.DataFrame(rows)


def main():
    print("loading e-commerce raw campaign...")
    ec_raw = pd.concat([pd.read_parquet(crv2.EC_RAW_V1), pd.read_parquet(crv2.EC_RAW_V2)], ignore_index=True)
    ec_feat = pd.read_parquet(crv2.EC_FEATS)
    pf_ec = evaluate_per_rule_tuned(ec_raw, ec_feat)

    print("loading IoT raw campaign...")
    iot_raw = pd.read_parquet(crv2.IOT_RAW)
    iot_feat = pd.read_parquet(crv2.IOT_FEATS)
    pf_iot = evaluate_per_rule_tuned(iot_raw, iot_feat)

    pf_ec.to_csv(OUT_DIR / "curated_rules_per_rule_tuned_ecommerce_per_fold.csv", index=False)
    pf_iot.to_csv(OUT_DIR / "curated_rules_per_rule_tuned_iot_per_fold.csv", index=False)

    rows = []
    for label, df in [("ecommerce", pf_ec), ("iot", pf_iot)]:
        if len(df) == 0:
            continue
        rows.append({
            "workload": label,
            "n_folds": len(df),
            "tp_mean": df["tp"].mean(), "fp_mean": df["fp"].mean(),
            "fn_mean": df["fn"].mean(), "tn_mean": df["tn"].mean(),
            "tpr_mean": df["tpr"].mean(), "fpr_mean": df["fpr"].mean(),
            "f1_mean": df["f1"].mean(), "f1_std": df["f1"].std(),
            "median_lag_t":  float(df["best_lag_t"].median()),
            "median_bp_t":   float(df["best_bp_t"].median()),
            "median_heap_t": float(df["best_heap_t"].median()),
        })
    summary = pd.DataFrame(rows)
    summary.to_csv(OUT_DIR / "curated_rules_per_rule_tuned_summary.csv", index=False)
    print("\n=== PER-RULE TUNED RULE BASELINE SUMMARY ===")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
