"""
Curated production-style rule baseline.

A small set of named alerting rules in the style of Kafka and Flink monitoring guides:
Burrow-style consumer-lag thresholds, Flink checkpoint-failure alerts, backpressure-ratio
detectors and GC-pressure rules. Unlike the max-z diagnostic, which fires on every window
because some of the 475 features always exceeds any z-threshold, the rules fire only on
the signals they name.

Per-window features (post variance + correlation filter) are aggregated to a
small set of named signals, then thresholded. A window is anomalous if the
rule has fired in the current OR previous window (1-window persistence) for
sustained-state rules, or in the current window for instantaneous rules.

Outputs per-fold mean F1, AUC, etc. across both workloads. The 10-fold split
matches the ML protocol (test = one repetition; calibration/training reps
unused since rules have no learnable parameters).
"""
import json
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score, average_precision_score, f1_score, precision_score, recall_score


ROOT = Path(__file__).resolve().parents[2]
EC_FEATS = ROOT / "data/features_v3/features_30s.parquet"
IOT_FEATS = ROOT / "data/v3/iot/features/features_30s.parquet"


def aggregate_signal(df, contains, agg="max"):
    """Aggregate matching feature columns row-wise (max across instances)."""
    cols = [c for c in df.columns if all(t in c for t in contains)]
    if not cols:
        return pd.Series(np.zeros(len(df)), index=df.index)
    if agg == "max":
        return df[cols].max(axis=1)
    if agg == "mean":
        return df[cols].mean(axis=1)
    if agg == "any_pos":
        return (df[cols] > 0).any(axis=1).astype(int)
    raise ValueError(agg)


def build_signals(df):
    """Extract a small set of named monitoring signals from the feature parquet."""
    sig = pd.DataFrame(index=df.index)

    # Consumer lag: max records_lag across all consumer subtasks (mean aggregation).
    sig["consumer_lag_max"] = aggregate_signal(df, ("records_lag", "_mean"), "max")
    # Failed checkpoints: any Flink job had a failed checkpoint in this window.
    sig["failed_checkpoints"] = aggregate_signal(df, ("numberOfFailedCheckpoints", "_max"), "max")
    # Backpressure: max backPressuredTimeMsPerSecond mean across subtasks.
    sig["backpressure_ms"] = aggregate_signal(df, ("backPressuredTimeMsPerSecond", "_mean"), "max")
    # GC rate: max gc_count_slope across brokers (slope of cumulative count = rate).
    sig["gc_count_slope"] = aggregate_signal(df, ("gc_collection_seconds_count", "_slope"), "max")
    # JVM heap pressure: max heap memory used across processes.
    sig["heap_used"] = aggregate_signal(df, ("Memory_Heap_Used", "_mean"), "max")

    return sig


# Production thresholds (chosen on the calibration repetition per fold).
# We pick a single threshold-scaling factor per signal globally so the rules
# are agnostic to per-fault tuning, then report results.
RULES = [
    ("consumer_lag_high", "consumer_lag_max", "absolute", 10_000.0),
    ("checkpoint_failed", "failed_checkpoints", "any_positive", 0.0),
    ("backpressure_high", "backpressure_ms", "absolute", 500.0),  # >50% of 1000ms/s
    ("gc_rate_high", "gc_count_slope", "absolute", 1.0),  # GC events accelerating
]


def evaluate_rules(signals):
    """Apply rules with 1-window persistence for sustained-state rules."""
    fired = pd.DataFrame(index=signals.index)
    for name, col, kind, thr in RULES:
        if kind == "absolute":
            f = (signals[col] > thr).astype(int)
        elif kind == "any_positive":
            f = (signals[col] > thr).astype(int)
        else:
            raise ValueError(kind)
        fired[name] = f

    # Persistence: consumer_lag and backpressure require 2 consecutive windows.
    persistent = ["consumer_lag_high", "backpressure_high"]
    for r in persistent:
        fired[r] = ((fired[r] == 1) & (fired[r].shift(1).fillna(0).astype(int) == 1)).astype(int)

    # Anomaly score: number of rules fired (0..4); binary prediction: any rule fired.
    score = fired.sum(axis=1)
    pred = (score > 0).astype(int)
    return pred, score, fired


def per_fold_metrics(df, label_col="label_binary"):
    """Same fold layout as ML: test rep i, others train (unused for rules)."""
    rows = []
    reps = sorted(r for r in df["repetition"].unique() if r >= 1)
    for fold_idx, test_rep in enumerate(reps, start=1):
        # Test set = pre-fault NORMAL of this rep + fault windows + recovery of this rep
        # Match ML protocol: include NORMAL+ANOMALOUS+RECOVERY (RECOVERY treated as positive=0).
        # We follow the labelling already used in ML evaluation: ANOMALOUS=1, NORMAL/RECOVERY=0,
        # and exclude EXCLUDED windows.
        m_test = (df["repetition"] == test_rep) & (df["label"].isin(["NORMAL", "ANOMALOUS", "RECOVERY"]))
        # Also include a slice of the 24h baseline NORMAL pool to match per-fold prevalence.
        # The ML pipeline uses test_rep's runs only; we follow the same.
        sub = df[m_test].sort_values("window_mid").reset_index(drop=True)
        if len(sub) == 0:
            continue
        signals = build_signals(sub)
        pred, score, fired = evaluate_rules(signals)
        y = (sub["label"] == "ANOMALOUS").astype(int).values
        if y.sum() == 0 or y.sum() == len(y):
            continue
        rows.append({
            "fold": fold_idx,
            "test_rep": int(test_rep),
            "n_windows": len(sub),
            "n_anom": int(y.sum()),
            "precision": precision_score(y, pred, zero_division=0),
            "recall": recall_score(y, pred, zero_division=0),
            "f1": f1_score(y, pred, zero_division=0),
            "auc_roc": roc_auc_score(y, score) if len(set(y)) > 1 else float("nan"),
            "pr_auc": average_precision_score(y, score) if len(set(y)) > 1 else float("nan"),
        })
    return pd.DataFrame(rows)


def per_fault_metrics(df):
    """Per-fault F1 across all repetitions of that fault."""
    rows = []
    for fault in sorted(f for f in df["fault_id"].unique() if f and f != ""):
        m = (df["fault_id"] == fault) & (df["label"].isin(["NORMAL", "ANOMALOUS", "RECOVERY"]))
        # Add some per-rep aggregation
        per_rep = []
        for rep in sorted(df.loc[m, "repetition"].unique()):
            sub = df[(df["fault_id"] == fault) & (df["repetition"] == rep) & (df["label"].isin(["NORMAL", "ANOMALOUS", "RECOVERY"]))].sort_values("window_mid").reset_index(drop=True)
            if len(sub) == 0: continue
            signals = build_signals(sub)
            pred, _, _ = evaluate_rules(signals)
            y = (sub["label"] == "ANOMALOUS").astype(int).values
            if y.sum() == 0: continue
            per_rep.append(f1_score(y, pred, zero_division=0))
        if per_rep:
            rows.append({"fault_id": fault, "f1_mean": float(np.mean(per_rep)), "f1_std": float(np.std(per_rep)), "n_reps": len(per_rep)})
    return pd.DataFrame(rows)


def main():
    out = {}
    for wl, path in [("ecommerce", EC_FEATS), ("iot", IOT_FEATS)]:
        print(f"\n=== {wl} ===")
        df = pd.read_parquet(path)
        per_fold = per_fold_metrics(df)
        per_fault = per_fault_metrics(df)
        print("per-fold:")
        print(per_fold)
        print("\nper-fault:")
        print(per_fault)
        agg = per_fold[["precision", "recall", "f1", "auc_roc", "pr_auc"]].agg(["mean", "std"]).round(3)
        print("\nFold-aggregate (mean ± std):")
        print(agg)
        out[wl] = {"per_fold": per_fold, "per_fault": per_fault, "agg": agg}
        per_fold.to_csv(f"curated_rules_{wl}_per_fold.csv", index=False)
        per_fault.to_csv(f"curated_rules_{wl}_per_fault.csv", index=False)

    # Summary table for direct comparison
    summary = pd.DataFrame({
        wl: {
            "Precision": f"{out[wl]['agg'].loc['mean','precision']:.3f}±{out[wl]['agg'].loc['std','precision']:.3f}",
            "Recall": f"{out[wl]['agg'].loc['mean','recall']:.3f}±{out[wl]['agg'].loc['std','recall']:.3f}",
            "F1": f"{out[wl]['agg'].loc['mean','f1']:.3f}±{out[wl]['agg'].loc['std','f1']:.3f}",
            "AUC-ROC": f"{out[wl]['agg'].loc['mean','auc_roc']:.3f}±{out[wl]['agg'].loc['std','auc_roc']:.3f}",
            "PR-AUC": f"{out[wl]['agg'].loc['mean','pr_auc']:.3f}±{out[wl]['agg'].loc['std','pr_auc']:.3f}",
        }
        for wl in out
    })
    print("\n=== Summary ===")
    print(summary)
    summary.to_csv("curated_rules_summary.csv")


if __name__ == "__main__":
    main()
