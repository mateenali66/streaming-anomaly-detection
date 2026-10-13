"""Curated production-style rule baseline, evaluated on RAW campaign metrics.

The v1 baseline (curated_rules.py) operated on the post-feature-selection ML
matrix, which had filtered out UnderReplicatedPartitions, ISR shrinks, and
rebalance counters during variance thresholding, which compared ML against a
rule system deprived of its inputs.

This v2 baseline reads the raw campaign parquets directly (which preserve the
~6500 raw metric instances before any selection), applies the canonical
production rules from Burrow, Xinfra Monitor, and the Ververica Flink
monitoring guide, and reports per-fold metrics aligned with the ML protocol.

Rules:
  R1 consumer_lag       Kafka records_lag_max > 10000 for 2 consecutive 30s windows
  R2 under_replicated   Kafka UnderReplicatedPartitions > 0
  R3 failed_checkpoint  Flink numberOfFailedCheckpoints > 0 (delta > 0 in window)
  R4 backpressure       Flink backPressuredTimeMsPerSecond > 100 for 2 consecutive windows
  R5 broker_offline     Kafka activeControllerCount drops or offlinePartitionsCount > 0
  R6 rebalance_burst    failed_rebalance_rate_per_hour > 0
  R7 heap_pressure      JVM Memory_Heap_Used > 0.9 * Heap_Committed
"""
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score, average_precision_score

ROOT = Path(__file__).resolve().parents[2]
EC_RAW_V1 = ROOT / "data/campaign/campaign_v1_metrics.parquet"
EC_RAW_V2 = ROOT / "data/campaign/campaign_v2_metrics.parquet"
EC_FEATS = ROOT / "data/features_v3/features_30s.parquet"   # for fold/label alignment
IOT_RAW = ROOT / "data/v3/iot/campaign_iot.parquet"
IOT_FEATS = ROOT / "data/v3/iot/features/features_30s.parquet"

WINDOW_SEC = 30


def _agg_30s(raw_df, ts_col="timestamp"):
    """Aggregate the raw 10s metrics into 30s windows by max (production rules use max-over-window).
    timestamp is epoch seconds (float) in the raw parquets.
    """
    raw_df = raw_df.sort_values(ts_col).copy()
    raw_df["window_start"] = (raw_df[ts_col].astype("int64") // WINDOW_SEC) * WINDOW_SEC
    out = raw_df.groupby("window_start").max(numeric_only=True).reset_index()
    return out


def _aggregate_signal(df, contains, agg="max"):
    cols = [c for c in df.columns if all(t.lower() in c.lower() for t in contains)]
    if not cols:
        return pd.Series(np.zeros(len(df)), index=df.index)
    if agg == "max":
        return df[cols].max(axis=1)
    if agg == "mean":
        return df[cols].mean(axis=1)
    if agg == "any_pos":
        return (df[cols] > 0).any(axis=1).astype(int)
    raise ValueError(agg)


def build_signals_raw(df):
    """Extract production monitoring signals from a raw campaign parquet."""
    sig = pd.DataFrame(index=df.index)
    sig["consumer_lag"]   = _aggregate_signal(df, ("records_lag_max",), "max")
    sig["under_repl"]     = _aggregate_signal(df, ("underreplicatedpartitions",), "max")
    sig["failed_ckpt"]    = _aggregate_signal(df, ("numberOfFailedCheckpoints",), "max")
    sig["backpressure"]   = _aggregate_signal(df, ("backPressuredTimeMsPerSecond",), "max")
    sig["offline_part"]   = _aggregate_signal(df, ("offlinepartitionscount",), "max")
    sig["active_ctrl"]    = _aggregate_signal(df, ("activecontrollercount",), "max")
    sig["rebal_failed"]   = _aggregate_signal(df, ("failed_rebalance_rate_per_hour",), "max")
    sig["heap_used"]      = _aggregate_signal(df, ("Memory_Heap_Used",), "max")
    sig["heap_committed"] = _aggregate_signal(df, ("Memory_Heap_Committed",), "max")
    return sig


def fire_rules(signals):
    fired = pd.DataFrame(index=signals.index)
    fired["R1_consumer_lag"]    = (signals["consumer_lag"] > 10_000).astype(int)
    fired["R2_under_repl"]      = (signals["under_repl"] > 0).astype(int)
    # numberOfFailedCheckpoints is cumulative; fire on delta > 0 (a new failure this window)
    ckpt_delta = signals["failed_ckpt"].diff().fillna(0).clip(lower=0)
    fired["R3_failed_ckpt"]     = (ckpt_delta > 0).astype(int)
    fired["R4_backpressure"]    = (signals["backpressure"] > 100).astype(int)  # 10% backpressured per second
    # R5: any active controllers below the expected 1, OR any offline partitions
    fired["R5_broker_offline"]  = ((signals["active_ctrl"] < 1) | (signals["offline_part"] > 0)).astype(int)
    fired["R6_rebal_burst"]     = (signals["rebal_failed"] > 0).astype(int)
    # R7: heap > 90% of committed (when committed > 0)
    ratio = np.where(signals["heap_committed"] > 0, signals["heap_used"] / signals["heap_committed"], 0.0)
    fired["R7_heap_pressure"]   = (ratio > 0.9).astype(int)

    # Persistence: lag and backpressure require 2 consecutive windows
    for r in ["R1_consumer_lag", "R4_backpressure"]:
        fired[r] = ((fired[r] == 1) & (fired[r].shift(1).fillna(0).astype(int) == 1)).astype(int)

    score = fired.sum(axis=1)
    pred = (score > 0).astype(int)
    return pred, score, fired


def evaluate(raw_df, feat_df, label="ANOMALOUS"):
    """Align raw signals to feature-window labels and evaluate per-fold."""
    # Aggregate raw to 30s
    agg = _agg_30s(raw_df.copy())
    # Round feature window timestamps to the same grid
    feat_df = feat_df.copy()
    feat_df["ws"] = (pd.to_datetime(feat_df["window_start"]).astype("int64") // 1_000_000_000 // WINDOW_SEC) * WINDOW_SEC
    merged = feat_df.merge(agg, left_on="ws", right_on="window_start", how="left", suffixes=("", "_raw"))

    rows = []
    reps = sorted(r for r in merged["repetition"].unique() if r >= 1)
    for fold_idx, test_rep in enumerate(reps, start=1):
        m_test = (merged["repetition"] == test_rep) & (merged["label"].isin(["NORMAL", "ANOMALOUS", "RECOVERY"]))
        sub = merged[m_test].sort_values("ws").reset_index(drop=True)
        if len(sub) == 0:
            continue
        signals = build_signals_raw(sub)
        pred, score, _ = fire_rules(signals)
        y = (sub["label"] == label).astype(int).values
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


def per_fault(raw_df, feat_df):
    agg = _agg_30s(raw_df.copy())
    feat_df = feat_df.copy()
    feat_df["ws"] = (pd.to_datetime(feat_df["window_start"]).astype("int64") // 1_000_000_000 // WINDOW_SEC) * WINDOW_SEC
    merged = feat_df.merge(agg, left_on="ws", right_on="window_start", how="left", suffixes=("", "_raw"))
    rows = []
    for fault in sorted(f for f in merged["fault_id"].unique() if f and f != ""):
        per_rep_f1 = []
        for rep in sorted(merged.loc[merged["fault_id"] == fault, "repetition"].unique()):
            sub = merged[(merged["fault_id"] == fault) & (merged["repetition"] == rep) &
                         (merged["label"].isin(["NORMAL", "ANOMALOUS", "RECOVERY"]))].sort_values("ws").reset_index(drop=True)
            if len(sub) == 0:
                continue
            signals = build_signals_raw(sub)
            pred, _, _ = fire_rules(signals)
            y = (sub["label"] == "ANOMALOUS").astype(int).values
            if y.sum() == 0:
                continue
            per_rep_f1.append(f1_score(y, pred, zero_division=0))
        if per_rep_f1:
            rows.append({"fault_id": fault, "f1_mean": float(np.mean(per_rep_f1)), "f1_std": float(np.std(per_rep_f1)), "n_reps": len(per_rep_f1)})
    return pd.DataFrame(rows)


def main():
    out = {}
    # E-commerce raw is split across v1 + v2 campaign parquets; concatenate
    print("loading e-commerce raw campaign...")
    ec_raw = pd.concat([pd.read_parquet(EC_RAW_V1), pd.read_parquet(EC_RAW_V2)], ignore_index=True)
    print(f"  ec raw rows: {len(ec_raw)}")
    ec_feat = pd.read_parquet(EC_FEATS)
    print(f"  ec features: {len(ec_feat)}")
    pf_ec = evaluate(ec_raw, ec_feat)
    fault_ec = per_fault(ec_raw, ec_feat)
    print("e-commerce per-fold:"); print(pf_ec)
    print("\ne-commerce per-fault:"); print(fault_ec)

    print("\nloading IoT raw campaign...")
    iot_raw = pd.read_parquet(IOT_RAW)
    iot_feat = pd.read_parquet(IOT_FEATS)
    pf_iot = evaluate(iot_raw, iot_feat)
    fault_iot = per_fault(iot_raw, iot_feat)
    print("IoT per-fold:"); print(pf_iot)
    print("\nIoT per-fault:"); print(fault_iot)

    out_dir = Path(__file__).parent
    pf_ec.to_csv(out_dir / "curated_rules_v2_ecommerce_per_fold.csv", index=False)
    pf_iot.to_csv(out_dir / "curated_rules_v2_iot_per_fold.csv", index=False)
    fault_ec.to_csv(out_dir / "curated_rules_v2_ecommerce_per_fault.csv", index=False)
    fault_iot.to_csv(out_dir / "curated_rules_v2_iot_per_fault.csv", index=False)

    print("\n=== SUMMARY (mean +/- std across folds) ===")
    for label, df in [("ecommerce", pf_ec), ("iot", pf_iot)]:
        if len(df) == 0:
            print(f"{label}: no usable folds")
            continue
        agg = df[["precision", "recall", "f1", "auc_roc", "pr_auc"]].agg(["mean", "std"]).round(3)
        print(f"\n{label}:")
        print(agg)


if __name__ == "__main__":
    main()
