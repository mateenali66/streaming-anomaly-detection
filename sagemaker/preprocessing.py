#!/usr/bin/env python3
"""
Feature engineering pipeline for streaming anomaly detection benchmark.

Reads raw Prometheus metrics (Parquet) and campaign manifests, produces
labeled feature files for ML training.

Pipeline:
  1. Load raw metrics + manifests
  2. Normalize columns (strip volatile pod IDs, aggregate by role)
  3. Window aggregation (30s non-overlapping, 6 stats per metric)
  4. Compute derived streaming-specific features
  5. Label windows (NORMAL, ANOMALOUS, RECOVERY, EXCLUDED)
  6. Feature selection (variance threshold + correlation filter)
  7. 10-fold CV split (by repetition index)
  8. Output: feature Parquet files per window size

Usage:
    python preprocessing.py --data-dir ../data --output-dir ../data/features
    python preprocessing.py --data-dir ../data --output-dir ../data/features --window-sizes 15 30 60 120
"""
import argparse
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("preprocessing")

# ---------------------------------------------------------------------------
# Column normalization
# ---------------------------------------------------------------------------

def normalize_column(col: str) -> str:
    """Strip volatile Flink labels to create a stable column key.

    Volatile labels change on every pod restart: host IP, job_id,
    task_attempt_id, task_attempt_num, task_id, tm_id.

    Stable labels are preserved: job_name, operator_name, subtask_index,
    kafka_broker name, topic.
    """
    if col == "timestamp":
        return col
    col = re.sub(r'__host_\d[\d_]*', '', col)
    col = re.sub(r'__host_streaming_flink_streaming', '__host_jobmanager', col)
    col = re.sub(r'__job_id_[a-f0-9]+', '', col)
    col = re.sub(r'__task_attempt_id_[a-f0-9]+', '', col)
    col = re.sub(r'__task_attempt_num_\d+', '', col)
    col = re.sub(r'__task_id_[a-f0-9]+', '', col)
    col = re.sub(r'__tm_id_[a-zA-Z0-9_]+', '', col)
    col = re.sub(r'_{3,}', '__', col)
    col = col.rstrip('_')
    return col


def build_normalization_map(columns: list[str]) -> dict[str, list[str]]:
    """Map normalized column names to their original column names."""
    norm_map = {}
    for c in columns:
        if c == "timestamp":
            continue
        norm = normalize_column(c)
        if norm not in norm_map:
            norm_map[norm] = []
        norm_map[norm].append(c)
    return norm_map


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse volatile columns by averaging across pod restarts."""
    norm_map = build_normalization_map([c for c in df.columns if c != "timestamp"])

    # Build all columns at once to avoid fragmentation
    col_data = {"timestamp": df["timestamp"].values}
    for norm_name, orig_cols in sorted(norm_map.items()):
        if len(orig_cols) == 1:
            col_data[norm_name] = df[orig_cols[0]].values
        else:
            col_data[norm_name] = df[orig_cols].mean(axis=1).values

    result = pd.DataFrame(col_data)
    log.info("Normalized %d columns -> %d columns", len(df.columns) - 1, len(result.columns) - 1)
    return result


# ---------------------------------------------------------------------------
# Window aggregation
# ---------------------------------------------------------------------------

AGGREGATIONS = ["mean", "std", "min", "max", "slope", "iqr"]


def _slope_agg(series: pd.Series) -> float:
    """Linear regression slope over series indices."""
    valid = series.dropna()
    n = len(valid)
    if n < 2:
        return 0.0
    x = np.arange(n, dtype=float)
    y = valid.values
    x_mean = x.mean()
    y_mean = y.mean()
    denom = ((x - x_mean) ** 2).sum()
    if denom == 0:
        return 0.0
    return float(((x - x_mean) * (y - y_mean)).sum() / denom)


def _iqr_agg(series: pd.Series) -> float:
    """Interquartile range."""
    valid = series.dropna()
    if len(valid) < 4:
        return 0.0
    return float(np.percentile(valid, 75) - np.percentile(valid, 25))


def window_aggregate(df: pd.DataFrame, window_seconds: int = 30) -> pd.DataFrame:
    """Non-overlapping window aggregation using vectorized pandas groupby."""
    metric_cols = [c for c in df.columns if c != "timestamp"]
    timestamps = df["timestamp"].values

    t_start = timestamps[0]

    # Assign each row to a window bin
    df = df.copy()
    df["_window_bin"] = ((df["timestamp"] - t_start) // window_seconds).astype(int)

    grouped = df.groupby("_window_bin")

    # Vectorized aggregations (fast)
    log.info("Computing vectorized aggregations (mean, std, min, max)...")
    agg_mean = grouped[metric_cols].mean()
    agg_std = grouped[metric_cols].std(ddof=0)
    agg_min = grouped[metric_cols].min()
    agg_max = grouped[metric_cols].max()

    # Rename columns
    agg_mean.columns = [f"{c}__mean" for c in metric_cols]
    agg_std.columns = [f"{c}__std" for c in metric_cols]
    agg_min.columns = [f"{c}__min" for c in metric_cols]
    agg_max.columns = [f"{c}__max" for c in metric_cols]

    # Slope and IQR (slower, apply-based)
    log.info("Computing slope and IQR (this takes a moment)...")
    agg_slope = grouped[metric_cols].agg(_slope_agg)
    agg_slope.columns = [f"{c}__slope" for c in metric_cols]

    agg_iqr = grouped[metric_cols].agg(_iqr_agg)
    agg_iqr.columns = [f"{c}__iqr" for c in metric_cols]

    # Metadata
    meta = pd.DataFrame({
        "window_start": grouped["timestamp"].min().values,
        "window_end": grouped["timestamp"].min().values + window_seconds,
        "window_mid": grouped["timestamp"].min().values + window_seconds / 2,
        "n_samples": grouped["timestamp"].count().values,
    }, index=agg_mean.index)

    # Combine all
    result = pd.concat([meta, agg_mean, agg_std, agg_min, agg_max, agg_slope, agg_iqr], axis=1)
    result = result.reset_index(drop=True)

    log.info("Window aggregation (%ds): %d timestamps -> %d windows, %d features",
             window_seconds, len(df), len(result),
             len(result.columns) - 4)
    return result


# ---------------------------------------------------------------------------
# Derived features
# ---------------------------------------------------------------------------

def compute_derived_features(df: pd.DataFrame, window_seconds: int = 30) -> pd.DataFrame:
    """Compute 8 streaming-specific derived features."""
    df = df.copy()

    # Helper: find columns matching a pattern
    def find_col(pattern):
        matches = [c for c in df.columns if pattern in c]
        return matches[0] if matches else None

    # 1. lag_velocity: delta(consumer_lag_mean) / window_duration
    lag_col = find_col("KafkaConsumer_records_lag_max__mean")
    if lag_col:
        df["derived__lag_velocity"] = df[lag_col].diff() / float(window_seconds)
    else:
        df["derived__lag_velocity"] = 0.0

    # 2. lag_acceleration: delta(lag_velocity) / window_duration
    df["derived__lag_acceleration"] = df["derived__lag_velocity"].diff() / float(window_seconds)

    # 3. throughput_ratio: records_out / records_in per Flink job
    in_col = find_col("numRecordsInPerSecond__mean")
    out_col = find_col("numRecordsOutPerSecond__mean")
    if in_col and out_col:
        df["derived__throughput_ratio"] = df[out_col] / df[in_col].replace(0, np.nan)
    else:
        df["derived__throughput_ratio"] = np.nan

    # 4. checkpoint_overhead_ratio: checkpoint_duration / checkpoint_interval (60s)
    cp_col = find_col("lastCheckpointDuration__mean")
    if cp_col:
        df["derived__checkpoint_overhead_ratio"] = df[cp_col] / 60000.0  # ms -> ratio of 60s
    else:
        df["derived__checkpoint_overhead_ratio"] = 0.0

    # 5. isr_stability: 1 - (shrinks + expands) / (2 * partition_count)
    shrink_col = find_col("isrshrinks_total__mean")
    expand_col = find_col("isrexpands_total__mean")
    part_col = find_col("partitioncount__mean")
    if shrink_col and expand_col and part_col:
        denom = 2 * df[part_col].replace(0, 1)
        df["derived__isr_stability"] = 1.0 - (df[shrink_col].fillna(0) + df[expand_col].fillna(0)) / denom
    else:
        df["derived__isr_stability"] = 1.0

    # 6. rebalance_frequency: rebalance_total slope (rate of change per window)
    rebal_col = find_col("rebalance_total__slope")
    if rebal_col:
        df["derived__rebalance_frequency"] = df[rebal_col].fillna(0)
    else:
        df["derived__rebalance_frequency"] = 0.0

    # 7. backpressure_ratio: backpressured / (backpressured + busy + idle)
    bp_col = find_col("backPressuredTimeMsPerSecond__mean")
    busy_col = find_col("busyTimeMsPerSecond__mean")
    idle_col = find_col("idleTimeMsPerSecond__mean")
    if bp_col and busy_col and idle_col:
        total = df[bp_col].fillna(0) + df[busy_col].fillna(0) + df[idle_col].fillna(0)
        df["derived__backpressure_ratio"] = df[bp_col].fillna(0) / total.replace(0, np.nan)
    else:
        df["derived__backpressure_ratio"] = 0.0

    # 8. watermark_lag: processing_time - current_watermark
    wm_col = find_col("currentInputWatermark__mean")
    if wm_col:
        df["derived__watermark_lag"] = df["window_mid"] * 1000 - df[wm_col]  # both in ms
    else:
        df["derived__watermark_lag"] = 0.0

    derived_cols = [c for c in df.columns if c.startswith("derived__")]
    log.info("Computed %d derived features: %s", len(derived_cols),
             [c.replace("derived__", "") for c in derived_cols])
    return df


# ---------------------------------------------------------------------------
# Labeling
# ---------------------------------------------------------------------------

def load_manifests(data_dir: Path, workload: str = "ecommerce") -> list[dict]:
    """Load and merge valid runs from manifests.

    workload="ecommerce": merges v1 (F1-F4) + v2 (F5-F8) e-commerce manifests
    workload="iot": loads single IoT campaign manifest
    """
    runs = []

    if workload == "iot":
        iot_path = data_dir / "campaign_manifest_iot.json"
        if not iot_path.exists():
            iot_path = data_dir / "v3" / "iot" / "campaign_manifest_iot.json"
        m = json.loads(iot_path.read_text())
        valid_recovery = {"recovered", "recovered_after_remediation"}
        for r in m.get("runs", []):
            if r.get("status") == "completed" and r.get("recovery_status") in valid_recovery:
                r["_source"] = "iot"
                runs.append(r)
        log.info("Loaded %d valid runs from IoT manifest", len(runs))
        return runs

    v1_path = data_dir / "campaign" / "campaign_manifest_v1.json"
    v2_path = data_dir / "campaign" / "campaign_manifest_v2.json"

    if v1_path.exists():
        v1 = json.loads(v1_path.read_text())
        valid_faults = {"F1", "F2", "F3", "F4"}
        for r in v1.get("runs", []):
            if r["fault_id"] in valid_faults and r["recovery_status"] == "recovered":
                r["_source"] = "v1"
                runs.append(r)
        log.info("Loaded %d valid F1-F4 runs from v1 manifest", len(runs))

    if v2_path.exists():
        v2 = json.loads(v2_path.read_text())
        v2_count = 0
        for r in v2.get("runs", []):
            r["_source"] = "v2"
            runs.append(r)
            v2_count += 1
        log.info("Loaded %d F5-F8 runs from v2 manifest", v2_count)

    log.info("Total valid runs: %d", len(runs))
    return runs


def parse_iso(ts_str: str) -> float:
    """Parse ISO timestamp to epoch seconds."""
    return datetime.fromisoformat(ts_str).timestamp()


def label_windows(df: pd.DataFrame, runs: list[dict],
                  cooldown_seconds: int = 600) -> pd.DataFrame:
    """Label each window based on fault injection phases.

    Labels:
        NORMAL: baseline or pre-fault windows
        ANOMALOUS: active fault injection
        RECOVERY: post-fault observation
        EXCLUDED: cooldown period (excluded from ML training/eval)
    """
    df = df.copy()
    df["label"] = "NORMAL"
    df["fault_id"] = ""
    df["fault_category"] = ""
    df["run_number"] = -1
    df["repetition"] = -1
    df["run_source"] = ""

    fault_categories = {
        "F1": "broker", "F2": "broker", "F3": "broker", "F4": "consumer",
        "F5": "consumer", "F6": "consumer", "F7": "flink", "F8": "flink",
    }

    # F6 injection phase is near-zero in manifest (K8s Job launched async).
    # Actual fault runs for 180s starting at injection_start.
    FAULT_DURATIONS = {
        "F1": 300, "F2": 180, "F3": 300, "F4": 300,
        "F5": 300, "F6": 180, "F7": 300, "F8": 180,
    }

    for run in runs:
        phases = run.get("phases", {})
        fault_id = run["fault_id"]
        run_num = run["run_number"]
        rep = run["repetition"]
        source = run.get("_source", "")

        # Injection phase -> ANOMALOUS
        if "injection" in phases:
            inj_start = parse_iso(phases["injection"]["start"])
            inj_end = parse_iso(phases["injection"]["end"])

            # Fix for F6: manifest injection duration is ~0s (async K8s Job).
            # Use configured fault duration instead.
            if inj_end - inj_start < 10 and fault_id in FAULT_DURATIONS:
                inj_end = inj_start + FAULT_DURATIONS[fault_id]
                log.debug("Fixed %s rep %d injection end: +%ds from start",
                          fault_id, rep, FAULT_DURATIONS[fault_id])

            mask = (df["window_mid"] >= inj_start) & (df["window_mid"] < inj_end)
            df.loc[mask, "label"] = "ANOMALOUS"
            df.loc[mask, "fault_id"] = fault_id
            df.loc[mask, "fault_category"] = fault_categories.get(fault_id, "")
            df.loc[mask, "run_number"] = run_num
            df.loc[mask, "repetition"] = rep
            df.loc[mask, "run_source"] = source

        # Post-fault phase -> RECOVERY (but don't overwrite ANOMALOUS)
        if "post_fault" in phases:
            pf_start = parse_iso(phases["post_fault"]["start"])
            pf_end = parse_iso(phases["post_fault"]["end"])
            mask = (df["window_mid"] >= pf_start) & (df["window_mid"] < pf_end) & (df["label"] != "ANOMALOUS")
            df.loc[mask, "label"] = "RECOVERY"
            df.loc[mask, "fault_id"] = fault_id
            df.loc[mask, "fault_category"] = fault_categories.get(fault_id, "")
            df.loc[mask, "run_number"] = run_num
            df.loc[mask, "repetition"] = rep
            df.loc[mask, "run_source"] = source

        # Cooldown phase -> EXCLUDED
        if "cooldown" in phases:
            cd_start = parse_iso(phases["cooldown"]["start"])
            cd_end = parse_iso(phases["cooldown"]["end"])
            mask = (df["window_mid"] >= cd_start) & (df["window_mid"] < cd_end)
            df.loc[mask, "label"] = "EXCLUDED"
            df.loc[mask, "fault_id"] = fault_id
            df.loc[mask, "run_number"] = run_num
            df.loc[mask, "repetition"] = rep
            df.loc[mask, "run_source"] = source

        # Pre-fault phase -> NORMAL (already default, but tag with run metadata)
        if "pre_fault" in phases:
            pf_start = parse_iso(phases["pre_fault"]["start"])
            pf_end = parse_iso(phases["pre_fault"]["end"])
            mask = (df["window_mid"] >= pf_start) & (df["window_mid"] < pf_end) & (df["label"] == "NORMAL")
            df.loc[mask, "run_number"] = run_num
            df.loc[mask, "repetition"] = rep
            df.loc[mask, "run_source"] = source

    label_counts = df["label"].value_counts().to_dict()
    log.info("Label distribution: %s", label_counts)
    return df


# ---------------------------------------------------------------------------
# Feature selection
# ---------------------------------------------------------------------------

def select_features(df: pd.DataFrame,
                    variance_threshold: float = 0.01,
                    correlation_threshold: float = 0.95) -> tuple[pd.DataFrame, dict]:
    """Feature selection on training normal data only.

    1. Remove features with variance < threshold
    2. Remove correlated features (|r| > threshold, keep higher-variance one)
    """
    meta_cols = ["window_start", "window_end", "window_mid", "n_samples",
                 "label", "fault_id", "fault_category", "run_number",
                 "repetition", "run_source"]
    feature_cols = [c for c in df.columns if c not in meta_cols]

    # Use only NORMAL data for fitting
    normal_mask = df["label"] == "NORMAL"
    X_normal = df.loc[normal_mask, feature_cols]

    report = {"initial_features": len(feature_cols)}

    # 0. Remove all-NaN columns first
    all_nan_cols = X_normal.columns[X_normal.isna().all()].tolist()
    feature_cols = [c for c in feature_cols if c not in all_nan_cols]
    X_normal = X_normal[feature_cols]
    report["removed_all_nan"] = len(all_nan_cols)
    if all_nan_cols:
        log.info("Removed %d all-NaN columns", len(all_nan_cols))

    # 1. Variance threshold
    variances = X_normal.var()
    low_var = variances[variances < variance_threshold].index.tolist()
    keep_after_var = [c for c in feature_cols if c not in low_var]
    report["removed_low_variance"] = len(low_var)
    report["after_variance"] = len(keep_after_var)
    log.info("Variance filter: %d -> %d features (removed %d with var < %.3f)",
             len(feature_cols), len(keep_after_var), len(low_var), variance_threshold)

    # 2. Correlation filter
    if len(keep_after_var) > 1:
        corr_matrix = X_normal[keep_after_var].corr().abs()
        upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
        to_drop = set()
        for col in upper.columns:
            high_corr = upper.index[upper[col] > correlation_threshold].tolist()
            for hc in high_corr:
                if hc in to_drop:
                    continue
                # Drop the one with lower variance
                if variances.get(col, 0) >= variances.get(hc, 0):
                    to_drop.add(hc)
                else:
                    to_drop.add(col)
        keep_final = [c for c in keep_after_var if c not in to_drop]
    else:
        to_drop = set()
        keep_final = keep_after_var

    report["removed_correlated"] = len(to_drop)
    report["final_features"] = len(keep_final)
    log.info("Correlation filter: %d -> %d features (removed %d with |r| > %.2f)",
             len(keep_after_var), len(keep_final), len(to_drop), correlation_threshold)

    result = df[meta_cols + keep_final].copy()
    return result, report


# ---------------------------------------------------------------------------
# CV splits
# ---------------------------------------------------------------------------

def assign_folds(df: pd.DataFrame, n_folds: int = 10) -> pd.DataFrame:
    """Assign n-fold CV splits by repetition index with 3-way split.

    V3 design: 10-fold leave-one-rep-out with separate calibration split.
    Each fold uses 3 disjoint repetitions for test, calibration, and HPO
    validation, which keeps information from leaking between HPO/early-stopping
    and threshold selection.

    For fold i (1-indexed):
      test:    rep i
      cal:     rep (i % 10) + 1       (next rep, for threshold calibration)
      hpo_val: rep ((i + 1) % 10) + 1 (next-next rep, for HPO + early stopping)
      train:   remaining 7 reps

    Split values: train, hpo_val, cal, test, excluded
    """
    df = df.copy()
    reps = list(range(1, n_folds + 1))  # [1, 2, ..., 10]

    fold_assignments = {}
    for fold_id in range(1, n_folds + 1):
        test_rep = reps[fold_id - 1]
        cal_rep = reps[fold_id % n_folds]
        hpo_val_rep = reps[(fold_id + 1) % n_folds]
        train_reps = set(reps) - {test_rep, cal_rep, hpo_val_rep}
        fold_assignments[fold_id] = {
            "test": {test_rep},
            "cal": {cal_rep},
            "hpo_val": {hpo_val_rep},
            "train": train_reps,
        }

    for fold_id, assignment in fold_assignments.items():
        col_name = f"fold_{fold_id}_split"
        df[col_name] = "unused"
        for rep in assignment["train"]:
            df.loc[df["repetition"] == rep, col_name] = "train"
        for rep in assignment["test"]:
            df.loc[df["repetition"] == rep, col_name] = "test"
        for rep in assignment["cal"]:
            df.loc[df["repetition"] == rep, col_name] = "cal"
        for rep in assignment["hpo_val"]:
            df.loc[df["repetition"] == rep, col_name] = "hpo_val"
        # NORMAL baseline windows (rep -1) always go to train
        df.loc[(df["label"] == "NORMAL") & (df["repetition"] == -1), col_name] = "train"
        # EXCLUDED (cooldown) windows must never be in any split
        df.loc[df["label"] == "EXCLUDED", col_name] = "excluded"

    log.info(
        "Assigned %d-fold CV splits by repetition index "
        "(3-way: train/hpo_val/cal/test)",
        n_folds,
    )
    return df


# ---------------------------------------------------------------------------
# Broker reachable features
# ---------------------------------------------------------------------------

def add_broker_reachable(df: pd.DataFrame) -> pd.DataFrame:
    """Add binary broker_X_reachable features (1 if JMX metrics exist, 0 if NaN)."""
    df = df.copy()
    broker_patterns = ["kafka_brokers_0", "kafka_brokers_1", "kafka_brokers_2"]
    for i, pattern in enumerate(broker_patterns):
        broker_cols = [c for c in df.columns if pattern in c and not c.startswith("derived")]
        if broker_cols:
            df[f"derived__broker_{i}_reachable"] = (~df[broker_cols].isna().all(axis=1)).astype(float)
        else:
            df[f"derived__broker_{i}_reachable"] = 1.0
    return df


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_pipeline(data_dir: Path, output_dir: Path, window_sizes: list[int],
                 n_folds: int = 10, workload: str = "ecommerce"):
    """Execute the full feature engineering pipeline."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load raw data
    log.info("Loading raw metrics for workload=%s ...", workload)

    if workload == "iot":
        iot_dir = data_dir if (data_dir / "baseline_24h_iot.parquet").exists() else (data_dir / "v3" / "iot")
        baseline = pd.read_parquet(iot_dir / "baseline_24h_iot.parquet")
        campaign = pd.read_parquet(iot_dir / "campaign_iot.parquet")
        log.info("IoT baseline: %d rows x %d cols", *baseline.shape)
        log.info("IoT campaign: %d rows x %d cols", *campaign.shape)

        baseline_norm = normalize_dataframe(baseline)
        campaign_norm = normalize_dataframe(campaign)

        canonical_cols = ["timestamp"] + sorted(
            (set(baseline_norm.columns) | set(campaign_norm.columns)) - {"timestamp"}
        )
        log.info("IoT canonical schema: %d columns", len(canonical_cols) - 1)
        baseline_aligned = baseline_norm.reindex(columns=canonical_cols)
        campaign_aligned = campaign_norm.reindex(columns=canonical_cols)
        combined = pd.concat([baseline_aligned, campaign_aligned], ignore_index=True)
        combined = combined.sort_values("timestamp").reset_index(drop=True)
        log.info("Combined: %d rows x %d cols", *combined.shape)
    else:
        baseline = pd.read_parquet(data_dir / "baseline_24h.parquet")
        campaign_v1 = pd.read_parquet(data_dir / "campaign" / "campaign_v1_metrics.parquet")
        campaign_v2 = pd.read_parquet(data_dir / "campaign" / "campaign_v2_metrics.parquet")

        log.info("Baseline: %d rows x %d cols", *baseline.shape)
        log.info("Campaign v1: %d rows x %d cols", *campaign_v1.shape)
        log.info("Campaign v2: %d rows x %d cols", *campaign_v2.shape)

        log.info("Normalizing columns...")
        baseline_norm = normalize_dataframe(baseline)
        v1_norm = normalize_dataframe(campaign_v1)
        v2_norm = normalize_dataframe(campaign_v2)

        canonical_cols = ["timestamp"] + sorted(
            set(baseline_norm.columns) - {"timestamp"}
        )
        log.info("Canonical schema: %d columns", len(canonical_cols) - 1)

        for name, df in [("baseline", baseline_norm), ("v1", v1_norm), ("v2", v2_norm)]:
            missing = set(canonical_cols) - set(df.columns)
            extra = set(df.columns) - set(canonical_cols)
            if missing:
                log.info("  %s: adding %d missing columns (NaN)", name, len(missing))
            if extra:
                log.info("  %s: dropping %d extra columns", name, len(extra))

        baseline_aligned = baseline_norm.reindex(columns=canonical_cols)
        v1_aligned = v1_norm.reindex(columns=canonical_cols)
        v2_aligned = v2_norm.reindex(columns=canonical_cols)

        combined = pd.concat([baseline_aligned, v1_aligned, v2_aligned], ignore_index=True)
        combined = combined.sort_values("timestamp").reset_index(drop=True)
        log.info("Combined: %d rows x %d cols", *combined.shape)

    runs = load_manifests(data_dir, workload=workload)

    # Process each window size
    for ws in window_sizes:
        log.info("=" * 60)
        log.info("Processing window size: %d seconds", ws)
        log.info("=" * 60)

        # Window aggregation
        windowed = window_aggregate(combined, window_seconds=ws)

        # Derived features (window-size-aware)
        windowed = compute_derived_features(windowed, window_seconds=ws)

        # Broker reachable features
        windowed = add_broker_reachable(windowed)

        # Label windows
        windowed = label_windows(windowed, runs)

        # Feature selection (fit on NORMAL data only)
        selected, selection_report = select_features(windowed)

        # CV fold assignment
        selected = assign_folds(selected, n_folds=n_folds)

        # Save
        out_path = output_dir / f"features_{ws}s.parquet"
        selected.to_parquet(out_path, index=False)
        log.info("Saved: %s (%d rows x %d cols, %.1f MB)",
                 out_path, len(selected), len(selected.columns),
                 out_path.stat().st_size / 1e6)

        # NaN report used to plan imputation in training
        _meta = {"window_start", "window_end", "window_mid", "n_samples",
                 "label", "fault_id", "fault_category", "run_number",
                 "repetition", "run_source"} | {f"fold_{i}_split" for i in range(1, n_folds + 1)}
        feature_final = [c for c in selected.columns if c not in _meta]
        nan_per_col = selected[feature_final].isna().mean()
        normal_nan_rate = selected.loc[selected["label"] == "NORMAL", feature_final].isna().any(axis=1).mean()
        nan_report = {
            "total_features": len(feature_final),
            "features_with_nan": int((nan_per_col > 0).sum()),
            "features_all_clean": int((nan_per_col == 0).sum()),
            "max_nan_pct": float(nan_per_col.max() * 100),
            "normal_windows_with_nan_pct": float(normal_nan_rate * 100),
            "imputation_strategy": "Per-fold median from NORMAL training data (applied in the training loop, not here)",
            "nan_columns_over_30pct": sorted([c for c in feature_final if nan_per_col[c] > 0.3]),
        }

        # Save selection report (includes NaN info)
        selection_report["nan_report"] = nan_report
        report_path = output_dir / f"selection_report_{ws}s.json"
        report_path.write_text(json.dumps(selection_report, indent=2))

        log.info("NaN report: %d/%d features have NaN, %.1f%% of NORMAL windows affected, max %.1f%% per column",
                 nan_report["features_with_nan"], nan_report["total_features"],
                 nan_report["normal_windows_with_nan_pct"], nan_report["max_nan_pct"])

        # Summary stats
        log.info("Feature selection: %d -> %d -> %d features",
                 selection_report["initial_features"],
                 selection_report["after_variance"],
                 selection_report["final_features"])

    log.info("Pipeline complete.")


def main():
    parser = argparse.ArgumentParser(description="Feature engineering pipeline")
    parser.add_argument("--data-dir", type=Path,
                        default=Path(__file__).parent.parent / "data",
                        help="Directory with raw Parquet and manifest files")
    parser.add_argument("--output-dir", type=Path,
                        default=Path(__file__).parent.parent / "data" / "features",
                        help="Output directory for feature files")
    parser.add_argument("--window-sizes", type=int, nargs="+",
                        default=[30],
                        help="Window sizes in seconds (default: 30)")
    parser.add_argument("--n-folds", type=int, default=10,
                        help="Number of CV folds (default: 10)")
    parser.add_argument("--workload", choices=["ecommerce", "iot"], default="ecommerce",
                        help="Workload: ecommerce (v1+v2 manifests) or iot (single manifest)")
    args = parser.parse_args()

    if not args.data_dir.exists():
        log.error("Data directory not found: %s", args.data_dir)
        sys.exit(1)

    run_pipeline(args.data_dir, args.output_dir, args.window_sizes,
                 n_folds=args.n_folds, workload=args.workload)


if __name__ == "__main__":
    main()
