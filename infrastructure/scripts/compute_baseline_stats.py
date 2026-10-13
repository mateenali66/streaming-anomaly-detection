#!/usr/bin/env python3
"""
Compute per-feature baseline statistics from the 24h baseline Parquet file.

Produces a JSON file with mean, std, min, max, median, p95, p99, count, nan_count
for every feature column. extract_orchestrator_stats.py reduces this file to the
values that the campaign orchestrator's recovery validation gate reads.

Usage:
    # From local Parquet
    python compute_baseline_stats.py --input data/baseline_24h.parquet --output data/baseline_stats.json

    # With S3 upload
    python compute_baseline_stats.py --input data/baseline_24h.parquet \\
        --output data/baseline_stats.json \\
        --s3-bucket <your-bucket> --s3-prefix baseline/
"""
import argparse
import json
import logging
import math
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("baseline-stats")


def compute_stats(values: np.ndarray) -> dict:
    """Compute stats for a single feature, handling NaN and edge cases."""
    valid = values[~np.isnan(values)]
    n = len(valid)

    if n == 0:
        return {
            "count": 0, "nan_count": int(len(values)),
            "mean": None, "std": None, "min": None, "max": None,
            "median": None, "p95": None, "p99": None,
        }

    mean = float(np.mean(valid))
    std = float(np.std(valid, ddof=1)) if n > 1 else 0.0
    return {
        "count": int(n),
        "nan_count": int(len(values) - n),
        "mean": mean,
        "std": std,
        "min": float(np.min(valid)),
        "max": float(np.max(valid)),
        "median": float(np.median(valid)),
        "p95": float(np.percentile(valid, 95)),
        "p99": float(np.percentile(valid, 99)),
    }


def sanitize_json(value):
    """Replace NaN/Inf with None for JSON compatibility."""
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, dict):
        return {k: sanitize_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize_json(v) for v in value]
    return value


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to baseline Parquet file")
    parser.add_argument("--output", required=True, help="Path to output JSON file")
    parser.add_argument("--s3-bucket", help="S3 bucket for upload")
    parser.add_argument("--s3-prefix", default="baseline/", help="S3 key prefix")
    args = parser.parse_args()

    log.info("Loading %s...", args.input)
    if not Path(args.input).exists():
        log.error("Input file not found: %s", args.input)
        sys.exit(1)

    table = pq.read_table(args.input)
    df_size_mb = os.path.getsize(args.input) / 1024 / 1024
    log.info("Loaded: %d rows x %d columns (%.1f MB)",
             table.num_rows, table.num_columns, df_size_mb)

    # Compute stats per column
    stats = {}
    for col_name in table.column_names:
        if col_name == "timestamp":
            continue
        values = table.column(col_name).to_numpy().astype(np.float64)
        stats[col_name] = compute_stats(values)

    # Aggregate stats
    total_features = len(stats)
    zero_variance = sum(1 for s in stats.values() if s["std"] == 0.0)
    all_nan = sum(1 for s in stats.values() if s["count"] == 0)
    mostly_nan = sum(1 for s in stats.values()
                     if s["count"] > 0 and s["nan_count"] / (s["count"] + s["nan_count"]) > 0.5)

    log.info("Feature statistics:")
    log.info("  Total features:     %d", total_features)
    log.info("  Zero variance:      %d (%.1f%%)", zero_variance, zero_variance / total_features * 100)
    log.info("  All-NaN features:   %d", all_nan)
    log.info("  >50%% NaN features:  %d", mostly_nan)

    # Extract category breakdown
    categories = {"broker": 0, "consumer": 0, "flink": 0, "jvm": 0, "other": 0}
    for col_name in stats:
        cat = col_name.split("__")[0] if "__" in col_name else "other"
        categories[cat] = categories.get(cat, 0) + 1
    log.info("  By category: %s", categories)

    # Build output JSON
    output = {
        "metadata": {
            "source_file": str(Path(args.input).name),
            "source_size_mb": round(df_size_mb, 2),
            "num_timestamps": table.num_rows,
            "num_features": total_features,
            "feature_categories": categories,
            "quality": {
                "zero_variance_features": zero_variance,
                "all_nan_features": all_nan,
                "mostly_nan_features": mostly_nan,
            },
        },
        "features": sanitize_json(stats),
    }

    # Save JSON
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(output, f, indent=2)

    output_size_kb = os.path.getsize(args.output) / 1024
    log.info("Saved %s (%.1f KB)", args.output, output_size_kb)

    # Upload to S3
    if args.s3_bucket:
        s3_key = f"{args.s3_prefix}{Path(args.output).name}"
        s3_uri = f"s3://{args.s3_bucket}/{s3_key}"
        log.info("Uploading to %s ...", s3_uri)
        result = subprocess.run(
            ["aws", "s3", "cp", args.output, s3_uri, "--region", "us-east-2"],
            capture_output=True, text=True, timeout=120
        )
        if result.returncode == 0:
            log.info("Uploaded to S3: %s", s3_uri)
        else:
            log.warning("S3 upload failed: %s", result.stderr.strip())

    log.info("=== Complete ===")


if __name__ == "__main__":
    main()
