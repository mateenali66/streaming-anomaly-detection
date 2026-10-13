#!/usr/bin/env python3
"""
Merge results from the three training instances into unified result files.
Downloads from S3, validates completeness, and produces merged CSVs.

Usage:
    python merge_results.py --bucket <results bucket> --output results/merged
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd


REGION = "us-east-2"
INSTANCES = ["gpu-1", "gpu-2", "cpu-1"]

# Expected completeness
EXPECTED_MODELS = {"IF", "OC_SVM", "LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE", "RF", "RuleBased"}
EXPECTED_FOLDS = set(range(1, 11))
EXPECTED_FAULTS = {f"F{i}" for i in range(1, 9)}


def s3_sync(bucket, prefix, local_dir, region):
    """Download from S3."""
    cmd = [
        "aws", "s3", "sync",
        f"s3://{bucket}/{prefix}", str(local_dir),
        "--region", region, "--quiet",
    ]
    print(f"  Syncing s3://{bucket}/{prefix} -> {local_dir}")
    subprocess.run(cmd, check=True)


def check_completion(bucket, region):
    """Check which instances have completed (DONE file exists)."""
    completed = []
    for inst in INSTANCES:
        result = subprocess.run(
            ["aws", "s3", "ls", f"s3://{bucket}/results/{inst}/DONE",
             "--region", region],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            completed.append(inst)
        else:
            print(f"  WARNING: {inst} not yet complete (no DONE file)")
    return completed


def merge_csvs(dirs, filename, output_path):
    """Merge CSV files from multiple directories."""
    dfs = []
    for d in dirs:
        path = d / filename
        if path.exists():
            try:
                df = pd.read_csv(path, on_bad_lines="warn")
                dfs.append(df)
            except Exception as e:
                print(f"  WARNING: Could not read {path}: {e}")
    if dfs:
        merged = pd.concat(dfs, ignore_index=True)
        # Remove duplicates (in case of re-runs / appended results)
        if "model" in merged.columns and "fold" in merged.columns:
            key_cols = ["model", "fold"]
            if "category" in merged.columns:
                key_cols.append("category")
            if "fault_id" in merged.columns:
                key_cols.append("fault_id")
            if "repetition" in merged.columns:
                key_cols.append("repetition")
            merged = merged.drop_duplicates(subset=key_cols, keep="last")
        merged.to_csv(output_path, index=False)
        return merged
    return pd.DataFrame()


def merge_npy_files(dirs, output_dir):
    """Copy .npy files from all instances to merged output."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for d in dirs:
        raw_dir = d / "raw_scores"
        if raw_dir.exists():
            for f in raw_dir.glob("*.npy"):
                dest = output_dir / f.name
                if not dest.exists():
                    import shutil
                    shutil.copy2(f, dest)


def validate_results(results_df, fault_df, latency_df):
    """Validate merged results for completeness."""
    errors = []

    # Check all models present
    if results_df is not None and not results_df.empty:
        models = set(results_df["model"].unique())
        missing_models = EXPECTED_MODELS - models
        if missing_models:
            errors.append(f"Missing models: {missing_models}")

        # Check all folds for each model
        for model in models:
            model_folds = set(results_df[results_df["model"] == model]["fold"].unique())
            missing_folds = EXPECTED_FOLDS - model_folds
            if missing_folds:
                errors.append(f"{model}: missing folds {missing_folds}")

        # Check no NaN in key metrics
        for col in ["f1_score", "auc_roc"]:
            if col in results_df.columns:
                n_nan = results_df[col].isna().sum()
                if n_nan > 0:
                    errors.append(f"{n_nan} NaN values in {col}")

    # Check fault results
    if fault_df is not None and not fault_df.empty:
        faults = set(fault_df["fault_id"].unique())
        missing_faults = EXPECTED_FAULTS - faults
        if missing_faults:
            errors.append(f"Missing faults in per-fault results: {missing_faults}")

    return errors


def main():
    parser = argparse.ArgumentParser(description="Merge distributed training results")
    parser.add_argument("--bucket", required=True, help="S3 bucket holding the per-instance results")
    parser.add_argument("--region", default=REGION)
    parser.add_argument("--output", default="results/merged",
                        help="Local output directory for merged results")
    parser.add_argument("--local-only", action="store_true",
                        help="Skip S3 download, merge from local results/ dirs")
    parser.add_argument("--skip-check", action="store_true",
                        help="Skip completion check")
    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not args.local_only:
        # Check completion
        if not args.skip_check:
            print("Checking instance completion...")
            completed = check_completion(args.bucket, args.region)
            print(f"  Completed: {completed}")
            if len(completed) < 3:
                print("WARNING: Not all instances complete. Proceed anyway? (y/n)")
                if input().strip().lower() != "y":
                    sys.exit(1)

        # Download results from S3
        print("\nDownloading results from S3...")
        local_results = Path("results/s3_download")
        local_results.mkdir(parents=True, exist_ok=True)
        for inst in INSTANCES:
            inst_dir = local_results / inst
            inst_dir.mkdir(parents=True, exist_ok=True)
            s3_sync(args.bucket, f"results/{inst}/", inst_dir, args.region)

        result_dirs = [local_results / inst for inst in INSTANCES]
    else:
        # Use local result directories
        result_dirs = [Path(f"results/{inst}") for inst in INSTANCES
                       if Path(f"results/{inst}").exists()]
        if not result_dirs:
            # Try single results/ directory
            if Path("results/model_results.csv").exists():
                result_dirs = [Path("results")]

    if not result_dirs:
        print("ERROR: No result directories found")
        sys.exit(1)

    print(f"\nMerging from {len(result_dirs)} directories...")

    # Merge CSVs
    results_df = merge_csvs(result_dirs, "model_results.csv",
                            output_dir / "model_results.csv")
    print(f"  model_results.csv: {len(results_df)} rows")

    fault_df = merge_csvs(result_dirs, "per_fault_type_results.csv",
                          output_dir / "per_fault_type_results.csv")
    print(f"  per_fault_type_results.csv: {len(fault_df)} rows")

    latency_df = merge_csvs(result_dirs, "detection_latency_results.csv",
                            output_dir / "detection_latency_results.csv")
    print(f"  detection_latency_results.csv: {len(latency_df)} rows")

    optuna_df = merge_csvs(result_dirs, "optuna_results.csv",
                           output_dir / "optuna_results.csv")
    print(f"  optuna_results.csv: {len(optuna_df)} rows")

    # V3: Merge new output files
    for extra_csv in ["confusion_matrices.csv", "model_results_unsupervised.csv",
                      "rf_feature_importance.csv"]:
        extra_df = merge_csvs(result_dirs, extra_csv,
                              output_dir / extra_csv)
        if len(extra_df) > 0:
            print(f"  {extra_csv}: {len(extra_df)} rows")

    # Merge raw scores
    merge_npy_files(result_dirs, output_dir / "raw_scores")

    # Also merge ablation results if present
    ablation_dirs = []
    for d in result_dirs:
        ab = d / "ablation_category"
        if ab.exists():
            ablation_dirs.append(ab)
    if ablation_dirs:
        ablation_out = output_dir / "ablation_category"
        ablation_out.mkdir(parents=True, exist_ok=True)
        ab_results = merge_csvs(ablation_dirs, "model_results.csv",
                                ablation_out / "model_results.csv")
        print(f"  ablation_category model_results.csv: {len(ab_results)} rows")

    # Also merge ablation_derived results
    ablation_derived_dirs = []
    for d in result_dirs:
        ab = d / "ablation_derived"
        if ab.exists():
            ablation_derived_dirs.append(ab)
    if ablation_derived_dirs:
        ablation_der_out = output_dir / "ablation_derived"
        ablation_der_out.mkdir(parents=True, exist_ok=True)
        ab_der_results = merge_csvs(ablation_derived_dirs, "model_results.csv",
                                    ablation_der_out / "model_results.csv")
        print(f"  ablation_derived model_results.csv: {len(ab_der_results)} rows")

    # Sensitivity results
    for ws in [15, 60, 120]:
        sens_dirs = []
        for d in result_dirs:
            sd = d / f"sensitivity_ws_{ws}s"
            if sd.exists():
                sens_dirs.append(sd)
        if sens_dirs:
            sens_out = output_dir / f"sensitivity_ws_{ws}s"
            sens_out.mkdir(parents=True, exist_ok=True)
            sens_df = merge_csvs(sens_dirs, "model_results.csv",
                                 sens_out / "model_results.csv")
            print(f"  sensitivity_{ws}s model_results.csv: {len(sens_df)} rows")

    # Validate
    print("\nValidation...")
    errors = validate_results(results_df, fault_df, latency_df)
    if errors:
        print("VALIDATION ERRORS:")
        for e in errors:
            print(f"  - {e}")
    else:
        print("  All checks passed.")

    # Summary table
    if not results_df.empty:
        print("\n" + "=" * 70)
        print("RESULTS SUMMARY (mean across folds)")
        print("=" * 70)
        summary = results_df.groupby("model")[
            ["precision", "recall", "f1_score", "auc_roc", "pr_auc"]
        ].agg(["mean", "std"])
        # Flatten column names
        summary.columns = [f"{col}_{stat}" for col, stat in summary.columns]
        pd.set_option("display.max_columns", 20)
        pd.set_option("display.width", 120)
        print(summary.to_string())

        # Save summary
        summary_data = {
            "n_models": len(results_df["model"].unique()),
            "n_folds": len(results_df["fold"].unique()),
            "total_rows": len(results_df),
            "per_model": {},
        }
        for model in sorted(results_df["model"].unique()):
            mdf = results_df[results_df["model"] == model]
            summary_data["per_model"][model] = {
                "f1_mean": round(float(mdf["f1_score"].mean()), 4),
                "f1_std": round(float(mdf["f1_score"].std()), 4),
                "auc_mean": round(float(mdf["auc_roc"].mean()), 4),
                "auc_std": round(float(mdf["auc_roc"].std()), 4),
            }
        with open(output_dir / "training_summary.json", "w") as f:
            json.dump(summary_data, f, indent=2)

    # Detection latency summary
    if not latency_df.empty:
        print("\nDetection Latency Summary:")
        detected = latency_df[latency_df["detected"]]
        if len(detected) > 0:
            lat_summary = detected.groupby("model")["latency_seconds"].agg(
                ["median", "mean", lambda x: np.percentile(x, 95)]
            )
            lat_summary.columns = ["median_s", "mean_s", "p95_s"]
            print(lat_summary.to_string())

            det_rate = latency_df.groupby("model")["detected"].mean()
            print("\nDetection Rate:")
            print(det_rate.to_string())

    print(f"\nMerged results saved to: {output_dir}")


if __name__ == "__main__":
    main()
