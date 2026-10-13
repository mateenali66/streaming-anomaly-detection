"""Sub-experiment 1: Bucket consistency.

Groups the eight faults into three buckets by affected layer and compares per-fault F1 of
the reconstruction autoencoders across the two workloads. Note that the F1 and F2 injections
in bucket B did not take effect (injection_check.py), so their windows contain no fault.

Output:
  analysis/cross_workload/subexp1_bucket_consistency.csv  (per-fault mean F1 + std)
  analysis/cross_workload/subexp1_bucket_table.txt        (readable table)
"""
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "analysis" / "cross_workload"

# Buckets by primary affected layer
BUCKETS = {
    "F1": "B (broker_crash)",
    "F2": "B (network_partition)",
    "F3": "B (disk_io_saturation)",
    "F4": "A (consumer_lag)",
    "F5": "C (rebalancing)",
    "F6": "C (poison_pill)",
    "F7": "A (checkpoint_failure)",
    "F8": "A (oom_resource)",
}

AE_MODELS = ["LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE"]
ALL_DEEP = AE_MODELS + ["DeepSVDD", "DAGMM"]

# 30-second anchor window only, so both workloads are compared on the same window size
df = pd.read_csv(OUT / "panel_per_fault.csv")
df = df[df["window_s"] == 30]

# Mean per-fault F1 per workload × model
agg = (df.groupby(["workload", "model", "fault_id"])["f1_score"]
         .agg(["mean", "std"])
         .reset_index()
         .rename(columns={"mean": "f1_mean", "std": "f1_std"}))
agg["bucket"] = agg["fault_id"].map(BUCKETS)

# Save raw aggregated table
agg.to_csv(OUT / "subexp1_bucket_consistency.csv", index=False)

# Pivot: AE models by faults, per workload
def fmt(row):
    return f"{row['f1_mean']:.3f} +/- {row['f1_std']:.3f}"

agg["f1_str"] = agg.apply(fmt, axis=1)

print("=" * 80)
print("BUCKET CONSISTENCY (30s window, mean F1 +/- std across 10 folds)")
print("=" * 80)
print()

for workload in ["ecommerce", "iot"]:
    sub = agg[agg["workload"] == workload].copy()
    pivot = sub.pivot_table(index="model", columns="fault_id",
                            values="f1_mean", aggfunc="first")
    print(f"--- {workload.upper()} ---")
    pivot = pivot.reindex(ALL_DEEP).round(3)
    print(pivot.to_string())
    print()

# Bucket-level summary: avg F1 per bucket per workload (AE models only, mean across faults+models+folds)
print("=" * 80)
print("BUCKET-LEVEL SUMMARY: mean F1 across AE models (LSTM_AE, Trans_AE, CNN_AE, LSTM_VAE)")
print("Bucket B against buckets A and C, both workloads")
print("=" * 80)
ae_only = agg[agg["model"].isin(AE_MODELS)].copy()
ae_only["bucket_label"] = ae_only["bucket"].str[0]  # A, B, C
bucket_summary = ae_only.groupby(["workload", "bucket_label"])["f1_mean"].agg(["mean", "std", "count"])
print(bucket_summary.round(3).to_string())
print()

# Deep SVDD and DAGMM on the bucket B (broker) faults
print("=" * 80)
print("DENSITY-BASED MODELS ON BUCKET B (F1, F2, F3)")
print("=" * 80)
density = agg[agg["model"].isin(["DeepSVDD", "DAGMM"]) & agg["fault_id"].isin(["F1", "F2", "F3"])]
density_pivot = density.pivot_table(index=["workload", "model"], columns="fault_id",
                                     values="f1_mean", aggfunc="first").round(3)
print(density_pivot.to_string())

# Save text summary for paper
with open(OUT / "subexp1_bucket_table.txt", "w") as f:
    f.write("BUCKET CONSISTENCY (30s, mean F1)\n\n")
    for workload in ["ecommerce", "iot"]:
        sub = agg[agg["workload"] == workload].copy()
        pivot = sub.pivot_table(index="model", columns="fault_id",
                                values="f1_mean", aggfunc="first")
        f.write(f"--- {workload.upper()} ---\n")
        pivot = pivot.reindex(ALL_DEEP).round(3)
        f.write(pivot.to_string() + "\n\n")
    f.write("Bucket-level (AE models only):\n")
    f.write(bucket_summary.round(3).to_string() + "\n\n")
    f.write("Density-based on bucket B (DSVDD/DAGMM):\n")
    f.write(density_pivot.to_string() + "\n")

print(f"\nSaved: {OUT / 'subexp1_bucket_consistency.csv'}")
print(f"Saved: {OUT / 'subexp1_bucket_table.txt'}")
