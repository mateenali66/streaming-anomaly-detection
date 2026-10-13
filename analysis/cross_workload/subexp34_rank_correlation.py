"""Sub-experiment 3: Per-fault generalization (Spearman rank correlation
   of per-fault F1 between e-commerce and IoT, per model at 30s).

Sub-experiment 4: Window-size transferability (Spearman of per-fault
   optimal-window-size ranks between workloads, per model).

Outputs: subexp3_per_fault_spearman.csv, subexp4_window_transfer_spearman.csv,
subexp4_optimal_windows.csv and subexp34_summary.txt (analysis/cross_workload/)
"""
from pathlib import Path
import pandas as pd
from scipy.stats import spearmanr

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "analysis" / "cross_workload"

ALL_MODELS = ["IF", "OC_SVM", "LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE",
              "DeepSVDD", "DAGMM", "RF", "RuleBased"]

df = pd.read_csv(OUT / "panel_per_fault.csv")

# ---------------------------------------------------------------------------
# Sub-exp 3: per-fault rank Spearman across workloads, at 30s anchor
# ---------------------------------------------------------------------------
print("=" * 80)
print("SUB-EXP 3: Per-fault F1 rank correlation between e-commerce and IoT (30s)")
print("Spearman on per-fault mean F1 across 8 faults, per model.")
print("=" * 80)
df30 = df[df["window_s"] == 30]
agg = (df30.groupby(["workload", "model", "fault_id"])["f1_score"]
            .mean().reset_index())

rows3 = []
for model in ALL_MODELS:
    e = agg[(agg["workload"] == "ecommerce") & (agg["model"] == model)].set_index("fault_id")["f1_score"]
    i = agg[(agg["workload"] == "iot") & (agg["model"] == model)].set_index("fault_id")["f1_score"]
    common = sorted(set(e.index) & set(i.index))
    if len(common) < 4:
        rows3.append((model, len(common), None, None))
        continue
    rho, p = spearmanr(e.loc[common].values, i.loc[common].values)
    rows3.append((model, len(common), round(float(rho), 3), round(float(p), 4)))

rho_df = pd.DataFrame(rows3, columns=["model", "n_faults", "spearman_rho", "p_value"])
print(rho_df.to_string(index=False))
print()

# ---------------------------------------------------------------------------
# Sub-exp 4: window-size transferability
#   For each fault, find which window size maximizes mean F1 (per workload).
#   Spearman between e-commerce and IoT optimal-window ranks per fault.
# ---------------------------------------------------------------------------
print("=" * 80)
print("SUB-EXP 4: Optimal-window-size transferability (Spearman of optimal-window")
print("           rank per fault, between workloads)")
print("=" * 80)

# Mean F1 per (workload, model, fault, window), averaged across folds
agg_w = (df.groupby(["workload", "model", "fault_id", "window_s"])["f1_score"]
           .mean().reset_index())

# For each (workload, model, fault), find argmax window
def best_window(group):
    return group.loc[group["f1_score"].idxmax(), "window_s"]

best = (agg_w.groupby(["workload", "model", "fault_id"])
              .apply(best_window, include_groups=False).reset_index(name="best_window_s"))

# For each model, compute Spearman of best-window ranks across faults between workloads
rows4 = []
for model in ALL_MODELS:
    e = best[(best["workload"] == "ecommerce") & (best["model"] == model)].set_index("fault_id")["best_window_s"]
    i = best[(best["workload"] == "iot") & (best["model"] == model)].set_index("fault_id")["best_window_s"]
    common = sorted(set(e.index) & set(i.index))
    if len(common) < 4:
        rows4.append((model, len(common), None, None))
        continue
    # Note: ties on best_window are common (only 4-5 windows). Spearman handles ties.
    rho, p = spearmanr(e.loc[common].values, i.loc[common].values)
    rows4.append((model, len(common), round(float(rho), 3),
                   round(float(p), 4) if p == p else None))

win_rho = pd.DataFrame(rows4, columns=["model", "n_faults", "spearman_rho", "p_value"])
print(win_rho.to_string(index=False))
print()

# Per-fault optimal window comparison table
print("Per-fault optimal window (averaged across all models with both workloads):")
fault_best = (best.groupby(["workload", "fault_id"])["best_window_s"]
                  .agg(lambda x: x.mode()[0] if len(x.mode()) else x.iloc[0])
                  .unstack("workload"))
print(fault_best.to_string())

# Save
rho_df.to_csv(OUT / "subexp3_per_fault_spearman.csv", index=False)
win_rho.to_csv(OUT / "subexp4_window_transfer_spearman.csv", index=False)
best.to_csv(OUT / "subexp4_optimal_windows.csv", index=False)

with open(OUT / "subexp34_summary.txt", "w") as f:
    f.write("SUB-EXP 3: Per-fault F1 Spearman (e-commerce vs IoT, 30s)\n")
    f.write(rho_df.to_string(index=False) + "\n\n")
    f.write("SUB-EXP 4: Optimal-window transferability Spearman\n")
    f.write(win_rho.to_string(index=False) + "\n\n")
    f.write("Per-fault optimal window per workload:\n")
    f.write(fault_best.to_string() + "\n")

print(f"\nSaved: {OUT}/subexp3_per_fault_spearman.csv")
print(f"Saved: {OUT}/subexp4_window_transfer_spearman.csv")
print(f"Saved: {OUT}/subexp34_summary.txt")
