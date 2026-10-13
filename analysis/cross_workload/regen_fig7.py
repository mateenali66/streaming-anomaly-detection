"""Fig. 7: detection latency at F1-optimal thresholds, e-commerce, 30-second windows.

Reads the same per-episode latency files as detection_latency_table.py: the run behind the
RQ1 tables and the Deep SVDD and DAGMM run on the same folds. Written to $FIG_DIR (default:
figures/ at the repository root).
"""
import os
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[2]
SOURCES = [
    ROOT / "data/results_v3/merged/detection_latency_results.csv",
    ROOT / "data/v3/results/ecom-gapfill/detection_latency_results.csv",
]
OUT = Path(os.environ.get("FIG_DIR", ROOT / "figures")) / "fig7_detection_latency.pdf"

df = pd.concat([pd.read_csv(p) for p in SOURCES], ignore_index=True)
df = df[df["category"] == "all"]

LABEL = {"IF": "IF", "OC_SVM": "OC-SVM", "LSTM_AE": "LSTM-AE", "Trans_AE": "Trans-AE",
         "CNN_AE": "CNN-AE", "LSTM_VAE": "LSTM-VAE", "DeepSVDD": "Deep SVDD", "DAGMM": "DAGMM", "RF": "RF",
         "RuleBased": "Statistical (max-z)"}
df["model_label"] = df["model"].map(LABEL)
order = ["IF", "OC-SVM", "LSTM-AE", "Trans-AE", "CNN-AE", "LSTM-VAE", "Deep SVDD", "DAGMM", "RF",
         "Statistical (max-z)"]

fig, ax = plt.subplots(figsize=(7.6, 3.6))
data_per_model, det_rates = [], []
for m in order:
    sub = df[df["model_label"] == m]
    det = sub["detected"].astype(bool)
    # half up, as in the table: 77/80 prints as 96.3%
    rate = float(int(det.mean() * 1000 + 0.5)) / 10 if len(sub) else 0
    det_rates.append(rate)
    lat = sub.loc[det, "latency_seconds"].astype(float).values
    data_per_model.append(lat)

bp = ax.boxplot(data_per_model, tick_labels=order, showfliers=True, patch_artist=True,
                widths=0.55, flierprops=dict(marker='o', markersize=3, alpha=0.5))
for patch in bp["boxes"]:
    patch.set_facecolor("#cce5ff")
    patch.set_alpha(0.7)

for i, rate in enumerate(det_rates):
    ax.text(i + 1, 418, f"{rate:.1f}%", ha="center", va="bottom", fontsize=8)

ax.set_ylabel("Detection latency (seconds)")
ax.set_title("Detection latency, e-commerce 30s (detected episodes only)")
ax.set_ylim(-20, 450)
ax.tick_params(axis="x", rotation=25, labelsize=8.5)
ax.grid(True, alpha=0.3, axis="y")
plt.tight_layout()
plt.savefig(OUT, bbox_inches="tight", dpi=300)
plt.close()
print(f"saved {OUT}")
