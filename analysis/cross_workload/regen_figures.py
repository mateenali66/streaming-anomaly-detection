"""Figs. 6 and 8 from panel_overall.csv and panel_per_fault.csv.

fig6_per_fault_heatmap.pdf    eight faults by ten detectors, e-commerce, 30-second windows
fig8_window_sensitivity.pdf   F1 across the window grid, one panel per workload

Figures are written to $FIG_DIR (default: figures/ at the repository root). Fig. 3 and 4 come
from regen_fig3_fig4.py and Fig. 7 from regen_fig7.py.
"""
import os
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[2]
FIG_DIR = Path(os.environ.get("FIG_DIR", ROOT / "figures"))
PANEL = pd.read_csv(Path(__file__).parent / "panel_overall.csv")
PER_FAULT = pd.read_csv(Path(__file__).parent / "panel_per_fault.csv")

MODEL_ORDER = ["IF", "OC_SVM", "LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE",
               "DeepSVDD", "DAGMM", "RF", "RuleBased"]
LABELS = {
    "IF": "IF", "OC_SVM": "OC-SVM",
    "LSTM_AE": "LSTM-AE", "Trans_AE": "Trans-AE",
    "CNN_AE": "CNN-AE", "LSTM_VAE": "LSTM-VAE",
    "DeepSVDD": "Deep SVDD", "DAGMM": "DAGMM",
    "RF": "RF", "RuleBased": "Statistical (max-z)",
}
TYPE_COLOR = {
    "IF": "tab:blue", "OC_SVM": "tab:orange",
    "LSTM_AE": "tab:green", "Trans_AE": "tab:red",
    "CNN_AE": "tab:purple", "LSTM_VAE": "tab:brown",
    "DeepSVDD": "tab:pink", "DAGMM": "tab:olive",
    "RF": "tab:cyan", "RuleBased": "gray",
}


def fig6_per_fault_heatmap():
    """Per-fault F1 heatmap, e-commerce 30s, all 10 models."""
    sub = PER_FAULT[(PER_FAULT["workload"] == "ecommerce") & (PER_FAULT["window_s"] == 30)]
    pivot = sub.groupby(["fault_id", "model"])["f1_score"].mean().unstack("model")
    pivot = pivot.reindex(columns=MODEL_ORDER)
    fault_labels = {
        "F1": "F1 broker crash", "F2": "F2 net partition", "F3": "F3 disk I/O",
        "F4": "F4 consumer lag", "F5": "F5 rebalance storm", "F6": "F6 poison pill",
        "F7": "F7 checkpoint fail", "F8": "F8 OOM",
    }
    pivot.index = [fault_labels.get(f, f) for f in pivot.index]
    col_labels = [LABELS[m] for m in pivot.columns]

    fig, ax = plt.subplots(figsize=(7.0, 4.2))
    im = ax.imshow(pivot.values, cmap="YlOrRd", aspect="auto", vmin=0, vmax=1)
    ax.set_xticks(np.arange(len(col_labels)))
    ax.set_xticklabels(col_labels, rotation=45, ha="right", fontsize=8)
    ax.set_yticks(np.arange(len(pivot.index)))
    ax.set_yticklabels(pivot.index, fontsize=9)
    for i in range(pivot.shape[0]):
        for j in range(pivot.shape[1]):
            v = pivot.iloc[i, j]
            if pd.notna(v):
                ax.text(j, i, f"{v:.2f}", ha="center", va="center",
                        color="black" if v < 0.6 else "white", fontsize=7)
    plt.colorbar(im, ax=ax, label="F1 score", shrink=0.7)
    ax.set_title("Per-fault detection performance, e-commerce 30s (F1)", fontsize=10)
    plt.tight_layout()
    out = FIG_DIR / "fig6_per_fault_heatmap.pdf"
    plt.savefig(out, bbox_inches="tight", dpi=300)
    plt.close()
    print(f"saved {out}")


def fig8_window_sensitivity():
    """Two stacked panels, only plotting at points where data exists.

    Uses dotted lines to connect, but DSVDD/DAGMM on e-commerce only show as
    isolated marker at 30s. e-commerce x-axis shows up to 120s only (no 300s).
    IoT x-axis shows the full 15-300s grid.
    """
    fig, axes = plt.subplots(2, 1, figsize=(4.0, 5.6), sharex=False)
    agg = PANEL.groupby(["workload", "window_s", "model"]).agg(
        f1_m=("f1_score", "mean"), f1_s=("f1_score", "std")).reset_index()

    for ax, wl, title, xt in [
        (axes[0], "ecommerce", "E-commerce (30s anchor + 15/60/120s sensitivity)", [15, 30, 60, 120]),
        (axes[1], "iot", "IoT (full 15-300s grid)", [15, 30, 60, 120, 300]),
    ]:
        sub = agg[agg["workload"] == wl]
        for m in MODEL_ORDER:
            ms = sub[sub["model"] == m].sort_values("window_s")
            if len(ms) == 0:
                continue
            xs = ms["window_s"].astype(int).values
            ys = ms["f1_m"].values
            es = ms["f1_s"].values
            if len(xs) == 1:
                # Single point: marker only (DSVDD/DAGMM at e-commerce)
                ax.errorbar(xs, ys, yerr=es, marker="s", ms=6, capsize=2,
                            label=LABELS[m], color=TYPE_COLOR[m], linestyle="none",
                            markeredgecolor="black", markeredgewidth=0.5)
            else:
                ax.errorbar(xs, ys, yerr=es, marker="o", ms=4, capsize=2,
                            label=LABELS[m], color=TYPE_COLOR[m],
                            linewidth=1.0, alpha=0.85)
        ax.set_title(title, fontsize=9)
        ax.set_ylabel("F1", fontsize=9)
        ax.set_xscale("log")
        ax.set_xticks(xt)
        ax.set_xticklabels([str(v) for v in xt])
        ax.xaxis.set_minor_formatter(plt.NullFormatter())  # no 2x10^1-style minor labels
        ax.tick_params(labelsize=8)
        ax.grid(True, alpha=0.3)
        ax.set_ylim(0.27, 0.65)

    axes[1].set_xlabel("Window size (seconds)", fontsize=9)
    handles, labels = axes[1].get_legend_handles_labels()
    axes[0].legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, -0.20),
                   ncol=5, fontsize=6.0, frameon=False, columnspacing=0.8, handletextpad=0.3)
    plt.tight_layout()
    out = FIG_DIR / "fig8_window_sensitivity.pdf"
    plt.savefig(out, bbox_inches="tight", dpi=300)
    plt.close()
    print(f"saved {out}")


def main():
    fig6_per_fault_heatmap()
    fig8_window_sensitivity()


if __name__ == "__main__":
    main()
