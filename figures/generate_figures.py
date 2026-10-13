#!/usr/bin/env python3
"""
Result figures from the merged training results.

The paper's Fig. 5 (RQ3 feature category ablation heatmap) comes from this script. The
paper's Figs. 3, 4, 6, 7 and 8 are drawn by analysis/cross_workload/regen_fig3_fig4.py,
regen_figures.py and regen_fig7.py, which supersede the versions below.

Figures:
  Fig 3: RQ1 model comparison bar chart (F1 + AUC-ROC, grouped bars with error bars)
  Fig 4: RQ2 point vs sequence comparison (paired bar chart)
  Fig 5: RQ3 feature category ablation heatmap
  Fig 6: Per-fault-type heatmap (8 faults x 8 models, F1)
  Fig 7: Detection latency distribution (box plot per model)
  Fig 8: Window size sensitivity (line plot, F1 vs window size)

Usage:
    python figures/generate_figures.py --results results/merged --output figures/
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

# Consistent style
plt.rcParams.update({
    "font.family": "serif",
    "font.size": 9,
    "axes.labelsize": 10,
    "axes.titlesize": 11,
    "xtick.labelsize": 8,
    "ytick.labelsize": 8,
    "legend.fontsize": 8,
    "figure.dpi": 300,
    "savefig.bbox": "tight",
    "savefig.pad_inches": 0.05,
})

# Model display order and labels
MODEL_ORDER = ["IF", "OC_SVM", "LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE", "RF", "RuleBased"]
MODEL_LABELS = {
    "IF": "IF", "OC_SVM": "OC-SVM",
    "LSTM_AE": "LSTM-AE", "Trans_AE": "Trans-AE",
    "CNN_AE": "CNN-AE", "LSTM_VAE": "LSTM-VAE",
    "RF": "RF", "RuleBased": "Rule-Based",
}

# Color scheme: point=blue, sequence=orange, supervised=green, rule=gray
MODEL_COLORS = {
    "IF": "#4878CF", "OC_SVM": "#6A9FD6",
    "LSTM_AE": "#D65F5F", "Trans_AE": "#B47CC7",
    "CNN_AE": "#C4AD66", "LSTM_VAE": "#77BEDB",
    "RF": "#2CA02C", "RuleBased": "#999999",
}

MODEL_TYPES = {
    "IF": "Point", "OC_SVM": "Point",
    "LSTM_AE": "Sequence", "Trans_AE": "Sequence",
    "CNN_AE": "Sequence", "LSTM_VAE": "Sequence",
    "RF": "Supervised", "RuleBased": "Baseline",
}

TYPE_COLORS = {
    "Point": "#4878CF",
    "Sequence": "#D65F5F",
    "Supervised": "#2CA02C",
    "Baseline": "#999999",
}


def fig3_model_comparison(df, output_dir):
    """RQ1: Grouped bar chart of F1 and AUC-ROC by model."""
    fig, axes = plt.subplots(1, 2, figsize=(7, 3))

    models = [m for m in MODEL_ORDER if m in df["model"].unique()]
    x = np.arange(len(models))
    width = 0.6

    for ax, metric, title in [
        (axes[0], "f1_score", "F1 Score"),
        (axes[1], "auc_roc", "AUC-ROC"),
    ]:
        means = []
        stds = []
        colors = []
        for m in models:
            mdf = df[df["model"] == m]
            means.append(mdf[metric].mean())
            stds.append(mdf[metric].std())
            colors.append(MODEL_COLORS.get(m, "#333333"))

        bars = ax.bar(x, means, width, yerr=stds, capsize=3,
                      color=colors, edgecolor="white", linewidth=0.5,
                      error_kw={"linewidth": 0.8})
        ax.set_xticks(x)
        ax.set_xticklabels([MODEL_LABELS.get(m, m) for m in models],
                           rotation=45, ha="right")
        ax.set_ylabel(title)
        ax.set_ylim(0, 1.0)
        ax.yaxis.set_major_locator(mticker.MultipleLocator(0.1))
        ax.grid(axis="y", alpha=0.3, linewidth=0.5)
        ax.set_axisbelow(True)

    # Legend for model types
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=TYPE_COLORS["Point"], label="Point-in-time"),
        Patch(facecolor=TYPE_COLORS["Sequence"], label="Sequence"),
        Patch(facecolor=TYPE_COLORS["Supervised"], label="Supervised"),
        Patch(facecolor=TYPE_COLORS["Baseline"], label="Rule-based"),
    ]
    axes[1].legend(handles=legend_elements, loc="upper right", framealpha=0.9)

    fig.tight_layout()
    fig.savefig(output_dir / "fig3_model_comparison.pdf")
    fig.savefig(output_dir / "fig3_model_comparison.png", dpi=300)
    plt.close(fig)
    print("  Fig 3: Model comparison bar chart")


def fig4_rq2_point_vs_sequence(df, output_dir):
    """RQ2: Point vs sequence comparison with individual models."""
    fig, axes = plt.subplots(1, 2, figsize=(7, 3))

    point_models = ["IF", "OC_SVM"]
    seq_models = ["LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE"]
    all_models = point_models + seq_models
    models = [m for m in all_models if m in df["model"].unique()]

    x = np.arange(len(models))
    width = 0.35

    for ax, metric, title in [
        (axes[0], "f1_score", "F1 Score"),
        (axes[1], "auc_roc", "AUC-ROC"),
    ]:
        means = []
        stds = []
        colors = []
        for m in models:
            mdf = df[df["model"] == m]
            means.append(mdf[metric].mean())
            stds.append(mdf[metric].std())
            t = "Point" if m in point_models else "Sequence"
            colors.append(TYPE_COLORS[t])

        ax.bar(x, means, width * 2, yerr=stds, capsize=3,
               color=colors, edgecolor="white", linewidth=0.5,
               error_kw={"linewidth": 0.8})

        # Add group averages as horizontal lines
        point_mean = np.mean([means[i] for i, m in enumerate(models) if m in point_models])
        seq_mean = np.mean([means[i] for i, m in enumerate(models) if m in seq_models])
        ax.axhline(y=point_mean, color=TYPE_COLORS["Point"], linestyle="--",
                   linewidth=1, alpha=0.7, label=f"Point avg: {point_mean:.3f}")
        ax.axhline(y=seq_mean, color=TYPE_COLORS["Sequence"], linestyle="--",
                   linewidth=1, alpha=0.7, label=f"Seq avg: {seq_mean:.3f}")

        ax.set_xticks(x)
        ax.set_xticklabels([MODEL_LABELS.get(m, m) for m in models],
                           rotation=45, ha="right")
        ax.set_ylabel(title)
        ax.set_ylim(0, 0.85)
        ax.grid(axis="y", alpha=0.3, linewidth=0.5)
        ax.set_axisbelow(True)
        ax.legend(loc="upper right", framealpha=0.9, fontsize=7)

    fig.tight_layout()
    fig.savefig(output_dir / "fig4_rq2_point_vs_sequence.pdf")
    fig.savefig(output_dir / "fig4_rq2_point_vs_sequence.png", dpi=300)
    plt.close(fig)
    print("  Fig 4: RQ2 point vs sequence comparison")


def fig5_ablation_heatmap(ablation_df, output_dir):
    """RQ3: Feature category ablation heatmap."""
    if ablation_df is None or ablation_df.empty:
        print("  Fig 5: SKIPPED (no ablation data)")
        return

    fig, ax = plt.subplots(figsize=(5, 4))

    # Filter to IF model only (as stated in caption)
    ablation_df = ablation_df[ablation_df["model"] == "IF"].copy()

    # Pivot: category configs as rows, metrics as columns
    configs = sorted(ablation_df["category"].unique())
    metrics = ["f1_score", "auc_roc", "precision", "recall"]
    metric_labels = ["F1", "AUC-ROC", "Precision", "Recall"]

    data = np.zeros((len(configs), len(metrics)))
    for i, config in enumerate(configs):
        cdf = ablation_df[ablation_df["category"] == config]
        for j, metric in enumerate(metrics):
            data[i, j] = cdf[metric].mean()

    im = ax.imshow(data, cmap="YlOrRd", aspect="auto", vmin=0.0, vmax=0.8)

    ax.set_xticks(range(len(metrics)))
    ax.set_xticklabels(metric_labels)
    ax.set_yticks(range(len(configs)))
    config_labels = [c.replace("no_", "w/o ").replace("_", " ").title() for c in configs]
    ax.set_yticklabels(config_labels)

    # Annotate cells
    for i in range(len(configs)):
        for j in range(len(metrics)):
            color = "white" if data[i, j] > 0.5 else "black"
            ax.text(j, i, f"{data[i, j]:.3f}", ha="center", va="center",
                    color=color, fontsize=7)

    plt.colorbar(im, ax=ax, shrink=0.8, label="Score")
    ax.set_title("Feature Category Ablation (IF)")

    fig.tight_layout()
    fig.savefig(output_dir / "fig5_ablation_heatmap.pdf")
    fig.savefig(output_dir / "fig5_ablation_heatmap.png", dpi=300)
    plt.close(fig)
    print("  Fig 5: RQ3 ablation heatmap")


def fig6_per_fault_heatmap(fault_df, output_dir):
    """Per-fault-type heatmap: 8 faults x 8 models (mean F1)."""
    if fault_df is None or fault_df.empty:
        print("  Fig 6: SKIPPED (no per-fault data)")
        return

    fig, ax = plt.subplots(figsize=(7, 4))

    models = [m for m in MODEL_ORDER if m in fault_df["model"].unique()]
    faults = sorted(fault_df["fault_id"].unique())

    data = np.zeros((len(faults), len(models)))
    for i, fault in enumerate(faults):
        for j, model in enumerate(models):
            fdf = fault_df[(fault_df["fault_id"] == fault) & (fault_df["model"] == model)]
            data[i, j] = fdf["f1_score"].mean() if len(fdf) > 0 else 0.0

    im = ax.imshow(data, cmap="YlOrRd", aspect="auto", vmin=0.0, vmax=1.0)

    ax.set_xticks(range(len(models)))
    ax.set_xticklabels([MODEL_LABELS.get(m, m) for m in models], rotation=45, ha="right")
    ax.set_yticks(range(len(faults)))

    # Fault labels with categories
    fault_labels = {
        "F1": "F1: Broker crash", "F2": "F2: Net partition",
        "F3": "F3: Disk I/O", "F4": "F4: Consumer lag",
        "F5": "F5: Rebalance storm", "F6": "F6: Poison pill",
        "F7": "F7: Checkpoint fail", "F8": "F8: OOM",
    }
    ax.set_yticklabels([fault_labels.get(f, f) for f in faults])

    # Annotate
    for i in range(len(faults)):
        for j in range(len(models)):
            color = "white" if data[i, j] > 0.5 else "black"
            ax.text(j, i, f"{data[i, j]:.2f}", ha="center", va="center",
                    color=color, fontsize=6)

    plt.colorbar(im, ax=ax, shrink=0.8, label="F1 Score")
    ax.set_title("Per-Fault Detection Performance (F1)")

    fig.tight_layout()
    fig.savefig(output_dir / "fig6_per_fault_heatmap.pdf")
    fig.savefig(output_dir / "fig6_per_fault_heatmap.png", dpi=300)
    plt.close(fig)
    print("  Fig 6: Per-fault heatmap")


def fig7_detection_latency(latency_df, output_dir):
    """Detection latency box plot per model."""
    if latency_df is None or latency_df.empty:
        print("  Fig 7: SKIPPED (no latency data)")
        return

    fig, ax = plt.subplots(figsize=(7, 3.5))

    # Only detected episodes
    detected = latency_df[latency_df["detected"] == True].copy()
    models = [m for m in MODEL_ORDER if m in detected["model"].unique()]

    box_data = [detected[detected["model"] == m]["latency_seconds"].values for m in models]

    bp = ax.boxplot(box_data, labels=[MODEL_LABELS.get(m, m) for m in models],
                    patch_artist=True, widths=0.6,
                    medianprops={"color": "black", "linewidth": 1.5},
                    flierprops={"markersize": 3, "alpha": 0.5})

    for patch, model in zip(bp["boxes"], models):
        patch.set_facecolor(MODEL_COLORS.get(model, "#cccccc"))
        patch.set_alpha(0.7)

    ax.set_ylabel("Detection Latency (seconds)")
    ax.set_xticklabels([MODEL_LABELS.get(m, m) for m in models],
                       rotation=45, ha="right")
    ax.grid(axis="y", alpha=0.3, linewidth=0.5)
    ax.set_axisbelow(True)

    # Add detection rate annotation
    for i, model in enumerate(models):
        total = len(latency_df[latency_df["model"] == model])
        det = len(detected[detected["model"] == model])
        rate = det / total if total > 0 else 0
        ax.text(i + 1, ax.get_ylim()[1] * 0.95, f"{rate:.0%}",
                ha="center", va="top", fontsize=7, fontweight="bold", color="#444444")

    ax.set_title("Detection Latency (detected episodes only)")
    fig.tight_layout()
    fig.savefig(output_dir / "fig7_detection_latency.pdf")
    fig.savefig(output_dir / "fig7_detection_latency.png", dpi=300)
    plt.close(fig)
    print("  Fig 7: Detection latency box plot")


def fig8_window_sensitivity(results_dir, output_dir):
    """Window size sensitivity: F1 vs window size for top models."""
    fig, ax = plt.subplots(figsize=(5, 3.5))

    window_sizes = [15, 30, 60, 120]
    models_to_plot = ["IF", "RF", "OC_SVM", "LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE"]

    for model in models_to_plot:
        means = []
        stds = []
        valid_ws = []

        for ws in window_sizes:
            if ws == 30:
                # Main results
                main_path = results_dir / "model_results.csv"
                if main_path.exists():
                    df = pd.read_csv(main_path)
                    mdf = df[df["model"] == model]
                    if len(mdf) > 0:
                        means.append(mdf["f1_score"].mean())
                        stds.append(mdf["f1_score"].std())
                        valid_ws.append(ws)
            else:
                sens_path = results_dir / f"sensitivity_ws_{ws}s" / "model_results.csv"
                if not sens_path.exists():
                    # Try parent path or gpu-3 path
                    sens_path = results_dir.parent / f"sensitivity_ws_{ws}s" / "model_results.csv"
                if not sens_path.exists():
                    sens_path = results_dir.parent / f"cpu-1" / f"sensitivity_ws_{ws}s" / "model_results.csv"
                if sens_path.exists():
                    df = pd.read_csv(sens_path)
                    mdf = df[df["model"] == model]
                    if len(mdf) > 0:
                        means.append(mdf["f1_score"].mean())
                        stds.append(mdf["f1_score"].std())
                        valid_ws.append(ws)

        if valid_ws:
            color = MODEL_COLORS.get(model, "#333333")
            ax.errorbar(valid_ws, means, yerr=stds, marker="o", markersize=5,
                       capsize=3, linewidth=1.5, color=color,
                       label=MODEL_LABELS.get(model, model))

    ax.set_xlabel("Window Size (seconds)")
    ax.set_ylabel("F1 Score")
    ax.set_xticks(window_sizes)
    ax.set_xticklabels([f"{ws}s" for ws in window_sizes])
    ax.grid(alpha=0.3, linewidth=0.5)
    ax.set_axisbelow(True)
    ax.legend(loc="best", framealpha=0.9)
    ax.set_title("Window Size Sensitivity")

    fig.tight_layout()
    fig.savefig(output_dir / "fig8_window_sensitivity.pdf")
    fig.savefig(output_dir / "fig8_window_sensitivity.png", dpi=300)
    plt.close(fig)
    print("  Fig 8: Window size sensitivity")


def main():
    parser = argparse.ArgumentParser(description="Generate result figures")
    parser.add_argument("--results", default="results/merged",
                        help="Path to merged results directory")
    parser.add_argument("--output", default="figures/",
                        help="Output directory for figures")
    args = parser.parse_args()

    results_dir = Path(args.results)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Loading data...")
    model_results = pd.read_csv(results_dir / "model_results.csv")
    print(f"  model_results: {len(model_results)} rows")

    fault_df = None
    fault_path = results_dir / "per_fault_type_results.csv"
    if fault_path.exists():
        fault_df = pd.read_csv(fault_path)
        print(f"  per_fault_type: {len(fault_df)} rows")

    latency_df = None
    latency_path = results_dir / "detection_latency_results.csv"
    if latency_path.exists():
        latency_df = pd.read_csv(latency_path)
        print(f"  detection_latency: {len(latency_df)} rows")

    ablation_df = None
    ablation_path = results_dir / "ablation_category" / "model_results.csv"
    if ablation_path.exists():
        ablation_df = pd.read_csv(ablation_path)
        print(f"  ablation: {len(ablation_df)} rows")

    print("\nGenerating figures...")
    fig3_model_comparison(model_results, output_dir)
    fig4_rq2_point_vs_sequence(model_results, output_dir)
    fig5_ablation_heatmap(ablation_df, output_dir)
    fig6_per_fault_heatmap(fault_df, output_dir)
    fig7_detection_latency(latency_df, output_dir)
    fig8_window_sensitivity(results_dir, output_dir)

    print(f"\nAll figures saved to: {output_dir}")
    print("Note: Fig 1 (architecture) and Fig 2 (protocol) are created separately with Python diagrams library.")


if __name__ == "__main__":
    main()
