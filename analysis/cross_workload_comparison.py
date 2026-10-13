#!/usr/bin/env python3
"""
Cross-workload comparison of the two e-commerce runs: Phase A at 5,000 events/s
against Phase B at 500 events/s.

Per-model Wilcoxon signed-rank tests paired by fold, effect sizes,
summary table, and comparison figure.

Usage:
    python analysis/cross_workload_comparison.py
"""

from __future__ import annotations

import json
from itertools import combinations
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import stats

ALPHA = 0.05
ALL_MODELS = ["IF", "OC_SVM", "LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE", "RF", "RuleBased"]

PHASE_A_DIR = Path("data/results_v3/merged")
PHASE_B_DIR = Path("data/results_v3_phase_b/merged")
OUTPUT_DIR = Path("data/results_v3/cross_workload")


def cliffs_delta(a: np.ndarray, b: np.ndarray) -> tuple[float, str]:
    n_a, n_b = len(a), len(b)
    if n_a == 0 or n_b == 0:
        return 0.0, "negligible"
    more = sum(1 for ai in a for bi in b if ai > bi)
    less = sum(1 for ai in a for bi in b if ai < bi)
    delta = (more - less) / (n_a * n_b)
    abs_d = abs(delta)
    if abs_d < 0.147:
        mag = "negligible"
    elif abs_d < 0.33:
        mag = "small"
    elif abs_d < 0.474:
        mag = "medium"
    else:
        mag = "large"
    return float(delta), mag


def cohens_d(a: np.ndarray, b: np.ndarray) -> float:
    na, nb = len(a), len(b)
    if na < 2 or nb < 2:
        return 0.0
    pooled_std = np.sqrt(((na - 1) * np.var(a, ddof=1) + (nb - 1) * np.var(b, ddof=1))
                         / (na + nb - 2))
    if pooled_std < 1e-10:
        return 0.0
    return float((np.mean(a) - np.mean(b)) / pooled_std)


def holm_bonferroni(p_values: list[tuple[str, float]]) -> list[tuple[str, float, bool]]:
    sorted_pvals = sorted(p_values, key=lambda x: x[1])
    m = len(sorted_pvals)
    results = []
    for i, (label, p) in enumerate(sorted_pvals):
        adjusted = min(p * (m - i), 1.0)
        results.append((label, adjusted, adjusted < ALPHA))
    for i in range(1, len(results)):
        if results[i][1] < results[i - 1][1]:
            results[i] = (results[i][0], results[i - 1][1], results[i][2])
    return results


def run_comparison():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df_a = pd.read_csv(PHASE_A_DIR / "model_results.csv")
    df_b = pd.read_csv(PHASE_B_DIR / "model_results.csv")

    df_a["workload"] = "5000eps"
    df_b["workload"] = "500eps"

    print(f"Phase A: {len(df_a)} rows, models: {sorted(df_a['model'].unique())}")
    print(f"Phase B: {len(df_b)} rows, models: {sorted(df_b['model'].unique())}")

    results = []
    raw_pvals = []

    for metric in ["f1_score", "auc_roc"]:
        print(f"\n{'='*70}")
        print(f"CROSS-WORKLOAD COMPARISON: {metric}")
        print(f"{'='*70}")

        for model in ALL_MODELS:
            a_vals = df_a[df_a["model"] == model].sort_values("fold")[metric].values
            b_vals = df_b[df_b["model"] == model].sort_values("fold")[metric].values

            if len(a_vals) != 10 or len(b_vals) != 10:
                print(f"  SKIP {model}: Phase A has {len(a_vals)} folds, Phase B has {len(b_vals)}")
                continue

            try:
                stat, p = stats.wilcoxon(a_vals, b_vals, alternative="two-sided")
            except ValueError:
                stat, p = 0.0, 1.0

            d = cohens_d(b_vals, a_vals)
            cd, cd_mag = cliffs_delta(b_vals, a_vals)

            label = f"{model}|{metric}"
            raw_pvals.append((label, p))

            row = {
                "model": model,
                "metric": metric,
                "mean_5000eps": round(float(np.mean(a_vals)), 4),
                "std_5000eps": round(float(np.std(a_vals)), 4),
                "mean_500eps": round(float(np.mean(b_vals)), 4),
                "std_500eps": round(float(np.std(b_vals)), 4),
                "delta": round(float(np.mean(b_vals) - np.mean(a_vals)), 4),
                "pct_change": round(float((np.mean(b_vals) - np.mean(a_vals)) / np.mean(a_vals) * 100), 1),
                "wilcoxon_W": round(float(stat), 4),
                "p_value": round(float(p), 6),
                "cohens_d": round(d, 4),
                "cliffs_delta": round(cd, 4),
                "cliffs_magnitude": cd_mag,
            }
            results.append(row)

            direction = "+" if row["delta"] > 0 else ""
            print(f"  {model:12s}: 5000eps={row['mean_5000eps']:.4f}  500eps={row['mean_500eps']:.4f}  "
                  f"delta={direction}{row['delta']:.4f} ({direction}{row['pct_change']}%)  "
                  f"W={stat:.1f}  p={p:.6f}  d={d:+.4f}  delta={cd:+.4f} ({cd_mag})")

    if raw_pvals:
        corrected = holm_bonferroni(raw_pvals)
        print(f"\nHolm-Bonferroni corrected ({len(corrected)} tests):")
        for label, adj_p, sig in corrected:
            model_name, metric_name = label.split("|")
            for r in results:
                if r["model"] == model_name and r["metric"] == metric_name:
                    r["p_adjusted"] = round(float(adj_p), 6)
                    r["significant"] = bool(sig)
            marker = "***" if sig else "   "
            print(f"  {marker} {label:25s}: p_adj={adj_p:.6f}")

    df_results = pd.DataFrame(results)
    df_results.to_csv(OUTPUT_DIR / "cross_workload_comparison.csv", index=False)
    print(f"\nSaved: {OUTPUT_DIR / 'cross_workload_comparison.csv'}")

    with open(OUTPUT_DIR / "cross_workload_comparison.json", "w") as f:
        json.dump(results, f, indent=2)

    generate_figure(df_a, df_b)
    generate_latex_table(df_results)

    return df_results


def generate_figure(df_a: pd.DataFrame, df_b: pd.DataFrame):
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.5), sharey=False)

    for ax_idx, metric in enumerate(["f1_score", "auc_roc"]):
        ax = axes[ax_idx]
        models = ALL_MODELS
        x = np.arange(len(models))
        width = 0.35

        means_a, stds_a, means_b, stds_b = [], [], [], []
        for model in models:
            va = df_a[df_a["model"] == model][metric].values
            vb = df_b[df_b["model"] == model][metric].values
            means_a.append(np.mean(va))
            stds_a.append(np.std(va))
            means_b.append(np.mean(vb))
            stds_b.append(np.std(vb))

        bars_a = ax.bar(x - width/2, means_a, width, yerr=stds_a, capsize=3,
                        label="5,000 eps", color="#4e79a7", alpha=0.85)
        bars_b = ax.bar(x + width/2, means_b, width, yerr=stds_b, capsize=3,
                        label="500 eps", color="#e15759", alpha=0.85)

        metric_label = "F1 Score" if metric == "f1_score" else "AUC-ROC"
        ax.set_ylabel(metric_label, fontsize=11)
        ax.set_xticks(x)
        ax.set_xticklabels([m.replace("_", "\n") for m in models], fontsize=8, rotation=0)
        ax.set_ylim(0, 1.0)
        ax.legend(fontsize=9, loc="upper left")
        ax.set_title(f"({chr(97 + ax_idx)}) {metric_label}", fontsize=11)
        ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    out_path = OUTPUT_DIR / "fig_cross_workload.pdf"
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {out_path}")

    out_png = OUTPUT_DIR / "fig_cross_workload.png"
    fig2, axes2 = plt.subplots(1, 2, figsize=(10, 4.5), sharey=False)
    for ax_idx, metric in enumerate(["f1_score", "auc_roc"]):
        ax = axes2[ax_idx]
        models = ALL_MODELS
        x = np.arange(len(models))
        width = 0.35
        means_a, stds_a, means_b, stds_b = [], [], [], []
        for model in models:
            va = df_a[df_a["model"] == model][metric].values
            vb = df_b[df_b["model"] == model][metric].values
            means_a.append(np.mean(va))
            stds_a.append(np.std(va))
            means_b.append(np.mean(vb))
            stds_b.append(np.std(vb))
        ax.bar(x - width/2, means_a, width, yerr=stds_a, capsize=3,
               label="5,000 eps", color="#4e79a7", alpha=0.85)
        ax.bar(x + width/2, means_b, width, yerr=stds_b, capsize=3,
               label="500 eps", color="#e15759", alpha=0.85)
        metric_label = "F1 Score" if metric == "f1_score" else "AUC-ROC"
        ax.set_ylabel(metric_label, fontsize=11)
        ax.set_xticks(x)
        ax.set_xticklabels([m.replace("_", "\n") for m in models], fontsize=8)
        ax.set_ylim(0, 1.0)
        ax.legend(fontsize=9, loc="upper left")
        ax.set_title(f"({chr(97 + ax_idx)}) {metric_label}", fontsize=11)
        ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    fig2.savefig(out_png, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {out_png}")


def generate_latex_table(df: pd.DataFrame):
    f1_rows = df[df["metric"] == "f1_score"].sort_values("model")

    lines = []
    lines.append(r"\begin{table}[t]")
    lines.append(r"\caption{Cross-workload comparison: model performance at 5{,}000 and 500 events/s.}")
    lines.append(r"\label{tab:cross-workload}")
    lines.append(r"\centering")
    lines.append(r"\small")
    lines.append(r"\begin{tabular}{lcccccc}")
    lines.append(r"\toprule")
    lines.append(r"Model & \multicolumn{2}{c}{F1 Score} & $\Delta$ & $p$ & $p_{\mathrm{adj}}$ & Cliff's $\delta$ \\")
    lines.append(r"\cmidrule(lr){2-3}")
    lines.append(r" & 5{,}000\,eps & 500\,eps & & & & \\")
    lines.append(r"\midrule")

    for _, row in f1_rows.iterrows():
        model_display = row["model"].replace("_", r"\_")
        sig = row.get("significant", False)
        p_adj = row.get("p_adjusted", row["p_value"])
        p_str = f"{p_adj:.3f}" if p_adj >= 0.001 else "<0.001"
        if sig:
            p_str = r"\textbf{" + p_str + "}"

        delta_str = f"{row['delta']:+.3f}"
        cd_str = f"{row['cliffs_delta']:+.3f}"

        lines.append(
            f"  {model_display} & "
            f"{row['mean_5000eps']:.3f}$\\pm${row['std_5000eps']:.3f} & "
            f"{row['mean_500eps']:.3f}$\\pm${row['std_500eps']:.3f} & "
            f"{delta_str} & "
            f"{row['p_value']:.3f} & "
            f"{p_str} & "
            f"{cd_str} \\\\"
        )

    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    lines.append(r"\end{table}")

    table_str = "\n".join(lines)
    out_path = OUTPUT_DIR / "tab_cross_workload.tex"
    with open(out_path, "w") as f:
        f.write(table_str)
    print(f"Saved: {out_path}")
    print(table_str)


if __name__ == "__main__":
    run_comparison()
