"""Figs. 3 and 4 from the analysis outputs.

fig3_model_comparison.pdf       RQ1 panel on both workloads, with the EWMA classical baseline.
fig4_rq2_point_vs_sequence.pdf  the matched comparison, where every model is scored on
                                identical windows at identical prevalence, on both workloads.

Figures are written to $FIG_DIR (default: figures/ at the repository root).
"""
import os
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
A = Path(__file__).resolve().parent
FIG_DIR = Path(os.environ.get("FIG_DIR", ROOT / "figures"))

LABELS = {
    "IF": "IF", "OC_SVM": "OC-SVM", "LSTM_AE": "LSTM-AE", "Trans_AE": "Trans-AE",
    "CNN_AE": "CNN-AE", "LSTM_VAE": "LSTM-VAE", "DeepSVDD": "Deep SVDD",
    "DAGMM": "DAGMM", "RF": "RF", "RuleBased": "Statistical (max-z)",
    "EWMA": "EWMA (classical)",
}
FAMILY_COLOR = {"point": "tab:blue", "sequence": "tab:red",
                "reference": "gray", "supervised reference": "tab:cyan"}


def fig3_with_ewma():
    """RQ1 comparison at the e-commerce 30s anchor, with EWMA in the panel."""
    d = pd.read_csv(A / "rq1_panel_with_ewma.csv")
    d = d[d["workload"] == "ecommerce"]
    agg = d.groupby("model").agg(f1_m=("f1_score", "mean"), f1_s=("f1_score", "std"),
                                 auc_m=("auc_roc", "mean"), auc_s=("auc_roc", "std"))
    order = agg.sort_values("auc_m", ascending=False).index.tolist()
    agg = agg.reindex(order)

    colors = []
    for m in order:
        if m == "EWMA":
            colors.append("tab:green")
        elif m == "RF":
            colors.append("tab:cyan")
        elif m == "RuleBased":
            colors.append("gray")
        else:
            colors.append("tab:blue" if m in ("IF", "OC_SVM") else "tab:red")

    fig, axes = plt.subplots(1, 2, figsize=(7.5, 3.6))
    x = np.arange(len(agg))
    labels = [LABELS.get(m, m) for m in order]

    for ax, mcol, scol, title, ylim in (
            (axes[0], "f1_m", "f1_s", "F1 (e-commerce, 30s)", (0, 0.70)),
            (axes[1], "auc_m", "auc_s", "AUC-ROC (e-commerce, 30s)", (0.45, 0.85))):
        bars = ax.bar(x, agg[mcol], yerr=agg[scol], color=colors, capsize=3,
                      edgecolor="black", linewidth=0.4)
        for i, m in enumerate(order):
            if m == "EWMA":
                bars[i].set_hatch("//")
        ax.set_title(title, fontsize=9)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=7)
        ax.set_ylim(*ylim)
        ax.grid(True, alpha=0.3, axis="y")
    axes[0].set_ylabel("F1 score")
    axes[1].set_ylabel("AUC-ROC")
    axes[1].axhline(0.5, color="black", ls=":", lw=0.8)
    axes[1].text(-0.4, 0.505, "chance", fontsize=6, ha="left", va="bottom", alpha=0.7)

    handles = [plt.Rectangle((0, 0), 1, 1, fc=c, ec="black", lw=0.4, hatch=h)
               for c, h in (("tab:cyan", ""), ("tab:blue", ""), ("tab:red", ""),
                            ("tab:green", "//"), ("gray", ""))]
    fig.legend(handles, ["supervised", "point", "sequence", "EWMA baseline", "max-z"],
               loc="upper center", ncol=5, fontsize=7, frameon=False,
               bbox_to_anchor=(0.5, 1.06))
    plt.tight_layout()
    out = FIG_DIR / "fig3_model_comparison.pdf"
    plt.savefig(out, bbox_inches="tight", dpi=300)
    plt.close()
    print(f"saved {out}  ({len(order)} models, EWMA included)")


def fig4_matched_rq2():
    """Matched point-vs-sequence comparison on identical windows, both workloads."""
    d = pd.read_csv(A / "rq2_matched_summary.csv")
    fam_rank = {"supervised reference": 0, "reference": 0, "point": 1, "sequence": 2}

    fig, axes = plt.subplots(1, 2, figsize=(7.5, 3.8), sharex=True)
    for ax, wl, title in ((axes[0], "ecommerce", "e-commerce"), (axes[1], "iot", "IoT")):
        s = d[d["workload"] == wl].copy()
        s["fam"] = s["family"].map(fam_rank)
        s = s.sort_values(["auc_mean"], ascending=True)
        y = np.arange(len(s))
        colors = ["tab:cyan" if m == "RF" else FAMILY_COLOR.get(f, "gray")
                  for m, f in zip(s["model"], s["family"])]
        ax.barh(y, s["auc_mean"], xerr=s["auc_se"], color=colors, capsize=2.5,
                edgecolor="black", linewidth=0.4)
        ax.set_yticks(y)
        ax.set_yticklabels([LABELS.get(m, m) for m in s["model"]], fontsize=7.5)
        ax.axvline(0.5, color="black", ls=":", lw=0.9)
        ax.set_xlim(0.45, 0.80)
        ax.set_title(f"{title} (prevalence {s['prevalence'].iloc[0]:.3f})", fontsize=9)
        ax.grid(True, alpha=0.3, axis="x")
        ax.set_xlabel("AUC-ROC")
    for ax in axes:
        ax.text(0.5, ax.get_ylim()[1], " chance", fontsize=6, va="top", ha="left",
                color="black", alpha=0.7)

    handles = [plt.Rectangle((0, 0), 1, 1, fc=FAMILY_COLOR[f], ec="black", lw=0.4)
               for f in ("supervised reference", "point", "sequence")]
    fig.legend(handles, ["supervised reference", "point-in-time", "sequence"],
               loc="upper center", ncol=3, fontsize=7.5, frameon=False,
               bbox_to_anchor=(0.5, 1.05))
    plt.tight_layout()
    out = FIG_DIR / "fig4_rq2_point_vs_sequence.pdf"
    plt.savefig(out, bbox_inches="tight", dpi=300)
    plt.close()
    print(f"saved {out}  (matched comparison, both workloads)")


if __name__ == "__main__":
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    fig3_with_ewma()
    fig4_matched_rq2()
