"""make figures (protocol 7.1 step 9): figures as PDF from the analysis outputs.

Added on 18 October 2026, after registration (D4). Each PDF's metadata names the hashes of its inputs (7.4.3).

    python3 -m analysis.figures --out DIR --dest DIR/figures
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

PIPE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPE))
from analysis import metrics as M  # noqa: E402
from analysis.sources import DEEP, PANEL  # noqa: E402
from analysis.tables import NAMES  # noqa: E402
from streamprep.contract import sha256_file  # noqa: E402

DELTA = 0.03
WL = {"ecommerce": "E-commerce", "iot": "IoT"}
plt.rcParams.update({"font.size": 8, "font.family": "serif", "axes.linewidth": 0.6, "pdf.fonttype": 42,
                     "xtick.major.width": 0.6, "ytick.major.width": 0.6})
COLORS = {"pooled": "#1f4e79", "corrected": "#b85c00", "plain": "#5b8c3a"}
LAT_COLORS = {"RuleBased": "#000000", "IF": "#1f77b4", "OC_SVM": "#2ca02c", "LSTM_AE": "#9467bd",
              "DeepSVDD": "#8c564b", "DAGMM": "#d62728", "EWMA": "#ff7f0e"}


def save(fig, path: Path, inputs: dict):
    fig.savefig(path, metadata={"Subject": "; ".join(f"{k} {v[:16]}" for k, v in sorted(inputs.items())),
                                "Creator": "analysis/figures.py"}, bbox_inches="tight")
    plt.close(fig)


def forest(ax, labels, series, title):
    y = np.arange(len(labels))[::-1]
    off = np.linspace(-0.25, 0.25, len(series))
    for (name, vals), o in zip(series.items(), off):
        m = np.array([v[0] for v in vals])
        lo = np.array([v[1] for v in vals])
        hi = np.array([v[2] for v in vals])
        ax.errorbar(m, y + o, xerr=[m - lo, hi - m], fmt="o", ms=3, lw=0.9, capsize=1.5,
                    color=COLORS[name.split()[0].lower()] if name.split()[0].lower() in COLORS else None, label=name)
    ax.axvline(0, color="0.3", lw=0.6)
    ax.axvspan(-DELTA, DELTA, color="0.9", zorder=0)
    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.set_title(title)


def fig_c1(res, ph, dest, inputs):
    fig, axes = plt.subplots(1, 2, figsize=(7.0, 3.0), sharey=True)
    dets = DEEP + ["EWMA"]
    for ax, w in zip(axes, ("ecommerce", "iot")):
        H = res["workloads"][w]["hypotheses"]
        get = lambda m, k: (H["C2a"]["contrasts"][k] if m == "EWMA" else H["C1"]["contrasts"][f"{m}|{k}"])["pooled"]
        tri = lambda p: (p["mean"], p["ci95_lo"], p["ci95_hi"])
        series = {"Pooled AUC": [tri(get(m, "pooled_auc")) for m in dets],
                  "Corrected within-run": [tri(get(m, "within_run_pc")) for m in dets],
                  "Plain within-run (post hoc)": [tri(ph["within_run"][w]["C1_within_run_auc"][m]["pooled"])
                                                  for m in dets]}
        forest(ax, [NAMES[m] for m in dets], series, WL[w])
        ax.set_xlabel("Difference from static max-z (AUC)")
    h, l_ = axes[0].get_legend_handles_labels()
    fig.legend(h, l_, loc="lower center", ncol=3, fontsize=7, frameon=False, bbox_to_anchor=(0.5, -0.11))
    save(fig, dest / "fig_c1.pdf", inputs)


def fig_c6(res, dest, inputs):
    fig, axes = plt.subplots(1, 2, figsize=(7.0, 2.8), sharey=True)
    for ax, w in zip(axes, ("ecommerce", "iot")):
        C6 = res["workloads"][w]["hypotheses"]["C6"]["contrasts"]
        tri = lambda p: (p["mean"], p["ci95_lo"], p["ci95_hi"])
        forest(ax, [NAMES[m] for m in PANEL], {"Pooled AUC": [tri(C6[m]["pooled"]) for m in PANEL]},
               f"{WL[w]} targets")
        ax.set_xlabel("Within-workload minus cross-workload AUC")
    save(fig, dest / "fig_c6.pdf", inputs)


def fig_c7(res, dest, inputs):
    dets = PANEL + ["RF", "RuleBased", "EWMA"]
    fig, axes = plt.subplots(1, 2, figsize=(7.0, 3.0), sharey=True)
    for ax, w in zip(axes, ("ecommerce", "iot")):
        y = np.arange(len(dets))[::-1]
        for yi, d in zip(y, dets):
            v = res["C7"][w][d]
            pm = v["pooled_mean"]
            ax.add_patch(plt.Rectangle((pm - DELTA, yi - 0.35), 2 * DELTA, 0.7, color="0.9", zorder=0))
            for k, (c, m) in enumerate(sorted(v["campaign_means"].items())):
                ax.plot(m, yi, ["o", "s", "^"][k], ms=3.5, color=["0.5", "#1f4e79", "#b85c00"][k],
                        label=f"Campaign {k + 1}" if yi == y[0] else None)
        ax.set_yticks(y)
        ax.set_yticklabels([NAMES[d] for d in dets])
        ax.set_xlabel("Mean pooled AUC over ten folds")
        ax.set_title(WL[w])
    h, l_ = axes[0].get_legend_handles_labels()
    fig.legend(h, l_, loc="lower center", ncol=3, fontsize=7, frameon=False, bbox_to_anchor=(0.5, -0.11))
    save(fig, dest / "fig_c7.pdf", inputs)


def km_curve(times, events, tau):
    """Kaplan-Meier share detected, as step points (x, y) for ax.step(where="post"), starting at 0 before onset."""
    s, xs, ys = 1.0, [], []
    for x in np.sort(np.unique(times[events])):
        n = (times >= x).sum()
        s *= 1 - (events & (times == x)).sum() / n
        xs.append(float(x))
        ys.append(1 - s)
    xs, ys = [-1e-9] + xs + [tau], [0.0] + ys + [ys[-1] if ys else 0.0]
    return xs, ys


def fig_latency(runs, dest, inputs, budget="fpb_0.01"):
    dets = ["RuleBased", "IF", "OC_SVM", "LSTM_AE", "DeepSVDD", "DAGMM", "EWMA"]
    fig, axes = plt.subplots(1, 2, figsize=(7.0, 2.6), sharey=True)
    fr = runs[(runs["run_type"] != "F0") & ~runs["campaign_id"].str.endswith("-a")]
    for ax, w in zip(axes, ("ecommerce", "iot")):
        x = fr[fr["campaign_id"].str.startswith("ec" if w == "ecommerce" else "iot")]
        for d in dets:
            g = x[x["detector"] == d]
            xs, ys = km_curve(g[f"latency_s_{budget}"].to_numpy(), g[f"detected_{budget}"].to_numpy(bool),
                              float(g[f"span_s_{budget}"].max()))
            ax.step(xs, ys, where="post", lw=1.0, color=LAT_COLORS[d], label=NAMES[d])
        ax.set_xlabel("Seconds from verified onset to first alerted window")
        ax.set_title(f"{WL[w]}, 1% budget")
        ax.set_ylim(0, 1)
    axes[0].set_ylabel("Share of fault runs detected")
    h, l_ = axes[0].get_legend_handles_labels()
    fig.legend(h, l_, loc="lower center", ncol=4, fontsize=7, frameon=False, bbox_to_anchor=(0.5, -0.22))
    save(fig, dest / "fig_latency.pdf", inputs)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--out", required=True)
    p.add_argument("--dest", required=True)
    a = p.parse_args(argv)
    out, dest = Path(a.out), Path(a.dest)
    dest.mkdir(parents=True, exist_ok=True)
    res = json.loads((out / "confirm.json").read_text())
    ph = json.loads((out / "posthoc.json").read_text())
    runs = pd.read_parquet(out / "run_metrics.parquet")
    h = lambda *fs: {f: sha256_file(out / f) for f in fs}
    fig_c1(res, ph, dest, h("confirm.json", "posthoc.json"))
    fig_c6(res, dest, h("confirm.json"))
    fig_c7(res, dest, h("confirm.json"))
    fig_latency(runs, dest, h("run_metrics.parquet"))
    print(f"figures: {len(list(dest.glob('*.pdf')))} files in {dest}")


if __name__ == "__main__":
    main()
