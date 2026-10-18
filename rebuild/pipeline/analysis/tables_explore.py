"""make tables, second part: the gate and label summary, the exploratory analyses of 5.9, the drift check and cost.

Added on 18 October 2026, after registration (D6). Same rules as tables.py: input hashes in each file, the formats of
numbers_tex.py, captions of 20 words or fewer. Everything here except the gate and label tables is exploratory.

    python3 -m analysis.tables_explore --out DIR --dest DIR/tables
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import matplotlib
import matplotlib.ticker

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

PIPE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPE))
from analysis.figures import save  # noqa: E402
from analysis.numbers_tex import f3, pfmt, s3, tex_num  # noqa: E402
from analysis.sources import PANEL  # noqa: E402
from analysis.tables import NAMES, WL, ci, table  # noqa: E402
from streamprep.contract import sha256_file  # noqa: E402

FAULT_NAMES = {"F0": "Sham", "F1": "Broker pod kill", "F2": "Broker network partition", "F4": "TaskManager CPU stress",
               "F5": "TaskManager loss", "F6": "Poison pill", "F7": "TaskManager packet loss",
               "F8": "TaskManager memory pressure"}
CAMPS = {"ecommerce": ["ec-a", "ec-b", "ec-c"], "iot": ["iot-a", "iot-b", "iot-c"]}
DETS = PANEL + ["RF", "RuleBased", "EWMA"]


def t_gate(out, inputs):
    f = pd.read_csv(out / "gate" / "faults.csv")
    sfp = pd.read_csv(out / "gate" / "sham_false_passes.csv")
    rows = []
    for rt in ["F0", "F1", "F2", "F4", "F5", "F6", "F7", "F8"]:
        cells = [rt, FAULT_NAMES[rt]]
        for w, cs in CAMPS.items():
            x = f[(f["run_type"] == rt) & f["campaign_id"].isin(cs)]
            src = json.loads(x["onset_sources"].iloc[0])
            onset = {"PLANNED": "plan", "CHAOS_APPLY_EVENT": "apply event",
                     "FIRST_PRODUCE_ACK": "first ack"}.get(next(iter(src)), next(iter(src)))
            span = f"{x['span_min_s'].min():.0f}--{x['span_max_s'].max():.0f}"
            if rt == "F0":
                fp = int(sfp[sfp["campaign_id"].isin(cs)]["false_passes"].sum())
                n = int(sfp[sfp["campaign_id"].isin(cs)]["sham_attempts"].sum())
                extra = f"{fp} of {n}"
            else:
                extra = str(int(x["flag_F7_TM_LOSS"].sum())) if rt == "F7" else "0"
            cells += [f"{int(x['VALID'].sum())}/{int(x['attempts'].sum())}", onset, span, extra]
        rows.append(" & ".join(cells))
    return table("tab_gate.tex", "Verified runs per fault and workload over three campaigns, with spans and flags.",
                 "tab:gate", "llllllllll",
                 "& & \\multicolumn{4}{c}{E-commerce} & \\multicolumn{4}{c}{IoT} \\\\ ID & Fault & Valid/att. & Onset "
                 "& Span (s) & Flags$^{a}$ & Valid/att. & Onset & Span (s) & Flags$^{a}$", rows,
                 "Every attempt passed the gate on its first try, with no repeat, halt or remediation. The offline "
                 "gate reproduces all 480 online results. $^{a}$F7 counts runs flagged F7\\_TM\\_LOSS. For the sham, "
                 "the cell gives fault signatures that passed on shams, out of those checked.", inputs, wide=True)


def t_labels(out, inputs):
    l_ = pd.read_csv(out / "gate" / "labels.csv").set_index("campaign_id")
    rows = []
    for c in ["ec-a", "ec-b", "ec-c", "iot-a", "iot-b", "iot-c"]:
        r = l_.loc[c]
        rows.append(" & ".join([c, *(f"{int(r[k]):,}" for k in ("w_BASELINE__NORMAL", "w_PRE_FAULT__NORMAL",
                                                                   "w_INJECTION__ANOMALOUS", "w_RECOVERY__RECOVERY",
                                                                   "w_INJECTION__SHAM", "w_RECOVERY__SHAM_POST",
                                                                   "w_TRAILING__TRAILING")),
                                f3(r["anomalous_prevalence_attempt_windows"])]))
    return table("tab_labels.tex", "Labelled 30 s windows per campaign.", "tab:labels", "lllllllll",
                 "Campaign & Baseline & Pre-fault & Anomalous & Recovery & Sham & Sham post & Trailing & Prevalence",
                 rows, "Baseline and pre-fault windows are NORMAL. No window failed the normal-time health rule and "
                 "no attempt failed, so nothing was excluded. Prevalence is over the attempt windows the test blocks "
                 "draw from.", inputs)


def t_ewma(out, inputs):
    a = pd.read_csv(out / "ewma_mechanism" / "ablation_summary.csv").set_index(["workload", "variant"])
    names = {"centre_train_mean": "Centre at train mean (C2b)", "running_centre": "Running centre per stretch",
             "scale_global_sd": "Scale by train SD", "static_z": "Static z (mean and SD)",
             "mean_sq_pool": "Mean square in place of max", "mean_sq_centre_train_mean": "Mean square, train mean",
             "sigmoid_squash": "Sigmoid squash", "scale_floor": "Scale floored at 0.1 SD",
             "drop_dominant": "Drop three dominant features"}
    rows = []
    for v, label in names.items():
        cells = [label]
        for w in ("ecommerce", "iot"):
            r = a.loc[(w, v)]
            cells += [ci(r["d_pooled_auc"], r["d_pooled_auc_lo"], r["d_pooled_auc_hi"], True),
                      ci(r["d_within_run_auc"], r["d_within_run_auc_lo"], r["d_within_run_auc_hi"], True)]
        rows.append(" & ".join(cells))
    return table("tab_ewma_mechanism.tex", "EWMA with one component changed, minus the published EWMA.",
                 "tab:ewma-mech", "lllll", "& \\multicolumn{2}{c}{E-commerce} & \\multicolumn{2}{c}{IoT} \\\\ "
                 "Variant & Pooled AUC & Within-run AUC & Pooled AUC & Within-run AUC", rows,
                 "Exploratory (5.9.7). Paired fold differences pooled over campaigns 2 and 3 with 95\\% intervals, "
                 "no adjustment. Every variant gets the panel's per-fold features.", inputs, wide=True)


def t_classical(out, inputs):
    c = pd.read_csv(out / "classical" / "classical_summary.csv").set_index(["workload", "detector"])
    names = {"CuratedRules": "Curated rules, fixed", "CuratedRules_tuned": "Curated rules, tuned on cal",
             "LODA": "LODA", "COPOD": "COPOD", "HBOS": "HBOS", "POT": "POT (registered)",
             "POT_monotone": "POT, monotone score", "CUSUM": "CUSUM", "MP": "Matrix profile"}
    rows = []
    for d, label in names.items():
        cells = [label]
        for w in ("ecommerce", "iot"):
            r = c.loc[(w, d)]
            cells += [ci(r["d_pooled_auc_vs_RuleBased"], r["d_pooled_auc_vs_RuleBased_lo"],
                         r["d_pooled_auc_vs_RuleBased_hi"], True), tex_num(s3(r["d_within_run_auc_vs_RuleBased"]))]
        rows.append(" & ".join(cells))
    return table("tab_classical.tex", "Curated rules and other classical detectors minus static max-z.",
                 "tab:classical", "lllll", "& \\multicolumn{2}{c}{E-commerce} & \\multicolumn{2}{c}{IoT} \\\\ "
                 "Detector & Pooled AUC & Within-run AUC & Pooled AUC & Within-run AUC", rows,
                 "Exploratory (5.9.5, 5.9.6). Paired fold differences pooled over campaigns 2 and 3, 95\\% "
                 "intervals for pooled AUC. CUSUM and matrix profile run as R1 coded them.", inputs)


def t_cost(out, inputs):
    c = pd.read_csv(out / "cost" / "cost.csv").set_index(["workload", "model"])
    m = json.loads((out / "cost" / "machine.json").read_text())
    rows = []
    for d in DETS:
        cells = [NAMES[d], c.loc[("ecommerce", d), "device"]]
        for w in ("ecommerce", "iot"):
            r = c.loc[(w, d)]
            cells += [f"{r['fit_s']:.2f}", f"{r['score_ms_per_window']:.3f}", f"{r['single_ms_p50']:.3f}"]
        rows.append(" & ".join(cells))
    machine = m.get("cpu", "the analysis machine")
    return table("tab_cost.tex", "Training time per fit and scoring time per window on one machine.", "tab:cost",
                 "llllllll", "& & \\multicolumn{3}{c}{E-commerce} & \\multicolumn{3}{c}{IoT} \\\\ Detector & Device "
                 "& Fit (s) & Batch (ms) & Single (ms) & Fit (s) & Batch (ms) & Single (ms)", rows,
                 f"One fit per model on fold 1 of campaign 1, frozen configuration, {machine}. Batch is the whole "
                 "test block per window, single is the median of 256 one-window calls. Fit time excludes the fold's "
                 "feature preparation.", inputs, wide=True)


def t_replay(out, inputs):
    r = pd.read_csv(out / "explore" / "replay.csv").set_index(["workload", "detector", "op", "metric"])
    rows = []
    for d in DETS:
        cells = [NAMES[d]]
        for w in ("ecommerce", "iot"):
            g = lambda m: r.loc[(w, d, "fpb_0.01", m), "pooled_c2_c3"]
            cells += [f"{g('false_per_h'):.2f}", f"{g('trailing_episodes_per_h'):.2f}", f3(g("detection_rate")),
                      f"{g('alert_windows_per_h'):.1f}"]
        rows.append(" & ".join(cells))
    return table("tab_replay.tex", "Chronological alert replay at the 1\\% budget: false episodes and detected faults.",
                 "tab:replay", "lllllllll", "& \\multicolumn{4}{c}{E-commerce} & \\multicolumn{4}{c}{IoT} \\\\ "
                 "Detector & False/h & Trailing/h & Detected & Windows/h & False/h & Trailing/h & Detected & "
                 "Windows/h", rows,
                 "Exploratory (5.9.18). Alerted windows within 120 s join one episode. False episodes hold only NORMAL "
                 "windows. Windows/h counts alerted negative windows, most of them RECOVERY right after a fault.",
                 inputs, wide=True)


def t_drift(out, inputs):
    d = pd.read_csv(out / "explore" / "drift.csv").set_index(["campaign_id", "test", "features"])
    rows = []
    for c in ["ec-a", "ec-b", "ec-c", "iot-a", "iot-b", "iot-c"]:
        rows.append(" & ".join([c, *(f"{d.loc[(c, t, f), 'cv_auc']:.2f}" for t in ("first6h_vs_last6h",
                                                                                  "leading_vs_trailing")
                                     for f in ("all_features", "without_jvm_buffer_pool"))]))
    return table("tab_drift.tex", "Drift check: cross-validated AUC of a classifier separating baseline periods.",
                 "tab:drift", "lllll", "& \\multicolumn{2}{c}{First vs last 6 h} & \\multicolumn{2}{c}{Leading vs "
                 "trailing} \\\\ Campaign & All & No buffer pools$^{a}$ & All & No buffer pools$^{a}$", rows,
                 "Random forest, five-fold cross-validation (1.5.4). Above 0.70 counts as drift, so every pooled "
                 "result carries the drift note of 4.6.4. $^{a}$Post hoc.", inputs)


def t_checks(out, inputs):
    car = pd.read_csv(out / "explore" / "carryover.csv").set_index("scope")
    ray = pd.read_csv(out / "explore" / "load_phase_rayleigh.csv").set_index(["workload", "scope", "run_type"])
    lofo = pd.read_csv(out / "explore" / "lofo.csv")
    rows = []
    for w, cs in CAMPS.items():
        lab = WL[w]
        rows.append(f"{lab} & Carryover, Kruskal-Wallis $p$ & "
                    + " / ".join(tex_num(pfmt(car.loc[c, 'p'])) for c in cs)
                    + f" & {tex_num(pfmt(car.loc[f'{w} campaigns 2 and 3', 'p']))}")
        rp = [ray.loc[(w, "all three", f), "p"] for f in ["F1", "F2", "F4", "F5", "F6", "F7", "F8"]]
        rows.append(f"{lab} & Onset phase by fault, smallest Rayleigh $p$ & n/a & {tex_num(pfmt(min(rp)))}")
        x = lofo[(lofo["workload"] == w) & (lofo["metric"] == "f1_cost")]
        rows.append(f"{lab} & Threshold-only LOFO, mean F1 cost & n/a & {f3(x['pooled_c2_c3'].mean())}")
    return table("tab_checks.tex", "Design checks: carryover, load phase balance and label-free threshold transfer.",
                 "tab:checks", "llll", "Workload & Check & Campaigns 1 / 2 / 3 & Pooled", rows,
                 "Exploratory (5.9.14, 5.9.15, 5.9.9), no adjustment. Carryover compares pre-fault max-z by the "
                 "preceding run type. Rayleigh tests the uniformity of each fault's onset phase in the 30-minute "
                 "load cycle over three campaigns.", inputs)


def fig_perfault(out, dest, inputs):
    pf = pd.read_csv(out / "explore" / "per_fault.csv")
    faults = ["F1", "F2", "F4", "F5", "F6", "F7", "F8"]
    fig, axes = plt.subplots(1, 2, figsize=(7.0, 3.4), sharey=True)
    for ax, w in zip(axes, ("ecommerce", "iot")):
        x = pf[(pf["workload"] == w) & (pf["metric"] == "pooled_auc")].pivot(index="detector", columns="fault",
                                                                             values="pooled_c2_c3")
        x = x.reindex(index=DETS, columns=faults)
        im = ax.imshow(x.to_numpy(), cmap="RdYlBu", vmin=0.3, vmax=1.0, aspect="auto")
        for i in range(x.shape[0]):
            for j in range(x.shape[1]):
                ax.text(j, i, f"{x.iat[i, j]:.2f}", ha="center", va="center", fontsize=5.5)
        ax.set_xticks(range(len(faults)))
        ax.set_xticklabels(faults)
        ax.set_yticks(range(len(DETS)))
        ax.set_yticklabels([NAMES[d] for d in DETS])
        ax.set_title(WL[w])
    fig.colorbar(im, ax=axes, fraction=0.025, label="Pooled AUC, campaigns 2 and 3")
    save(fig, dest / "fig_perfault.pdf", inputs)


def fig_windows(out, dest, inputs):
    wv = pd.read_csv(out / "explore" / "windows.csv")
    fig, axes = plt.subplots(1, 2, figsize=(7.0, 2.6), sharey=True)
    for ax, (c, w) in zip(axes, (("ec-a", "ecommerce"), ("iot-a", "iot"))):
        for d in PANEL:
            x = wv[(wv["campaign_id"] == c) & (wv["detector"] == d)].sort_values("window_s")
            ax.plot(x["window_s"], x["pooled_auc"], "o-", ms=2.5, lw=0.9, label=NAMES[d])
        ax.set_xscale("log")
        ax.xaxis.set_minor_locator(matplotlib.ticker.NullLocator())
        ticks = sorted(wv[wv["campaign_id"] == c]["window_s"].unique())
        ax.set_xticks(ticks)
        ax.set_xticklabels([str(t) for t in ticks])
        ax.set_xlabel("Window length (s), campaign 1")
        ax.set_title(WL[w])
    axes[0].set_ylabel("Pooled AUC")
    h, l_ = axes[0].get_legend_handles_labels()
    fig.legend(h, l_, loc="lower center", ncol=4, fontsize=7, frameon=False, bbox_to_anchor=(0.5, -0.2))
    save(fig, dest / "fig_windows.pdf", inputs)


def fig_prevalence(out, dest, inputs):
    pv = pd.read_csv(out / "explore" / "prevalence.csv")
    fig, axes = plt.subplots(1, 2, figsize=(7.0, 2.6), sharey=True)
    for ax, w in zip(axes, ("ecommerce", "iot")):
        for d in ["RuleBased", "IF", "OC_SVM", "LSTM_AE", "DAGMM", "EWMA", "RF"]:
            x = pv[(pv["workload"] == w) & (pv["detector"] == d) & (pv["op"] == "fpb_0.01")].sort_values("prevalence")
            ax.plot(100 * x["prevalence"], x["f1"], "o-", ms=2.5, lw=0.9, label=NAMES[d])
        x = pv[(pv["workload"] == w) & (pv["detector"] == "IF") & (pv["op"] == "fpb_0.01")].sort_values("prevalence")
        ax.plot(100 * x["prevalence"], x["always_fire_f1"], "k--", lw=0.8, label="Always-fire")
        ax.set_xscale("log")
        ax.xaxis.set_minor_locator(matplotlib.ticker.NullLocator())
        ax.set_xticks([1, 5, 10, 20, 40])
        ax.set_xticklabels(["1", "5", "10", "20", "40"])
        ax.set_xlabel("Anomaly prevalence (%)")
        ax.set_title(f"{WL[w]}, 1% budget")
    axes[0].set_ylabel("F1")
    h, l_ = axes[0].get_legend_handles_labels()
    fig.legend(h, l_, loc="lower center", ncol=4, fontsize=7, frameon=False, bbox_to_anchor=(0.5, -0.2))
    save(fig, dest / "fig_prevalence.pdf", inputs)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--out", required=True)
    p.add_argument("--dest", required=True)
    p.add_argument("--figures", default=None)
    a = p.parse_args(argv)
    out, dest = Path(a.out), Path(a.dest)
    figs = Path(a.figures) if a.figures else out / "figures"
    dest.mkdir(parents=True, exist_ok=True)
    figs.mkdir(parents=True, exist_ok=True)
    h = lambda *fs: {f: sha256_file(out / f) for f in fs}
    made = [t_gate(out, h("gate/faults.csv", "gate/sham_false_passes.csv")),
            t_labels(out, h("gate/labels.csv")),
            t_ewma(out, h("ewma_mechanism/ablation_summary.csv")),
            t_classical(out, h("classical/classical_summary.csv")),
            t_cost(out, h("cost/cost.csv", "cost/machine.json")),
            t_replay(out, h("explore/replay.csv")),
            t_drift(out, h("explore/drift.csv")),
            t_checks(out, h("explore/carryover.csv", "explore/load_phase_rayleigh.csv", "explore/lofo.csv"))]
    for name, text in made:
        (dest / name).write_text(text)
    fig_perfault(out, figs, h("explore/per_fault.csv"))
    fig_windows(out, figs, h("explore/windows.csv"))
    fig_prevalence(out, figs, h("explore/prevalence.csv"))
    print(f"tables_explore: {len(made)} tables in {dest}, 3 figures in {figs}")


if __name__ == "__main__":
    main()
