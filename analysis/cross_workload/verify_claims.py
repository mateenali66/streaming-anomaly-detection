"""Verify every numeric claim in the paper against its source artifact.

Each check states the claim, recomputes it from the source artifact, and reports PASS or
FAIL with both values.

Run:  python analysis/cross_workload/verify_claims.py
"""
import json
import re
import sys
import os
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
A = ROOT / "analysis" / "cross_workload"
# The paper's LaTeX source is not part of the data deposit. Without it, checks that look for a
# number in the text are reported as skipped; every recomputation from data still runs.
# Point PAPER_TEX at that source to run the text checks as well.
_TEX_PATH = Path(os.environ.get("PAPER_TEX", ROOT / "paper" / "main.tex"))
TEX = _TEX_PATH.read_text() if _TEX_PATH.exists() else None

RESULTS = []


def check(section, claim, got, expected, tol=1e-9, note=""):
    if expected is None or got is None:
        ok = False
    elif isinstance(expected, str):
        ok = str(got) == expected
    else:
        ok = abs(float(got) - float(expected)) <= tol
    RESULTS.append(dict(section=section, claim=claim, doc=got, source=expected,
                        ok=ok, note=note))


def in_tex(pattern, label, section, note=""):
    """Assert a literal string appears in the paper source."""
    if TEX is None:
        RESULTS.append(dict(section=section, claim=label, doc=None, source=pattern, ok=None, note="no paper source"))
        return
    RESULTS.append(dict(section=section, claim=label, doc="present" if pattern in TEX else "ABSENT",
                        source="present", ok=pattern in TEX, note=note))


# ---------------------------------------------------------------- data integrity
def data_integrity():
    rep = json.load(open(ROOT / "data" / "features_v3" / "selection_report_30s.json"))
    for k, v in (("initial_features", 6491), ("removed_all_nan", 97),
                 ("removed_low_variance", 3646), ("after_variance", 2748),
                 ("removed_correlated", 2273), ("final_features", 475)):
        check("data", f"selection report {k}", rep[k], v)
    # arithmetic must reconcile
    check("data", "6491 - 97 = 6394", rep["initial_features"] - rep["removed_all_nan"], 6394)
    check("data", "6394 - 3646 = after_variance",
          rep["initial_features"] - rep["removed_all_nan"] - rep["removed_low_variance"],
          rep["after_variance"])
    check("data", "after_variance - removed_correlated = final",
          rep["after_variance"] - rep["removed_correlated"], rep["final_features"])
    check("data", "category counts sum to 475", 40 + 111 + 90 + 234, 475)

    d = pd.read_parquet(ROOT / "data" / "features_v3" / "features_30s.parquet")
    meta = {"window_start", "window_end", "window_mid", "n_samples", "label", "fault_id",
            "fault_category", "run_number", "repetition", "run_source"}
    feat = [c for c in d.columns if c not in meta and not c.startswith("fold_")]
    check("data", "30s dataset rows", len(d), 10682)
    check("data", "30s dataset features", len(feat), 475)
    check("data", "8 fault types", d[d.fault_id != ""].fault_id.nunique(), 8)
    check("data", "10 repetitions", d[d.repetition > 0].repetition.nunique(), 10)
    # each fold's test split is exactly one repetition
    ok = all(sorted(d.loc[d[f"fold_{i}_split"] == "test", "repetition"].unique()) == [i]
             for i in range(1, 11))
    check("fold-design", "each fold tests exactly repetition i", ok, True)


# ---------------------------------------------------------------- feature selection
def feature_selection():
    ov = pd.read_csv(A / "feature_selection_overlap.csv")
    check("selection", "per-fold selection min", ov.n_selected_perfold.min(), 471)
    check("selection", "per-fold selection max", ov.n_selected_perfold.max(), 474)
    check("selection", "jaccard min", round(ov.jaccard.min(), 3), 0.957, 0.0006)
    check("selection", "jaccard max", round(ov.jaccard.max(), 3), 0.977, 0.0006)
    se = pd.read_csv(A / "feature_selection_sensitivity.csv")
    g = se[se.arm == "global_selection"].set_index(["fold", "model"])
    p = se[se.arm == "per_fold_selection"].set_index(["fold", "model"])
    dl = (p.auc - g.auc).groupby("model")
    for m, mean, sd in (("IF", 0.0080, 0.0117), ("OC_SVM", 0.0007, 0.0024),
                        ("RF", -0.0032, 0.0138)):
        check("selection", f"{m} delta AUC mean", round(dl.mean()[m], 4), mean, 0.0001)
        check("selection", f"{m} delta AUC sd", round(dl.std()[m], 4), sd, 0.0001)


# ---------------------------------------------------------------- label dependence
def label_dependence():
    """Label-free (IQR) thresholds against the label-calibrated F1, eight semi-supervised models."""
    u = pd.read_csv(ROOT / "data" / "results_v3" / "merged" / "model_results_unsupervised.csv")
    g = pd.read_csv(ROOT / "data" / "v3" / "results" / "ecom-gapfill" / "model_results_unsupervised.csv")
    g = g[g.threshold_type == "iqr"]
    semi = ["IF", "OC_SVM", "LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE", "DeepSVDD", "DAGMM"]
    uns = pd.concat([u, g])
    uns = uns[uns.model.isin(semi)]
    c = pd.read_csv(A / "panel_overall.csv")
    c = c[(c.workload == "ecommerce") & (c.window_s == 30) & (c.category == "all")]
    loss = c.groupby("model").f1_score.mean()[semi] - uns.groupby("model").f1_score.mean()[semi]
    for m, v in (("IF", 0.133), ("OC_SVM", 0.232), ("CNN_AE", 0.421), ("DeepSVDD", 0.443), ("DAGMM", 0.268)):
        check("labels", f"{m} label-free F1 loss", round(loss[m], 3), v, 0.0006)
    check("labels", "loss range low", round(loss.min(), 2), 0.13, 0.006)
    check("labels", "loss range high", round(loss.max(), 2), 0.44, 0.006)
    check("labels", "semi-supervised runs with F1 exactly 0", int((uns.f1_score == 0).sum()), 41)
    check("labels", "semi-supervised model-fold runs", len(uns), 80)


# ---------------------------------------------------------------- broker faults
def broker_faults():
    f = pd.read_csv(A / "full_lofo_summary.csv")
    for fault, f1lo, f1hi, alo, ahi in (("F1", 0.017, 0.075, 0.200, 0.491),
                                        ("F4", 0.094, 0.168, 0.748, 0.886),
                                        ("F7", 0.116, 0.302, 0.759, 0.985)):
        s = f[f.held_out_fault == fault]
        check("broker-faults", f"{fault} LOFO F1 min", round(s.f1_mean.min(), 3), f1lo, 0.0006)
        check("broker-faults", f"{fault} LOFO F1 max", round(s.f1_mean.max(), 3), f1hi, 0.0006)
        check("broker-faults", f"{fault} LOFO AUC min", round(s.auc_mean.min(), 3), alo, 0.0006)
        check("broker-faults", f"{fault} LOFO AUC max", round(s.auc_mean.max(), 3), ahi, 0.0006)
    s = f[f.held_out_fault == "F1"]
    check("broker-faults", "all F1-holdout AUC below 0.5", bool((s.auc_mean < 0.5).all()), True)


# ---------------------------------------------------------------- temporal context
def temporal_context():
    au = pd.read_csv(A / "sequence_contiguity_audit.csv")
    t = au[(au.workload == "ecommerce") & (au.window_seconds == 30) & (au.split == "test")]
    for L, frac in ((5, 0.126), (8, 0.224), (15, 0.462), (20, 0.642)):
        v = t[t.seq_length == L].frac_straddling.mean()
        check("temporal", f"test straddling frac L={L}", round(v, 3), frac, 0.0006)

    # cap saturation
    import csv
    # capped, contiguity-corrected HPO rerun (Optuna trial logs, one row per model-fold)
    sp = A / "capped_hpo"
    total = at_cap = 0
    for wl in ("ecommerce", "iot"):
        p = sp / wl / "optuna_results.csv"
        if not p.exists():
            continue
        rows = list(csv.reader(open(p)))
        h = rows[0]
        mi, si = h.index("model"), h.index("seq_length")
        for r in rows[1:]:
            if len(r) <= max(mi, si):
                continue
            try:
                v = int(float(r[si]))
            except ValueError:
                continue
            total += 1
            at_cap += (v == 11)
    check("temporal", "seq_length selections at cap", f"{at_cap}/{total}", "114/120")

    ab = pd.read_csv(A / "seq_boundary_ablation_raw.csv")
    for wl, exp in (("iot", 0.0002), ("ecommerce", 0.0026)):
        w = ab[ab.workload == wl]
        k = ["seq_length", "model", "fold", "seed"]
        b = w[w.arm == "B_dense_contig"].set_index(k).auc
        c = w[w.arm == "C_contig_contig"].set_index(k).auc
        i = b.index.intersection(c.index)
        if len(i) == 0:
            check("temporal", f"{wl} B->C train effect", None, exp)
            continue
        d = c.loc[i] - b.loc[i]
        check("temporal", f"{wl} B->C train effect", round(d.mean(), 4), exp, 0.0001)
        check("temporal", f"{wl} B->C n", len(d), 450)
        # prevalence / F1 / AUC by L in arm C
        cc = w[w.arm == "C_contig_contig"].groupby("seq_length").agg(
            auc=("auc", "mean"), f1=("f1", "mean"), p=("eval_prevalence", "mean"))
        for L in (5, 8, 10):
            af = 2 * cc.p[L] / (1 + cc.p[L])
            check("temporal", f"{wl} L={L} F1 exceeds always-fire",
                  bool(cc.f1[L] > af), True,
                  note=f"F1={cc.f1[L]:.3f} vs always-fire {af:.3f}")
        check("temporal", f"{wl} AUC falls with L",
              bool(cc.auc[5] > cc.auc[8] > cc.auc[10]), True)

    # matched comparison
    mm = pd.read_csv(A / "rq2_matched_per_fold.csv")
    from scipy.stats import wilcoxon
    pooled = []
    for wl, exp in (("ecommerce", -0.0283), ("iot", -0.0282)):
        w = mm[mm.workload == wl]
        pt = w[w.family == "point"].groupby("fold").auc.max()
        sq = w[w.family == "sequence"].groupby("fold").auc.max()
        i = pt.index.intersection(sq.index)
        d = (sq.loc[i] - pt.loc[i]).astype(float)
        check("temporal", f"{wl} matched delta", round(d.mean(), 4), exp, 0.0001)
        pooled.append(d.values)
    pool = np.concatenate(pooled)
    check("temporal", "pooled matched delta", round(pool.mean(), 4), -0.0283, 0.0001)
    check("temporal", "pooled SE", round(pool.std(ddof=1) / np.sqrt(len(pool)), 4), 0.0127, 0.0001)
    check("temporal", "pooled p", round(wilcoxon(pool)[1], 3), 0.036, 0.0006)
    check("temporal", "folds favouring sequence", f"{int((pool > 0).sum())}/{len(pool)}", "6/20")

    # F1 decomposition
    p = pd.read_csv(A / "panel_overall.csv")
    p = p[(p.workload == "ecommerce") & (p.window_s == 30) & (p.category == "all")]
    pt = p[p.model.isin(["IF", "OC_SVM"])]
    sq = p[p.model.isin(["LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE"])]
    prev_pt = pt.true_anomalies.mean() / pt.total_samples.mean()
    prev_sq = sq.true_anomalies.mean() / sq.total_samples.mean()
    af = lambda x: 2 * x / (1 + x)
    check("temporal", "point prevalence", round(prev_pt, 3), 0.319, 0.0006)
    check("temporal", "sequence prevalence", round(prev_sq, 3), 0.327, 0.0006)
    check("temporal", "point always-fire F1", round(af(prev_pt), 3), 0.484, 0.0006)
    check("temporal", "sequence always-fire F1", round(af(prev_sq), 3), 0.493, 0.0006)
    check("temporal", "sequence obs minus always-fire",
          round(sq.f1_score.mean() - af(prev_sq), 3), -0.002, 0.0011)
    check("temporal", "point obs minus always-fire",
          round(pt.f1_score.mean() - af(prev_pt), 3), -0.044, 0.0011)
    decomp = (af(prev_sq) - af(prev_pt)) + \
             ((sq.f1_score.mean() - af(prev_sq)) - (pt.f1_score.mean() - af(prev_pt)))
    check("temporal", "decomposition sums to observed contrast",
          round(decomp, 3), round(sq.f1_score.mean() - pt.f1_score.mean(), 3), 0.0011)


# ---------------------------------------------------------------- symptom alignment
def symptom_labels():
    bf = pd.read_csv(A / "symptom_onset_by_fault.csv")
    check("symptoms", "all 8 faults present", len(bf), 8)
    check("symptoms", "all faults 10/10 with symptom",
          bool((bf.n_with_symptom == bf.n_runs).all()), True)
    check("symptoms", "median lag 0 for all", bool((bf.median_lag_s == 0).all()), True)
    check("symptoms", "max lag over all faults", bf.max_lag_s.max(), 60.0)
    sens = pd.read_csv(A / "symptom_alignment_sensitivity.csv")
    pub = sens[sens.variant == "published"].groupby("model").auc.mean()
    worst = 0.0
    for v in ("symptom_relabel", "symptom_exclude"):
        alt = sens[sens.variant == v].groupby("model").auc.mean()
        worst = max(worst, (alt - pub).abs().max())
    check("symptoms", "max AUC shift under relabelling", round(worst, 4), 0.004, 0.0006,
          note=f"actual {worst:.4f}")


# ---------------------------------------------------------------- operational replay
def operational():
    s = pd.read_csv(A / "operational_replay_summary.csv").set_index("model")
    for m, duty, famin, firing in (("RuleBased", 1.000, 60.0, 80),
                                   ("EWMA", 0.809, 45.1, 59),
                                   ("IF", 0.964, 58.4, 80),
                                   ("RF", 0.538, 25.9, 29)):
        check("operational", f"{m} duty cycle", round(s.duty_cycle[m], 3), duty, 0.0006)
        check("operational", f"{m} false-alert min/hr", round(s.false_alert_min_per_hour[m], 1),
              famin, 0.06)
        check("operational", f"{m} already-firing", int(s.faults_already_firing[m]), firing)
    check("operational", "all models always-on (duty > 0.5)", bool((s.duty_cycle > 0.5).all()), True)
    # saturation
    tree = ROOT / "data" / "results_v3" / "merged" / "raw_scores"
    gap = ROOT / "data" / "v3" / "results" / "ecom-gapfill" / "raw_scores"
    for m, frac in (("CNN_AE", 0.238), ("Trans_AE", 0.233), ("LSTM_AE", 0.206),
                    ("DeepSVDD", 0.154), ("IF", 0.005), ("RF", 0.001)):
        src = tree if (tree / f"{m}_fold1_scores.npy").exists() else gap
        pool = np.concatenate([np.load(src / f"{m}_fold{f}_scores.npy")[
            np.load(src / f"{m}_fold{f}_labels.npy") == 0] for f in range(1, 11)])
        check("operational", f"{m} normal scores at max", round(float((pool == pool.max()).mean()), 3),
              frac, 0.0006)
    fp = pd.read_csv(A / "latency_fixed_fpr_v2_summary.csv")
    check("operational", "fixed-FPR table covers 10 models", fp.model.nunique(), 10)
    check("operational", "all models have 10 folds", bool((fp.n_folds == 10).all()), True)


# ---------------------------------------------------------------- EWMA in the main panel
def ewma_panel():
    s = pd.read_csv(A / "rq1_with_ewma_summary.csv")
    e = s[(s.workload == "ecommerce") & (s.model == "EWMA")].iloc[0]
    check("baseline", "EWMA e-com F1", round(e.f1_score_mean, 3), 0.527, 0.0006)
    check("baseline", "EWMA e-com AUC", round(e.auc_roc_mean, 3), 0.650, 0.0006)
    i = s[(s.workload == "iot") & (s.model == "EWMA")].iloc[0]
    check("baseline", "EWMA IoT F1", round(i.f1_score_mean, 3), 0.510, 0.0006)
    check("baseline", "EWMA IoT AUC", round(i.auc_roc_mean, 3), 0.611, 0.0006)
    # abstract claim: no deep model beats EWMA on either metric, either workload
    deep = ["LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE", "DeepSVDD", "DAGMM"]
    for wl, ew in (("ecommerce", e), ("iot", i)):
        d = s[(s.workload == wl) & (s.model.isin(deep))]
        check("baseline", f"{wl}: no deep model beats EWMA on F1",
              bool((d.f1_score_mean < ew.f1_score_mean).all()), True)
        check("baseline", f"{wl}: no deep model beats EWMA on AUC",
              bool((d.auc_roc_mean < ew.auc_roc_mean).all()), True)
    # IoT max-z edges EWMA on AUC
    mz = s[(s.workload == "iot") & (s.model == "RuleBased")].iloc[0]
    check("baseline", "IoT max-z AUC exceeds EWMA", bool(mz.auc_roc_mean > i.auc_roc_mean), True,
          note=f"{mz.auc_roc_mean:.4f} vs {i.auc_roc_mean:.4f}")
    con = pd.read_csv(A / "rq1_with_ewma_contrasts.csv")
    ec = con[(con.workload == "ecommerce") & (con.metric == "f1_score") & (con.model != "RF")]
    check("baseline", "e-com: all non-RF models sig worse than EWMA on F1",
          bool(ec.sig_holm_05.all()), True)


# ---------------------------------------------------------------- cross-workload transfer
def transfer():
    dist = pd.read_csv(A / "subexp2_scaler_distortion.csv").set_index("case")
    check("transfer", "ecom_to_iot max |z|", float(f"{dist.max_abs_z['ecom_to_iot']:.2g}"),
          2.1e6, 1e5)
    check("transfer", "top-8 share of squared z",
          round(dist.top8_share_of_squared_z["ecom_to_iot"] * 100, 3), 99.996, 0.002)
    txt = (A / "subexp2_transfer_stats.txt").read_text()
    for label, val in (("Scaler effect, ecom_to_iot", 0.0736),
                       ("Scaler effect, iot_to_ecom", 0.0340),
                       ("Transfer effect on target=ecom", -0.0040),
                       ("Transfer effect on target=iot", -0.0078)):
        m = re.search(re.escape(label) + r".*?effect \([^)]*\)\s*=\s*([+-][\d.]+)", txt, re.S)
        check("transfer", f"{label} effect", float(m.group(1)) if m else None, val, 0.0001)
    raw = pd.read_csv(A / "subexp2_cross_train_folds_raw.csv")
    raw = raw[raw.model != "DAGMM"]   # stats script excludes the non-converging model
    for case, exp in (("ecom_to_iot", 0.571), ("iot_to_ecom", 0.829)):
        v = raw[(raw.scaling == "source_fit") & (raw.case == case)].constant_scores.mean()
        check("transfer", f"{case} source-fit degenerate frac", round(v, 3), exp, 0.0011)
        v2 = raw[(raw.scaling == "target_fit") & (raw.case == case)].constant_scores.mean()
        check("transfer", f"{case} target-fit degenerate frac", round(v2, 3), 0.0, 1e-9)


# ---------------------------------------------------------------- EWMA mechanism, latency, corrections
def tex_has(section, claim, s):
    if TEX is None:
        RESULTS.append(dict(section=section, claim=claim, doc=None, source=s, ok=None, note="no paper source"))
        return
    RESULTS.append(dict(section=section, claim=claim, doc="present" if s in TEX else "ABSENT",
                        source=s, ok=s in TEX, note=""))


def tex_lacks(section, claim, s):
    if TEX is None:
        RESULTS.append(dict(section=section, claim=claim, doc=None, source=s, ok=None, note="no paper source"))
        return
    RESULTS.append(dict(section=section, claim=claim, doc="absent" if s not in TEX else "PRESENT",
                        source=f"no {s!r}", ok=s not in TEX, note=""))


def f3(x):
    return f"{x:.3f}"[1:]


def half_up(x, q):
    """Round half up, the convention the paper's tables use (0.125 -> 0.13)."""
    from decimal import Decimal, ROUND_HALF_UP
    return str(Decimal(str(round(float(x), 6))).quantize(Decimal(q), rounding=ROUND_HALF_UP))


def tables_and_text():
    from scipy.stats import spearmanr, wilcoxon
    S = "tables"
    # EWMA ablation table: every row recomputed from the per-fold file
    ab = pd.read_csv(A / "ewma_mechanism_ablation_per_fold.csv")
    m = ab.groupby(["variant", "workload"])[["auc", "f1", "modal_tie"]].mean()
    for label, v in (("Published EWMA", "published"), ("Reference: training mean", "centre_train_mean"),
                     ("Reference: running, per test run", "running_centre"),
                     ("Scale: training SD", "scale_global_sd"),
                     ("Reference mean, scale SD (max-z)", "static_z"),
                     ("Mean-squared pooling", "mean_sq_pool"),
                     ("\\quad with training-mean reference", "mean_sq_centre_train_mean"),
                     ("Autoencoder sigmoid on the score", "sigmoid_squash"),
                     ("Scale floored at 0.1 SD", "scale_floor"),
                     ("Three dominant features removed", "drop_dominant")):
        e, i = m.loc[(v, "ecommerce")], m.loc[(v, "iot")]
        tex_has(S, f"tab:ewma row {v}",
                f"{label} & {f3(e.auc)} & {f3(i.auc)} & {f3(e.f1)} & {f3(i.f1)} & "
                f"{round(100 * e.modal_tie)} & {round(100 * i.modal_tie)} \\\\")
    mz = pd.read_csv(A / "rq1_with_ewma_summary.csv").set_index(["workload", "model"]).auc_roc_mean
    for wl in ("ecommerce", "iot"):
        best_no_ref = max(m.loc[(v, wl)].auc for v in ("centre_train_mean", "mean_sq_centre_train_mean", "static_z", "running_centre"))
        best_v = max(("centre_train_mean", "mean_sq_centre_train_mean", "static_z", "running_centre"),
                     key=lambda v: m.loc[(v, wl)].auc)
        check(S, f"{wl}: best variant without the end-of-training reference is static max-z", best_v, "static_z",
              note=f"best {best_no_ref:.4f} vs RQ1 max-z {mz[(wl, 'RuleBased')]:.4f}")
    check(S, "IoT drop-dominant EWMA outranks max-z", bool(m.loc[("drop_dominant", "iot")].auc > mz[("iot", "RuleBased")]), True)
    pub = m.loc["published"]
    check(S, "published EWMA reproduces RQ1 e-com AUC", round(pub.loc["ecommerce"].auc, 3), 0.650, 0.0006)
    # saturation bound recomputed from the raw score files
    from sklearn.metrics import roc_auc_score
    trees = {"ecommerce": [ROOT / "data/results_v3/merged/raw_scores", ROOT / "data/v3/results/ecom-gapfill/raw_scores"],
             "iot": [ROOT / "data/v3/iot/results/gpu-1/raw_scores", ROOT / "data/v3/iot/results/gpu-2/raw_scores"]}
    deep = ["LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE", "DeepSVDD", "DAGMM"]
    bound = {}
    for wl, ts in trees.items():
        best = 0.0
        for mdl in deep:
            vals = []
            for f in range(1, 11):
                src = next((tr for tr in ts if (tr / f"{mdl}_fold{f}_scores.npy").exists()), None)
                s = np.load(src / f"{mdl}_fold{f}_scores.npy").astype(float)
                y = np.load(src / f"{mdl}_fold{f}_labels.npy").astype(int)
                tied = s == s.max()
                vals.append(roc_auc_score(y, s) + 0.5 * (tied & (y == 1)).sum() * (tied & (y == 0)).sum()
                            / (y.sum() * (1 - y).sum()))
            best = max(best, float(np.mean(vals)))
        bound[wl] = best
    tex_has(S, "saturation bound row", f"saturation removed$^{{\\ast}}$ & {f3(bound['ecommerce'])} & {f3(bound['iot'])}")
    tex_has(S, "saturation bound e-com in text", f"would be 0{f3(bound['ecommerce'])}, against 0.650")
    tex_has(S, "saturation bound IoT in text", f"the same bound reaches 0{f3(bound['iot'])} for Deep SVDD")
    # per-fault EWMA and the F7 arg-max category
    pf = pd.read_csv(A / "ewma_mechanism_per_fault_auc.csv")
    ew = pf[pf.model == "EWMA"].set_index(["workload", "fault_id"])["mean"]
    check(S, "EWMA identical AUC F1-F4 e-com", ew.loc["ecommerce"].loc[["F1", "F2", "F3", "F4"]].nunique(), 1)
    check(S, "EWMA identical AUC F2-F6 IoT", ew.loc["iot"].loc[["F2", "F3", "F4", "F5", "F6"]].nunique(), 1)
    check(S, "EWMA F1-F4 e-com value", round(ew.loc[("ecommerce", "F1")], 3), 0.713, 0.0006)
    check(S, "EWMA F2-F6 IoT value", round(ew.loc[("iot", "F2")], 3), 0.584, 0.0006)
    check(S, "EWMA F7 e-com", round(ew.loc[("ecommerce", "F7")], 2), 0.96, 0.006)
    check(S, "EWMA F7 IoT", round(ew.loc[("iot", "F7")], 2), 0.90, 0.006)
    f7 = pf[(pf.fault_id == "F7") & pf.model.isin(deep)]["mean"]
    check(S, "deep F7 range low", round(f7.min(), 2), 0.86, 0.006)
    check(S, "deep F7 range high", round(f7.max(), 2), 0.91, 0.006)
    ag = pd.read_csv(A / "ewma_mechanism_argmax_category.csv")
    c = ag[(ag.kind == "flagged_fault") & (ag.fault_id == "F7") & (ag.category == "consumer")].set_index("workload").share
    check(S, "F7 consumer arg-max share IoT", round(c["iot"] * 100), 76)
    check(S, "F7 consumer arg-max share e-com", round(c["ecommerce"] * 100), 80)
    # 675x scale ratio for the dominant e-commerce feature (fold 1 training normal)
    d = pd.read_parquet(ROOT / "data/features_v3/features_30s.parquet")
    feat = "jvm__jvm_buffer_pool_used_buffers__job_kafka_brokers__kafka_broker_streaming_kafka_brokers_1__pool_direct__max"
    x = d[(d.fold_1_split == "train") & (d.label == "NORMAL")][feat].fillna(0)
    check(S, "dominant feature global/EWMA scale ratio", round(x.std() / x.ewm(alpha=0.3, adjust=False).std().mean()), 676, 1)
    # max-z contrasts recomputed from the RQ1 panel
    panel = pd.read_csv(A / "rq1_panel_with_ewma.csv")
    def contrasts(wl, met):
        b = panel[(panel.workload == wl) & (panel.model == "RuleBased")].set_index("fold")[met]
        out = {}
        for mdl in deep:
            dd = (panel[(panel.workload == wl) & (panel.model == mdl)].set_index("fold")[met] - b).dropna()
            out[mdl] = (dd.mean(), wilcoxon(dd).pvalue)
        ps = np.array([v[1] for v in out.values()]); order = np.argsort(ps); holm = np.empty(len(ps)); run = 0
        for k, i in enumerate(order):
            run = max(run, min(1, (len(ps) - k) * ps[i])); holm[i] = run
        return {mdl: (out[mdl][0], holm[j]) for j, mdl in enumerate(out)}
    ea = contrasts("ecommerce", "auc_roc")
    check(S, "no deep model above max-z AUC e-com", all(v[0] <= 0 for v in ea.values()), True)
    check(S, "DAGMM vs max-z e-com", round(ea["DAGMM"][0], 3), -0.001, 0.0006)
    others = [-v[0] for k, v in ea.items() if k != "DAGMM"]
    check(S, "other five below max-z e-com low", round(min(others), 3), 0.030, 0.0006)
    check(S, "other five below max-z e-com high", round(max(others), 3), 0.039, 0.0006)
    check(S, "e-com deep sig below max-z after Holm", sum(v[1] < 0.05 for v in ea.values()), 1)
    ia = contrasts("iot", "auc_roc")
    check(S, "no deep model above max-z AUC IoT", all(v[0] <= 0 for v in ia.values()), True)
    aes = ["LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE"]
    check(S, "IoT AE gap low", round(min(-ia[a][0] for a in aes), 3), 0.044, 0.0006)
    check(S, "IoT AE gap high", round(max(-ia[a][0] for a in aes), 3), 0.072, 0.0006)
    check(S, "IoT all four AEs sig after Holm", all(ia[a][1] < 0.05 for a in aes), True)
    for wl in ("ecommerce", "iot"):
        f = contrasts(wl, "f1_score")
        check(S, f"{wl} no deep F1 sig vs max-z", any(v[1] < 0.05 for v in f.values()), False)
    # fault-rank transfer on per-fault AUC
    w = pf.pivot_table(index=["model", "fault_id"], columns="workload", values="mean").reset_index()
    rho = {mdl: spearmanr(g.ecommerce, g.iot) for mdl, g in w.groupby("model")}
    strong = {k: v for k, v in rho.items() if v[1] <= 0.037 + 1e-9}
    check(S, "detectors with significant rank transfer", len(strong), 9)
    check(S, "rank transfer rho low", round(min(v[0] for v in strong.values()), 2), 0.74, 0.006)
    check(S, "rank transfer rho high", round(max(v[0] for v in strong.values()), 2), 0.98, 0.006)
    check(S, "max-z AUC rank rho", round(rho["RuleBased"][0], 2), 0.62, 0.006)
    s3 = pd.read_csv(A / "subexp3_per_fault_spearman.csv").set_index("model").spearman_rho
    check(S, "max-z F1 rank rho (artifact)", round(s3["RuleBased"], 2), 0.96, 0.006)
    # inference timing column of tab:cost
    it = pd.read_csv(A / "inference_timing.csv").set_index("model").median_ms
    for label, mdl, fmt in (("Statistical (max-z)", "RuleBased", "{:.3f}"), ("RF (supervised)", "RF", "{:.1f}"),
                            ("IF", "IF", "{:.2f}"), ("OC-SVM", "OC_SVM", "{:.3f}"), ("LSTM-AE", "LSTM_AE", "{:.3f}"),
                            ("CNN-AE", "CNN_AE", "{:.3f}"), ("LSTM-VAE", "LSTM_VAE", "{:.3f}"),
                            ("Deep SVDD", "DeepSVDD", "{:.3f}"), ("Trans-AE", "Trans_AE", "{:.3f}"), ("DAGMM", "DAGMM", "{:.3f}")):
        if TEX is None:
            tex_has(S, f"tab:cost score {mdl}", fmt.format(it[mdl]))
            continue
        row = [l for l in TEX.splitlines() if l.startswith(label + " & ") and "GPU" in l or l.startswith(label + " & ") and "& CPU" in l]
        cell = row[0].split("&")[3].strip() if row else None
        check(S, f"tab:cost score {mdl}", cell, fmt.format(it[mdl]))
    # fixed-FPR table rows
    fp = pd.read_csv(A / "latency_fixed_fpr_v2_summary.csv")
    lab = {"IF": "IF", "OC_SVM": "OC-SVM", "LSTM_AE": "LSTM-AE", "Trans_AE": "Trans-AE", "CNN_AE": "CNN-AE",
           "LSTM_VAE": "LSTM-VAE", "DeepSVDD": "Deep SVDD", "DAGMM": "DAGMM", "RF": "RF (supervised)",
           "RuleBased": "Statistical max-z"}
    for mdl, l in lab.items():
        r = fp[fp.model == mdl].set_index("fpr_budget")
        cells = []
        for b in (0.01, 0.05):
            cells += [half_up(r.det_rate_mean[b], "0.01"), half_up(r.median_latency_s[b], "1"),
                      half_up(r.fpr_realised[b], "0.001")]
        tex_has(S, f"tab:fpr_latency row {mdl}", f"{l} & " + " & ".join(cells) + " \\\\")
    # replay table rows
    rp = pd.read_csv(A / "operational_replay_summary.csv").set_index("model")
    rlab = {"RuleBased": "Statistical max-z", "EWMA": "EWMA", "IF": "Isolation Forest", "DeepSVDD": "Deep SVDD",
            "CNN_AE": "CNN-AE", "LSTM_VAE": "LSTM-VAE", "LSTM_AE": "LSTM-AE", "Trans_AE": "Transformer-AE",
            "DAGMM": "DAGMM", "OC_SVM": "OC-SVM", "RF": "Random Forest"}
    for mdl, l in rlab.items():
        r = rp.loc[mdl]
        tex_has(S, f"tab:replay duty/false-alert {mdl}",
                f"{l} & {r.duty_cycle:.3f} & ")
        tex_has(S, f"tab:replay already-firing {mdl}", f"& {r.false_alert_min_per_hour:.1f} & {int(r.faults_already_firing)}/80 &")
    # recovery windows and three-state regimes on AUC-ROC (primary-run scores)
    tsa = pd.read_csv(A / "three_state_auc_summary.csv")
    tsa["delta"] = tsa.active - tsa.binary
    check(S, "active above binary AUC, detector-workload pairs", int((tsa.delta > 0).sum()), 21)
    check(S, "of 22 pairs", len(tsa), 22)
    check(S, "significant and positive", int(((tsa.delta > 0) & (tsa.p_active_vs_binary < 0.05)).sum()), 18)
    check(S, "largest rise", round(tsa.delta.max(), 3), 0.064, 0.0006)
    ifr = tsa[(tsa.workload == "ecommerce") & (tsa.model == "IF")].iloc[0]
    check(S, "IF e-com falls", round(ifr.delta, 3), -0.014, 0.0006)
    aes = tsa[tsa.model.isin(["LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE"])]
    check(S, "AE recovery AUC within 0.02 of active", bool(((aes.recovery - aes.active).abs() < 0.02).all()), True)
    rf = tsa[(tsa.workload == "ecommerce") & (tsa.model == "RF")].iloc[0]
    check(S, "RF e-com recovery AUC", round(rf.recovery, 2), 0.55, 0.006)
    check(S, "RF e-com active AUC", round(rf.active, 2), 0.75, 0.006)
    cr = pd.read_csv(A / "calibrated_regimes_per_fold.csv")
    pv = cr[cr.model == "IF"].groupby(["workload", "regime"]).prevalence.mean()
    for wl in ("ecommerce", "iot"):
        check(S, f"{wl} binary prevalence, full test fold", round(pv[(wl, "binary")], 2), 0.32, 0.006)
        check(S, f"{wl} active prevalence, full test fold", half_up(pv[(wl, "active")], "0.01"),
              {"ecommerce": "0.47", "iot": "0.48"}[wl])
        check(S, f"{wl} hybrid prevalence, full test fold", round(pv[(wl, "hybrid")], 2), 0.65, 0.006)
    r1 = pd.read_csv(A / "rq1_with_ewma_summary.csv").set_index(["workload", "model"]).auc_roc_mean
    r1f = pd.read_csv(A / "rq1_with_ewma_summary.csv").set_index(["workload", "model"]).f1_score_mean
    bm = tsa.set_index(["workload", "model"]).binary
    check(S, "binary-regime AUC reproduces the RQ1 tables",
          bool(all(abs(bm[k] - r1[k]) < 0.0006 for k in bm.index if k in r1.index)), True)
    lab3 = {"IF": "IF", "OC_SVM": "OC-SVM", "LSTM_AE": "LSTM-AE", "Trans_AE": "Trans-AE", "CNN_AE": "CNN-AE",
            "LSTM_VAE": "LSTM-VAE", "DeepSVDD": "Deep SVDD", "DAGMM": "DAGMM", "RF": "RF (supervised)",
            "RuleBased": "Max-z", "EWMA": "EWMA"}
    cs = pd.read_csv(A / "calibrated_regimes_summary.csv").set_index(["workload", "model"])
    t3 = tsa.set_index(["workload", "model"])
    regs = ("active", "recovery", "hybrid", "binary")
    check(S, "calibrated-threshold AUC equals three_state_auc AUC",
          bool(all(abs(cs.loc[k, f"auc_{g}"] - t3.loc[k, g]) < 1e-6 for k in t3.index for g in regs)), True)
    check(S, "calibrated thresholds reproduce the RQ1 F1",
          bool(all(abs(cs.loc[k, "f1_binary"] - r1f[k]) < 0.0006 for k in cs.index if k in r1f.index)), True)
    for k, lab in lab3.items():
        cells = [f"{half_up(cs.loc[(wl, k), 'f1_' + g], '0.001')[1:]} ({half_up(cs.loc[(wl, k), 'auc_' + g], '0.01')[1:]})"
                 for wl in ("ecommerce", "iot") for g in regs]
        tex_has(S, f"tab:three_state row {k}", f"{lab} & " + " & ".join(cells) + " \\\\")
        cells = []
        for wl in ("ecommerce", "iot"):
            x = cs.loc[(wl, k)]
            cells += [half_up(x.f1_binary, "0.001")[1:], half_up(x.f1_active, "0.001")[1:],
                      "+" + half_up(x.f1_active_minus_binary, "0.001")[1:]]
        tex_has(S, f"tab:cooldown row {k}", f"{lab} & " + " & ".join(cells) + " \\\\")
    # recovery-window exclusion at the calibrated threshold (text of the recovery subsection)
    gain = cs.f1_active_minus_binary
    check(S, "F1 gain without recovery, low", round(gain.min(), 2), 0.11, 0.006)
    check(S, "F1 gain without recovery, high", round(gain.max(), 2), 0.17, 0.006)
    check(S, "every pair at the minimum Wilcoxon p", int((cs.p_f1_active_vs_binary < 0.0021).sum()), len(cs))
    af = cr[cr.model == "IF"].groupby(["workload", "regime"]).always_fire_f1.mean()
    for wl in ("ecommerce", "iot"):
        check(S, f"{wl} always-fire gain", round(af[(wl, "active")] - af[(wl, "binary")], 3), 0.160, 0.0006)
    above = sorted(f"{w}/{m}" for (w, m), v in cs.f1_over_always_fire_active.items() if v > 1e-9)
    check(S, "above always-fire without recovery", str(above),
          str(["ecommerce/EWMA", "ecommerce/RF", "iot/DAGMM", "iot/EWMA", "iot/RF"]))
    # three-state text
    check(S, "always-fire F1 binary e-com", round(af[("ecommerce", "binary")], 3), 0.484, 0.0006)
    check(S, "always-fire F1 hybrid e-com", round(af[("ecommerce", "hybrid")], 3), 0.785, 0.0006)
    check(S, "F1 hybrid above binary for every model", bool((cs.f1_hybrid > cs.f1_binary).all()), True)
    check(S, "no model above always-fire, recovery and hybrid",
          bool((cs[["f1_over_always_fire_recovery", "f1_over_always_fire_hybrid"]] <= 1e-9).all().all()), True)
    ae = cs[cs.index.get_level_values("model").isin(["LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE"])]
    check(S, "AE recovery AUC range", f"{ae.auc_recovery.min():.2f}-{ae.auc_recovery.max():.2f}", "0.56-0.62")
    check(S, "AE active AUC range", f"{ae.auc_active.min():.2f}-{ae.auc_active.max():.2f}", "0.56-0.63")
    check(S, "RF recovery AUC both workloads",
          f"{cs.loc[('ecommerce', 'RF')].auc_recovery:.2f},{cs.loc[('iot', 'RF')].auc_recovery:.2f}", "0.55,0.56")
    # threshold-only leave-one-fault-out
    ls = pd.read_csv(A / "fault_holdout_threshold_summary.csv")
    lf = pd.read_csv(A / "fault_holdout_threshold_per_fold.csv")
    llab = {k: v for k, v in lab3.items() if k != "EWMA"}
    for wl in ("ecommerce", "iot"):
        g = ls[ls.workload == wl].set_index(["model", "fault"])
        for k, lab in llab.items():
            cells = [f"{half_up(g.loc[(k, f'F{i}')].f1, '0.001')[1:]} ({half_up(g.loc[(k, f'F{i}')].auc, '0.01')[1:]})"
                     for i in range(1, 9)]
            tex_has(S, f"tab:threshold_lofo {wl} row {k}", f"{lab} & " + " & ".join(cells) + " \\\\")
    eff = (lf.f1 - lf.f1_all_faults).groupby([lf.workload, lf.model]).mean()
    check(S, "hold-out effect per model, low", round(eff.min(), 3), -0.037, 0.0006)
    check(S, "hold-out effect per model, high", round(eff.max(), 3), 0.001, 0.0006)
    check(S, "held-out F1 low", round(ls.f1.min(), 3), 0.004, 0.0006)
    check(S, "held-out F1 high", round(ls.f1.max(), 3), 0.239, 0.0006)
    pr1 = lf[lf.model == "IF"].assign(p=lambda x: x.n_pos / x.n).groupby(["workload", "fault"]).p.mean()
    check(S, "single-fault prevalence low", round(100 * pr1.min()), 4)
    check(S, "single-fault prevalence high", round(100 * pr1.max()), 8)
    over = (lf.f1 - lf.always_fire_f1).groupby([lf.workload, lf.model, lf.fault]).mean().reset_index(name="d")
    semi_no_oc = ["IF", "LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE", "DeepSVDD", "DAGMM"]
    check(S, "semi (not OC-SVM) at most 0.025 above always-fire",
          bool(over[over.model.isin(semi_no_oc)].d.max() <= 0.025), True)
    check(S, "OC-SVM more than 0.025 above always-fire somewhere",
          bool(over[over.model == "OC_SVM"].d.max() > 0.025), True)
    dif = ls[ls.model.isin(deep + ["IF"])]
    brk = dif[dif.fault.isin(["F1", "F2", "F3"])].auc
    good = dif[dif.fault.isin(["F4", "F7", "F8"])].auc
    check(S, "deep and IF broker AUC range", f"{brk.min():.2f}-{brk.max():.2f}", "0.25-0.47")
    check(S, "deep and IF F4/F7/F8 AUC range", f"{good.min():.2f}-{good.max():.2f}", "0.68-0.98")
    rfa = ls[ls.model == "RF"]
    check(S, "RF below chance only on F5", str(sorted(set(rfa[rfa.auc < 0.5].fault))), "['F5']")
    pool = pd.read_csv(A / "fault_holdout_pooling_check.csv")
    pool_err = (pool.f1_pooled - pool.f1_published).abs().groupby(pool.workload).mean()
    check(S, "pooled search reproduces published F1 within 0.03 on average", bool((pool_err < 0.03).all()), True,
          note=str(pool_err.round(4).to_dict()))
    oc = ls[(ls.workload == "ecommerce") & (ls.model == "OC_SVM") & (ls.fault == "F1")].auc.iloc[0]
    check(S, "OC-SVM broker crash, threshold only", round(oc, 2), 0.64, 0.006)
    fl = pd.read_csv(A / "full_lofo_summary.csv")
    check(S, "OC-SVM broker crash, full retraining",
          round(fl[(fl.held_out_fault == "F1") & (fl.model == "OC_SVM")].auc_mean.iloc[0], 2), 0.49, 0.006)
    # detection latency table (primary run plus the Deep SVDD and DAGMM run)
    lat = pd.concat([pd.read_csv(ROOT / "data/results_v3/merged/detection_latency_results.csv"),
                     pd.read_csv(ROOT / "data/v3/results/ecom-gapfill/detection_latency_results.csv")])
    lat = lat[lat.category == "all"]
    llab = {"IF": "IF", "OC_SVM": "OC-SVM", "LSTM_AE": "LSTM-AE", "Trans_AE": "Trans-AE", "CNN_AE": "CNN-AE",
            "LSTM_VAE": "LSTM-VAE", "DeepSVDD": "Deep SVDD", "DAGMM": "DAGMM", "RF": "RF (supervised)",
            "RuleBased": "Statistical (max-z)"}
    num = lambda v: f"{round(float(v), 6):.0f}" if round(float(v), 6).is_integer() else f"{float(v):.1f}"
    p95d, p95c = {}, {}
    for k, lab in llab.items():
        x = lat[lat.model == k]
        det = x.detected.astype(bool)
        sec = x.latency_seconds.astype(float)
        cens = sec    # undetected episodes carry the length of their labelled span
        p95d[k], p95c[k] = float(np.percentile(sec[det], 95)), float(np.percentile(cens, 95))
        cells = [half_up(100 * det.mean(), "0.1") + "\\%", num(np.median(sec[det])), num(p95d[k]),
                 num(np.median(cens)), num(p95c[k]), f"{int(det.sum())}/{len(x)}"]
        tex_has(S, f"tab:latency row {k}", f"  {lab} & " + " & ".join(cells) + " \\\\")
    check(S, "every model scored on 80 episodes", str(sorted(set(lat.groupby("model").size()))), "[80]")
    aes4l = ["LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE"]
    check(S, "detected-only P95 zero for IF, AEs, max-z",
          str(sorted(k for k, v in p95d.items() if v == 0)), str(sorted(["IF", "RuleBased"] + aes4l)))
    check(S, "detected-only P95 max is DAGMM at 132 s", f"{max(p95d, key=p95d.get)} {max(p95d.values()):.0f}", "DAGMM 132")
    check(S, "censored P95 300 s", str(sorted(k for k, v in p95c.items() if abs(v - 300) < 1e-6)),
          str(["OC_SVM", "RF"]))
    check(S, "DAGMM censored P95", num(p95c["DAGMM"]), "210")
    check(S, "AE censored P95 range", f"{num(min(p95c[a] for a in aes4l))}-{num(max(p95c[a] for a in aes4l))}", "60-184.5")
    pm = pd.concat([pd.read_csv(ROOT / "data/results_v3/merged/model_results.csv"),
                    pd.read_csv(ROOT / "data/v3/results/ecom-gapfill/model_results.csv")])
    prec = pm[pm.category == "all"].groupby("model").precision.mean()
    sel = prec[["IF", "DeepSVDD"] + aes4l]
    check(S, "precision of IF, Deep SVDD and AEs", f"{sel.min():.2f}-{sel.max():.2f}", "0.31-0.35")
    # POT row of the classical-baselines table (monotone score)
    cb = pd.read_csv(A / "classical_baselines_per_fold.csv")
    pot = cb[cb.model == "POT"].groupby("workload")[["f1", "auc_roc"]].mean()
    cells = [half_up(pot.loc[wl, m], "0.001")[1:] for wl in ("ecommerce", "iot") for m in ("f1", "auc_roc")]
    tex_has(S, "tab:classical row POT", "POT (GPD)               & " + " & ".join(cells) + " \\\\")
    # RQ3 fusion, weights and thresholds fitted on the other nine folds
    fz = pd.read_csv(A / "rq3_fusion_corrected_summary.csv").set_index("strategy")
    for k, lab in (("weighted_loo", "Weighted"), ("majority_vote", "Majority vote"), ("max", "Max score"),
                   ("mean", "Mean score")):
        r = fz.loc[k]
        b = (lambda v: f"\\textbf{{{v}}}") if k == "weighted_loo" else (lambda v: v)
        tex_has(S, f"tab:fusion row {k}", f"{lab} & ")
        tex_has(S, f"tab:fusion row {k} F1", f"{b(half_up(r.f1, '0.001')[1:])}$\\pm${half_up(r.f1_se, '0.001')[1:]}")
        tex_has(S, f"tab:fusion row {k} AUC", f"{b(half_up(r.auc, '0.001')[1:])}$\\pm${half_up(r.auc_se, '0.001')[1:]}")
    check(S, "fusion F1 within 0.002 of always-fire", bool((fz.f1_over_always_fire.abs() <= 0.002).all()), True)
    check(S, "weighted fusion AUC", round(fz.auc["weighted_loo"], 3), 0.660, 0.0006)
    check(S, "majority vote AUC", round(fz.auc["majority_vote"], 3), 0.499, 0.0006)
    ifr = pd.read_csv(ROOT / "data/results_v3/merged/model_results.csv")
    ifr = ifr[(ifr.model == "IF") & (ifr.category == "all")]
    afi = cr[(cr.model == "IF") & (cr.workload == "ecommerce") & (cr.regime == "binary")].always_fire_f1.mean()
    dif = ifr.f1_score.mean() - afi
    tex_has(S, "tab:fusion IF single F1 minus always-fire",
            f"& $-$" + half_up(abs(dif), "0.001")[1:] + " \\\\" if dif < 0 else "& $+$" + half_up(dif, "0.001")[1:] + " \\\\")
    tex_has(S, "tab:fusion row IF single",
            f"IF (single) & All features & {half_up(ifr.f1_score.mean(), '0.001')[1:]}$\\pm${half_up(ifr.f1_score.std(ddof=1) / np.sqrt(len(ifr)), '0.001')[1:]} & "
            f"{half_up(ifr.auc_roc.mean(), '0.001')[1:]}$\\pm${half_up(ifr.auc_roc.std(ddof=1) / np.sqrt(len(ifr)), '0.001')[1:]}")
    # broker injections that did not take effect (injection_check.py)
    rs = pd.read_csv(A / "injection_check_counter_resets.csv")
    check(S, "broker counter resets in either campaign", int(rs.counter_resets.sum()), 0)
    ir = pd.read_csv(A / "injection_check_runs.csv")
    f12 = ir[ir.fault.isin(["F1", "F2"])]
    check(S, "F1/F2 runs checked", len(f12), 40)
    check(S, "F1/F2 runs with ISR shrink or under-replication",
          int(((f12.isr_shrinks > 0) | (f12.max_under_replicated > 0)).sum()), 0)
    check(S, "runs of any fault with under-replication", int((ir.max_under_replicated > 0).sum()), 0)
    rc = [f"msg_rate_ratio_b{b}" for b in range(3)]
    f2 = ir[ir.fault == "F2"].groupby("workload")[rc].mean()
    check(S, "F2 per-broker message rate within 2% of pre-fault", bool(((f2 - 1).abs() < 0.02).all().all()), True)
    f57 = ir[ir.fault.isin(["F5", "F7"])].groupby(["workload", "fault"])[rc].mean()
    check(S, "F5/F7 message rate range", f"{round(100 * f57.min().min())}-{round(100 * f57.max().max())}", "49-69")
    bs = pd.read_csv(A / "broker_fault_sensitivity_summary.csv").set_index(["workload", "model"])
    for k, lab in lab3.items():
        cells = []
        for wl in ("ecommerce", "iot"):
            x = bs.loc[(wl, k)]
            cells += [half_up(x[c], "0.001")[1:] for c in ("auc_all", "auc_without_F1_F2", "auc_without_F1_F3")]
        tex_has(S, f"tab:broker_sensitivity row {k}", f"{lab} & " + " & ".join(cells) + " \\\\")
    check(S, "sensitivity 'all' reproduces RQ1 AUC",
          bool(all(abs(bs.loc[k, "auc_all"] - r1[k]) < 0.0006 for k in bs.index if k in r1.index)), True)
    dlt = bs.auc_without_F1_F2 - bs.auc_all
    grp = dlt[dlt.index.get_level_values("model").isin(deep + ["IF", "RuleBased"])]
    check(S, "AUC rise without F1/F2, deep+IF+max-z", f"{grp.min():.3f}-{grp.max():.3f}", "0.035-0.064")
    o = dlt[dlt.index.get_level_values("model").isin(["OC_SVM", "RF"])]
    check(S, "AUC rise without F1/F2, OC-SVM and RF", f"{o.min():.3f}-{o.max():.3f}", "0.002-0.025")
    check(S, "EWMA AUC falls without F1/F2", bool((dlt.xs("EWMA", level="model") < 0).all()), True)
    w12 = bs.auc_without_F1_F2
    check(S, "DAGMM vs max-z e-com without F1/F2",
          f"{w12[('ecommerce', 'DAGMM')]:.3f} {w12[('ecommerce', 'RuleBased')]:.3f}", "0.635 0.634")
    others = [w12[(wl, m)] < w12[(wl, "RuleBased")] for wl in ("ecommerce", "iot") for m in deep
              if not (wl == "ecommerce" and m == "DAGMM")]
    check(S, "other deep models below max-z without F1/F2", all(others), True)
    check(S, "EWMA vs max-z without F1/F2",
          " ".join(f"{w12[(wl, m)]:.3f}" for m in ("EWMA", "RuleBased") for wl in ("ecommerce", "iot")),
          "0.632 0.603 0.634 0.661")
    w13 = bs.auc_without_F1_F3
    check(S, "Deep SVDD vs max-z IoT without F1-F3",
          f"{w13[('iot', 'DeepSVDD')]:.3f} {w13[('iot', 'RuleBased')]:.3f}", "0.746 0.729")
    vn = pd.read_csv(A / "broker_fault_vs_normal_auc.csv")
    vn12 = vn[vn.fault.isin(["F1", "F2"])]
    aev = vn12[vn12.model.isin(["LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE"])].auc
    check(S, "AE F1/F2 AUC against normal windows", f"{aev.min():.2f}-{aev.max():.2f}", "0.28-0.43")
    oth = vn12[vn12.model.isin(["RF", "OC_SVM", "EWMA"])].auc
    check(S, "RF, OC-SVM, EWMA F1/F2 AUC against normal windows", f"{oth.min():.2f}-{oth.max():.2f}", "0.57-0.85")
    # six-fault rank transfer
    w6 = w[~w.fault_id.isin(["F1", "F2"])]
    r6 = {mdl: spearmanr(g.ecommerce, g.iot) for mdl, g in w6.groupby("model")}
    s6 = {k: v for k, v in r6.items() if v[1] < 0.05}
    check(S, "six-fault rank transfer, significant detectors", len(s6), 8)
    check(S, "six-fault rho range", f"{min(v[0] for v in s6.values()):.2f}-{max(v[0] for v in s6.values()):.2f}", "0.83-0.94")
    check(S, "six-fault max p", round(max(v[1] for v in s6.values()), 3), 0.042, 0.0006)
    # Holm over the 22 active-vs-binary AUC pairs
    pv22 = tsa.p_active_vs_binary.to_numpy(); order = np.argsort(pv22); hb = np.empty(len(pv22)); run = 0
    for k_, i_ in enumerate(order):
        run = max(run, min(1, (len(pv22) - k_) * pv22[i_])); hb[i_] = run
    check(S, "active above binary AUC, significant after Holm", int(((tsa.delta > 0) & (hb < 0.05)).sum()), 8)
    # pooled-threshold gap per model, and OC-SVM on folds 1-5
    gap = (pool.f1_pooled - pool.f1_published).abs().groupby([pool.workload, pool.model]).mean()
    check(S, "pooled gap under 0.06 except OC-SVM e-com",
          bool((gap.drop(("ecommerce", "OC_SVM")) < 0.06).all()), True)
    check(S, "pooled gap OC-SVM e-com", round(gap[("ecommerce", "OC_SVM")], 2), 0.10, 0.006)
    ocf = lf[(lf.workload == "ecommerce") & (lf.model == "OC_SVM") & (lf.fault == "F1") & (lf.fold <= 5)].auc.mean()
    check(S, "OC-SVM F1 hold-out, threshold only, folds 1-5", round(ocf, 2), 0.62, 0.006)
    # always-fire F1 of sequence models' own windows
    seqm = ["LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE", "DeepSVDD", "DAGMM"]
    afm = cr[cr.model.isin(seqm)].groupby(["workload", "model", "regime"]).always_fire_f1.mean()
    act = afm.xs("active", level="regime")
    check(S, "sequence always-fire F1 without recovery", f"{act.min():.2f}-{act.max():.2f}", "0.66-0.67")
    full = cr[cr.model == "IF"].groupby(["workload", "regime"]).always_fire_f1.mean()
    diff = max(afm[(wl, m, g)] - full[(wl, g)] for (wl, m, g) in afm.index)
    check(S, "sequence always-fire F1 at most 0.03 above max-z row", bool(diff <= 0.03), True)
    rp2 = pd.read_csv(A / "operational_replay_summary.csv").set_index("model")
    check(S, "Deep SVDD duty cycle", round(100 * rp2.duty_cycle["DeepSVDD"], 1), 89.6, 0.06)
    # Deep SVDD and DAGMM across the e-commerce window grid (separate gap-fill runs)
    for mdl, exp in (("DeepSVDD", ("0.492", "0.482", "0.471", "0.441")), ("DAGMM", ("0.490", "0.472", "0.474", "0.416"))):
        got = []
        for w in ("15s", "30s", "60s", "120s"):
            src = (ROOT / "data/v3/results/ecom-gapfill/model_results.csv" if w == "30s"
                   else ROOT / f"data/v3/results/ecom-gapfill-mw/{w}/model_results.csv")
            mr = pd.read_csv(src)
            got.append(half_up(mr[(mr.model == mdl) & (mr.category == "all")].f1_score.mean(), "0.001"))
        check(S, f"{mdl} e-com F1 at 15/30/60/120 s", ",".join(got), ",".join(exp))
        check(S, f"{mdl} peaks at 15 s", got.index(max(got)), 0)
    # prevalence table, every model row and column
    ps = pd.read_csv(A / "prevalence_sweep.csv")
    ps = ps[ps.workload == "ecommerce"].set_index("model")
    for lab, k in (("DAGMM", "DAGMM"), ("OC-SVM", "OC_SVM"), ("Trans-AE", "Trans_AE"), ("LSTM-VAE", "LSTM_VAE"),
                   ("CNN-AE", "CNN_AE"), ("LSTM-AE", "LSTM_AE"), ("Deep SVDD", "DeepSVDD"), ("IF", "IF")):
        cells = [half_up(ps.loc[k, c], "0.001") for c in ("tpr", "fpr", "f1_p010", "f1_p050", "f1_p079", "f1_p319")]
        tex_has(S, f"tab:prevalence row {k}", f"{lab} & " + " & ".join(cells) + " &")
    semi = ["IF", "OC_SVM", "LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE", "DeepSVDD", "DAGMM"]
    aes4 = ["LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE"]
    check(S, "semi F1 at 1% low", half_up(ps.loc[semi].f1_p010.min(), "0.001"), "0.019")
    check(S, "semi F1 at 1% high", half_up(ps.loc[semi].f1_p010.max(), "0.001"), "0.024")
    check(S, "AE F1 at 1% high", half_up(ps.loc[aes4].f1_p010.max(), "0.001"), "0.022")
    check(S, "AE F1 at 5% range", f"{half_up(ps.loc[aes4].f1_p050.min(), '0.001')}-{half_up(ps.loc[aes4].f1_p050.max(), '0.001')}", "0.099-0.102")
    check(S, "AE F1 at 7.9% range", f"{half_up(ps.loc[aes4].f1_p079.min(), '0.001')}-{half_up(ps.loc[aes4].f1_p079.max(), '0.001')}", "0.151-0.156")
    check(S, "RF F1 at 1%", half_up(ps.loc["RF"].f1_p010, "0.001"), "0.034")
    bal = (ps.tpr - ps.fpr)
    check(S, "DAGMM balance", round(bal["DAGMM"], 2), 0.12, 0.006)
    check(S, "OC-SVM balance", round(bal["OC_SVM"], 2), 0.10, 0.006)
    check(S, "DAGMM best balance among semi", bal[semi].idxmax(), "DAGMM")
    # bucket AUC means
    b = {"A": ["F4", "F7", "F8"], "B": ["F1", "F2", "F3"], "C": ["F5", "F6"]}
    for wl, exp in (("ecommerce", (0.83, 0.32, 0.47)), ("iot", (0.84, 0.31, 0.47))):
        x = pf[(pf.workload == wl) & pf.model.isin(aes4)]
        got = tuple(float(round(x[x.fault_id.isin(b[k])]["mean"].mean(), 2)) for k in "ABC")
        check(S, f"{wl} AE bucket AUC A/B/C", str(got), str(exp))
    for mdl, exp in (("DeepSVDD", (0.30, 0.37)), ("DAGMM", (0.34, 0.39))):
        got = tuple(float(round(pf[(pf.workload == wl) & (pf.model == mdl) & pf.fault_id.isin(b["B"])]["mean"].mean(), 2))
                    for wl in ("ecommerce", "iot"))
        check(S, f"{mdl} bucket B AUC", str(got), str(exp))
    ifa = pf[(pf.workload == "ecommerce") & (pf.model == "IF")].set_index("fault_id")["mean"]
    check(S, "IF per-fault AUC F5", round(ifa["F5"], 2), 0.13, 0.006)
    check(S, "IF per-fault AUC F7", round(ifa["F7"], 2), 0.98, 0.006)
    # window size
    po = pd.read_csv(A / "panel_overall.csv")
    iot = po[(po.workload == "iot") & (po.category == "all")].groupby(["model", "window_s"]).f1_score.mean().unstack()
    check(S, "IoT models higher at 300 s than 30 s", int((iot[300] > iot[30]).sum()), 9)
    check(S, "the exception is RF", str(list(iot.index[iot[300] <= iot[30]])), "['RF']")
    w4 = pd.read_csv(A / "subexp4_window_transfer_spearman.csv").set_index("model")
    check(S, "OC-SVM window-transfer rho", round(w4.spearman_rho["OC_SVM"], 2), 0.68, 0.006)
    check(S, "OC-SVM window-transfer p", round(w4.p_value["OC_SVM"], 2), 0.07, 0.006)
    # Friedman p implies eight models (df = 7)
    from scipy.stats import chi2
    check(S, "Friedman chi2 23.20 at df=7", round(chi2.sf(23.20, 7), 4), 0.0016, 0.00006)
    # PR-AUC order among semi-supervised models, e-commerce
    pr = po[(po.workload == "ecommerce") & (po.window_s == 30) & (po.category == "all") & po.model.isin(semi)]
    pr = pr.groupby("model").pr_auc.mean().sort_values(ascending=False)
    check(S, "PR-AUC order top three", str(list(pr.index[:3])), "['OC_SVM', 'IF', 'DAGMM']")
    # Holm over 45 pairs cannot reject with ten folds
    check(S, "45 x minimum Wilcoxon p exceeds 0.05", 45 * 2 / 1024 > 0.05, True)
    # full leave-one-fault-out test prevalence and always-fire F1
    t1 = d[d["fold_1_split"] == "test"]
    pf1 = 10 / ((t1.label == "NORMAL").sum() + (t1.label == "RECOVERY").sum() + 10)
    check(S, "LOFO test prevalence about 0.06", round(pf1, 2), 0.06, 0.006)
    check(S, "LOFO always-fire F1 about 0.11", round(2 * pf1 / (1 + pf1), 2), 0.11, 0.006)
    # RQ2 matched per-workload statistics
    mm = pd.read_csv(A / "rq2_matched_per_fold.csv")
    for wl, se, pv in (("ecommerce", 0.0200, 0.193), ("iot", 0.0168, 0.105)):
        ww = mm[mm.workload == wl]
        dd = (ww[ww.family == "sequence"].groupby("fold").auc.max() - ww[ww.family == "point"].groupby("fold").auc.max()).dropna()
        check(S, f"{wl} matched SE", round(dd.std(ddof=1) / np.sqrt(len(dd)), 4), se, 0.0001)
        check(S, f"{wl} matched p", round(wilcoxon(dd).pvalue, 3), pv, 0.0006)
        word = "Wilcoxon " if wl == "ecommerce" else ""
        name = "e-commerce" if wl == "ecommerce" else "IoT"
        tex_has(S, f"{wl} matched p in text",
                f"(SE {se:.4f}, {word}$p = {round(wilcoxon(dd).pvalue, 3):.3f}$) on {name}")
    # LSTM-VAE saturation share
    tree = ROOT / "data" / "results_v3" / "merged" / "raw_scores"
    pool = np.concatenate([np.load(tree / f"LSTM_VAE_fold{f}_scores.npy")[np.load(tree / f"LSTM_VAE_fold{f}_labels.npy") == 0]
                           for f in range(1, 11)])
    check(S, "LSTM_VAE normal scores at max", round(float((pool == pool.max()).mean()), 3), 0.219, 0.0006)
    tex_has(S, "LSTM-VAE share in text", "21.9\\% of LSTM-VAE")
    # the paper states how many numbers the harness recomputes from data alone
    n_numeric = sum(1 for r in RESULTS if r["note"] != "no paper source" and not str(r["claim"]).startswith(
        ("tab:", "LSTM-VAE share in text", "saturation bound", "ecommerce matched p in text",
         "iot matched p in text", "cited keys", "bibitems")))
    check(S, "numeric checks run from data alone equal the 267 stated in the text", n_numeric, 267)
    # citations: every key cited has an entry and every entry is cited
    if TEX is None:
        return
    import re as _re
    cited = set()
    for grp in _re.findall(r"\\cite[pt]?\{([^}]*)\}", TEX):
        cited |= {k.strip() for k in grp.split(",")}
    items = set(_re.findall(r"\\bibitem\{([^}]*)\}", TEX))
    check(S, "cited keys without bibitem", ",".join(sorted(cited - items)) or "none", "none")
    check(S, "bibitems never cited", ",".join(sorted(items - cited)) or "none", "none")


def main():
    for fn in (data_integrity, feature_selection, label_dependence, broker_faults, temporal_context,
               symptom_labels, operational, ewma_panel, transfer, tables_and_text):
        try:
            fn()
        except Exception as exc:  # noqa: BLE001
            RESULTS.append(dict(section=fn.__name__, claim=f"CHECKER ERROR: {type(exc).__name__}: {exc}",
                                doc=None, source=None, ok=False, note=""))
    df = pd.DataFrame(RESULTS)
    df.to_csv(A / "verify_claims_results.csv", index=False)
    skipped = df[df.ok.isna()]
    ran = df[df.ok.notna()]
    fails = ran[ran.ok != True]  # noqa: E712
    msg = f"{len(df)} checks, {len(ran) - len(fails)} PASS, {len(fails)} FAIL"
    if len(skipped):
        msg += f", {len(skipped)} SKIPPED (paper source not present; text checks only)"
    print(msg + "\n")
    by = ran.assign(ok=ran.ok.astype(bool)).groupby("section").ok.agg(["sum", "count"])
    by.columns = ["pass", "total"]
    print(by.to_string())
    if len(fails):
        print("\n=== FAILURES ===")
        for _, r in fails.iterrows():
            print(f"  [{r.section}] {r.claim}\n      doc={r.doc!r}  source={r.source!r}  {r.note}")
    return 1 if len(fails) else 0


if __name__ == "__main__":
    sys.exit(main())
