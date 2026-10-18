"""Confirmatory hypotheses C1 to C7 of protocol 5.8, with the decisions of 5.4, Holm of 5.7 and replication of 5.5.

Added on 18 October 2026, after registration. The choices 5.8 leaves open are recorded as D4 in protocol/DEVIATIONS.md.
All use the 30 s anchor and the primary labels. Campaigns 2 and 3 of each workload are confirmatory and pooled
(5.3). Campaign 1, where the search ran, is reported beside them (5.5) with the frozen refit of 6.1.4.

Each contrast is a set of paired fold differences per campaign, summarised by stats.within_campaign, pooled over the
confirmatory campaigns by stats.pooled, with stats.random_effects and stats.sign_flip_p beside it.

The Holm family of each workload (5.7.1) holds one p-value per hypothesis, the p of the test that can end in
"superior": C1 the smallest Holm-adjusted p over the six deep models, C2a, C2b, C3, C4, C5 its permutation p, and C6
the smallest Holm-adjusted p over the eight panel models. Non-superiority and equivalence are intersection-union tests
at alpha with no adjustment (5.7.2, 5.7.3).
"""
from __future__ import annotations

import itertools
import math

import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

from . import stats as S
from .sources import DEEP, PANEL, POINT

DELTA = 0.03
ALPHA = 0.05
WORKLOADS = {"ecommerce": ["ec-a", "ec-b", "ec-c"], "iot": ["iot-a", "iot-b", "iot-c"]}
CONFIRMATORY = {w: c[1:] for w, c in WORKLOADS.items()}
C5_DETECTORS = PANEL + ["RF", "RuleBased", "EWMA"]
C7_DETECTORS = PANEL + ["RF", "RuleBased", "EWMA"]
FAULTS = ["F1", "F2", "F4", "F5", "F6", "F7", "F8"]
METRICS_C1 = ["pooled_auc", "within_run_pc"]


def workload_of(cid: str) -> str:
    return "ecommerce" if cid.startswith("ec-") else "iot"


def fold_series(fm: pd.DataFrame, detector: str, cid: str, metric: str) -> pd.Series:
    s = fm[(fm["detector"] == detector) & (fm["campaign_id"] == cid)].set_index("fold")[metric].sort_index()
    if len(s) != 10 or s.isna().any():
        raise SystemExit(f"ERROR: {detector} {cid} {metric} has {len(s)} folds, {int(s.isna().sum())} missing")
    return s


def contrast_from_diffs(diffs: dict, workload: str, two_sided: bool = False) -> dict:
    """diffs: campaign -> 10 fold differences, for all three campaigns of the workload."""
    conf = {c: diffs[c] for c in CONFIRMATORY[workload]}
    per = {c: S.within_campaign(v) for c, v in diffs.items()}
    pooled = S.pooled(conf)
    return {"per_campaign": per, "pooled": pooled, "random_effects": S.random_effects(conf),
            "sign_flip": S.sign_flip_p(conf), "two_sided": two_sided}


def contrast(fm, a, b, metric, workload, two_sided=False) -> dict:
    diffs = {c: (fold_series(fm, a, c, metric) - fold_series(fm, b, c, metric)).to_numpy()
             for c in WORKLOADS[workload]}
    r = contrast_from_diffs(diffs, workload, two_sided)
    r.update({"a": a, "b": b, "metric": metric, "workload": workload})
    return r


def finish(r: dict, claim_p_adj: float | None) -> dict:
    """Decision on the pooled estimate, decisions per confirmatory campaign alone, and replication status."""
    d = S.decide(r["pooled"], DELTA, claim_p_adj, ALPHA, r["two_sided"])
    per_hold = {}
    for c in CONFIRMATORY[r["workload"]]:
        dc = S.decide(r["per_campaign"][c], DELTA, None, ALPHA, r["two_sided"])
        per_hold[c] = dc["outcome"] == d["outcome"]
    rep = S.replication(d["outcome"], d["outcome"] != "inconclusive",
                        {c: r["per_campaign"][c] for c in CONFIRMATORY[r["workload"]]}, per_hold, DELTA)
    r.update({"decision": d, "replication": rep})
    return r


# ---------------------------------------------------------------- C1, C2a, C2b
def c1(fm, workload):
    rs = {(m, k): contrast(fm, m, "RuleBased", k, workload) for m in DEEP for k in METRICS_C1}
    p_m = {}
    for m in DEEP:
        a, b = rs[(m, "pooled_auc")]["pooled"], rs[(m, "within_run_pc")]["pooled"]
        p_m[m] = max(a["p_t"], b["p_t"]) if (a["mean"] > 0 and b["mean"] > 0) else 1.0
    adj = S.holm(p_m)
    nonsup_all = all(S.decide(r["pooled"], DELTA)["not_superior"] for r in rs.values())
    return {"contrasts": rs, "p_model": p_m, "p_model_holm": adj, "no_deep_beats": nonsup_all,
            "family_p": min(adj.values())}


def c1_decide(res, family_adj):
    beats = [m for m in DEEP if res["p_model_holm"][m] < ALPHA and family_adj < ALPHA]
    for (m, k), r in res["contrasts"].items():
        finish(r, max(res["p_model_holm"][m], family_adj))
    res["beats"] = beats
    res["outcome"] = ("no deep model beats max-z" if res["no_deep_beats"] else
                      ("model beats max-z: " + ", ".join(beats)) if beats else "inconclusive")
    return res


def c2a(fm, workload):
    rs = {k: contrast(fm, "EWMA", "RuleBased", k, workload, two_sided=True) for k in METRICS_C1}
    a, b = rs["pooled_auc"]["pooled"], rs["within_run_pc"]["pooled"]
    p = max(a["p_t"], b["p_t"]) if np.sign(a["mean"]) == np.sign(b["mean"]) != 0 else 1.0
    return {"contrasts": rs, "family_p": p}


def c2a_decide(res, family_adj):
    outs = {k: finish(r, family_adj)["decision"]["outcome"] for k, r in res["contrasts"].items()}
    if outs["pooled_auc"] == outs["within_run_pc"]:
        res["outcome"] = outs["pooled_auc"]
    elif {"superior", "inferior"} & set(outs.values()):
        res["outcome"] = "metric-dependent"
    else:
        res["outcome"] = "inconclusive"
    res["by_metric"] = outs
    return res


def c2b(fm, workload):
    r = contrast(fm, "EWMA", "EWMA_train_mean", "pooled_auc", workload)
    return {"contrasts": {"pooled_auc": r}, "family_p": r["pooled"]["p_t"]}


def single_decide(res, family_adj):
    for r in res["contrasts"].values():
        finish(r, family_adj)
    res["outcome"] = next(iter(res["contrasts"].values()))["decision"]["outcome"]
    return res


# ---------------------------------------------------------------- C3
def matched_auc(scores: dict, labels: pd.DataFrame) -> dict:
    """scores: model -> test-role score frame of one fold. Pooled AUC of every model on the windows all of them score."""
    keep = None
    for f in scores.values():
        ids = set(f.loc[f["role"] == "test", "window_id"])
        keep = ids if keep is None else keep & ids
    keep = sorted(keep)
    lab = labels.loc[keep, "label"]
    ev = lab.isin(["ANOMALOUS", "NORMAL", "RECOVERY", "SHAM", "SHAM_POST"])
    ids = lab.index[ev]
    y = (lab[ev] == "ANOMALOUS").astype(int).to_numpy()
    out = {}
    for m, f in scores.items():
        s = f.set_index("window_id").loc[ids, "score"].to_numpy(np.float64)
        out[m] = float(roc_auc_score(y, s))
    return {"auc": out, "n_matched": int(len(ids)), "n_pos": int(y.sum())}


def c3(fm, matched: pd.DataFrame, workload):
    """matched: campaign_id, fold, model, matched_auc."""
    seq = [m for m in DEEP]
    diffs_p, diffs_w = {}, {}
    for c in WORKLOADS[workload]:
        mm = matched[matched["campaign_id"] == c].pivot(index="fold", columns="model", values="matched_auc").sort_index()
        diffs_p[c] = (mm[seq].mean(axis=1) - mm[POINT].mean(axis=1)).to_numpy()
        w = pd.DataFrame({m: fold_series(fm, m, c, "within_run_pc") for m in PANEL})
        diffs_w[c] = (w[seq].mean(axis=1) - w[POINT].mean(axis=1)).to_numpy()
    rp = contrast_from_diffs(diffs_p, workload, two_sided=True)
    rp.update({"a": "mean of six sequence models", "b": "mean of IF and OC-SVM", "metric": "pooled_auc_matched",
               "workload": workload})
    rw = contrast_from_diffs(diffs_w, workload, two_sided=True)
    rw.update({"a": "mean of six sequence models", "b": "mean of IF and OC-SVM", "metric": "within_run_pc",
               "workload": workload})
    return {"contrasts": {"pooled_auc_matched": rp, "within_run_pc": rw}, "family_p": rp["pooled"]["p_t"]}


def c3_decide(res, family_adj):
    for r in res["contrasts"].values():
        finish(r, family_adj)
    o_p = res["contrasts"]["pooled_auc_matched"]["decision"]["outcome"]
    o_w = res["contrasts"]["within_run_pc"]["decision"]["outcome"]
    res["outcome"] = o_p
    res["headline_ok"] = o_p == o_w
    return res


def c3_mixed(matched: pd.DataFrame, workload) -> dict:
    """Secondary: the R1 mixed model, matched pooled AUC ~ family with a random intercept for model."""
    import statsmodels.formula.api as smf
    df = matched[matched["campaign_id"].isin(CONFIRMATORY[workload])].copy()
    df["family"] = pd.Categorical(np.where(df["model"].isin(POINT), "point", "sequence"),
                                  categories=["point", "sequence"])
    res = smf.mixedlm("matched_auc ~ family", data=df, groups=df["model"]).fit(reml=True, method="lbfgs")
    name = "family[T.sequence]"
    lo, hi = res.conf_int().loc[name].tolist()
    return {"coef": float(res.params[name]), "se": float(res.bse[name]), "ci95_lo": lo, "ci95_hi": hi,
            "p": float(res.pvalues[name]), "sigma_model": float(res.cov_re.iloc[0, 0]) ** 0.5, "n_obs": len(df)}


# ---------------------------------------------------------------- C4
def fusion_fold_aucs(cat_scores: dict, if_scores: dict, labels: pd.DataFrame) -> dict:
    """cat_scores: fold -> DataFrame (window_id index) with one column per category of IF test scores;
    if_scores: fold -> Series of IF all-feature test scores. Weights are a logistic regression on the other nine folds'
    test windows (class_weight balanced, random_state 42, max_iter 1000), as in analysis/cross_workload/
    rq3_fusion_corrected.py. Returns fold -> (fused AUC, IF AUC) on the same windows."""
    data = {}
    for k, X in cat_scores.items():
        ids = X.index.intersection(if_scores[k].index)
        lab = labels.loc[ids, "label"]
        ev = lab.isin(["ANOMALOUS", "NORMAL", "RECOVERY", "SHAM", "SHAM_POST"])
        ids = ids[ev.to_numpy()]
        data[k] = (X.loc[ids].to_numpy(np.float64), (labels.loc[ids, "label"] == "ANOMALOUS").astype(int).to_numpy(),
                   if_scores[k].loc[ids].to_numpy(np.float64))
    out = {}
    for k, (X, y, s_if) in data.items():
        others = [o for o in data if o != k]
        Xtr = np.vstack([data[o][0] for o in others])
        ytr = np.concatenate([data[o][1] for o in others])
        lr = LogisticRegression(class_weight="balanced", random_state=42, max_iter=1000).fit(Xtr, ytr)
        fused = lr.predict_proba(X)[:, 1]
        out[k] = (float(roc_auc_score(y, fused)), float(roc_auc_score(y, s_if)), lr.coef_[0].tolist())
    return out


def c4(fusion: pd.DataFrame, workload):
    """fusion: campaign_id, fold, fused_auc, if_auc."""
    diffs = {}
    for c in WORKLOADS[workload]:
        f = fusion[fusion["campaign_id"] == c].sort_values("fold")
        if len(f) != 10:
            raise SystemExit(f"ERROR: C4 {c} has {len(f)} folds")
        diffs[c] = (f["fused_auc"] - f["if_auc"]).to_numpy()
    r = contrast_from_diffs(diffs, workload, two_sided=True)
    r.update({"a": "IF weighted fusion", "b": "IF all features", "metric": "pooled_auc", "workload": workload})
    return {"contrasts": {"pooled_auc": r}, "family_p": r["pooled"]["p_t"]}


# ---------------------------------------------------------------- C5
def per_fault_pc(runs: pd.DataFrame, detector: str, campaigns: list) -> pd.Series:
    """Per fault, the placebo-corrected within-run AUC (each run minus its fold's sham placebo), mean over folds, then
    the mean over the given campaigns."""
    vals = []
    for c in campaigns:
        r = runs[(runs["detector"] == detector) & (runs["campaign_id"] == c)]
        sham = r[r["run_type"] == "F0"].set_index("fold")["within_run_auc"]
        f = r[r["run_type"] != "F0"].copy()
        f["pc"] = f["within_run_auc"].to_numpy() - sham.reindex(f["fold"]).to_numpy()
        per = f.groupby("run_type")["pc"].mean().reindex(FAULTS)
        if per.isna().any() or f.groupby("run_type").size().reindex(FAULTS).ne(10).any():
            raise SystemExit(f"ERROR: C5 {detector} {c} lacks a fault or a fold")
        vals.append(per)
    return pd.concat(vals, axis=1).mean(axis=1)


def _rho(a, b) -> float:
    return float(spearmanr(a, b).statistic)


def c5(runs: pd.DataFrame, positions: pd.DataFrame) -> dict:
    """positions: campaign_id, run_type, position for every fault run. Exact permutation over the 5,040 orderings of
    the IoT fault labels, applied to all detectors at once; one-sided p, the share of orderings whose median rho is at
    least the observed one."""
    E = np.array([per_fault_pc(runs, d, CONFIRMATORY["ecommerce"]).to_numpy() for d in C5_DETECTORS])
    I = np.array([per_fault_pc(runs, d, CONFIRMATORY["iot"]).to_numpy() for d in C5_DETECTORS])
    rhos = np.array([_rho(E[i], I[i]) for i in range(len(C5_DETECTORS))])
    obs = float(np.median(rhos))
    null = []
    for perm in itertools.permutations(range(len(FAULTS))):
        null.append(np.median([_rho(E[i], I[i][list(perm)]) for i in range(len(C5_DETECTORS))]))
    null = np.array(null)
    p = float(np.mean(null >= obs - 1e-12))
    ceiling = {}
    for w, (c2, c3_) in CONFIRMATORY.items():
        r = [_rho(per_fault_pc(runs, d, [c2]), per_fault_pc(runs, d, [c3_])) for d in C5_DETECTORS]
        ceiling[w] = {"median_rho": float(np.median(r)), "per_detector": dict(zip(C5_DETECTORS, map(float, r)))}
    pos = {w: positions[positions["campaign_id"].isin(CONFIRMATORY[w])].groupby("run_type")["position"].mean()
           .reindex(FAULTS) for w in CONFIRMATORY}
    time_ref = _rho(pos["ecommerce"].to_numpy(), pos["iot"].to_numpy())
    return {"median_rho": obs, "per_detector_rho": dict(zip(C5_DETECTORS, map(float, rhos))), "p_perm": p,
            "n_perm": int(len(null)), "ceiling": ceiling, "time_reference_rho": time_ref,
            "mean_position": {w: v.to_dict() for w, v in pos.items()}, "family_p": p,
            "per_fault": {"ecommerce": dict(zip(C5_DETECTORS, E.tolist())), "iot": dict(zip(C5_DETECTORS, I.tolist()))}}


# ---------------------------------------------------------------- C6
def c6(cross: pd.DataFrame, workload) -> dict:
    """cross: source, target, detector, fold, pooled_auc. For each target campaign of the workload and test block, the
    within value is the mean over the two same-workload sources and the cross value the mean over the three
    other-workload sources (D2, item 3.6)."""
    rs, p_m = {}, {}
    for m in PANEL + ["RF"]:
        diffs = {}
        for t in WORKLOADS[workload]:
            x = cross[(cross["detector"] == m) & (cross["target"] == t)]
            same = x["source"].map(workload_of) == workload
            if same.groupby(x["fold"]).sum().ne(2).any() or (~same).groupby(x["fold"]).sum().ne(3).any():
                raise SystemExit(f"ERROR: C6 {m} {t} lacks a source")
            wv = x[same].groupby("fold")["pooled_auc"].mean().sort_index()
            cv = x[~same].groupby("fold")["pooled_auc"].mean().sort_index()
            diffs[t] = (wv - cv).to_numpy()
        r = contrast_from_diffs(diffs, workload, two_sided=False)
        r.update({"a": f"{m} within-workload", "b": f"{m} cross-workload", "metric": "pooled_auc",
                  "workload": workload})
        rs[m] = r
        if m != "RF":
            p_m[m] = r["pooled"]["p_t"] if r["pooled"]["mean"] > 0 else 1.0
    adj = S.holm(p_m)
    return {"contrasts": rs, "p_model": p_m, "p_model_holm": adj, "family_p": min(adj.values())}


def c6_decide(res, family_adj):
    for m, r in res["contrasts"].items():
        finish(r, max(res["p_model_holm"].get(m, 1.0), family_adj) if m != "RF" else None)
    eq = [m for m in PANEL if res["contrasts"][m]["decision"]["equivalent"]]
    res["equivalent_models"] = eq
    res["outcome"] = ("transfer costs less than delta for all eight" if len(eq) == len(PANEL) else
                      "per-model results")
    return res


# ---------------------------------------------------------------- C7
def c7(fm: pd.DataFrame) -> dict:
    out = {}
    for w, cs in WORKLOADS.items():
        out[w] = {}
        for d in C7_DETECTORS:
            means = {c: float(fold_series(fm, d, c, "pooled_auc").mean()) for c in cs}
            pm = float(np.mean(list(means.values())))
            dev = {c: v - pm for c, v in means.items()}
            out[w][d] = {"campaign_means": means, "pooled_mean": pm, "deviation": dev,
                         "max_abs_deviation": max(abs(v) for v in dev.values()),
                         "reproducible": all(abs(v) < DELTA for v in dev.values())}
    return out


# ---------------------------------------------------------------- 4.6.4 time-only flag
def time_only_flags(fm: pd.DataFrame) -> dict:
    """Per campaign, the time-only pooled AUC with its t-based 95% interval over the ten folds. A pooled result of the
    campaign is flagged when the interval excludes 0.5 (4.6.4). The drift-check half of the rule is in explore.py."""
    out = {}
    for cs in WORKLOADS.values():
        for c in cs:
            w = S.within_campaign(fold_series(fm, "TimeOnly", c, "pooled_auc").to_numpy() - 0.5)
            out[c] = {"mean": 0.5 + w["mean"], "ci95_lo": 0.5 + w["ci95_lo"], "ci95_hi": 0.5 + w["ci95_hi"],
                      "flag": bool(w["ci95_lo"] > 0 or w["ci95_hi"] < 0)}
    return out


# ---------------------------------------------------------------- all
def run_all(fm, runs, matched, fusion, cross, positions) -> dict:
    out = {"delta": DELTA, "alpha": ALPHA, "confirmatory": CONFIRMATORY, "workloads": {},
           "time_only_flags": time_only_flags(fm)}
    c5r = c5(runs, positions)
    for w in WORKLOADS:
        H = {"C1": c1(fm, w), "C2a": c2a(fm, w), "C2b": c2b(fm, w), "C3": c3(fm, matched, w),
             "C4": c4(fusion, w), "C5": c5r, "C6": c6(cross, w)}
        fam = {k: v["family_p"] for k, v in H.items()}
        adj = S.holm(fam)
        c1_decide(H["C1"], adj["C1"])
        c2a_decide(H["C2a"], adj["C2a"])
        single_decide(H["C2b"], adj["C2b"])
        c3_decide(H["C3"], adj["C3"])
        H["C3"]["mixed"] = c3_mixed(matched, w)
        single_decide(H["C4"], adj["C4"])
        c6_decide(H["C6"], adj["C6"])
        out["workloads"][w] = {"hypotheses": H, "family_p": fam, "family_p_holm": adj,
                               "C5_superior": bool(adj["C5"] < ALPHA)}
    out["C7"] = c7(fm)
    return out
