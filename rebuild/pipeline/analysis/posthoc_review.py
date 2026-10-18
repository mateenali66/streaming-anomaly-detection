"""Sensitivity analyses added after an internal review of the R2 draft on 18 October 2026 (D5 and D6 addenda).

All are post hoc or exploratory and change no confirmatory decision.

matched_c1       C1 on the test windows all eight panel models score, with max-z and EWMA scored on the same windows
carryover        plain within-run AUC and the R1 placebo without the runs that follow a broker kill (F1), and C1 on
                 the plain within-run AUC without them
without_f8       pooled AUC with the F8 windows removed from the positives, C1 on it, and F8 against NORMAL negatives only
magnitude_tm     the F4 and F7 within-run contrast without the F7 runs flagged F7_TM_LOSS
normal_fpr       the realised false-positive rate on NORMAL test windows only, at both fixed budgets, and the share of
                 the false alerts on NORMAL and RECOVERY test windows that fall on RECOVERY windows (false_alert_share)
ewma_r1_inputs   EWMA on the earlier manuscript's inputs against EWMA on the panel's features and against max-z
c5_power         the median rho C5 needs to reach p below 0.05 with seven faults
drift_blocked    the drift check with contiguous (time-blocked) cross-validation folds in place of shuffled ones

    python3 -m analysis.posthoc_review --derived DIR --phase-b DIR --baselines DIR --extra DIR --results DIR [--drift]
writes DIR/posthoc_review/*.csv and summary.json.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

PIPE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPE))
from analysis import confirm as C  # noqa: E402
from analysis import metrics as M  # noqa: E402
from analysis import stats as S  # noqa: E402
from analysis.sources import DEEP, PANEL  # noqa: E402

DETS = PANEL + ["RF", "RuleBased", "EWMA"]
FAULTS = ["F1", "F2", "F4", "F5", "F6", "F7", "F8"]
NEG = list(M.NEG_LABELS)


def score_path(src, cid, det, k):
    if det in ("RuleBased", "EWMA"):
        return src["baselines"] / cid / det / f"{det}_fold{k}.parquet"
    if det == "EWMA_r1_inputs":
        return src["extra"] / cid / det / f"{det}_fold{k}.parquet"
    return src["phase_b"] / cid / "w30-frozen" / det / f"fold{k}" / "scores" / f"{det}_fold{k}.parquet"


def test_scores(src, cid, det, k):
    f = pd.read_parquet(score_path(src, cid, det, k))
    return f[f["role"] == "test"].set_index("window_id")["score"]


def pooled_contrast(per_campaign: dict, workload: str, two_sided=True) -> dict:
    r = C.contrast_from_diffs(per_campaign, workload, two_sided)
    p = r["pooled"]
    d = S.decide(p, C.DELTA, None, C.ALPHA, two_sided)
    return {"est": p["mean"], "lo": p["ci95_lo"], "hi": p["ci95_hi"], "p": p["p_t"], "outcome": d["outcome"],
            "c1": r["per_campaign"][C.WORKLOADS[workload][0]]["mean"]}


def matched_c1(src, camps) -> pd.DataFrame:
    """C1 and C2a pooled AUC on the windows every panel model scores."""
    rows = []
    for c, camp in camps.items():
        for k in range(1, 11):
            sc = {m: test_scores(src, c, m, k) for m in PANEL + ["RuleBased", "EWMA"]}
            ids = set.intersection(*[set(s.index) for m, s in sc.items() if m in PANEL])
            ids = sorted(ids)
            lab = camp.labels.loc[ids, "label"]
            ev = lab.isin(M.EVAL_LABELS)
            ids = lab.index[ev]
            y = (lab[ev] == "ANOMALOUS").astype(int).to_numpy()
            for m, s in sc.items():
                rows.append({"campaign_id": c, "fold": k, "detector": m,
                             "auc": float(roc_auc_score(y, s.loc[ids].to_numpy()))})
    df = pd.DataFrame(rows)
    out = []
    for w in C.WORKLOADS:
        for m in DEEP + ["EWMA"]:
            diffs = {}
            for c in C.WORKLOADS[w]:
                x = df[df["campaign_id"] == c].pivot(index="fold", columns="detector", values="auc")
                diffs[c] = (x[m] - x["RuleBased"]).to_numpy()
            out.append({"workload": w, "detector": m, **pooled_contrast(diffs, w, two_sided=(m == "EWMA"))})
    return pd.DataFrame(out)


def carryover(runs: pd.DataFrame, carry: pd.DataFrame) -> pd.DataFrame:
    """Plain within-run AUC and R1 placebo without the fault runs that follow F1, and C1 on that within-run AUC."""
    prev = carry.set_index("attempt_key")["preceding"]
    r = runs[runs["run_type"] != "F0"].copy()
    r["after_f1"] = r["attempt_key"].map(prev).eq("F1")
    out = []
    for w in C.WORKLOADS:
        cs = C.WORKLOADS[w]
        sub = r[r["campaign_id"].isin(cs)]
        fold = sub[~sub["after_f1"]].groupby(["detector", "campaign_id", "fold"])[["within_run_auc", "r1_placebo"]].mean()
        allf = sub.groupby(["detector", "campaign_id", "fold"])[["within_run_auc", "r1_placebo"]].mean()
        for d in DETS:
            row = {"workload": w, "detector": d, "n_runs_after_f1": int(sub[(sub["detector"] == d) & sub["after_f1"]
                                                                           & sub["campaign_id"].isin(cs[1:])].shape[0])}
            for m in ("within_run_auc", "r1_placebo"):
                vals = {c: fold.loc[d].loc[c][m].to_numpy() for c in cs[1:]}
                vall = {c: allf.loc[d].loc[c][m].to_numpy() for c in cs[1:]}
                row[f"{m}_without"] = float(np.mean([v.mean() for v in vals.values()]))
                row[f"{m}_all"] = float(np.mean([v.mean() for v in vall.values()]))
                if m == "r1_placebo":
                    p = S.pooled({c: v - 0.5 for c, v in vals.items()})
                    row["r1_placebo_without_lo"], row["r1_placebo_without_hi"] = 0.5 + p["ci95_lo"], 0.5 + p["ci95_hi"]
                    pa = S.pooled({c: v - 0.5 for c, v in vall.items()})
                    row["r1_placebo_all_lo"], row["r1_placebo_all_hi"] = 0.5 + pa["ci95_lo"], 0.5 + pa["ci95_hi"]
            if d in DEEP:
                diffs = {c: fold.loc[d].loc[c]["within_run_auc"].to_numpy() - fold.loc["RuleBased"].loc[c]["within_run_auc"].to_numpy()
                         for c in cs}
                pc = pooled_contrast(diffs, w, two_sided=False)
                row.update({f"c1_without_{k}": v for k, v in pc.items()})
            out.append(row)
    return pd.DataFrame(out)


def without_f8(src, camps, fm) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    for c, camp in camps.items():
        for k in range(1, 11):
            for d in DETS:
                s = test_scores(src, c, d, k)
                lab = camp.labels.loc[s.index]
                ev = lab["label"].isin(M.EVAL_LABELS).to_numpy()
                lab, sv = lab[ev], s.to_numpy()[ev]
                pos = (lab["label"] == "ANOMALOUS").to_numpy()
                f8 = pos & (lab["run_type"] == "F8").to_numpy()
                neg = lab["label"].isin(NEG).to_numpy()
                normal = (lab["label"] == "NORMAL").to_numpy()
                keep = ~f8
                rows.append({"campaign_id": c, "fold": k, "detector": d,
                             "auc_without_f8": float(roc_auc_score(pos[keep].astype(int), sv[keep])),
                             "f8_vs_normal": float(roc_auc_score(f8[f8 | normal].astype(int), sv[f8 | normal])),
                             "f8_share_of_pos": float(f8.sum() / pos.sum())})
    df = pd.DataFrame(rows)
    summ = []
    for w in C.WORKLOADS:
        cs = C.WORKLOADS[w]
        for d in DETS:
            x = df[(df["detector"] == d) & df["campaign_id"].isin(cs[1:])]
            row = {"workload": w, "detector": d, "auc_without_f8": x["auc_without_f8"].mean(),
                   "f8_vs_normal": x["f8_vs_normal"].mean(), "f8_share_of_pos": x["f8_share_of_pos"].mean(),
                   "auc_all": float(fm[(fm["detector"] == d) & fm["campaign_id"].isin(cs[1:])]["pooled_auc"].mean())}
            if d in DEEP:
                diffs = {c: (df[(df["detector"] == d) & (df["campaign_id"] == c)].set_index("fold")["auc_without_f8"] -
                             df[(df["detector"] == "RuleBased") & (df["campaign_id"] == c)].set_index("fold")["auc_without_f8"]
                             ).sort_index().to_numpy() for c in cs}
                row.update({f"c1_{k}": v for k, v in pooled_contrast(diffs, w, two_sided=False).items()})
            summ.append(row)
    return df, pd.DataFrame(summ)


def magnitude_tm(runs: pd.DataFrame) -> pd.DataFrame:
    out = []
    r = runs[runs["run_type"] != "F0"].copy()
    r["tm_loss"] = r["flags"].astype(str).str.contains("F7_TM_LOSS")
    for w in C.WORKLOADS:
        cs = C.WORKLOADS[w]
        for d in DETS:
            diffs = {}
            for c in cs:
                x = r[(r["detector"] == d) & (r["campaign_id"] == c)]
                mag = x[x["run_type"].isin(["F4", "F7"]) & ~x["tm_loss"]].groupby("fold")["within_run_auc"].mean()
                oth = x[x["run_type"].isin(["F1", "F2", "F5", "F6", "F8"])].groupby("fold")["within_run_auc"].mean()
                j = pd.concat([mag, oth], axis=1, keys=["m", "o"]).dropna()
                diffs[c] = (j["m"] - j["o"]).to_numpy()
            conf = {c: diffs[c] for c in cs[1:]}
            p = S.pooled(conf)
            out.append({"workload": w, "detector": d, "d_mean": p["mean"], "d_lo": p["ci95_lo"], "d_hi": p["ci95_hi"],
                        "n_folds": int(sum(len(v) for v in conf.values()))})
    return pd.DataFrame(out)


def normal_fpr(src, camps) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    for c, camp in camps.items():
        for k in range(1, 11):
            for d in DETS:
                f = pd.read_parquet(score_path(src, c, d, k))
                lab = camp.labels.loc[f["window_id"], "label"].to_numpy()
                cal_n = f["score"].to_numpy()[(f["role"] == "cal").to_numpy() & (lab == "NORMAL")]
                tst = (f["role"] == "test").to_numpy()
                for b in M.FP_BUDGETS:
                    thr = float(np.quantile(cal_n, 1.0 - b))
                    s = f["score"].to_numpy()
                    for name, mask in (("normal", tst & (lab == "NORMAL")), ("recovery", tst & (lab == "RECOVERY"))):
                        rows.append({"campaign_id": c, "fold": k, "detector": d, "budget": b, "set": name,
                                     "fpr": float((s[mask] >= thr).mean()), "n_false": int((s[mask] >= thr).sum())})
    df = pd.DataFrame(rows)
    df["workload"] = df["campaign_id"].map(C.workload_of)
    conf = df[~df["campaign_id"].str.endswith("-a")]
    fpr = conf.groupby(["workload", "detector", "budget", "set"])["fpr"].mean().reset_index()
    return fpr, false_alert_share(conf)


def false_alert_share(rows: pd.DataFrame) -> pd.DataFrame:
    """Of the false alerts on NORMAL and RECOVERY test windows, summed over folds and campaigns, the share on RECOVERY."""
    n = rows.pivot_table(index=["workload", "detector", "budget"], columns="set", values="n_false", aggfunc="sum")
    return pd.DataFrame({"n_false_normal": n["normal"], "n_false_recovery": n["recovery"],
                         "recovery_share": n["recovery"] / (n["normal"] + n["recovery"])}).reset_index()


def ewma_inputs(src, camps, fm) -> pd.DataFrame:
    srcs = [{"campaign_id": c, "detector": "EWMA_r1_inputs", "fold": k, "path": str(score_path(src, c, "EWMA_r1_inputs", k))}
            for c in camps for k in range(1, 11)]
    if not all(Path(s["path"]).is_file() for s in srcs):
        return pd.DataFrame()
    fm2, _, _ = M.compute(src["derived"], srcs)
    allfm = pd.concat([fm, fm2], ignore_index=True)
    out = []
    for w in C.WORKLOADS:
        for ref in ("EWMA", "RuleBased"):
            for m in ("pooled_auc", "within_run_auc"):
                r = C.contrast(allfm, "EWMA_r1_inputs", ref, m, w, two_sided=True)
                p = r["pooled"]
                out.append({"workload": w, "reference": ref, "metric": m, "est": p["mean"], "lo": p["ci95_lo"],
                            "hi": p["ci95_hi"], "c1": r["per_campaign"][C.WORKLOADS[w][0]]["mean"]})
        x = fm2[fm2["campaign_id"].isin(C.WORKLOADS[w])].groupby("campaign_id")["pooled_auc"].mean()
        out.append({"workload": w, "reference": "absolute", "metric": "pooled_auc",
                    "est": float(x[C.WORKLOADS[w][1:]].mean()), "c1": float(x[C.WORKLOADS[w][0]])})
    return pd.DataFrame(out)


def critical_median_rho(E: np.ndarray, I: np.ndarray, alpha: float = 0.05) -> tuple[float, int]:
    """The smallest median Spearman rho v with P(null median >= v) <= alpha, over every ordering of the columns of I.
    E and I hold one row per detector and one column per fault."""
    import itertools
    from scipy.stats import spearmanr
    n = E.shape[1]
    null = np.sort([np.median([spearmanr(E[i], I[i][list(pm)]).statistic for i in range(len(E))])
                    for pm in itertools.permutations(range(n))])
    crit = next(v for v in np.unique(null) if np.mean(null >= v - 1e-12) <= alpha)
    return float(crit), int(len(null))


def c5_power(runs: pd.DataFrame) -> pd.DataFrame:
    """The median rho C5 would need for its exact one-sided permutation p to fall below 0.05 over the 5,040
    orderings, from the same per-fault values as C5."""
    E = np.array([C.per_fault_pc(runs, d, C.CONFIRMATORY["ecommerce"]).to_numpy() for d in C.C5_DETECTORS])
    I = np.array([C.per_fault_pc(runs, d, C.CONFIRMATORY["iot"]).to_numpy() for d in C.C5_DETECTORS])
    crit, n = critical_median_rho(E, I)
    return pd.DataFrame([{"test": "C5", "critical_median_rho": crit, "n_perm": n}])


def drift_blocked(derived: Path, n_folds=5, seed=42) -> pd.DataFrame:
    """The drift check of 1.5.4 with contiguous folds: each period is cut into n_folds consecutive chunks, and fold i
    tests chunk i of both periods, so neighbouring windows never straddle a split."""
    from sklearn.ensemble import RandomForestClassifier
    import pyarrow.parquet as pq
    out = []
    for c in sorted(C.WORKLOADS["ecommerce"] + C.WORKLOADS["iot"]):
        p = derived / c / "windows_30s.parquet"
        cols = [x for x in pq.read_schema(p).names if x.startswith("f__")]
        df = pd.read_parquet(p, columns=["start_at", "end_at", "segment_type", "label"] + cols).sort_values("start_at")
        base = df[(df["segment_type"] == "BASELINE") & (df["label"] == "NORMAL")]
        trail = df[(df["segment_type"] == "TRAILING") & (df["label"] == "TRAILING")]
        t0, t1 = base["start_at"].min(), base["end_at"].max()
        first = base[base["start_at"] < t0 + 6 * 3600]
        last = base[base["end_at"] > t1 - 6 * 3600]
        for test, (a, b) in (("first6h_vs_last6h", (first, last)), ("leading_vs_trailing", (base, trail))):
            ca, cb = np.array_split(np.arange(len(a)), n_folds), np.array_split(np.arange(len(b)), n_folds)
            aucs = []
            for i in range(n_folds):
                te_a, te_b = ca[i], cb[i]
                tr_a = np.setdiff1d(np.arange(len(a)), te_a)
                tr_b = np.setdiff1d(np.arange(len(b)), te_b)
                Xtr = np.vstack([a[cols].to_numpy()[tr_a], b[cols].to_numpy()[tr_b]])
                ytr = np.r_[np.zeros(len(tr_a)), np.ones(len(tr_b))]
                Xte = np.vstack([a[cols].to_numpy()[te_a], b[cols].to_numpy()[te_b]])
                yte = np.r_[np.zeros(len(te_a)), np.ones(len(te_b))]
                rf = RandomForestClassifier(n_estimators=200, random_state=seed, n_jobs=-1).fit(Xtr, ytr)
                aucs.append(float(roc_auc_score(yte, rf.predict_proba(Xte)[:, 1])))
            out.append({"campaign_id": c, "test": test, "cv_auc_blocked": float(np.mean(aucs)),
                        "min": min(aucs), "max": max(aucs)})
    return pd.DataFrame(out)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    for k in ("derived", "phase-b", "baselines", "extra", "results"):
        p.add_argument(f"--{k}", required=True, type=Path)
    p.add_argument("--drift", action="store_true")
    p.add_argument("--only", nargs="*")
    a = p.parse_args(argv)
    src = {"derived": a.derived, "phase_b": a.phase_b, "baselines": a.baselines, "extra": a.extra}
    out = a.results / "posthoc_review"
    out.mkdir(exist_ok=True)
    fm = pd.read_parquet(a.results / "fold_metrics.parquet")
    runs = pd.read_parquet(a.results / "run_metrics.parquet")
    camps = {c: M.load_campaign(a.derived, c) for cs in C.WORKLOADS.values() for c in cs}
    want = set(a.only or ["matched_c1", "carryover", "without_f8", "magnitude_tm", "normal_fpr", "ewma_r1_inputs",
                          "c5_power"])
    if "matched_c1" in want:
        matched_c1(src, camps).to_csv(out / "matched_c1.csv", index=False)
    if "carryover" in want:
        carryover(runs, pd.read_csv(a.results / "explore" / "carryover_attempts.csv")).to_csv(out / "carryover.csv", index=False)
    if "without_f8" in want:
        per, summ = without_f8(src, camps, fm)
        per.to_csv(out / "without_f8_per_fold.csv", index=False)
        summ.to_csv(out / "without_f8.csv", index=False)
    if "magnitude_tm" in want:
        magnitude_tm(runs).to_csv(out / "magnitude_without_tm_loss.csv", index=False)
    if "normal_fpr" in want:
        fpr, share = normal_fpr(src, camps)
        fpr.to_csv(out / "normal_fpr.csv", index=False)
        share.to_csv(out / "false_alert_share.csv", index=False)
    if "ewma_r1_inputs" in want:
        e = ewma_inputs(src, camps, fm)
        if len(e):
            e.to_csv(out / "ewma_r1_inputs.csv", index=False)
    if "c5_power" in want:
        c5_power(runs).to_csv(out / "c5_power.csv", index=False)
    if a.drift:
        drift_blocked(a.derived).to_csv(out / "drift_blocked.csv", index=False)
    print(f"posthoc_review: wrote {sorted(x.name for x in out.glob('*.csv'))}")


if __name__ == "__main__":
    main()
