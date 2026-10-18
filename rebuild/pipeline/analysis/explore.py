#!/usr/bin/env python3
"""Exploratory analyses of protocol 5.9 from stored scores, plus the drift check of 1.5.4 (make stats, 7.1 step 8).

Added on 18 October 2026, after registration and after the confirmatory analysis was tagged. Every result here is
exploratory (5.7.5) unless marked post hoc, and the choices the protocol leaves open are recorded as D6 in
protocol/DEVIATIONS.md. Nothing here trains a detector except the random forests of the drift check, which are the
classifier two-sample test of 1.5.4.

    python3 -m analysis.explore --derived DIR --phase-b DIR --baselines DIR --search-run DIR --copies DIR \
        --results DIR [--only NAME ...]

Reads results/fold_metrics.parquet and results/run_metrics.parquet from analysis.run metrics, and writes
results/explore/<name>.csv plus results/explore/summary.json. Analyses, by name:
  replay        5.9.18 chronological alert replay of each test block and of the trailing baseline
  drift         5.9.16 and 1.5.4, random forest two-sample tests on the baseline windows
  per_fault     5.9.1 per-fault pooled, within-run and placebo-corrected within-run AUC
  prevalence    5.9.3 F1 at prevalences 40, 20, 10, 5 and 1%, from fold TPR and FPR
  iqr           5.9.4 IQR thresholds without labels
  carryover     5.9.14 max-z on pre-fault windows by the preceding run type, Kruskal-Wallis
  load_phase    5.9.15 load-cycle phase at onset, Rayleigh tests, and phase as a covariate of within-run AUC
  rf            5.9.8 random forest importance and confusion summaries
  three_state   5.9.11 three-state regimes and RECOVERY dropped
  lofo          5.9.9 leave-one-fault-out with thresholds only
  windows       5.9.2 window-size sensitivity on campaign 1
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats as st
from sklearn.metrics import roc_auc_score

PIPE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPE))
from analysis import metrics as M  # noqa: E402
from analysis import sources as SRC  # noqa: E402

WINDOW_S = 30
CONFIRMATORY = {"ecommerce": ["ec-b", "ec-c"], "iot": ["iot-b", "iot-c"]}
CAMPAIGN1 = {"ecommerce": "ec-a", "iot": "iot-a"}
WORKLOAD_CAMPAIGNS = {"ecommerce": ["ec-a", "ec-b", "ec-c"], "iot": ["iot-a", "iot-b", "iot-c"]}
DETECTORS = SRC.PANEL + ["RF", "RuleBased", "EWMA"]          # the eleven detectors of C5 and C7
DIAG = ["TimeOnly", "PhasePosition"]
FAULTS = M.FAULTS
NEG = list(M.NEG_LABELS)
SUPPRESSION_S = 4 * WINDOW_S      # operational_replay.py SUPPRESSION_WINDOWS = 4, as time between alerted windows
PREVALENCES = [0.40, 0.20, 0.10, 0.05, 0.01]
BOOT_N = 10000
BOOT_SEED = 20261018
LOAD_CYCLE_S = 1800.0
DRIFT_SPLIT_S = 6 * 3600
DRIFT_THRESHOLD = 0.70
RF_SEED = 42
OPS = [f"fpb_{b}" for b in M.FP_BUDGETS] + ["f1opt"]


def workload_of(cid):
    return "ecommerce" if cid.startswith("ec-") else "iot"


# ------------------------------------------------------------------ shared helpers
class Inputs:
    """Campaign labels and score files, loaded once."""

    def __init__(self, a):
        self.a = a
        self.derived = Path(a.derived)
        self.results = Path(a.results)
        self.out = self.results / "explore"
        self.out.mkdir(parents=True, exist_ok=True)
        self.camps = {c: M.load_campaign(self.derived, c) for c in SRC.CAMPAIGNS}
        self.src = SRC.frozen_sources(Path(a.phase_b), Path(a.baselines), SRC.CAMPAIGNS)
        self.by = {(s["campaign_id"], s["detector"], s["fold"]): s["path"] for s in self.src}
        self._fm = self._runs = None
        self.hashes = {}

    @property
    def fm(self):
        if self._fm is None:
            self._fm = pd.read_parquet(self.results / "fold_metrics.parquet")
        return self._fm

    @property
    def runs(self):
        if self._runs is None:
            self._runs = pd.read_parquet(self.results / "run_metrics.parquet")
        return self._runs

    def joined(self, cid, det, fold):
        """The score file checked against the labels (metrics.check_scores) and joined to them."""
        p = Path(self.by[(cid, det, fold)])
        return M.check_scores(self.camps[cid], pd.read_parquet(p), fold, f"{det} {cid} fold {fold}")


def thresholds(cal: pd.DataFrame) -> dict:
    """The operating points of metrics.fold_metrics, plus the IQR threshold of 5.9.4 on the cal NORMAL scores."""
    cn = cal.loc[cal["label"] == "NORMAL", "score"].to_numpy(np.float64)
    out = {f"fpb_{b}": float(np.quantile(cn, 1.0 - b)) for b in M.FP_BUDGETS}
    y = (cal["label"] == "ANOMALOUS").to_numpy().astype(int)
    out["f1opt"] = M.find_optimal_threshold(cal["score"].to_numpy(np.float64), y)
    q1, q3 = np.quantile(cn, [0.25, 0.75])
    out["iqr"] = float(q3 + 1.5 * (q3 - q1))
    return out


def splits(j):
    test = j[(j["role"] == "test") & j["label"].isin(M.EVAL_LABELS)].reset_index(drop=True)
    cal = j[(j["role"] == "cal") & j["label"].isin(M.EVAL_LABELS)].reset_index(drop=True)
    trailing = j[(j["role"] == "eval_only") & (j["label"] == "TRAILING")].reset_index(drop=True)
    return test, cal, trailing


def pooled_campaign_mean(df: pd.DataFrame, value: str, keys: list) -> pd.DataFrame:
    """Per workload, the mean of the confirmatory campaigns' means (5.3.1), with campaign 1 beside it."""
    rows = []
    for w, cs in CONFIRMATORY.items():
        sub = df[df["campaign_id"].isin(cs + [CAMPAIGN1[w]])]
        cm = sub.groupby(keys + ["campaign_id"])[value].mean().unstack("campaign_id")
        for k, r in cm.iterrows():
            rows.append({"workload": w, **dict(zip(keys, k if isinstance(k, tuple) else (k,))),
                         "pooled_c2_c3": float(np.mean([r[c] for c in cs])),
                         "campaign_2": float(r[cs[0]]), "campaign_3": float(r[cs[1]]),
                         "campaign_1": float(r[CAMPAIGN1[w]])})
    return pd.DataFrame(rows)


# ------------------------------------------------------------------ 5.9.18 chronological replay
def alert_episodes(start, end, alert, gap_s=SUPPRESSION_S):
    """Maximal groups of alerted windows in time order, joined when the quiet time between one alerted window's end
    and the next one's start is at most gap_s. Returns a list of index arrays."""
    idx = np.flatnonzero(alert)
    if len(idx) == 0:
        return []
    order = idx[np.argsort(start[idx], kind="stable")]
    eps, cur = [], [order[0]]
    for i in order[1:]:
        if start[i] - end[cur[-1]] <= gap_s:
            cur.append(i)
        else:
            eps.append(np.array(cur))
            cur = [i]
    eps.append(np.array(cur))
    return eps


def replay_block(frame: pd.DataFrame, alert: np.ndarray, camp: M.Campaign) -> dict:
    """One test block in wall-clock order. An alert episode is true if it holds an ANOMALOUS window, recovery if it
    holds only RECOVERY windows next to a fault, sham if it touches a SHAM window and no fault window, and false
    otherwise. Monitored time counts the scored windows only, as operational_replay.py does."""
    s, e = frame["start_at"].to_numpy(), frame["end_at"].to_numpy()
    lab = frame["label"].to_numpy()
    eps = alert_episodes(s, e, alert)
    kinds = []
    for ep in eps:
        l_ = set(lab[ep])
        if "ANOMALOUS" in l_:
            kinds.append("true")
        elif l_ & {"SHAM", "SHAM_POST"}:
            kinds.append("sham")
        elif "RECOVERY" in l_:
            kinds.append("recovery")
        else:
            kinds.append("false")
    hours = len(frame) * WINDOW_S / 3600.0
    fault_runs = frame[frame["label"] == "ANOMALOUS"].groupby("attempt_key")
    lags, detected = [], 0
    for key, g in fault_runs:
        g = g.sort_values("start_at", kind="stable")
        al = alert[g.index.to_numpy()]
        if al.any():
            detected += 1
            first = g["start_at"].to_numpy()[np.argmax(al)]
            lags.append(first - camp.attempts.loc[key, "onset"])
    n = {k: kinds.count(k) for k in ("true", "recovery", "sham", "false")}
    return {"monitored_h": hours, "n_episodes": len(eps), **{f"n_{k}": v for k, v in n.items()},
            "false_per_h": n["false"] / hours, "nonfault_per_h": (n["false"] + n["sham"] + n["recovery"]) / hours,
            "fault_runs": fault_runs.ngroups, "fault_runs_detected": detected,
            "median_lag_s": float(np.median(lags)) if lags else float("nan"),
            "alert_windows_per_h": float(alert.sum()) / hours}


def run_replay_iqr_lofo_three(I: Inputs, dets) -> dict:
    """One pass over the score files for the replay, the IQR thresholds, three-state and threshold-only LOFO."""
    rep, iqr, three, lofo = [], [], [], []
    for cid in SRC.CAMPAIGNS:
        camp = I.camps[cid]
        for det in dets:
            for k in range(1, 11):
                j = I.joined(cid, det, k)
                test, cal, trailing = splits(j)
                thr = thresholds(cal)
                meta = {"campaign_id": cid, "detector": det, "fold": k}
                for op, t in thr.items():
                    al = test["score"].to_numpy(np.float64) >= t
                    r = replay_block(test, al, camp)
                    if len(trailing):
                        at = trailing["score"].to_numpy(np.float64) >= t
                        te = alert_episodes(trailing["start_at"].to_numpy(), trailing["end_at"].to_numpy(), at)
                        r["trailing_episodes_per_h"] = len(te) / (len(trailing) * WINDOW_S / 3600.0)
                    else:
                        r["trailing_episodes_per_h"] = float("nan")
                    neg = test["label"].isin(NEG).to_numpy()
                    r["analytic_fa_per_h"] = float(al[neg].mean()) * 3600.0 / WINDOW_S
                    rep.append({**meta, "op": op, "threshold": t, **r})
                # 5.9.4 IQR threshold without labels
                op_, _, _ = M.operating_point(test, trailing, thr["iqr"])
                iqr.append({**meta, **op_})
                three.extend({**meta, **x} for x in three_state_rows(test, cal))
                lofo.extend({**meta, **x} for x in lofo_rows(test, cal, thr["f1opt"]))
    out = {}
    for name, rows in (("replay", rep), ("iqr", iqr), ("three_state", three), ("lofo", lofo)):
        df = pd.DataFrame(rows)
        df.to_csv(I.out / f"{name}_per_fold.csv", index=False)
        out[name] = df
    return out


def summarise_replay(df):
    keys = ["detector", "op"]
    vals = ["false_per_h", "nonfault_per_h", "trailing_episodes_per_h", "analytic_fa_per_h", "median_lag_s",
            "alert_windows_per_h"]
    df = df.assign(detection_rate=df["fault_runs_detected"] / df["fault_runs"])
    parts = [pooled_campaign_mean(df, v, keys).assign(metric=v) for v in vals + ["detection_rate"]]
    return pd.concat(parts, ignore_index=True)


# ------------------------------------------------------------------ 5.9.11 three-state
REGIMES = {   # (negative labels, positive labels); NORMAL here means the windows of no fault: NORMAL, SHAM, SHAM_POST
    "binary": (["NORMAL", "SHAM", "SHAM_POST", "RECOVERY"], ["ANOMALOUS"]),
    "active": (["NORMAL", "SHAM", "SHAM_POST"], ["ANOMALOUS"]),
    "recovery": (["NORMAL", "SHAM", "SHAM_POST"], ["RECOVERY"]),
    "hybrid": (["NORMAL", "SHAM", "SHAM_POST"], ["ANOMALOUS", "RECOVERY"]),
}


def three_state_rows(test, cal):
    rows = []
    for name, (neg, pos) in REGIMES.items():
        mt = test["label"].isin(neg + pos).to_numpy()
        mc = cal["label"].isin(neg + pos).to_numpy()
        y = test.loc[mt, "label"].isin(pos).to_numpy().astype(int)
        s = test.loc[mt, "score"].to_numpy(np.float64)
        yc = cal.loc[mc, "label"].isin(pos).to_numpy().astype(int)
        t = M.find_optimal_threshold(cal.loc[mc, "score"].to_numpy(np.float64), yc)
        pr = s >= t
        tp, fp, fn = int((pr & (y == 1)).sum()), int((pr & (y == 0)).sum()), int((~pr & (y == 1)).sum())
        p = float(y.mean())
        rows.append({"regime": name, "auc": M.auc(y, s), "prevalence": p, "always_fire_f1": 2 * p / (1 + p),
                     "f1": 2 * tp / (2 * tp + fp + fn) if (2 * tp + fp + fn) else 0.0, "n": int(len(y))})
    return rows


# ------------------------------------------------------------------ 5.9.9 threshold-only LOFO
def lofo_rows(test, cal, thr_all):
    """For each fault f, the F1-optimal threshold is refitted on cal without f's attempts (whole attempts, 3.4.1) and
    applied to the test block's f attempts and sham, as train.py --lofo-held-out builds its test set. The same windows
    at the threshold fitted with f gives the reference."""
    rows = []
    for f in FAULTS:
        c = cal[cal["run_type"] != f]
        t = M.find_optimal_threshold(c["score"].to_numpy(np.float64),
                                     (c["label"] == "ANOMALOUS").to_numpy().astype(int))
        sub = test[test["run_type"].isin([f, "F0"])]
        y = (sub["label"] == "ANOMALOUS").to_numpy()
        s = sub["score"].to_numpy(np.float64)
        r = {"held_out": f, "threshold_lofo": t, "threshold_all": thr_all, "auc": M.auc(y.astype(int), s)}
        for name, th in (("lofo", t), ("all", thr_all)):
            pr = s >= th
            tp, fp, fn = int((pr & y).sum()), int((pr & ~y).sum()), int((~pr & y).sum())
            r[f"recall_{name}"] = tp / y.sum() if y.sum() else float("nan")
            r[f"f1_{name}"] = 2 * tp / (2 * tp + fp + fn) if (2 * tp + fp + fn) else 0.0
            r[f"fpr_{name}"] = fp / (~y).sum() if (~y).sum() else float("nan")
        rows.append(r)
    return rows


# ------------------------------------------------------------------ 5.9.16 drift check
def drift_auc(A: np.ndarray, B: np.ndarray) -> list[float]:
    """Classifier two-sample test of 1.5.4: a random forest (200 trees, seed 42, missing values left to the forest)
    told to separate A from B, scored by AUC on each of five stratified shuffled folds."""
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import StratifiedKFold
    X = np.vstack([A, B])
    y = np.r_[np.zeros(len(A)), np.ones(len(B))].astype(int)
    aucs = []
    for tr, te in StratifiedKFold(5, shuffle=True, random_state=RF_SEED).split(X, y):
        rf = RandomForestClassifier(n_estimators=200, random_state=RF_SEED, n_jobs=-1)
        rf.fit(X[tr], y[tr])
        aucs.append(roc_auc_score(y[te], rf.predict_proba(X[te])[:, 1]))
    return aucs


def univariate_auc(A: np.ndarray, B: np.ndarray) -> np.ndarray:
    """Per column, max(AUC, 1 - AUC) of the column alone for B against A, missing values dropped, NaN when a side is
    empty or the column is constant over both."""
    out = np.full(A.shape[1], np.nan)
    for j in range(A.shape[1]):
        a, b = A[:, j], B[:, j]
        a, b = a[np.isfinite(a)], b[np.isfinite(b)]
        if len(a) == 0 or len(b) == 0 or (np.ptp(np.r_[a, b]) == 0):
            continue
        r = st.rankdata(np.r_[a, b])
        u = r[len(a):].sum() - len(b) * (len(b) + 1) / 2
        v = u / (len(a) * len(b))
        out[j] = max(v, 1 - v)
    return out


def drift(I: Inputs) -> pd.DataFrame:
    from sklearn.ensemble import RandomForestClassifier
    rows, uni, top = [], [], []
    for cid in SRC.CAMPAIGNS:
        p = I.derived / cid / f"windows_{WINDOW_S}s.parquet"
        M._check_sidecar(p)
        w = pd.read_parquet(p)
        cols = [c for c in w.columns if c.startswith("f__")]
        base = w[(w["segment_type"] == "BASELINE") & (w["label"] == "NORMAL")].sort_values("start_at")
        trail = w[(w["segment_type"] == "TRAILING") & (w["label"] == "TRAILING")]
        t0, t1 = base["start_at"].min(), base["end_at"].max()
        first = base[base["end_at"] <= t0 + DRIFT_SPLIT_S]
        last = base[base["start_at"] >= t1 - DRIFT_SPLIT_S]
        tests = {"first6h_vs_last6h": (first, last), "leading_vs_trailing": (base, trail)}
        for name, (a, b) in tests.items():
            for variant, use in (("all_features", cols),
                                 ("without_jvm_buffer_pool", [c for c in cols if "jvm_buffer_pool" not in c])):
                aucs = drift_auc(a[use].to_numpy(np.float64), b[use].to_numpy(np.float64))
                auc = float(np.mean(aucs))
                if variant == "all_features":
                    # post hoc: how widely the drift is spread, and the forest's top features fitted on all windows
                    A, B = a[use].to_numpy(np.float64), b[use].to_numpy(np.float64)
                    u = univariate_auc(A, B)
                    ok = np.isfinite(u)
                    uni.append({"campaign_id": cid, "test": name, "n_features_testable": int(ok.sum()),
                                "share_above_0.70": float((u[ok] > 0.70).mean()),
                                "share_above_0.90": float((u[ok] > 0.90).mean()),
                                "share_at_1.00": float((u[ok] >= 0.9999).mean()), "post_hoc": True})
                    rf = RandomForestClassifier(n_estimators=200, random_state=RF_SEED, n_jobs=-1)
                    rf.fit(np.vstack([A, B]), np.r_[np.zeros(len(A)), np.ones(len(B))].astype(int))
                    for r_, i in enumerate(np.argsort(rf.feature_importances_)[::-1][:10]):
                        top.append({"campaign_id": cid, "test": name, "rank": r_ + 1, "feature": use[i],
                                    "importance": float(rf.feature_importances_[i]), "univariate_auc": float(u[i]),
                                    "post_hoc": True})
                rows.append({"campaign_id": cid, "test": name, "features": variant, "n_a": len(a), "n_b": len(b),
                             "n_features": len(use), "cv_auc": auc, "cv_auc_min": float(np.min(aucs)),
                             "cv_auc_max": float(np.max(aucs)), "drift": auc > DRIFT_THRESHOLD,
                             "post_hoc": variant != "all_features"})
    df = pd.DataFrame(rows)
    df.to_csv(I.out / "drift.csv", index=False)
    pd.DataFrame(uni).to_csv(I.out / "drift_univariate_post_hoc.csv", index=False)
    pd.DataFrame(top).to_csv(I.out / "drift_top_features_post_hoc.csv", index=False)
    return df


# ------------------------------------------------------------------ 5.9.1 per fault
def per_fault(I: Inputs) -> pd.DataFrame:
    fm, runs = I.fm, I.runs
    dets = DETECTORS + DIAG
    rows = []
    for (cid, det), g in fm[fm["detector"].isin(dets)].groupby(["campaign_id", "detector"]):
        r = runs[(runs["campaign_id"] == cid) & (runs["detector"] == det)]
        sham = r[r["run_type"] == "F0"].set_index("fold")["within_run_auc"]
        for f in FAULTS:
            rf_ = r[r["run_type"] == f]
            pc = rf_["within_run_auc"].to_numpy() - sham.reindex(rf_["fold"]).to_numpy()
            rows.append({"campaign_id": cid, "detector": det, "fault": f,
                         "pooled_auc": float(g[f"pooled_auc_{f}"].mean()),
                         "within_run_auc": float(rf_["within_run_auc"].mean()),
                         "within_run_pc": float(np.mean(pc)), "n_runs": int(len(rf_))})
    df = pd.DataFrame(rows)
    df.to_csv(I.out / "per_fault_per_campaign.csv", index=False)
    s = pd.concat([pooled_campaign_mean(df, v, ["detector", "fault"]).assign(metric=v)
                   for v in ("pooled_auc", "within_run_auc", "within_run_pc")], ignore_index=True)
    s.to_csv(I.out / "per_fault.csv", index=False)
    return s


# ------------------------------------------------------------------ 5.9.3 prevalence sweep
def f1_at(tpr, fpr, p):
    den = tpr * p + fpr * (1 - p)
    prec = tpr * p / den if den > 0 else 0.0
    return 2 * prec * tpr / (prec + tpr) if (prec + tpr) > 0 else 0.0


def prevalence(I: Inputs) -> pd.DataFrame:
    """precision(p) = TPR p / (TPR p + FPR (1 - p)) from fold TPR and FPR at each operating point, as
    prevalence_sweep.py does. Pooled over the confirmatory campaigns with the mean of the campaign means of TPR and
    FPR, and a bootstrap that resamples folds within each campaign."""
    fm = I.fm[I.fm["detector"].isin(DETECTORS)]
    rng = np.random.default_rng(BOOT_SEED)
    rows = []
    for w, cs in CONFIRMATORY.items():
        for det in DETECTORS:
            for op in OPS:
                per = {c: fm[(fm["detector"] == det) & (fm["campaign_id"] == c)].sort_values("fold")
                       [[f"recall_{op}", f"fpr_{op}"]].to_numpy() for c in cs + [CAMPAIGN1[w]]}
                tpr = np.mean([per[c][:, 0].mean() for c in cs])
                fpr = np.mean([per[c][:, 1].mean() for c in cs])
                boots = np.empty((BOOT_N, 2))
                for b in range(BOOT_N):
                    sm = [per[c][rng.integers(0, len(per[c]), len(per[c]))].mean(axis=0) for c in cs]
                    boots[b] = np.mean(sm, axis=0)
                c1 = per[CAMPAIGN1[w]].mean(axis=0)
                for p in PREVALENCES:
                    fb = np.array([f1_at(t_, f_, p) for t_, f_ in boots])
                    rows.append({"workload": w, "detector": det, "op": op, "prevalence": p, "tpr": tpr, "fpr": fpr,
                                 "f1": f1_at(tpr, fpr, p), "f1_lo": float(np.quantile(fb, 0.025)),
                                 "f1_hi": float(np.quantile(fb, 0.975)), "always_fire_f1": 2 * p / (1 + p),
                                 "f1_campaign_1": f1_at(c1[0], c1[1], p)})
    df = pd.DataFrame(rows)
    df.to_csv(I.out / "prevalence.csv", index=False)
    return df


# ------------------------------------------------------------------ 5.9.14 carryover
def carryover(I: Inputs) -> pd.DataFrame:
    """Each attempt's mean max-z score over its PRE_FAULT windows, from the fold where its block is the test block,
    grouped by the run type of the attempt before it in time. The first attempt of the campaign follows the baseline
    and is left out. Kruskal-Wallis per campaign, and on within-campaign ranks pooled over campaigns 2 and 3."""
    rows = []
    for cid in SRC.CAMPAIGNS:
        camp = I.camps[cid]
        order = camp.attempts.sort_values("onset")
        prev = dict(zip(order.index[1:], order["run_type"].to_numpy()[:-1]))
        for k in range(1, 11):
            test, _, _ = splits(I.joined(cid, "RuleBased", k))
            pf = test[(test["segment_type"] == "PRE_FAULT") & (test["label"] == "NORMAL")]
            for key, g in pf.groupby("attempt_key"):
                if key in prev:
                    rows.append({"campaign_id": cid, "attempt_key": key, "run_type": g["run_type"].iloc[0],
                                 "preceding": prev[key], "fold": k, "pre_fault_maxz": float(g["score"].mean()),
                                 "n_windows": len(g)})
    df = pd.DataFrame(rows)
    df.to_csv(I.out / "carryover_attempts.csv", index=False)
    res = []
    for cid, g in df.groupby("campaign_id"):
        groups = [x["pre_fault_maxz"].to_numpy() for _, x in g.groupby("preceding")]
        h, p = st.kruskal(*groups)
        res.append({"scope": cid, "H": float(h), "p": float(p), "n_attempts": len(g), "n_groups": len(groups),
                    **{f"median_after_{t}": float(x["pre_fault_maxz"].median()) for t, x in g.groupby("preceding")}})
    for w, cs in CONFIRMATORY.items():
        g = df[df["campaign_id"].isin(cs)].copy()
        g["rank"] = g.groupby("campaign_id")["pre_fault_maxz"].rank(pct=True)
        h, p = st.kruskal(*[x["rank"].to_numpy() for _, x in g.groupby("preceding")])
        res.append({"scope": f"{w} campaigns 2 and 3", "H": float(h), "p": float(p), "n_attempts": len(g),
                    "n_groups": g["preceding"].nunique(),
                    **{f"median_rank_after_{t}": float(x["rank"].median()) for t, x in g.groupby("preceding")}})
    out = pd.DataFrame(res)
    out.to_csv(I.out / "carryover.csv", index=False)
    return out


# ------------------------------------------------------------------ 5.9.15 load phase
COPY_SUFFIX = "-final"   # --copy-suffix; empty for the deposit's campaign/<cid>/


def load_epoch(copies: Path, cid: str) -> tuple[float, float]:
    """The load generator's cycle epoch from its status probe (2.3, item 9), read from every attempt's probes and
    required to be one value. Returns (epoch_at, worst absolute error of the reconstructed phase against the
    generator's own cycle_phase_frac)."""
    base = copies / f"{cid}{COPY_SUFFIX}" / "attempts"
    epochs, err = set(), 0.0
    for d in sorted(base.iterdir()):
        pr = pd.read_parquet(d / "probes.parquet")
        lg = pr[pr["probe"] == "loadgen"].dropna(subset=["detail"])
        for det in lg["detail"].iloc[[0, -1]]:
            x = json.loads(det)
            epochs.add(float(x["epoch_at"]))
            ph = ((x["at"] - x["epoch_at"]) % LOAD_CYCLE_S) / LOAD_CYCLE_S
            e = abs(ph - x["cycle_phase_frac"])
            err = max(err, min(e, 1 - e))
    if len(epochs) != 1:
        raise SystemExit(f"ERROR: {cid} load generator reports {len(epochs)} cycle epochs")
    return epochs.pop(), err


def rayleigh(theta: np.ndarray) -> tuple[float, float, float]:
    """Rayleigh test of uniformity on angles in radians. Returns (mean resultant length R, Z = n R^2, p) with the
    p-value approximation of Zar (2010), equation 27.4."""
    n = len(theta)
    C, S = np.cos(theta).sum(), np.sin(theta).sum()
    Rn = math.hypot(C, S)
    R = Rn / n
    Z = Rn ** 2 / n
    p = math.exp(math.sqrt(1 + 4 * n + 4 * (n ** 2 - Rn ** 2)) - (1 + 2 * n))
    return R, Z, min(1.0, p)


def load_phase(I: Inputs) -> dict:
    import statsmodels.formula.api as smf
    copies = Path(I.a.copies)
    rows, checks = [], {}
    for cid in SRC.CAMPAIGNS:
        ep, err = load_epoch(copies, cid)
        checks[cid] = {"phase_reconstruction_max_error": err}
        a = I.camps[cid].attempts
        for key, r in a.iterrows():
            rows.append({"campaign_id": cid, "attempt_key": key, "run_type": r.run_type,
                         "phase": ((r.onset - ep) % LOAD_CYCLE_S) / LOAD_CYCLE_S})
    ph = pd.DataFrame(rows)
    ph.to_csv(I.out / "load_phase_attempts.csv", index=False)
    ray = []
    for w, cs in WORKLOAD_CAMPAIGNS.items():
        for scope, sub in [("all three", ph[ph["campaign_id"].isin(cs)])] + [(c, ph[ph["campaign_id"] == c]) for c in cs]:
            for f in FAULTS + ["F0", "all"]:
                x = sub if f == "all" else sub[sub["run_type"] == f]
                R, Z, p = rayleigh(2 * np.pi * x["phase"].to_numpy())
                ray.append({"workload": w, "scope": scope, "run_type": f, "n": len(x), "R": R, "Z": Z, "p": p})
    ray = pd.DataFrame(ray)
    ray.to_csv(I.out / "load_phase_rayleigh.csv", index=False)
    # onset phase as a covariate of the per-run within-run AUC, campaigns 2 and 3
    runs = I.runs[(I.runs["run_type"] != "F0") & I.runs["detector"].isin(DETECTORS)]
    runs = runs.merge(ph[["attempt_key", "phase"]], on="attempt_key", how="left")
    cov = []
    for w, cs in CONFIRMATORY.items():
        for det in DETECTORS:
            d = runs[(runs["detector"] == det) & runs["campaign_id"].isin(cs)].copy()
            d["s"], d["c"] = np.sin(2 * np.pi * d["phase"]), np.cos(2 * np.pi * d["phase"])
            full = smf.ols("within_run_auc ~ C(run_type) + C(campaign_id) + s + c", data=d).fit()
            red = smf.ols("within_run_auc ~ C(run_type) + C(campaign_id)", data=d).fit()
            f_, p_, _ = full.compare_f_test(red)
            cov.append({"workload": w, "detector": det, "n_runs": len(d), "F_phase": float(f_), "p_phase": float(p_),
                        "coef_sin": float(full.params["s"]), "coef_cos": float(full.params["c"])})
    cov = pd.DataFrame(cov)
    cov.to_csv(I.out / "load_phase_covariate.csv", index=False)
    return {"checks": checks, "rayleigh": ray, "covariate": cov}


# ------------------------------------------------------------------ 5.9.8 random forest
def rf_summaries(I: Inputs) -> dict:
    pb = Path(I.a.phase_b)
    imp, conf = [], []
    for cid in SRC.CAMPAIGNS:
        for k in range(1, 11):
            d = pb / cid / "w30-frozen" / "RF" / f"fold{k}"
            x = pd.read_csv(d / "rf_feature_importance.csv")
            imp.append(x.assign(campaign_id=cid, fold=k))
        for m in SRC.PANEL + ["RF"]:
            for k in range(1, 11):
                x = pd.read_csv(pb / cid / "w30-frozen" / m / f"fold{k}" / "confusion_matrices.csv")
                conf.append(x.assign(campaign_id=cid))
    imp = pd.concat(imp, ignore_index=True)
    imp["source"] = imp["feature"].str.split("__").str[1]
    imp["metric"] = imp["feature"].str.split("__").str[2].str.split(".").str[0]
    feat = (imp.groupby(["campaign_id", "feature"]).agg(mean_importance=("importance", lambda s: s.sum() / 10),
                                                         folds_in_top50=("fold", "nunique")).reset_index())
    feat = feat.sort_values(["campaign_id", "mean_importance"], ascending=[True, False])
    feat.groupby("campaign_id").head(20).to_csv(I.out / "rf_top_features.csv", index=False)
    src = imp.groupby(["campaign_id", "source"])["importance"].sum() / imp.groupby("campaign_id")["importance"].sum()
    src = src.rename("share_of_top50_importance").reset_index()
    src.to_csv(I.out / "rf_importance_by_source.csv", index=False)
    bp = imp[imp["metric"].str.startswith("jvm_buffer_pool")].groupby("campaign_id")["importance"].sum() / \
        imp.groupby("campaign_id")["importance"].sum()
    bp = bp.rename("jvm_buffer_pool_share").reset_index()
    bp.to_csv(I.out / "rf_buffer_pool_share.csv", index=False)
    conf = pd.concat(conf, ignore_index=True)
    cs = conf.groupby(["campaign_id", "model"])[["tn", "fp", "fn", "tp"]].sum().reset_index()
    cs.to_csv(I.out / "confusion_f1opt_by_campaign.csv", index=False)
    return {"top": feat, "by_source": src, "buffer_pool": bp, "confusion": cs}


# ------------------------------------------------------------------ 5.9.2 window sizes
def windows(I: Inputs) -> pd.DataFrame:
    """Pooled AUC per window size on campaign 1, from the search run (30 trials per fold at 15, 60, 120 and 300 s,
    100 at 30 s). Within-run AUC is kept only where every within-run negative is scored (decision 35)."""
    run = Path(I.a.search_run)
    rows, missing = [], []
    saved = M.WINDOW_S
    try:
        for cid in ("ec-a", "iot-a"):
            for wdir in sorted((run / cid).glob("w*"), key=lambda p: int(p.name[1:])):
                w = int(wdir.name[1:])
                M.WINDOW_S = w
                camp = M.load_campaign(I.derived, cid)
                for m in SRC.PANEL:
                    for k in range(1, 11):
                        p = wdir / m / f"fold{k}" / "scores" / f"{m}_fold{k}.parquet"
                        if not p.is_file():
                            missing.append({"campaign_id": cid, "window_s": w, "detector": m, "fold": k})
                            continue
                        j = M.check_scores(camp, pd.read_parquet(p), k, f"{m} {cid} w{w} fold {k}")
                        test = j[(j["role"] == "test") & j["label"].isin(M.EVAL_LABELS)]
                        y = (test["label"] == "ANOMALOUS").to_numpy().astype(int)
                        s = test["score"].to_numpy(np.float64)
                        wr = []
                        complete = True
                        for key, g in test[test["attempt_key"].notna() & (test["run_type"] != "F0")].groupby("attempt_key"):
                            pos = (g["label"] == "ANOMALOUS").to_numpy()
                            neg = g["within_run"].notna().to_numpy()
                            expected = camp.labels.loc[(camp.labels["attempt_key"] == key)
                                                       & camp.labels["within_run"].notna()].shape[0]
                            complete &= neg.sum() == expected and expected > 0
                            mm = pos | neg
                            wr.append(M.auc(pos[mm].astype(int), g["score"].to_numpy()[mm]))
                        rows.append({"campaign_id": cid, "window_s": w, "detector": m, "fold": k,
                                     "pooled_auc": M.auc(y, s), "n_pos": int(y.sum()), "n_test": int(len(y)),
                                     "within_run_auc": float(np.nanmean(wr)) if complete else float("nan"),
                                     "within_run_complete": bool(complete)})
    finally:
        M.WINDOW_S = saved
    df = pd.DataFrame(rows)
    df.to_csv(I.out / "windows_per_fold.csv", index=False)
    pd.DataFrame(missing).to_csv(I.out / "windows_not_estimable.csv", index=False)
    s = df.groupby(["campaign_id", "detector", "window_s"]).agg(
        pooled_auc=("pooled_auc", "mean"), se=("pooled_auc", lambda x: x.std(ddof=1) / np.sqrt(len(x))),
        n_folds=("pooled_auc", "size"), within_run_auc=("within_run_auc", "mean"),
        within_run_complete=("within_run_complete", "all"), n_pos_per_fold=("n_pos", "mean")).reset_index()
    s.to_csv(I.out / "windows.csv", index=False)
    return s


# ------------------------------------------------------------------ main
def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    for k in ("derived", "phase-b", "baselines", "search-run", "copies", "results"):
        p.add_argument(f"--{k}", required=True)
    p.add_argument("--only", nargs="*")
    p.add_argument("--copy-suffix", default="-final", help="campaign folder is <cid><suffix>")
    a = p.parse_args(argv)
    global COPY_SUFFIX
    COPY_SUFFIX = a.copy_suffix
    I = Inputs(a)
    want = set(a.only or ["replay", "drift", "per_fault", "prevalence", "carryover", "load_phase", "rf", "windows"])
    summ_path = I.out / "summary.json"
    summary = json.loads(summ_path.read_text()) if summ_path.is_file() else {}
    t0 = time.time()
    if want & {"replay", "iqr", "three_state", "lofo"}:
        r = run_replay_iqr_lofo_three(I, DETECTORS + DIAG)
        summarise_replay(r["replay"]).to_csv(I.out / "replay.csv", index=False)
        pooled_campaign_mean(r["iqr"], "f1", ["detector"]).merge(
            pooled_campaign_mean(r["iqr"], "fpr", ["detector"]), on=["workload", "detector"],
            suffixes=("_f1", "_fpr")).to_csv(I.out / "iqr.csv", index=False)
        pd.concat([pooled_campaign_mean(r["three_state"], v, ["detector", "regime"]).assign(metric=v)
                   for v in ("auc", "f1", "always_fire_f1", "prevalence")]).to_csv(I.out / "three_state.csv",
                                                                                  index=False)
        l = r["lofo"].assign(f1_cost=lambda d: d["f1_all"] - d["f1_lofo"],
                             recall_cost=lambda d: d["recall_all"] - d["recall_lofo"])
        pd.concat([pooled_campaign_mean(l, v, ["detector", "held_out"]).assign(metric=v)
                   for v in ("f1_lofo", "f1_all", "f1_cost", "recall_lofo", "recall_all")]).to_csv(
            I.out / "lofo.csv", index=False)
        summary["replay_iqr_three_state_lofo"] = {"rows": {k: len(v) for k, v in r.items()}}
    if "drift" in want:
        d = drift(I)
        summary["drift"] = {"flag_by_campaign": {c: bool(g[g["features"] == "all_features"]["drift"].any())
                                                 for c, g in d.groupby("campaign_id")}}
    if "per_fault" in want:
        per_fault(I)
    if "prevalence" in want:
        prevalence(I)
    if "carryover" in want:
        carryover(I)
    if "load_phase" in want:
        r = load_phase(I)
        summary["load_phase"] = r["checks"]
    if "rf" in want:
        rf_summaries(I)
    if "windows" in want:
        windows(I)
    summary["elapsed_s_last_run"] = round(time.time() - t0, 1)
    summ_path.write_text(json.dumps(summary, indent=1, default=str) + "\n")
    print(f"explore: {sorted(want)} in {summary['elapsed_s_last_run']} s -> {I.out}")


if __name__ == "__main__":
    main()
