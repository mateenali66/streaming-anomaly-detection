#!/usr/bin/env python3
"""Per-fold metrics of protocol 4.5 from stored per-window scores (make metrics, 7.1 step 7).

Added on 18 October 2026, after registration. The choices the protocol leaves open are recorded as D4 in
protocol/DEVIATIONS.md. Labels, roles and within-run tags come from the preprocessing outputs (11.9), never from a
score file, and every score file is checked against them.

For one campaign, detector and fold, on the fold's test block:
  pooled_auc        ANOMALOUS against NORMAL, RECOVERY, SHAM and SHAM_POST (4.5.1)
  within_run_auc    per fault run, its ANOMALOUS windows against its 10 within-run negatives, mean over the block's
                    seven fault runs (4.5.2)
  sham_placebo      the same on the block's sham, SHAM windows against its own 10 within-run negatives (4.5.3)
  within_run_pc     within_run_auc minus sham_placebo
  r1_placebo        per fault run, its 5 late within-run negatives against its 5 early ones, mean over the seven runs
  pr_auc            average precision, with the prevalence beside it (4.5.4)
At each operating point (fixed budgets b of training/config.json fp_budgets: the (1 - b) quantile of the cal block's
NORMAL scores, as train.py computes it, 4.5.5; and train.py find_optimal_threshold on cal, 4.5.6), a window is in alert
when its score is at or above the threshold, as in train.py:
  precision, recall, f1, always_fire_f1 = 2p/(1+p) at the test prevalence p
  fpr               realised false-positive rate on the test negatives, which is also the test duty cycle (4.5.8)
  fa_per_h          alert onsets on negative windows per hour of negative window time, on the test block and on the
                    trailing baseline; an onset is an alerted window whose predecessor in its seq_group is not in alert
                    or not scored
  trailing_duty     share of TRAILING windows in alert
  episode_rate      share of the seven fault runs with an alert in the labelled span (4.5.9)
  sham_episode      1 if the block's sham has an alert among its SHAM windows
Latency (4.5.10) is per fault run at the fixed budgets, with train.py evaluate_detection_latency's convention: the
first alerted ANOMALOUS window's index times the window length, censored at the span's window count times the window
length. latency_km() turns the per-run rows into Kaplan-Meier summaries.

    python3 analysis/metrics.py --derived DIR --scores SPEC.json --out DIR

SPEC.json lists score sources (see sources.py). Writes fold_metrics.parquet, run_metrics.parquet and
inputs_sha256.json.
"""
from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score

PIPE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPE))
from streamprep.contract import sha256_file  # noqa: E402

WINDOW_S = 30
NEG_LABELS = ("NORMAL", "RECOVERY", "SHAM", "SHAM_POST")
EVAL_LABELS = ("ANOMALOUS",) + NEG_LABELS
FAULTS = ["F1", "F2", "F4", "F5", "F6", "F7", "F8"]      # training/train.py FAULTS


# The two functions below are training/train.py _check_sidecar and find_optimal_threshold, copied so the analysis runs
# without PyTorch, which train.py imports at module level. tests/test_analysis_metrics.py checks they are identical.
def _check_sidecar(path: Path):
    side = path.parent / (path.name + ".sha256")
    if not side.is_file():
        raise SystemExit(f"ERROR: {side} is missing; outputs must carry their hash (11.9)")
    if side.read_text().strip() != sha256_file(path):
        raise SystemExit(f"ERROR: {path} does not match {side}")


def find_optimal_threshold(scores, y_true):
    """F1-optimal threshold over the observed calibration scores (Review 4 MIN-8).

    Every distinct observed score is a candidate, so the search always spans the scores, whatever their range. A
    window is flagged when its score is at or above the threshold. The lowest threshold with the highest F1 is kept,
    as R1's ascending grid did. R1 searched the fixed grid 0.01 to 0.99, which sits below almost every RuleBased score
    (max |z|), so its RuleBased F1 was the always-fire F1. With no positive window F1 is undefined, and the threshold
    is +inf, which flags nothing.
    """
    scores = np.asarray(scores, dtype=float)
    y = np.asarray(y_true).astype(int)
    if len(scores) == 0 or len(y) == 0:
        return 0.5
    if y.sum() == 0:
        return float("inf")
    ok = ~np.isnan(scores)
    cand = np.unique(scores[ok])
    pos = np.sort(scores[ok & (y == 1)])
    neg = np.sort(scores[ok & (y == 0)])
    tp = len(pos) - np.searchsorted(pos, cand, side="left")
    fp = len(neg) - np.searchsorted(neg, cand, side="left")
    fn = int(y.sum()) - tp
    f1 = 2.0 * tp / np.maximum(2.0 * tp + fp + fn, 1.0)
    return float(cand[int(np.argmax(f1))])
LABEL_COLS = ["window_id", "label", "segment_type", "attempt_key", "run_type", "block", "position", "within_run",
              "start_at", "end_at", "seq_group", "seq_index", "flags", "outcome"]


def _cfg_budgets() -> list[float]:
    return [float(b) for b in json.loads((PIPE / "training" / "config.json").read_text())["fp_budgets"]]


FP_BUDGETS = _cfg_budgets()


@dataclass
class Campaign:
    cid: str
    labels: pd.DataFrame        # indexed by window_id
    folds: pd.DataFrame         # window_id, fold, role, at WINDOW_S only
    attempts: pd.DataFrame      # indexed by attempt_key: run_type, block, position, onset, labelled_end, pre_start
    test_block: dict            # fold -> block
    inputs: dict                # file name -> sha256


def load_campaign(derived: Path, cid: str) -> Campaign:
    base = derived / cid
    paths = {"labels": base / f"labels_{WINDOW_S}s.parquet", "folds": base / "folds.parquet",
             "timeline": base / "timeline.parquet"}
    for p in paths.values():
        _check_sidecar(p)
    lab = pd.read_parquet(paths["labels"], columns=LABEL_COLS)
    if lab["window_id"].duplicated().any():
        raise SystemExit(f"ERROR: {cid} labels repeat a window_id")
    lab = lab.set_index("window_id")
    folds = pd.read_parquet(paths["folds"])
    folds = folds[folds["window_id"].isin(lab.index)].reset_index(drop=True)
    tl = pd.read_parquet(paths["timeline"])
    att = tl[tl["attempt_key"].notna()]
    inj = att[att["segment_type"] == "INJECTION"].set_index("attempt_key")
    pre = att[att["segment_type"] == "PRE_FAULT"].set_index("attempt_key")
    # labelled attempts only: a failed attempt's windows are EXCLUDED (3.3) and it may have no INJECTION segment
    a = lab[lab["attempt_key"].notna() & lab["label"].isin(EVAL_LABELS)].groupby("attempt_key").agg(
        run_type=("run_type", "first"), block=("block", "first"), position=("position", "first"))
    a["onset"] = inj["start_at"].reindex(a.index)
    a["labelled_end"] = inj["end_at"].reindex(a.index)
    a["pre_start"] = pre["start_at"].reindex(a.index)
    if a[["onset", "labelled_end", "pre_start"]].isna().any().any():
        raise SystemExit(f"ERROR: {cid} has a labelled attempt without its PRE_FAULT or INJECTION segment")
    test_block = {}
    for k, g in folds[folds["role"] == "test"].groupby("fold"):
        b = lab.loc[g["window_id"], "block"].unique()
        if len(b) != 1:
            raise SystemExit(f"ERROR: {cid} fold {k} test role spans blocks {sorted(b)}")
        test_block[int(k)] = int(b[0])
    return Campaign(cid, lab, folds, a, test_block, {p.name: sha256_file(p) for p in paths.values()})


def check_scores(camp: Campaign, sc: pd.DataFrame, fold: int, where: str) -> pd.DataFrame:
    """A score file must hold one fold, roles equal to folds.parquet, within-run tags equal to the labels, finite
    scores and no repeated window. Returns it joined to the labels."""
    if sc["window_id"].duplicated().any():
        raise SystemExit(f"ERROR: {where} repeats a window_id")
    if set(sc["fold"].unique()) != {fold}:
        raise SystemExit(f"ERROR: {where} holds folds {sorted(sc['fold'].unique())}, expected {fold}")
    if not sc["window_id"].isin(camp.labels.index).all():
        raise SystemExit(f"ERROR: {where} scores windows that are not in {camp.cid}")
    if not np.isfinite(sc["score"].to_numpy(np.float64)).all():
        raise SystemExit(f"ERROR: {where} has scores that are not finite")
    role = camp.folds[camp.folds["fold"] == fold].set_index("window_id")["role"]
    if not (sc["role"].to_numpy() == role.reindex(sc["window_id"]).to_numpy()).all():
        raise SystemExit(f"ERROR: {where} roles disagree with folds.parquet")
    lab = camp.labels.loc[sc["window_id"]]
    got = sc["within_run"].astype(object).where(sc["within_run"].notna(), "-").to_numpy()
    want = lab["within_run"].astype(object).where(lab["within_run"].notna(), "-").to_numpy()
    if not (got == want).all():
        raise SystemExit(f"ERROR: {where} within_run tags disagree with the labels")
    out = sc[["window_id", "role", "score"]].reset_index(drop=True)
    return pd.concat([out, lab.drop(columns=["within_run"]).reset_index(drop=True),
                      sc["within_run"].reset_index(drop=True)], axis=1)


def auc(y: np.ndarray, s: np.ndarray) -> float:
    y = np.asarray(y).astype(int)
    if len(y) == 0 or y.min() == y.max():
        return float("nan")
    return float(roc_auc_score(y, s))


def onsets(frame: pd.DataFrame, alert: np.ndarray) -> np.ndarray:
    """True where a window is in alert and its predecessor in the same seq_group (seq_index minus 1) is not in alert
    or not scored."""
    key = pd.MultiIndex.from_arrays([frame["seq_group"].to_numpy(), frame["seq_index"].to_numpy()])
    on = pd.Series(alert, index=key)
    prev = pd.MultiIndex.from_arrays([frame["seq_group"].to_numpy(), frame["seq_index"].to_numpy() - 1])
    prev_alert = on.reindex(prev, fill_value=False).to_numpy(dtype=bool)
    return alert & ~prev_alert


def run_rows(test: pd.DataFrame, alerts: dict[str, np.ndarray]) -> list[dict]:
    """Per attempt of the test block: within-run AUC, R1 placebo, and detection and latency at each threshold."""
    rows = []
    for key, g in test[test["attempt_key"].notna()].groupby("attempt_key", sort=True):
        rt = g["run_type"].iloc[0]
        wr = g["within_run"].notna().to_numpy()
        pos_label = "SHAM" if rt == "F0" else "ANOMALOUS"
        pos = (g["label"] == pos_label).to_numpy()
        s = g["score"].to_numpy(np.float64)
        m = pos | wr
        r = {"attempt_key": key, "run_type": rt, "block": int(g["block"].iloc[0]),
             "position": int(g["position"].iloc[0]), "n_pos": int(pos.sum()), "n_within_run": int(wr.sum()),
             "flags": g["flags"].iloc[0], "outcome": g["outcome"].iloc[0],
             "within_run_auc": auc(pos[m], s[m])}
        late = (g["within_run"] == "late").to_numpy()
        early = (g["within_run"] == "early").to_numpy()
        r["r1_placebo"] = auc(late[late | early], s[late | early])
        span = g[pos].sort_values("start_at", kind="stable")   # the labelled span, in time order
        for name, al in alerts.items():
            hits = al[span.index.to_numpy()]    # test is reset to 0..n-1, so the index is the position in al
            det = bool(hits.any())
            first = int(np.argmax(hits)) if det else -1
            r[f"detected_{name}"] = det
            r[f"latency_s_{name}"] = float(first * WINDOW_S) if det else float(len(span) * WINDOW_S)
            r[f"span_s_{name}"] = float(len(span) * WINDOW_S)
        rows.append(r)
    return rows


def operating_point(test, trailing, thr) -> tuple[dict, np.ndarray, np.ndarray]:
    y = (test["label"] == "ANOMALOUS").to_numpy()
    neg = test["label"].isin(NEG_LABELS).to_numpy()
    s = test["score"].to_numpy(np.float64)
    al = s >= thr
    tp, fp = int((al & y).sum()), int((al & neg).sum())
    fn = int((~al & y).sum())
    p = y.mean()
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    on = onsets(test, al)
    out = {"threshold": float(thr), "precision": prec, "recall": rec,
           "f1": 2 * tp / (2 * tp + fp + fn) if (2 * tp + fp + fn) else 0.0,
           "always_fire_f1": 2 * p / (1 + p) if p > 0 else 0.0,
           "fpr": fp / neg.sum() if neg.sum() else float("nan"),
           "fa_per_h": float((on & neg).sum()) / (neg.sum() * WINDOW_S / 3600.0) if neg.sum() else float("nan")}
    al_tr = np.zeros(0, dtype=bool)
    if len(trailing):
        al_tr = trailing["score"].to_numpy(np.float64) >= thr
        on_tr = onsets(trailing, al_tr)
        out["trailing_fa_per_h"] = float(on_tr.sum()) / (len(trailing) * WINDOW_S / 3600.0)
        out["trailing_duty"] = float(al_tr.mean())
    else:
        out["trailing_fa_per_h"] = out["trailing_duty"] = float("nan")
    return out, al, al_tr


def fold_metrics(camp: Campaign, sc: pd.DataFrame, fold: int, where: str) -> tuple[dict, list[dict]]:
    j = check_scores(camp, sc, fold, where)
    test = j[(j["role"] == "test") & j["label"].isin(EVAL_LABELS)].reset_index(drop=True)
    cal = j[(j["role"] == "cal") & j["label"].isin(EVAL_LABELS)].reset_index(drop=True)
    trailing = j[(j["role"] == "eval_only") & (j["label"] == "TRAILING")].reset_index(drop=True)
    if (test["block"] != camp.test_block[fold]).any():
        raise SystemExit(f"ERROR: {where} test windows outside block {camp.test_block[fold]}")
    y = (test["label"] == "ANOMALOUS").to_numpy().astype(int)
    s = test["score"].to_numpy(np.float64)
    row = {"campaign_id": camp.cid, "fold": fold, "test_block": camp.test_block[fold],
           "n_test": len(test), "n_pos": int(y.sum()), "n_cal": len(cal), "n_trailing": len(trailing),
           "prevalence": float(y.mean()), "pooled_auc": auc(y, s),
           "pr_auc": float(average_precision_score(y, s)) if y.sum() else float("nan")}
    thresholds = {}
    cal_norm = cal.loc[cal["label"] == "NORMAL", "score"].to_numpy(np.float64)
    for b in FP_BUDGETS:
        thresholds[f"fpb_{b}"] = float(np.quantile(cal_norm, 1.0 - b)) if len(cal_norm) else float("nan")
    y_cal = (cal["label"] == "ANOMALOUS").to_numpy().astype(int)
    thresholds["f1opt"] = find_optimal_threshold(cal["score"].to_numpy(np.float64), y_cal) if len(cal) else 0.5
    alerts = {}
    for name, thr in thresholds.items():
        op, al, _ = operating_point(test, trailing, thr)
        alerts[name] = al
        row.update({f"{k}_{name}": v for k, v in op.items()})
    runs = run_rows(test, alerts)
    faults = [r for r in runs if r["run_type"] != "F0"]
    shams = [r for r in runs if r["run_type"] == "F0"]
    if len(faults) != len(FAULTS) or len(shams) != 1:
        raise SystemExit(f"ERROR: {where} test block holds {len(faults)} fault runs and {len(shams)} shams")
    row["within_run_auc"] = float(np.mean([r["within_run_auc"] for r in faults]))
    row["sham_placebo"] = shams[0]["within_run_auc"]
    row["within_run_pc"] = row["within_run_auc"] - row["sham_placebo"]
    row["r1_placebo"] = float(np.mean([r["r1_placebo"] for r in faults]))
    row["within_run_complete"] = all(r["n_within_run"] == 10 for r in runs)
    for name in thresholds:
        row[f"episode_rate_{name}"] = float(np.mean([r[f"detected_{name}"] for r in faults]))
        row[f"sham_episode_{name}"] = float(shams[0][f"detected_{name}"])
    # per-fault pooled AUC (5.9.1): that fault's ANOMALOUS windows against every negative of the block
    neg = test["label"].isin(NEG_LABELS).to_numpy()
    for f in FAULTS:
        pos = ((test["run_type"] == f) & (test["label"] == "ANOMALOUS")).to_numpy()
        m = pos | neg
        row[f"pooled_auc_{f}"] = auc(pos[m].astype(int), s[m])
    for r in runs:
        r.update({"campaign_id": camp.cid, "fold": fold})
    return row, runs


def km(times: np.ndarray, events: np.ndarray, tau: float) -> dict:
    """Kaplan-Meier for time to detection. S(t) is the share not yet detected. Returns the restricted mean time to
    detection up to tau, the median where S falls to 0.5 or below, and 1 - S(tau)."""
    times, events = np.asarray(times, float), np.asarray(events, bool)
    surv, curve = 1.0, [(0.0, 1.0)]
    for t in np.unique(times[events]):
        n = (times >= t).sum()
        d = (events & (times == t)).sum()
        surv *= 1.0 - d / n
        curve.append((float(t), surv))
    rmst, prev_t, prev_s = 0.0, 0.0, 1.0
    for t, s_ in curve[1:]:
        if t > tau:
            break
        rmst += (t - prev_t) * prev_s
        prev_t, prev_s = t, s_
    rmst += (tau - prev_t) * prev_s
    median = next((t for t, s_ in curve if s_ <= 0.5), float("nan"))
    s_tau = [s_ for t, s_ in curve if t <= tau][-1]
    return {"n_runs": int(len(times)), "n_detected": int(events.sum()), "detection_rate": float(events.mean()),
            "km_detected_by_tau": 1.0 - s_tau, "rmst_s": rmst, "km_median_s": median, "tau_s": float(tau)}


def latency_km(runs: pd.DataFrame, by: list[str], budget: str) -> pd.DataFrame:
    """Kaplan-Meier latency per group of fault runs at one fixed budget, tau the longest labelled span in the group."""
    rows = []
    fr = runs[runs["run_type"] != "F0"]
    for key, g in fr.groupby(by):
        k = km(g[f"latency_s_{budget}"].to_numpy(), g[f"detected_{budget}"].to_numpy(),
               float(g[f"span_s_{budget}"].max()))
        rows.append({**dict(zip(by, key if isinstance(key, tuple) else (key,))), "budget": budget, **k})
    return pd.DataFrame(rows)


def compute(derived: Path, sources: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """sources: dicts with campaign_id, detector, fold, path. Returns fold metrics, run metrics and input hashes."""
    camps, rows, runs, hashes = {}, [], [], {}
    for src in sources:
        cid = src["campaign_id"]
        if cid not in camps:
            camps[cid] = load_campaign(derived, cid)
            hashes.update({f"{cid}/{k}": v for k, v in camps[cid].inputs.items()})
        p = Path(src["path"])
        sc = pd.read_parquet(p)
        r, rr = fold_metrics(camps[cid], sc, int(src["fold"]), f"{src['detector']} {cid} fold {src['fold']}")
        meta = {"detector": src["detector"], "family": src.get("family", ""), "source": src.get("run", "")}
        rows.append({**meta, **r})
        runs.extend({**meta, **x} for x in rr)
        hashes[str(p)] = sha256_file(p)
    return pd.DataFrame(rows), pd.DataFrame(runs), hashes
