#!/usr/bin/env python3
"""Exploratory analyses 5.9.5 (curated-rule variants) and 5.9.6 (the other classical detectors). Exploratory, no
adjustment (5.7.5).

Added on 18 October 2026, after registration. The choices these items leave open are recorded in
protocol/DEVIATIONS.md. Every detector writes score files in the layout of train.py, so analysis/metrics.py computes
all of 4.5 for it.

Curated rules (5.9.5), from the registered analysis/cross_workload/curated_rules_v2.py and
curated_rules_per_rule_tuned.py (commit d7521a1), on raw window values of windows_30s.parquet. R1 took the maximum of
the raw 10 s samples in a window over every matching instance. A rebuilt feature named <metric>__<scope>__max is the
maximum over the window's 10 s steps, and the tm_max scope is the maximum over TaskManagers at each step, so each signal
is the row maximum of the columns in RULE_COLUMNS.
  R1 consumer lag above 10,000 for 2 windows in a row     records_lag_max, tm_max, max
  R2 under-replicated partitions above 0                   underreplicatedpartitions, b0 to b2, max
  R3 a failed checkpoint in the window                     numberOfFailedCheckpoints_per_s, jm, max, above 0. The
                                                           rebuilt counter is already a per-second rate (4.1.2), so
                                                           R1's positive delta of the cumulative count becomes a
                                                           positive rate
  R4 backpressure above 100 ms/s for 2 windows in a row    backPressuredTimeMsPerSecond, tm_max, max
  R5 broker offline                                        offlinepartitionscount, b0 to b2, max, above 0. R1 also fired
                                                           when activeControllerCount fell below 1, a metric the rebuilt
                                                           KRaft telemetry does not export, so that half is dropped
  R6 failed rebalances above 0                             failed_rebalance_rate_per_hour, tm_max, max
  R7 heap used above 0.9 of heap committed                 Flink TaskManager Status_JVM_Memory_Heap_Used and
                                                           Heap_Committed, tm_max, max
"2 windows in a row" means the window and its predecessor in the same contiguous stretch (seq_group, seq_index), where
R1 shifted by one row over the whole test repetition. The score is the number of rules that fire. CuratedRules uses
the R1 thresholds. CuratedRules_tuned grid-searches the lag, backpressure and heap thresholds on the fold's cal block
by F1 of "any rule fires", R1's grids and default.

Classical detectors (5.9.6), the score functions of the registered analysis/cross_workload/classical_baselines.py
(commit d7521a1), unchanged, on the fold data of training/train.py, the same features as EWMA and max-z. The registered
POT returns the exceedance probability, which is small for the most extreme windows and 0 below the tail threshold, so
it is not monotone in the window's max |z|. POT_monotone is the R2 draft's fix of 13 October 2026 and is reported beside
it. LODA draws random projections from numpy's global generator, so the generator is seeded with 42 before every fit.
The fold features are float32, and MP alone gets them cast to float64 because stumpy accepts nothing else.

    python3 -m analysis.classical rules --derived DIR --campaign ec-b --out DIR --freeze-marker FILE
    python3 -m analysis.classical detectors --derived DIR --campaign ec-b --out DIR --freeze-marker FILE  (torch, pyod,
        stumpy)
    python3 -m analysis.classical summarize --derived DIR --extra DIR --results DIR --out DIR
"""
from __future__ import annotations

import argparse
import itertools
import json
import platform
import re
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

PIPE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPE))

WINDOW_S = 30
EVAL_LABELS = ("ANOMALOUS", "NORMAL", "RECOVERY", "SHAM", "SHAM_POST")
RULE_COLUMNS = {
    "consumer_lag": r"^f__consumer__flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max\..*__tm_max__max$",
    "under_repl": r"^f__broker__kafka_server_replicamanager_underreplicatedpartitions__b\d+__max$",
    "failed_ckpt_rate": r"^f__flink__flink_jobmanager_job_numberOfFailedCheckpoints_per_s\..*__jm__max$",
    "backpressure": r"^f__flink__flink_taskmanager_job_task_backPressuredTimeMsPerSecond\..*__tm_max__max$",
    "offline_part": r"^f__broker__kafka_controller_kafkacontroller_offlinepartitionscount__b\d+__max$",
    "rebal_failed": r"^f__consumer__flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_failed_rebalance_rate_per_hour\..*__tm_max__max$",
    "heap_used": r"^f__jvm__flink_taskmanager_Status_JVM_Memory_Heap_Used__tm_max__max$",
    "heap_committed": r"^f__jvm__flink_taskmanager_Status_JVM_Memory_Heap_Committed__tm_max__max$",
}
DROPPED = {"active_ctrl": "activeControllerCount is not in the rebuilt KRaft telemetry, so R5 keeps only offline "
                          "partitions"}
# curated_rules_per_rule_tuned.py grids and default
LAG_GRID = [1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000]
BP_GRID = [25, 50, 100, 200, 400, 600, 800]
HEAP_GRID = [0.70, 0.80, 0.85, 0.90, 0.95]
FIXED = (10_000, 100, 0.9)              # curated_rules_v2.py fire_rules
TUNED_DEFAULT = (LAG_GRID[3], BP_GRID[2], HEAP_GRID[3])
SPLITS = (("test", "test"), ("cal", "cal"), ("trailing", "eval_only"))
DETECTORS = ["CUSUM", "MP", "LODA", "HBOS", "COPOD", "POT", "POT_monotone"]


# ---------------------------------------------------------------- curated rules
def rule_columns(names: list[str]) -> dict:
    out = {}
    for sig, pat in RULE_COLUMNS.items():
        out[sig] = sorted(c for c in names if re.match(pat, c))
        if not out[sig]:
            raise SystemExit(f"ERROR: no rebuilt column matches the curated-rule signal {sig}")
    return out


def build_signals(f: pd.DataFrame, cols: dict) -> pd.DataFrame:
    sig = pd.DataFrame(index=f.index)
    for s, cs in cols.items():
        sig[s] = f[cs].max(axis=1)
    return sig


def previous_fired(f: pd.DataFrame, fired: np.ndarray) -> np.ndarray:
    """For each row, whether the window before it in the same contiguous stretch fired. A window with no
    predecessor in the split counts as not fired, as R1's shift(1).fillna(0) did at the start."""
    key = pd.MultiIndex.from_arrays([f["seq_group"].to_numpy(), f["seq_index"].to_numpy()])
    on = pd.Series(fired.astype(bool), index=key)
    prev = pd.MultiIndex.from_arrays([f["seq_group"].to_numpy(), f["seq_index"].to_numpy() - 1])
    return on.reindex(prev, fill_value=False).to_numpy(dtype=bool)


def fire_rules(f: pd.DataFrame, sig: pd.DataFrame, lag_t, bp_t, heap_t) -> pd.DataFrame:
    """curated_rules_per_rule_tuned.fire_rules_with_thresholds on the rebuilt signals (see the module docstring for the
    two changes, R3 and R5)."""
    fired = pd.DataFrame(index=sig.index)
    fired["R1"] = (sig["consumer_lag"] > lag_t).to_numpy()
    fired["R2"] = (sig["under_repl"] > 0).to_numpy()
    fired["R3"] = (sig["failed_ckpt_rate"] > 0).to_numpy()
    fired["R4"] = (sig["backpressure"] > bp_t).to_numpy()
    fired["R5"] = (sig["offline_part"] > 0).to_numpy()
    fired["R6"] = (sig["rebal_failed"] > 0).to_numpy()
    ratio = np.where(sig["heap_committed"].to_numpy() > 0,
                     sig["heap_used"].to_numpy() / np.where(sig["heap_committed"].to_numpy() > 0,
                                                            sig["heap_committed"].to_numpy(), 1.0), 0.0)
    fired["R7"] = ratio > heap_t
    for r in ("R1", "R4"):
        cur = fired[r].to_numpy(dtype=bool)
        fired[r] = cur & previous_fired(f, cur)
    return fired.astype(int)


def f1_any(y, fired) -> float:
    pred = fired.sum(axis=1).to_numpy() > 0
    tp = int((pred & (y == 1)).sum())
    fp = int((pred & (y == 0)).sum())
    fn = int((~pred & (y == 1)).sum())
    return 2 * tp / (2 * tp + fp + fn) if (2 * tp + fp + fn) else 0.0


def fold_masks(lab: pd.DataFrame, folds: pd.DataFrame, k: int) -> dict:
    role = lab["window_id"].map(folds[folds["fold"] == k].set_index("window_id")["role"])
    if role.isna().any():
        raise SystemExit(f"ERROR: windows without a role in fold {k}")
    L = lab["label"]
    return {"test": (role == "test") & L.isin(EVAL_LABELS), "cal": (role == "cal") & L.isin(EVAL_LABELS),
            "trailing": (role == "eval_only") & (L == "TRAILING")}


def rules_campaign(derived: Path, cid: str, out: Path, freeze_marker: str | None) -> dict:
    import pyarrow.parquet as pq
    from analysis.metrics import _check_sidecar
    from analysis.run import gate
    from streamprep.contract import sha256_file
    gate([cid], freeze_marker)
    base = derived / cid
    wp, fp = base / f"windows_{WINDOW_S}s.parquet", base / "folds.parquet"
    _check_sidecar(wp)
    _check_sidecar(fp)
    names = pq.read_schema(wp).names
    cols = rule_columns(names)
    meta = ["window_id", "label", "start_at", "seq_group", "seq_index", "within_run"]
    df = pd.read_parquet(wp, columns=meta + sorted({c for cs in cols.values() for c in cs}))
    df = df.sort_values("start_at", kind="stable").reset_index(drop=True)
    folds = pd.read_parquet(fp)
    sig_all = build_signals(df, cols)
    tuned = {}
    for k in range(1, int(folds["fold"].max()) + 1):
        m = fold_masks(df, folds, k)
        f_cal, s_cal = df[m["cal"]], sig_all[m["cal"]]
        y_cal = (f_cal["label"] == "ANOMALOUS").astype(int).to_numpy()
        best_f1, best = -1.0, TUNED_DEFAULT
        for combo in itertools.product(LAG_GRID, BP_GRID, HEAP_GRID):
            v = f1_any(y_cal, fire_rules(f_cal, s_cal, *combo))
            if v > best_f1:
                best_f1, best = v, combo
        tuned[k] = {"lag_t": best[0], "bp_t": best[1], "heap_t": best[2], "cal_f1": best_f1}
        for name, thr in (("CuratedRules", FIXED), ("CuratedRules_tuned", best)):
            rows = []
            for split, role in SPLITS:
                f = df[m[split]]
                if len(f) == 0:
                    continue
                fired = fire_rules(f, sig_all[m[split]], *thr)
                wr = f["within_run"].astype(object)
                rows.append(pd.DataFrame({"window_id": f["window_id"].to_numpy(), "fold": np.int32(k), "role": role,
                                          "model": name, "category": "all",
                                          "score": fired.sum(axis=1).to_numpy(np.float64),
                                          "within_run": wr.where(wr.notna(), None).to_numpy(),
                                          **{f"fired_{r}": fired[r].to_numpy(np.int8) for r in fired.columns}}))
            dest = out / cid / name
            dest.mkdir(parents=True, exist_ok=True)
            pd.concat(rows, ignore_index=True).to_parquet(dest / f"{name}_fold{k}.parquet", index=False)
    rec = {"campaign_id": cid, "rule_columns": cols, "dropped": DROPPED, "fixed_thresholds": FIXED,
           "tuned_thresholds": tuned, "inputs": {wp.name: sha256_file(wp), fp.name: sha256_file(fp)},
           "code": {"analysis/classical.py": sha256_file(Path(__file__))}}
    (out / cid).mkdir(parents=True, exist_ok=True)
    (out / cid / "curated_rules_run.json").write_text(json.dumps(rec, indent=1) + "\n")
    return rec


# ---------------------------------------------------------------- classical detectors
# The score functions below are analysis/cross_workload/classical_baselines.py at the registered commit d7521a1, with
# their code unchanged (score_hst is HBOS there, labelled HST in R1's outputs). tests/test_analysis_classical.py checks
# them against that commit.
def _imports():
    global PCA, stumpy, LODA, HBOS, COPOD, genpareto
    from pyod.models.copod import COPOD
    from pyod.models.hbos import HBOS
    from pyod.models.loda import LODA
    from scipy.stats import genpareto
    from sklearn.decomposition import PCA
    import stumpy


def score_cusum(X_train, X_eval, k=0.5):
    """Per-feature CUSUM positive arm; max over features."""
    mu = X_train.mean(axis=0)
    sd = X_train.std(axis=0)
    sd = np.where(sd > 1e-9, sd, 1.0)
    z = (X_eval - mu) / sd
    cs = np.zeros_like(z)
    cs[0] = np.maximum(0, z[0] - k)
    for i in range(1, len(z)):
        cs[i] = np.maximum(0, cs[i - 1] + z[i] - k)
    return np.nanmax(cs, axis=1)


def score_mp(X_train, X_eval, m=8):
    """Matrix-profile score on leading PCA component, computed on (train||eval)."""
    pca = PCA(n_components=1)
    pca.fit(X_train)
    s_full = pca.transform(np.vstack([X_train, X_eval])).ravel()
    if len(s_full) < 2 * m + 2:
        return np.zeros(len(X_eval))
    mp = stumpy.stump(s_full, m=m)
    profile = mp[:, 0].astype(float)
    if len(profile) < len(s_full):
        profile = np.concatenate([profile, np.full(len(s_full) - len(profile), np.nan)])
    eval_scores = profile[len(X_train):len(X_train) + len(X_eval)]
    return np.nan_to_num(eval_scores, nan=np.nanmean(profile))


def score_loda(X_train, X_eval):
    m = LODA(n_bins=20, n_random_cuts=100)
    m.fit(X_train)
    return m.decision_function(X_eval)


def score_hst(X_train, X_eval):
    """HBOS as a streaming histogram-based detector (HST class equivalent)."""
    m = HBOS(n_bins=10)
    m.fit(X_train)
    return m.decision_function(X_eval)


def score_copod(X_train, X_eval):
    m = COPOD()
    m.fit(X_train)
    return m.decision_function(X_eval)


def score_pot(X_train, X_eval, q=0.95):
    """POT/GPD: fit GPD on upper tail of training Mahalanobis-like score; score eval by exceedance."""
    z_train = np.abs((X_train - X_train.mean(axis=0)) / np.where(X_train.std(axis=0) > 0, X_train.std(axis=0), 1.0))
    s_train = np.nanmax(z_train, axis=1)
    z_eval = np.abs((X_eval - X_train.mean(axis=0)) / np.where(X_train.std(axis=0) > 0, X_train.std(axis=0), 1.0))
    s_eval = np.nanmax(z_eval, axis=1)
    u = float(np.quantile(s_train, q))
    excess = s_train[s_train > u] - u
    if len(excess) < 5:
        return s_eval
    try:
        c, _, scale = genpareto.fit(excess, floc=0)
        # exceedance probability for each eval window
        p_exc = np.where(s_eval > u, 1 - genpareto.cdf(s_eval - u, c, loc=0, scale=scale), 0.0)
        return p_exc
    except Exception:
        return s_eval


DETECTOR_FUNCS = {"CUSUM": "score_cusum", "MP": "score_mp", "LODA": "score_loda", "HBOS": "score_hst",
                  "COPOD": "score_copod", "POT": "score_pot"}


# score_pot_monotone is score_pot of the working-tree analysis/cross_workload/classical_baselines.py (sha256
# ac2de7ee8a9fdae54822927b7af221f05fbb6daa04402aa498527cac5ee545fb, 13 October 2026), renamed and otherwise unchanged.
def score_pot_monotone(X_train, X_eval, q=0.95):
    """Peaks over threshold on the per-window max |z|. Below the training q-quantile u the score
    is the window's quantile in the training distribution. Above u it is q plus (1 - q) times the
    GPD CDF of the excess, with the GPD fitted to the training excesses. The score therefore rises
    monotonically with max |z|."""
    mu = X_train.mean(axis=0)
    sd = np.where(X_train.std(axis=0) > 0, X_train.std(axis=0), 1.0)
    s_train = np.nanmax(np.abs((X_train - mu) / sd), axis=1)
    s_eval = np.nanmax(np.abs((X_eval - mu) / sd), axis=1)
    u = float(np.quantile(s_train, q))
    below = np.minimum(np.searchsorted(np.sort(s_train), s_eval, side="right") / len(s_train), q)
    excess = s_train[s_train > u] - u
    if len(excess) < 5:
        return np.where(s_eval > u, q + (1 - q) * (1 - np.exp(-(s_eval - u))), below)
    c, _, scale = genpareto.fit(excess, floc=0)
    return np.where(s_eval > u, q + (1 - q) * genpareto.cdf(s_eval - u, c, loc=0, scale=scale), below)


def detectors_campaign(derived: Path, cid: str, out: Path, freeze_marker: str | None, folds_only=None) -> dict:
    sys.path.insert(0, str(PIPE / "training"))
    import train as T  # noqa: E402  the registered training code, unchanged
    from analysis.baselines import gate
    from streamprep.contract import sha256_file
    from streamprep.fold_fit import feature_columns
    _imports()
    funcs = {d: globals()[f] for d, f in DETECTOR_FUNCS.items()}
    funcs["POT_monotone"] = score_pot_monotone
    cfg = T.load_training_config(None)
    idx = gate(cid, cfg, freeze_marker)
    base = derived / cid
    wp, fp = base / f"windows_{WINDOW_S}s.parquet", base / "folds.parquet"
    df, folds = T.load_inputs(wp, fp, cfg["_pipeline"])
    cols = feature_columns(df)
    fold_list = folds_only or list(range(1, int(folds["fold"].max()) + 1))
    t0, times = time.time(), {}
    for k in fold_list:
        T.validate_data(df, folds, k)
        d = T.prepare_fold_data(df, folds, k, cols, cfg)
        tr = d["X_normal"]
        for name, fn in funcs.items():
            rows, t1 = [], time.time()
            for split, role in SPLITS:
                X, f = d[f"X_{split}"], d[f"df_{split}"]
                if len(f) == 0:
                    continue
                if name == "LODA":
                    np.random.seed(42)
                # streamprep.fold_fit returns float32 features. stumpy accepts float64 only, so MP gets the same values
                # as float64. Every other detector gets them as they are, like EWMA and max-z.
                a_, b_ = (tr.astype(np.float64), X.astype(np.float64)) if name == "MP" else (tr, X)
                s = np.asarray(fn(a_, b_), dtype=np.float64)
                if len(s) != len(f) or not np.isfinite(s).all():
                    raise SystemExit(f"ERROR: {name} {cid} fold {k} {split}: {len(s)} scores, "
                                     f"{int((~np.isfinite(s)).sum())} not finite")
                wr = f["within_run"].astype(object)
                rows.append(pd.DataFrame({"window_id": f["window_id"].to_numpy(), "fold": np.int32(k), "role": role,
                                          "model": name, "category": "all", "score": s,
                                          "within_run": wr.where(wr.notna(), None).to_numpy()}))
            times.setdefault(name, []).append(round(time.time() - t1, 2))
            dest = out / cid / name
            dest.mkdir(parents=True, exist_ok=True)
            pd.concat(rows, ignore_index=True).to_parquet(dest / f"{name}_fold{k}.parquet", index=False)
    import pyod
    import stumpy as _st
    import numba
    rec = {"campaign_id": cid, "campaign_index": idx, "detectors": list(funcs), "seconds_per_fold": times,
           "elapsed_s": round(time.time() - t0, 1), "inputs": {wp.name: sha256_file(wp), fp.name: sha256_file(fp)},
           "code": {"analysis/classical.py": sha256_file(Path(__file__)),
                    "training/train.py": sha256_file(PIPE / "training" / "train.py")},
           "versions": {"python": platform.python_version(), "pyod": pyod.__version__, "stumpy": _st.__version__,
                        "numba": numba.__version__, "numpy": np.__version__, "pandas": pd.__version__},
           "machine": platform.machine()}
    (out / cid).mkdir(parents=True, exist_ok=True)
    (out / cid / "classical_run.json").write_text(json.dumps(rec, indent=1) + "\n")
    return rec


# ---------------------------------------------------------------- summary
def summarize(derived: Path, extra: Path, results: Path, out: Path, with_detectors: bool = True):
    from analysis import metrics as M
    from analysis import stats as S
    from analysis.confirm import CONFIRMATORY, WORKLOADS
    from analysis.sources import CAMPAIGNS
    out.mkdir(parents=True, exist_ok=True)
    names = ["CuratedRules", "CuratedRules_tuned"] + (DETECTORS if with_detectors else [])
    srcs = [{"campaign_id": c, "detector": n, "family": "classical", "fold": k, "run": "classical",
             "path": str(extra / c / n / f"{n}_fold{k}.parquet")}
            for c in CAMPAIGNS for n in names for k in range(1, 11)]
    fm, runs, hashes = M.compute(derived, srcs)
    # the rules' own operating point, "any rule fires" (score at or above 1)
    native = []
    camps = {c: M.load_campaign(derived, c) for c in CAMPAIGNS}
    for s in srcs:
        if not s["detector"].startswith("CuratedRules"):
            continue
        sc = pd.read_parquet(s["path"])
        t = sc[sc["role"] == "test"]
        lab = camps[s["campaign_id"]].labels.loc[t["window_id"], "label"].to_numpy()
        ev = np.isin(lab, M.EVAL_LABELS)
        y = lab[ev] == "ANOMALOUS"
        neg = ev & (lab != "ANOMALOUS")
        al = t["score"].to_numpy() >= 1
        tp, fp, fn = int((al[ev] & y).sum()), int((al[ev] & ~y).sum()), int((~al[ev] & y).sum())
        row = {"campaign_id": s["campaign_id"], "detector": s["detector"], "fold": s["fold"],
               "native_f1": 2 * tp / (2 * tp + fp + fn) if (2 * tp + fp + fn) else 0.0,
               "native_recall": tp / (tp + fn) if (tp + fn) else 0.0,
               "native_fpr": float(al[neg].mean()) if neg.any() else float("nan")}
        for r in [c for c in t.columns if c.startswith("fired_")]:
            row[f"rate_{r}"] = float(t[r].to_numpy()[ev].mean())
        native.append(row)
    native = pd.DataFrame(native)
    fm = fm.merge(native, on=["campaign_id", "detector", "fold"], how="left")
    fm.to_parquet(out / "classical_fold_metrics.parquet", index=False)
    runs.to_parquet(out / "classical_run_metrics.parquet", index=False)
    main_fm = pd.read_parquet(results / "fold_metrics.parquet")
    both = pd.concat([fm, main_fm[main_fm["detector"].isin(["RuleBased", "EWMA"])]], ignore_index=True)
    metrics_cols = ["pooled_auc", "within_run_auc", "within_run_pc", "pr_auc", "f1_f1opt", "always_fire_f1_f1opt",
                    "fpr_fpb_0.05", "trailing_fa_per_h_fpb_0.05", "episode_rate_fpb_0.05", "native_f1", "native_fpr"]
    rows = []
    for w, cs in WORKLOADS.items():
        for n in names:
            x = fm[(fm["detector"] == n) & fm["campaign_id"].isin(cs)]
            row = {"workload": w, "detector": n}
            for c in cs:
                row.update({f"{m}_{c}": float(x[x["campaign_id"] == c][m].mean()) for m in metrics_cols if m in x})
            for ref in ("RuleBased",):
                for m in ("pooled_auc", "within_run_auc"):
                    ds = {c: (both[(both["detector"] == n) & (both["campaign_id"] == c)].set_index("fold")[m].sort_index()
                              - both[(both["detector"] == ref) & (both["campaign_id"] == c)].set_index("fold")[m]
                              .sort_index()).to_numpy() for c in CONFIRMATORY[w]}
                    p = S.pooled(ds)
                    row.update({f"d_{m}_vs_{ref}": p["mean"], f"d_{m}_vs_{ref}_lo": p["ci95_lo"],
                                f"d_{m}_vs_{ref}_hi": p["ci95_hi"]})
            rows.append(row)
    summ = pd.DataFrame(rows)
    summ.to_csv(out / "classical_summary.csv", index=False)
    (out / "inputs_sha256.json").write_text(json.dumps(dict(sorted(hashes.items())), indent=1) + "\n")
    with pd.option_context("display.width", 250, "display.max_columns", 60):
        (out / "classical.txt").write_text(
            "CURATED RULES (5.9.5) AND OTHER CLASSICAL DETECTORS (5.9.6), exploratory. Means over ten folds per "
            "campaign. Differences against max-z are pooled over campaigns 2 and 3 with t-based 95% intervals.\n\n"
            + summ.round(3).to_string(index=False) + "\n")
    return summ


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    for cmd in ("rules", "detectors"):
        s = sub.add_parser(cmd)
        s.add_argument("--derived", required=True, type=Path)
        s.add_argument("--campaign", required=True)
        s.add_argument("--out", required=True, type=Path)
        s.add_argument("--freeze-marker", default=None)
        if cmd == "detectors":
            s.add_argument("--fold", type=int, action="append")
    m = sub.add_parser("summarize")
    for a in ("--derived", "--extra", "--results", "--out"):
        m.add_argument(a, required=True, type=Path)
    m.add_argument("--rules-only", action="store_true")
    a = p.parse_args(argv)
    if a.cmd == "rules":
        rules_campaign(a.derived, a.campaign, a.out, a.freeze_marker)
        print(f"{a.campaign}: curated rules written")
    elif a.cmd == "detectors":
        rec = detectors_campaign(a.derived, a.campaign, a.out, a.freeze_marker, a.fold)
        print(f"{a.campaign}: {len(rec['detectors'])} detectors in {rec['elapsed_s']} s")
    else:
        summarize(a.derived, a.extra, a.results, a.out, not a.rules_only)
        print(f"wrote {a.out}")


if __name__ == "__main__":
    main()
