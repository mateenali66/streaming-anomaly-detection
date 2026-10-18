#!/usr/bin/env python3
"""Reference and diagnostic scores that need no training (protocol 4.4 and 6.3), in the score-file layout of train.py.

Added on 18 October 2026, after registration, to carry out the registered references with the registered code. The
choices the protocol leaves open are recorded as D4 in protocol/DEVIATIONS.md.

Per campaign at the 30 s anchor and per fold, every detector here sees the fold data of training/train.py
(prepare_fold_data, imported unchanged): the selection, imputation and scaling every panel model gets, fitted on the
fold's train NORMAL windows, which train.py keeps in time order.

  RuleBased        static max-z, train.py train_rule_based and score_rule_based unchanged (4.4)
  EWMA             analysis/cross_workload/classical_baselines.py score_ewma, its code unchanged. The centre is the last
                   smoothed value (alpha 0.3) of the train NORMAL windows and the scale is the time-average of their
                   EWMA standard deviation, so the reference does not move over the scored windows
  EWMA_train_mean  the same score with the centre at the train NORMAL mean, ewma_mechanism.py variant
                   centre_train_mean, the comparator of C2b (5.8)
  TimeOnly         the window's start time (4.4.1)
  PhasePosition    seconds since the start of the attempt's PRE_FAULT segment (4.4.2). It has no value on TRAILING
                   windows, so they get no row
Always-fire (4.4.3) needs no score.

    python3 analysis/baselines.py --derived DIR --campaign ec-b --freeze-marker ../frozen/FREEZE.json --out DIR

writes DIR/<campaign>/<detector>/<detector>_fold<k>.parquet (window_id, fold, role, model, category, score,
within_run) and DIR/<campaign>/baselines_run.json with the input and code hashes.
"""
from __future__ import annotations

import argparse
import json
import platform
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

PIPE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPE / "training"))
sys.path.insert(0, str(PIPE))
import train as T  # noqa: E402  the registered training code, unchanged
from streamprep.contract import campaign_index_from_id, sha256_file  # noqa: E402
from streamprep.fold_fit import feature_columns  # noqa: E402

WINDOW_S = 30
DETECTORS = ["RuleBased", "EWMA", "EWMA_train_mean", "TimeOnly", "PhasePosition"]
SPLITS = (("test", "test"), ("cal", "cal"), ("trailing", "eval_only"))


# The body of score_ewma is analysis/cross_workload/classical_baselines.py score_ewma at the registered commit.
# tests/test_analysis_baselines.py compares the two.
def score_ewma(X_train, X_eval, alpha=0.3):
    """Per-feature EWMA-residual z-score. Returns max-feature score per row."""
    mu = pd.DataFrame(X_train).ewm(alpha=alpha, adjust=False).mean().to_numpy()
    sd = pd.DataFrame(X_train).ewm(alpha=alpha, adjust=False).std().to_numpy()
    sd_global = np.nanmean(sd, axis=0)
    sd_global = np.where(sd_global > 1e-9, sd_global, 1.0)
    mu_last = mu[-1]
    z = np.abs(X_eval - mu_last) / sd_global
    return np.nanmax(z, axis=1)


def score_ewma_train_mean(X_train, X_eval, alpha=0.3):
    """score_ewma with its centre moved from the last smoothed train value to the train mean, the scale unchanged.
    It equals analysis/cross_workload/ewma_mechanism.py score_variant("centre_train_mean")."""
    sd = pd.DataFrame(X_train).ewm(alpha=alpha, adjust=False).std().to_numpy()
    sd_global = np.nanmean(sd, axis=0)
    sd_global = np.where(sd_global > 1e-9, sd_global, 1.0)
    z = np.abs(X_eval - X_train.mean(axis=0)) / sd_global
    return np.nanmax(z, axis=1)


def gate(cid: str, cfg: dict, freeze_marker: str | None) -> int:
    """A replicate campaign is not read before the freeze tag exists (6.1, 7.1), as in train.py."""
    _wl, idx = campaign_index_from_id(cid)
    if idx not in (0, int(cfg["search_campaign_index"])):
        if not freeze_marker or not Path(freeze_marker).is_file() or Path(freeze_marker).stat().st_size == 0:
            raise SystemExit(f"ERROR: {cid} is a replicate campaign. It is not read before the freeze tag exists "
                             "(6.1, 7.1). Pass --freeze-marker.")
    return idx


def pre_fault_starts(timeline_path: Path) -> pd.Series:
    T._check_sidecar(timeline_path)
    tl = pd.read_parquet(timeline_path)
    pf = tl[tl["segment_type"] == "PRE_FAULT"]
    if pf["attempt_key"].duplicated().any():
        raise SystemExit(f"ERROR: {timeline_path} has an attempt with more than one PRE_FAULT segment")
    return pf.set_index("attempt_key")["start_at"]


def fold_scores(d: dict, fold_id: int, pre_start: pd.Series) -> dict[str, pd.DataFrame]:
    Xn = d["X_normal"]
    rb_mean, rb_std = T.train_rule_based(Xn)
    scorers = {
        "RuleBased": lambda X, f: T.score_rule_based(X, rb_mean, rb_std),
        "EWMA": lambda X, f: score_ewma(Xn, X),
        "EWMA_train_mean": lambda X, f: score_ewma_train_mean(Xn, X),
        "TimeOnly": lambda X, f: f["start_at"].to_numpy(np.float64),
        "PhasePosition": lambda X, f: (f["start_at"] - f["attempt_key"].map(pre_start)).to_numpy(np.float64),
    }
    out = {}
    for name, fn in scorers.items():
        rows = []
        for split, role in SPLITS:
            X, f = d[f"X_{split}"], d[f"df_{split}"]
            if len(f) == 0 or (name == "PhasePosition" and split == "trailing"):
                continue
            s = np.asarray(fn(X, f), dtype=np.float64)
            if len(s) != len(f) or not np.isfinite(s).all():
                raise SystemExit(f"ERROR: {name} fold {fold_id} {split}: {len(s)} scores for {len(f)} windows, "
                                 f"{int((~np.isfinite(s)).sum())} not finite")
            wr = f["within_run"].astype(object)
            rows.append(pd.DataFrame({"window_id": f["window_id"].to_numpy(), "fold": np.int32(fold_id),
                                      "role": role, "model": name, "category": "all", "score": s,
                                      "within_run": wr.where(wr.notna(), None).to_numpy()}))
        out[name] = pd.concat(rows, ignore_index=True)
    return out


def run(derived: Path, cid: str, out: Path, freeze_marker: str | None, folds_only: list[int] | None = None) -> dict:
    cfg = T.load_training_config(None)
    idx = gate(cid, cfg, freeze_marker)
    base = derived / cid
    windows, folds_path, timeline = base / f"windows_{WINDOW_S}s.parquet", base / "folds.parquet", base / "timeline.parquet"
    df, folds = T.load_inputs(windows, folds_path, cfg["_pipeline"])
    if df["campaign_id"].iloc[0] != cid or int(df["window_s"].iloc[0]) != WINDOW_S:
        raise SystemExit(f"ERROR: {windows} is not {cid} at {WINDOW_S} s")
    pre_start = pre_fault_starts(timeline)
    cols = feature_columns(df)
    n_folds = int(folds["fold"].max())
    fold_list = folds_only or list(range(1, n_folds + 1))
    t0 = time.time()
    for k in fold_list:
        T.validate_data(df, folds, k)
        d = T.prepare_fold_data(df, folds, k, cols, cfg)
        for name, frame in fold_scores(d, k, pre_start).items():
            dest = out / cid / name
            dest.mkdir(parents=True, exist_ok=True)
            frame.to_parquet(dest / f"{name}_fold{k}.parquet", index=False)
    rec = {"campaign_id": cid, "campaign_index": idx, "window_s": WINDOW_S, "folds": fold_list,
           "detectors": DETECTORS, "elapsed_s": round(time.time() - t0, 1),
           "inputs": {p.name: sha256_file(p) for p in (windows, folds_path, timeline)},
           "code": {"analysis/baselines.py": sha256_file(Path(__file__)),
                    "training/train.py": sha256_file(PIPE / "training" / "train.py")},
           "pipeline_config_sha256": cfg["pipeline_config_sha256"],
           "freeze_marker_sha256": sha256_file(Path(freeze_marker)) if freeze_marker else None,
           "python": platform.python_version(), "machine": platform.machine(), "system": platform.system(),
           "numpy": np.__version__, "pandas": pd.__version__}
    (out / cid).mkdir(parents=True, exist_ok=True)
    (out / cid / "baselines_run.json").write_text(json.dumps(rec, indent=1) + "\n")
    return rec


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--derived", required=True, type=Path, help="folder holding derived/<campaign_id>/")
    p.add_argument("--campaign", required=True)
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--freeze-marker", default=None)
    p.add_argument("--fold", type=int, action="append", help="limit to these folds (tests)")
    a = p.parse_args(argv)
    rec = run(a.derived, a.campaign, a.out, a.freeze_marker, a.fold)
    print(f"{a.campaign}: {len(rec['folds'])} folds x {len(DETECTORS)} detectors in {rec['elapsed_s']} s")


if __name__ == "__main__":
    main()
