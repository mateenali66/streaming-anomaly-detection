#!/usr/bin/env python3
"""EWMA on the earlier manuscript's inputs (exploratory, added after review on 18 October 2026, D6 addendum).

D4 gives EWMA the panel's per-fold features. The earlier manuscript gave it every feature column with gaps set to 0
and its own StandardScaler fitted on the training NORMAL windows (analysis/cross_workload/classical_baselines.py,
evaluate_workload). This scores EWMA that way, with score_ewma unchanged, so the two input choices can be compared.
The fold roles and label masks are those of training/train.py.

    python3 -m analysis.ewma_r1_inputs --derived DIR --campaign ec-b --out DIR --freeze-marker FILE
writes DIR/<campaign>/EWMA_r1_inputs/EWMA_r1_inputs_fold<k>.parquet in the score-file layout of train.py.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

PIPE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPE / "training"))
sys.path.insert(0, str(PIPE))
import train as T  # noqa: E402  load_inputs, fold_roles and the label sets, unchanged
from analysis.baselines import gate, score_ewma  # noqa: E402
from streamprep.fold_fit import feature_columns  # noqa: E402

NAME = "EWMA_r1_inputs"
EVAL = list(T.EVAL_LABELS)


def run(derived: Path, cid: str, out: Path, freeze_marker: str | None) -> float:
    cfg = T.load_training_config(None)
    gate(cid, cfg, freeze_marker)
    base = derived / cid
    df, folds = T.load_inputs(base / "windows_30s.parquet", base / "folds.parquet", cfg["_pipeline"])
    cols = feature_columns(df)
    X_all = df[cols].fillna(0).to_numpy(np.float64)          # R1: every column, gaps set to 0
    lab = df["label"]
    t0 = time.time()
    dest = out / cid / NAME
    dest.mkdir(parents=True, exist_ok=True)
    for k in range(1, int(folds["fold"].max()) + 1):
        role = T.fold_roles(df, folds, k)
        normal = ((role == "train") & (lab == "NORMAL")).to_numpy()
        scaler = StandardScaler().fit(X_all[normal])            # R1: its own scaler on training NORMAL
        Xtr = scaler.transform(X_all[normal])
        rows = []
        for r, mask in (("test", (role == "test") & lab.isin(EVAL)), ("cal", (role == "cal") & lab.isin(EVAL)),
                        ("eval_only", (role == "eval_only") & (lab == "TRAILING"))):
            m = mask.to_numpy()
            if not m.any():
                continue
            s = score_ewma(Xtr, scaler.transform(X_all[m]))
            f = df.loc[m]
            wr = f["within_run"].astype(object)
            rows.append(pd.DataFrame({"window_id": f["window_id"].to_numpy(), "fold": np.int32(k), "role": r,
                                      "model": NAME, "category": "all", "score": s.astype(float),
                                      "within_run": wr.where(wr.notna(), None).to_numpy()}))
        pd.concat(rows, ignore_index=True).to_parquet(dest / f"{NAME}_fold{k}.parquet", index=False)
    return time.time() - t0


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--derived", required=True, type=Path)
    p.add_argument("--campaign", required=True)
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--freeze-marker", default=None)
    a = p.parse_args(argv)
    print(f"{a.campaign}: {NAME} in {run(a.derived, a.campaign, a.out, a.freeze_marker):.0f} s")


if __name__ == "__main__":
    main()
