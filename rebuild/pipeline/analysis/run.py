#!/usr/bin/env python3
"""make metrics and make stats (protocol 7.1 steps 7 and 8) from stored scores.

    python3 -m analysis.run metrics --derived DIR --phase-b DIR --baselines DIR --out DIR [--freeze-marker FILE]
    python3 -m analysis.run stats --out DIR

metrics writes, under --out:
  fold_metrics.parquet   one row per detector, campaign and fold (metrics.fold_metrics)
  run_metrics.parquet    one row per detector, campaign, fold and attempt of the test block
  matched_auc.parquet    C3, pooled AUC of the eight panel models on the windows all eight score, per fold
  fusion.parquet         C4, weighted category fusion and IF on the same windows, per fold
  cross_auc.parquet      C6, pooled AUC of every cross fit on every target test block, checked against auc.csv
  positions.parquet      C5 time reference, block position of every fault run
  inputs_sha256.json     the hash of every file read
  env.json               the machine, Python and package versions (7.3.5)
stats writes confirm.json.
"""
from __future__ import annotations

import argparse
import json
import platform
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PIPE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPE))
from analysis import confirm, metrics, sources  # noqa: E402
from streamprep.contract import campaign_index_from_id, sha256_file  # noqa: E402


def env_record() -> dict:
    import scipy
    import sklearn
    return {"python": platform.python_version(), "machine": platform.machine(), "system": platform.system(),
            "release": platform.release(), "processor": platform.processor(), "numpy": np.__version__,
            "pandas": pd.__version__, "scipy": scipy.__version__, "sklearn": sklearn.__version__}


def gate(cids, freeze_marker):
    for cid in cids:
        _w, idx = campaign_index_from_id(cid)
        if idx not in (0, 1) and (not freeze_marker or not Path(freeze_marker).is_file()
                                  or Path(freeze_marker).stat().st_size == 0):
            raise SystemExit(f"ERROR: {cid} is a replicate campaign, not read before the freeze tag (6.1, 7.1)")


def cmd_metrics(a):
    cids = a.campaigns or sources.CAMPAIGNS
    gate(cids, a.freeze_marker)
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    src = sources.frozen_sources(Path(a.phase_b), Path(a.baselines), cids)
    fm, runs, hashes = metrics.compute(Path(a.derived), src)
    fm.to_parquet(out / "fold_metrics.parquet", index=False)
    runs.to_parquet(out / "run_metrics.parquet", index=False)
    camps = {c: metrics.load_campaign(Path(a.derived), c) for c in cids}
    by = {(s["campaign_id"], s["detector"], s["fold"]): s["path"] for s in src}
    # C3 matched windows
    rows = []
    for c in cids:
        for k in range(1, 11):
            sc = {m: pd.read_parquet(by[(c, m, k)]) for m in sources.PANEL}
            r = confirm.matched_auc(sc, camps[c].labels)
            rows += [{"campaign_id": c, "fold": k, "model": m, "matched_auc": v, "n_matched": r["n_matched"],
                      "n_pos": r["n_pos"]} for m, v in r["auc"].items()]
    pd.DataFrame(rows).to_parquet(out / "matched_auc.parquet", index=False)
    # C4 fusion
    rows = []
    for c in cids:
        cat, iff = {}, {}
        for k in range(1, 11):
            cols = {}
            for v in ("broker", "consumer", "flink", "jvm"):
                f = pd.read_parquet(by[(c, f"IF_{v}", k)])
                cols[v] = f[f["role"] == "test"].set_index("window_id")["score"]
            cat[k] = pd.DataFrame(cols).dropna()
            f = pd.read_parquet(by[(c, "IF", k)])
            iff[k] = f[f["role"] == "test"].set_index("window_id")["score"]
        for k, (fa, ia, coef) in confirm.fusion_fold_aucs(cat, iff, camps[c].labels).items():
            rows.append({"campaign_id": c, "fold": k, "fused_auc": fa, "if_auc": ia,
                         **{f"coef_{v}": x for v, x in zip(("broker", "consumer", "flink", "jvm"), coef)}})
    pd.DataFrame(rows).to_parquet(out / "fusion.parquet", index=False)
    # C6 cross-workload
    rows = []
    for s in sources.cross_sources(Path(a.phase_b), cids):
        p = Path(s["path"])
        f = pd.read_parquet(p)
        hashes[str(p)] = sha256_file(p)
        lab = camps[s["target"]].labels
        if not f["window_id"].isin(lab.index).all() or set(f["role"]) != {"test"}:
            raise SystemExit(f"ERROR: {p} does not match {s['target']}")
        auc_csv = pd.read_csv(p.parent.parent / "auc.csv")
        for k, g in f.groupby("fold"):
            l_ = lab.loc[g["window_id"], "label"]
            ev = l_.isin(metrics.EVAL_LABELS).to_numpy()
            if not ev.all():
                raise SystemExit(f"ERROR: {p} fold {k} scores windows outside the evaluation labels")
            y = (l_ == "ANOMALOUS").astype(int).to_numpy()
            v = metrics.auc(y, g["score"].to_numpy(np.float64))
            ref = auc_csv[(auc_csv["target"] == s["target"]) & (auc_csv["fold"] == k)]["auc_roc"]
            if len(ref) != 1 or abs(float(ref.iloc[0]) - v) > 1e-9:
                raise SystemExit(f"ERROR: {p} fold {k} pooled AUC disagrees with auc.csv")
            exp_block = camps[s["target"]].test_block[int(k)]
            if (lab.loc[g["window_id"], "block"] != exp_block).any():
                raise SystemExit(f"ERROR: {p} fold {k} scores windows outside test block {exp_block}")
            rows.append({"source": s["source"], "target": s["target"], "detector": s["detector"], "fold": int(k),
                         "pooled_auc": v, "n": int(len(y)), "n_pos": int(y.sum())})
    pd.DataFrame(rows).to_parquet(out / "cross_auc.parquet", index=False)
    # C5 positions
    rows = []
    for c in cids:
        at = camps[c].attempts
        rows += [{"campaign_id": c, "attempt_key": k, "run_type": r.run_type, "block": int(r.block),
                  "position": int(r.position)} for k, r in at[at["run_type"] != "F0"].iterrows()]
    pd.DataFrame(rows).to_parquet(out / "positions.parquet", index=False)
    roots = {str(Path(a.phase_b).resolve()): "phase_b", str(Path(a.baselines).resolve()): "baselines",
             str(Path(a.derived).resolve()): "derived"}

    def rel(k):     # relative keys, so the record holds no local path (7.5)
        rp = str(Path(k).resolve()) if k.startswith("/") else k
        for root, name in roots.items():
            if rp.startswith(root + "/"):
                return f"{name}/{rp[len(root) + 1:]}"
        return k
    (out / "inputs_sha256.json").write_text(json.dumps(dict(sorted((rel(k), v) for k, v in hashes.items())),
                                                       indent=1) + "\n")
    (out / "env.json").write_text(json.dumps(env_record(), indent=1) + "\n")
    print(f"metrics: {len(fm)} fold rows, {len(runs)} run rows, {len(hashes)} input files")


def _jsonable(o):
    if isinstance(o, dict):
        return {str(k) if not isinstance(k, tuple) else "|".join(map(str, k)): _jsonable(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_jsonable(v) for v in o]
    if isinstance(o, (np.floating, np.integer)):
        return o.item()
    if isinstance(o, np.ndarray):
        return o.tolist()
    if isinstance(o, np.bool_):
        return bool(o)
    return o


def cmd_stats(a):
    out = Path(a.out)
    fm = pd.read_parquet(out / "fold_metrics.parquet")
    runs = pd.read_parquet(out / "run_metrics.parquet")
    res = confirm.run_all(fm, runs, pd.read_parquet(out / "matched_auc.parquet"),
                          pd.read_parquet(out / "fusion.parquet"), pd.read_parquet(out / "cross_auc.parquet"),
                          pd.read_parquet(out / "positions.parquet"))
    (out / "confirm.json").write_text(json.dumps(_jsonable(res), indent=1, default=str) + "\n")
    print(f"stats: wrote {out / 'confirm.json'}")


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    m = sub.add_parser("metrics")
    m.add_argument("--derived", required=True)
    m.add_argument("--phase-b", required=True)
    m.add_argument("--baselines", required=True)
    m.add_argument("--out", required=True)
    m.add_argument("--freeze-marker", default=None)
    m.add_argument("--campaigns", nargs="*")
    s = sub.add_parser("stats")
    s.add_argument("--out", required=True)
    a = p.parse_args(argv)
    {"metrics": cmd_metrics, "stats": cmd_stats}[a.cmd](a)


if __name__ == "__main__":
    main()
