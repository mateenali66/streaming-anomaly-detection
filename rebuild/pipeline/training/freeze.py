#!/usr/bin/env python3
"""Consensus configurations from campaign 1's fold winners, and the freeze marker (protocol 6.1.2 and 7.1).

Added on 17 October 2026, after registration, to carry out the registered rule. For each workload and model, the ten
fold winners of the 30 s nested search are read from their Optuna databases, together with the distribution each
hyperparameter was drawn from, so every grid comes from the registered search space in train.py and none is copied.

The rule of 6.1.2, as applied here:
  * An integer hyperparameter takes the median of the ten winners, moved to the nearest point of its grid (low plus a
    multiple of its step). A median exactly halfway between two grid points takes the lower one, the smaller model.
  * A float hyperparameter takes the median of the ten winners, computed on the log scale when it was searched on the
    log scale. A continuous range has no grid, so the median stands. A float with a step is moved to its grid.
  * A categorical hyperparameter takes the most frequent value. A tie goes to the smallest value when the choices are
    numbers (the smaller model). Otherwise it goes to the choice listed first in the search space, since names such
    as rbf or sigmoid have no size order. Every tie is listed in FREEZE.json.

    python3 training/freeze.py --results /opt/sad/results --out frozen

writes frozen/ecommerce.json and frozen/iot.json (model -> configuration, the format --frozen-params reads) and
frozen/FREEZE.json (inputs, winners, consensus, ties and hashes). The author then commits frozen/ and tags the commit.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import sys
from pathlib import Path

import optuna
from optuna.distributions import CategoricalDistribution, FloatDistribution, IntDistribution

MODELS = ["IF", "OC_SVM", "LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE", "DeepSVDD", "DAGMM", "RF"]
CAMPAIGN_1 = {"ecommerce": "ec-a", "iot": "iot-a"}
WINDOW_S = 30
N_FOLDS = 10


def sha256(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def snap(value: float, low: float, step: float) -> float:
    k = (value - low) / step
    lo = math.floor(k + 1e-9)
    frac = k - lo
    return low + (lo + (1 if frac > 0.5 + 1e-9 else 0)) * step


def consensus(name: str, values: list, dist, ties: list) -> object:
    if isinstance(dist, CategoricalDistribution):
        counts = {}
        for v in values:
            counts[v] = counts.get(v, 0) + 1
        top = max(counts.values())
        best = [c for c in dist.choices if counts.get(c, 0) == top]
        if len(best) > 1:
            numeric = all(isinstance(c, (int, float)) and not isinstance(c, bool) for c in best)
            pick = min(best) if numeric else best[0]
            ties.append({"param": name, "tied": best, "count": top, "picked": pick,
                         "rule": "smallest value" if numeric else "first listed choice"})
            return pick
        return best[0]
    if isinstance(dist, IntDistribution):
        med = statistics.median(values)
        step = dist.step or 1
        v = int(round(snap(med, dist.low, step)))
        if abs((med - dist.low) / step - round((med - dist.low) / step)) > 1e-9 and \
                abs(((med - dist.low) / step) % 1 - 0.5) < 1e-9:
            ties.append({"param": name, "median": med, "picked": v, "rule": "halfway, lower grid point"})
        return min(max(v, dist.low), dist.high)
    if isinstance(dist, FloatDistribution):
        med = math.exp(statistics.median([math.log(x) for x in values])) if dist.log else statistics.median(values)
        if dist.step:
            med = snap(med, dist.low, dist.step)
        return min(max(med, dist.low), dist.high)
    raise SystemExit(f"{name}: unknown distribution {type(dist).__name__}")


def fold_winner(db: Path):
    storage = f"sqlite:///{db}"
    names = optuna.study.get_all_study_names(storage=storage)
    if len(names) != 1:
        raise SystemExit(f"{db}: expected one study, found {names}")
    study = optuna.load_study(study_name=names[0], storage=storage)
    done = [t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE]
    best = study.best_trial
    return names[0], len(done), best


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--results", required=True, help="the run_matrix.py output folder")
    p.add_argument("--out", required=True)
    p.add_argument("--expect-trials", type=int, default=None, help="refuse a study with fewer completed trials")
    args = p.parse_args()
    cfg = json.loads((Path(__file__).resolve().parent / "config.json").read_text())
    want = {m: (cfg["trials"]["RF"] if m == "RF" else cfg["trials"]["classical"] if m in ("IF", "OC_SVM")
                else cfg["trials"]["deep"]) for m in MODELS}
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    marker = {"protocol": "6.1.2", "window_s": WINDOW_S, "source_campaigns": CAMPAIGN_1,
              "train_py_sha256": sha256(Path(__file__).resolve().parent / "train.py"),
              "training_config_sha256": sha256(Path(__file__).resolve().parent / "config.json"),
              "pipeline_config_sha256_file": sha256(Path(__file__).resolve().parents[1] / "config" / "pipeline.json"),
              "workloads": {}}
    problems = []
    for workload, cid in CAMPAIGN_1.items():
        frozen, wl = {}, {}
        for model in MODELS:
            winners, dists, dbs = [], {}, {}
            for fold in range(1, N_FOLDS + 1):
                d = Path(args.results) / cid / f"w{WINDOW_S}" / model / f"fold{fold}" / "optuna_dbs"
                found = sorted(d.glob("*.db"))
                if len(found) != 1:
                    problems.append(f"{cid} {model} fold {fold}: {len(found)} databases")
                    continue
                name, n_done, best = fold_winner(found[0])
                need = args.expect_trials or want[model]
                if n_done < need:
                    problems.append(f"{cid} {model} fold {fold}: {n_done} completed trials, {need} expected")
                winners.append({"fold": fold, "study": name, "trials_completed": n_done,
                                "best_value": best.value, "params": best.params})
                for k, dist in best.distributions.items():
                    if k in dists and dists[k] != dist:
                        problems.append(f"{cid} {model}: {k} has different distributions across folds")
                    dists[k] = dist
                dbs[str(fold)] = sha256(found[0])
            if len(winners) != N_FOLDS:
                continue
            keys = set(winners[0]["params"])
            if any(set(w["params"]) != keys for w in winners):
                problems.append(f"{cid} {model}: folds disagree on the hyperparameter set")
                continue
            ties, conf = [], {}
            for k in sorted(keys):
                conf[k] = consensus(k, [w["params"][k] for w in winners], dists[k], ties)
            frozen[model] = conf
            wl[model] = {"consensus": conf, "ties": ties, "winners": winners, "optuna_db_sha256": dbs,
                         "distributions": {k: optuna.distributions.distribution_to_json(v) for k, v in dists.items()}}
        marker["workloads"][workload] = wl
        if frozen:
            path = out / f"{workload}.json"
            path.write_text(json.dumps(frozen, indent=1, sort_keys=True) + "\n")
            marker.setdefault("frozen_files_sha256", {})[path.name] = sha256(path)
    if problems:
        print("REFUSING to write FREEZE.json:")
        for x in problems:
            print("  ", x)
        sys.exit(2)
    (out / "FREEZE.json").write_text(json.dumps(marker, indent=1, sort_keys=True) + "\n")
    print(f"wrote {out}/ecommerce.json, {out}/iot.json and {out}/FREEZE.json")
    for workload in CAMPAIGN_1:
        for model, v in marker["workloads"][workload].items():
            print(f"  {workload:9s} {model:8s} {json.dumps(v['consensus'])}" + (f"  ties {v['ties']}" if v["ties"] else ""))


if __name__ == "__main__":
    main()
