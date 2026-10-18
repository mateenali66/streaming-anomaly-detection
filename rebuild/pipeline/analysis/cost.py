#!/usr/bin/env python3
"""Computational cost of protocol 4.5.11 and 5.9.18: training time per fit and scoring time per window, from one
script on one recorded machine, with hardware and batch size stated.

Added on 18 October 2026, after registration. Recorded with the other exploratory choices as D6 in
protocol/DEVIATIONS.md. It runs in the training environment because it trains, with training/train.py imported
unchanged and the frozen configurations of rebuild/frozen. One fit per model and workload, on fold 1 of campaign 1 at
the 30 s anchor, with the 6.2.2 seed. Each model runs in its own process so that train.py picks its device at import:
DAGMM on the CPU (the Mac GPU gives no finite DAGMM energy, campaign log 17 October 17:16) and the other deep models
where train.py puts them.

  fit_s                 wall time of the fit alone, after the fold's selection, imputation and scaling
  score_ms_per_window   the fold's test block scored in one call (deep models in batches of train.py BATCH_SIZE,
                        the others as one matrix), divided by the windows scored, the median of five calls after
                        one warm-up call
  single_ms_p50/p95     one window per call (a sequence model gets the one sequence that ends at the window), 20
                        warm-up calls then 256 timed calls, as analysis/cross_workload/inference_timing.py did
EWMA's fit is its reference, the last smoothed value and the mean EWMA standard deviation of the train NORMAL
windows, computed once. Its per-window score is then the registered score_ewma arithmetic on that reference.

    python3 analysis/cost.py --derived DIR --frozen-dir ../frozen --out DIR [--phase-b DIR]

writes DIR/cost.csv, DIR/machine.json and, with --phase-b, DIR/ec2_phase_b_train_time.csv from the summaries the
EC2 run wrote.
"""
from __future__ import annotations

import os
import sys
import argparse
import json
import platform
import subprocess
import time
from pathlib import Path

import numpy as np
import pandas as pd

PIPE = Path(__file__).resolve().parents[1]
MODELS = ["IF", "OC_SVM", "LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE", "DeepSVDD", "DAGMM", "RF", "RuleBased", "EWMA"]
CAMPAIGN = {"ecommerce": "ec-a", "iot": "iot-a"}
FOLD = 1
WARMUP, TIMED = 20, 256


def machine() -> dict:
    def sysctl(k):
        try:
            return subprocess.run(["sysctl", "-n", k], capture_output=True, text=True).stdout.strip()
        except OSError:
            return ""
    rec = {"system": platform.system(), "release": platform.release(), "machine": platform.machine(),
           "cpu": sysctl("machdep.cpu.brand_string"), "cores": os.cpu_count(),
           "memory_gib": round(int(sysctl("hw.memsize") or 0) / 2 ** 30, 1), "python": platform.python_version(),
           "load_average_at_start": [round(x, 2) for x in os.getloadavg()]}
    try:
        import torch
        rec.update({"torch": torch.__version__, "mps_available": bool(torch.backends.mps.is_available()),
                    "torch_threads": torch.get_num_threads()})
    except ImportError:
        rec["torch"] = None
    return rec


def worker(a):
    sys.path.insert(0, str(PIPE / "training"))
    sys.path.insert(0, str(PIPE))
    import train as T
    from streamprep.contract import campaign_index_from_id
    from streamprep.fold_fit import feature_columns
    from analysis.baselines import score_ewma
    cid, model = a.campaign, a.model
    wl, idx = campaign_index_from_id(cid)
    cfg = T.load_training_config(None)
    base = Path(a.derived) / cid
    df, folds = T.load_inputs(base / "windows_30s.parquet", base / "folds.parquet", cfg["_pipeline"])
    d = T.prepare_fold_data(df, folds, FOLD, feature_columns(df), cfg)
    frozen = json.loads((Path(a.frozen_dir) / f"{wl}.json").read_text())
    hp = dict(frozen.get(model, {}))
    seed = T.seed_for_fit(idx, FOLD, cfg)
    T.MODEL_SEED = T.SAMPLER_SEED = seed
    T.set_seeds(seed)
    Xn, Xt = d["X_normal"], d["X_test"]
    sync = (lambda: T.torch.mps.synchronize()) if T.DEVICE == "mps" else (lambda: None)
    batch = None
    t0 = time.perf_counter()
    if model == "IF":
        m = T.train_isolation_forest(Xn, n_estimators=hp["n_estimators"], contamination=hp["contamination"],
                                     max_features=hp["max_features"], max_samples=hp["max_samples"])
        sc = T.fit_score_scaler(-m.decision_function(Xn))
        score = lambda X: T.score_isolation_forest(m, X, sc)
    elif model == "OC_SVM":
        m = T.train_ocsvm(Xn, nu=hp["nu"], kernel=hp["kernel"], gamma=hp["gamma"])
        sc = T.fit_score_scaler(-m.decision_function(Xn))
        score = lambda X: T.score_ocsvm(m, X, sc)
    elif model == "RF":
        m = T.train_random_forest(d["X_train_rf"], d["y_train_rf"], n_estimators=hp["n_estimators"],
                                  max_depth=hp["max_depth"], min_samples_leaf=hp["min_samples_leaf"],
                                  max_features=hp["max_features"])
        score = lambda X: T.score_random_forest(m, X)
    elif model == "RuleBased":
        mu, sd = T.train_rule_based(Xn)
        score = lambda X: T.score_rule_based(X, mu, sd)
    elif model == "EWMA":
        mu_last = pd.DataFrame(Xn).ewm(alpha=0.3, adjust=False).mean().to_numpy()[-1]
        sdv = np.nanmean(pd.DataFrame(Xn).ewm(alpha=0.3, adjust=False).std().to_numpy(), axis=0)
        sdv = np.where(sdv > 1e-9, sdv, 1.0)
        score = lambda X: np.nanmax(np.abs(X - mu_last) / sdv, axis=1)
        # the reference above is score_ewma's, so the scores must agree with it
        assert np.allclose(score(Xt[:50]), score_ewma(Xn, Xt[:50]))
    else:
        m, ref, L, kind = T.train_deep_model_final(model, Xn.shape[1], Xn, d["X_hpo_val_normal"], hp)
        batch = T.BATCH_SIZE
        fn = {"ae": T.score_autoencoder, "vae": T.score_vae, "dsvdd": T.score_deep_svdd, "dagmm": T.score_dagmm}[kind]
    sync()
    fit_s = time.perf_counter() - t0
    if model in T.DEEP_MODELS:
        Xs, _ = T.build_sequences(Xt, L, "test")
        whole = lambda: fn(m, Xs, *ref)
        n = len(Xs)
        one = lambda i: fn(m, Xs[i:i + 1], *ref)
    else:
        whole = lambda: score(Xt)
        n = len(Xt)
        one = lambda i: score(Xt[i:i + 1])
    s = whole()          # warm-up, which also gives the scores checked below
    sync()
    reps = []
    for _ in range(5):
        t1 = time.perf_counter()
        whole()
        sync()
        reps.append(time.perf_counter() - t1)
    batch_ms = float(np.median(reps)) * 1000 / n
    for i in range(WARMUP):
        one(i % n)
    sync()
    times = []
    for i in range(TIMED):
        t2 = time.perf_counter()
        one(i % n)
        sync()
        times.append((time.perf_counter() - t2) * 1000)
    rec = {"workload": wl, "campaign_id": cid, "fold": FOLD, "model": model, "device": T.DEVICE if model in
           T.DEEP_MODELS else "cpu", "n_features": int(Xn.shape[1]), "n_train_normal": int(len(Xn)),
           "n_scored": int(n), "batch_size": batch if batch else "whole test block", "fit_s": round(fit_s, 3),
           "score_ms_per_window": round(batch_ms, 4), "single_ms_p50": round(float(np.median(times)), 4),
           "single_ms_p95": round(float(np.quantile(times, 0.95)), 4), "finite": bool(np.isfinite(s).all()),
           "load_average_1min": round(os.getloadavg()[0], 2)}
    print(json.dumps(rec))


def ec2_train_times(phase_b: Path) -> pd.DataFrame:
    """train_time_s of the frozen fits as the EC2 run recorded them. It is the fit, scoring and thresholds of one job,
    with four jobs sharing each A10G GPU and the CPU jobs running alongside them."""
    rows = []
    for cid in ["ec-a", "ec-b", "ec-c", "iot-a", "iot-b", "iot-c"]:
        for mdir in (phase_b / cid / "w30-frozen").iterdir():
            if mdir.name.startswith("IF_"):
                continue
            for f in mdir.glob("fold*/summary_*.json"):
                r = json.loads(f.read_text())
                pf = r["per_fold"][0]
                rows.append({"workload": "ecommerce" if cid.startswith("ec") else "iot", "campaign_id": cid,
                             "model": mdir.name, "device": r.get("device"), "train_time_s": pf["train_time_s"]})
    df = pd.DataFrame(rows)
    return df.groupby(["workload", "model", "device"])["train_time_s"].describe(
        percentiles=[0.5]).reset_index()[["workload", "model", "device", "count", "50%", "min", "max"]].rename(
        columns={"50%": "median_s", "min": "min_s", "max": "max_s", "count": "n_fits"})


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--derived", required=True)
    p.add_argument("--frozen-dir", required=True)
    p.add_argument("--out")
    p.add_argument("--phase-b")
    p.add_argument("--worker", action="store_true")
    p.add_argument("--campaign")
    p.add_argument("--model")
    p.add_argument("--models", nargs="*", default=MODELS)
    a = p.parse_args(argv)
    if a.worker:
        return worker(a)
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    rec = machine()
    (out / "machine.json").write_text(json.dumps(rec, indent=1) + "\n")
    rows = []
    for wl, cid in CAMPAIGN.items():
        for m in a.models:
            env = dict(os.environ)
            if m == "DAGMM":
                env["STREAMPREP_DEVICE"] = "cpu"
            else:
                env.pop("STREAMPREP_DEVICE", None)
            r = subprocess.run([sys.executable, "-m", "analysis.cost", "--worker", "--derived", a.derived,
                                "--frozen-dir", a.frozen_dir, "--campaign", cid, "--model", m],
                               capture_output=True, text=True, env=env, cwd=str(PIPE))
            line = [x for x in r.stdout.splitlines() if x.startswith("{")]
            if r.returncode != 0 or not line:
                raise SystemExit(f"ERROR: cost worker {cid} {m} failed:\n{r.stdout[-2000:]}\n{r.stderr[-2000:]}")
            rows.append(json.loads(line[-1]))
            print(f"{cid} {m}: fit {rows[-1]['fit_s']} s, {rows[-1]['single_ms_p50']} ms/window", flush=True)
    pd.DataFrame(rows).to_csv(out / "cost.csv", index=False)
    if a.phase_b:
        ec2_train_times(Path(a.phase_b)).to_csv(out / "ec2_phase_b_train_time.csv", index=False)


if __name__ == "__main__":
    main()
