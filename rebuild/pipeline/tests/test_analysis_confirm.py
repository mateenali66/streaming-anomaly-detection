"""analysis/confirm.py on constructed metric tables whose decisions are known (protocol 5.4, 5.5, 5.8)."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis import confirm as C  # noqa: E402
from analysis.sources import DEEP, PANEL  # noqa: E402

CAMPS = ["ec-a", "ec-b", "ec-c", "iot-a", "iot-b", "iot-c"]
DETS = PANEL + ["RF", "RuleBased", "EWMA", "EWMA_train_mean"]


def make_fm(shift: dict, seed=0, noise=0.005):
    rng = np.random.default_rng(seed)
    rows = []
    for c in CAMPS:
        base = rng.normal(0.85, 0.02, 10)
        for d in DETS:
            for k in range(10):
                rows.append({"detector": d, "campaign_id": c, "fold": k + 1,
                             "pooled_auc": base[k] + shift.get(d, 0.0) + rng.normal(0, noise),
                             "within_run_pc": 0.4 + shift.get(d, 0.0) + rng.normal(0, noise)})
    return pd.DataFrame(rows)


def test_c1_no_deep_model_beats_max_z_when_all_are_lower():
    fm = make_fm({m: -0.05 for m in DEEP})
    r = C.c1(fm, "ecommerce")
    r = C.c1_decide(r, r["family_p"])
    assert r["outcome"] == "no deep model beats max-z"
    assert all(x["decision"]["not_superior"] for x in r["contrasts"].values())


def test_c1_one_model_beats_max_z_on_both_metrics():
    fm = make_fm({**{m: -0.05 for m in DEEP}, "CNN_AE": 0.08})
    r = C.c1(fm, "iot")
    r = C.c1_decide(r, r["family_p"])
    assert r["beats"] == ["CNN_AE"] and r["outcome"] == "model beats max-z: CNN_AE"
    rep = r["contrasts"][("CNN_AE", "pooled_auc")]["replication"]
    assert rep["status"] == "replicated" and rep["replicated_in_each_campaign"]


def test_c2a_equivalence_and_c2b_superiority():
    fm = make_fm({"EWMA": 0.0, "RuleBased": 0.0, "EWMA_train_mean": -0.06})
    a = C.c2a_decide(C.c2a(fm, "ecommerce"), 0.5)
    assert a["outcome"] == "equivalent"
    b = C.single_decide(C.c2b(fm, "ecommerce"), 0.001)
    assert b["outcome"] == "superior"


def test_c2a_metric_dependent_when_the_metrics_disagree():
    fm = make_fm({})
    fm.loc[fm["detector"] == "EWMA", "pooled_auc"] += 0.08
    fm.loc[fm["detector"] == "EWMA", "within_run_pc"] -= 0.08
    a = C.c2a(fm, "iot")
    assert a["family_p"] == 1.0
    assert C.c2a_decide(a, 0.01)["outcome"] == "metric-dependent"


def test_c7_reproducible_only_when_every_campaign_is_within_delta():
    fm = make_fm({})
    r = C.c7(fm)
    assert set(r) == {"ecommerce", "iot"} and set(r["iot"]) == set(C.C7_DETECTORS)
    fm2 = fm.copy()
    fm2.loc[(fm2["detector"] == "IF") & (fm2["campaign_id"] == "iot-b"), "pooled_auc"] += 0.2
    assert not C.c7(fm2)["iot"]["IF"]["reproducible"]


def make_runs(ec_vals, iot_vals):
    rows = []
    for c in CAMPS:
        vals = ec_vals if c.startswith("ec") else iot_vals
        for d in C.C5_DETECTORS:
            for k in range(1, 11):
                rows.append({"detector": d, "campaign_id": c, "fold": k, "run_type": "F0", "within_run_auc": 0.5})
                for f, v in zip(C.FAULTS, vals):
                    rows.append({"detector": d, "campaign_id": c, "fold": k, "run_type": f,
                                 "within_run_auc": 0.5 + v})
    return pd.DataFrame(rows)


def test_c5_identical_fault_difficulty_gives_rho_1_and_the_smallest_p():
    v = [0.1, 0.2, 0.3, 0.35, 0.4, 0.45, 0.48]
    runs = make_runs(v, v)
    pos = pd.DataFrame([{"campaign_id": c, "run_type": f, "position": i % 8 + 1}
                        for c in CAMPS for i, f in enumerate(C.FAULTS)])
    r = C.c5(runs, pos)
    assert r["median_rho"] == pytest.approx(1.0) and r["n_perm"] == 5040
    assert r["p_perm"] == pytest.approx(1 / 5040)
    assert r["ceiling"]["iot"]["median_rho"] == pytest.approx(1.0)


def test_c6_equivalence_for_all_eight_when_transfer_is_free():
    rng = np.random.default_rng(5)
    rows = []
    for m in PANEL + ["RF"]:
        for s in CAMPS:
            for t in CAMPS:
                if s == t:
                    continue
                for k in range(1, 11):
                    rows.append({"source": s, "target": t, "detector": m, "fold": k,
                                 "pooled_auc": 0.8 + rng.normal(0, 0.003)})
    cross = pd.DataFrame(rows)
    r = C.c6_decide(C.c6(cross, "ecommerce"), 0.9)
    assert r["outcome"] == "transfer costs less than delta for all eight"
    cross.loc[(cross["detector"] == "DAGMM") & (cross["source"].str.startswith("iot")), "pooled_auc"] -= 0.1
    r = C.c6_decide(C.c6(cross, "ecommerce"), 0.001)
    assert r["outcome"] == "per-model results" and "DAGMM" not in r["equivalent_models"]
    assert r["contrasts"]["DAGMM"]["decision"]["outcome"] == "superior"
