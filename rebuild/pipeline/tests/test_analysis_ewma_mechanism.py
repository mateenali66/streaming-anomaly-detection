"""analysis/ewma_mechanism.py: the copied variants equal the R2 draft's analysis/cross_workload/ewma_mechanism.py, the
stretch boundaries follow seq_group and seq_index, and the saturation bound on a hand-built case."""
from __future__ import annotations

import hashlib
import importlib.util
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

PIPE = Path(__file__).resolve().parents[1]
REPO = PIPE.parents[1]
sys.path.insert(0, str(PIPE))
from analysis import ewma_mechanism as E  # noqa: E402

R1 = REPO / "analysis" / "cross_workload" / "ewma_mechanism.py"


@pytest.fixture(scope="module")
def r1():
    if not R1.is_file():
        pytest.skip("R2 draft script not present")
    assert hashlib.sha256(R1.read_bytes()).hexdigest() == E.R1_SCRIPT_SHA256
    spec = importlib.util.spec_from_file_location("r1_ewma_mechanism", R1)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.parametrize("variant", E.VARIANTS)
def test_every_variant_equals_the_r1_score_variant(r1, variant):
    rng = np.random.default_rng(4)
    Xt = rng.normal(size=(300, 12)).astype(np.float32)
    Xt[:, 5] = 0.0
    Xe = rng.normal(0.3, 1.2, size=(80, 12)).astype(np.float32)
    seg = np.zeros(80, bool)
    seg[[0, 17, 40, 41]] = True
    drop = [1, 5, 9]
    assert r1.VARIANTS == E.VARIANTS and r1.ALPHA == E.ALPHA
    a = E.score_variant(variant, Xt, Xe, seg, drop=drop)
    b = r1.score_variant(variant, Xt, Xe, seg, drop=drop)
    assert a.dtype == b.dtype and np.array_equal(a, b)


def test_published_variant_equals_baselines_score_ewma():
    pytest.importorskip("torch")
    from analysis.baselines import score_ewma
    rng = np.random.default_rng(9)
    Xt, Xe = rng.normal(size=(200, 6)), rng.normal(size=(40, 6))
    assert np.array_equal(E.score_variant("published", Xt, Xe, None), score_ewma(Xt, Xe))


def test_segment_starts_follow_seq_group_and_gaps():
    f = pd.DataFrame({"seq_group": ["a", "a", "a", "b", "b", "a"], "seq_index": [3, 4, 6, 0, 1, 7]})
    assert E.segment_starts(f).tolist() == [True, False, True, True, False, True]


def test_saturation_bound_breaks_top_ties_in_favour():
    from analysis.metrics import Campaign
    lab = pd.DataFrame({"label": ["ANOMALOUS", "ANOMALOUS", "NORMAL", "NORMAL", "NORMAL", "TRAILING"]},
                       index=[f"w{i}" for i in range(6)])
    camp = Campaign("ec-b", lab, None, None, {}, {})
    sc = pd.DataFrame({"window_id": [f"w{i}" for i in range(6)], "role": ["test"] * 5 + ["eval_only"],
                       "score": [1.0, 0.5, 1.0, 0.2, 0.1, 9.0]})
    r = E.saturation_rows(camp, sc, "X", 1)
    # six positive-negative pairs: four wins, one loss (0.5 below 1.0) and one tie at the top score (1.0 and 1.0)
    assert r["auc"] == pytest.approx(4.5 / 6) and r["auc_best_case"] == pytest.approx(5 / 6)
    assert r["frac_at_max"] == pytest.approx(2 / 5) and r["frac_normal_at_max"] == pytest.approx(1 / 3)
