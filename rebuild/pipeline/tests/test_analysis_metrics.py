"""analysis/metrics.py and analysis/baselines.py: the copied registered functions, the metric definitions of
protocol 4.5 on hand-built cases, and the whole metrics step on the synthetic campaign."""
from __future__ import annotations

import ast
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

PIPE = Path(__file__).resolve().parents[1]
REPO = PIPE.parents[1]
sys.path.insert(0, str(PIPE))
from analysis import metrics as M  # noqa: E402


def _func_src(path: Path, name: str) -> str:
    tree = ast.parse(path.read_text())
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
    return ast.dump(node)


@pytest.mark.parametrize("name", ["find_optimal_threshold", "_check_sidecar"])
def test_metrics_copies_equal_train_py(name):
    assert _func_src(PIPE / "analysis" / "metrics.py", name) == _func_src(PIPE / "training" / "train.py", name)


def test_score_ewma_equals_the_registered_classical_baselines_code():
    import subprocess
    reg = subprocess.run(["git", "-C", str(REPO), "show", "d7521a1:analysis/cross_workload/classical_baselines.py"],
                         capture_output=True, text=True)
    if reg.returncode != 0:
        pytest.skip("registered commit not available")
    tree = ast.parse(reg.stdout)
    want = ast.dump(next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "score_ewma"))
    assert _func_src(PIPE / "analysis" / "baselines.py", "score_ewma") == want


def test_ewma_train_mean_equals_the_ewma_mechanism_variant():
    import importlib.util
    pytest.importorskip("torch")
    from analysis import baselines as B
    spec = importlib.util.spec_from_file_location("em", REPO / "analysis" / "cross_workload" / "ewma_mechanism.py")
    em = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(em)
    rng = np.random.default_rng(0)
    Xt, Xe = rng.normal(size=(200, 7)), rng.normal(size=(50, 7))
    Xt[:, 3] = 1.0
    assert np.allclose(B.score_ewma_train_mean(Xt, Xe), em.score_variant("centre_train_mean", Xt, Xe, None))
    assert np.allclose(B.score_ewma(Xt, Xe), em.score_variant("published", Xt, Xe, None))


def test_onsets_count_transitions_inside_a_seq_group():
    f = pd.DataFrame({"seq_group": ["a"] * 6 + ["b"] * 3, "seq_index": [0, 1, 2, 3, 4, 5, 3, 4, 5]})
    al = np.array([1, 1, 0, 1, 0, 1, 1, 0, 1], bool)
    assert M.onsets(f, al).tolist() == [True, False, False, True, False, True, True, False, True]
    f2 = f.drop(index=[2]).reset_index(drop=True)
    al2 = np.delete(al, 2)
    al2[2] = True
    assert M.onsets(f2, al2)[2]


def test_km_restricted_mean_and_median():
    t = np.array([0, 30, 30, 60, 150, 150])
    e = np.array([1, 1, 1, 1, 0, 0], bool)
    k = M.km(t, e, 150.0)
    # S: 1 -> 5/6 at 0, -> 3/6 at 30, -> 2/6 at 60, censored at 150
    assert k["rmst_s"] == pytest.approx(0 * 1 + 30 * 5 / 6 + 30 * 3 / 6 + 90 * 2 / 6)
    assert k["km_median_s"] == 30 and k["detection_rate"] == pytest.approx(4 / 6)
    assert k["km_detected_by_tau"] == pytest.approx(1 - 2 / 6)


@pytest.fixture(scope="module")
def synth_metrics(full, tmp_path_factory):
    """baselines.py on the synthetic e-commerce campaign 1 (all folds), then metrics.compute on its scores."""
    pytest.importorskip("torch")
    from analysis import baselines as B
    out = tmp_path_factory.mktemp("bl")
    cid = full.rep["campaign_id"]
    derived = full.out.parent
    B.run(derived, cid, out, None)
    srcs = [{"campaign_id": cid, "detector": d, "fold": k, "path": str(out / cid / d / f"{d}_fold{k}.parquet")}
            for d in B.DETECTORS for k in range(1, 11)]
    fm, runs, hashes = M.compute(derived, srcs)
    return full, fm, runs, hashes


def test_synthetic_metrics_cover_every_fold_and_detector(synth_metrics):
    full, fm, runs, hashes = synth_metrics
    assert len(fm) == 50 and fm["within_run_complete"].all()
    assert (fm.groupby("detector").size() == 10).all()
    assert (runs.groupby(["detector", "fold"]).size() == 8).all()
    t = fm[fm["detector"] == "TimeOnly"]
    assert (t["within_run_auc"] == 1).all() and (t["sham_placebo"] == 1).all() and (t["within_run_pc"] == 0).all()
    assert fm["pooled_auc"].between(0, 1).all()


def test_synthetic_pooled_auc_equals_a_direct_computation(synth_metrics):
    from sklearn.metrics import roc_auc_score
    full, fm, runs, hashes = synth_metrics
    w = full.windows[30].set_index("window_id")
    r = fm[(fm["detector"] == "RuleBased") & (fm["fold"] == 3)].iloc[0]
    p = next(k for k in hashes if k.endswith("RuleBased_fold3.parquet"))
    sc = pd.read_parquet(p)
    sc = sc[sc["role"] == "test"]
    lab = w.loc[sc["window_id"], "label"]
    ev = lab.isin(M.EVAL_LABELS).to_numpy()
    assert r["pooled_auc"] == pytest.approx(roc_auc_score((lab[ev] == "ANOMALOUS").astype(int), sc["score"][ev]))


def test_a_score_file_with_a_wrong_role_is_refused(synth_metrics, tmp_path):
    full, fm, runs, hashes = synth_metrics
    p = next(k for k in hashes if k.endswith("EWMA_fold2.parquet"))
    sc = pd.read_parquet(p)
    sc.loc[sc.index[0], "role"] = "cal" if sc.loc[sc.index[0], "role"] == "test" else "test"
    camp = M.load_campaign(full.out.parent, full.rep["campaign_id"])
    with pytest.raises(SystemExit, match="roles disagree"):
        M.fold_metrics(camp, sc, 2, "edited")
