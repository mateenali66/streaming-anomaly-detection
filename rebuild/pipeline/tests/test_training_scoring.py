"""Review 4 CAM-5 (decision 26), MIN-8 and MIN-9 (decision 27) on training/train.py.

CAM-5: Isolation Forest and OC-SVM scores are scaled with a scaler fitted on the fold's train NORMAL scores and applied
to every split. DAGMM's non-finite energies take the train maximum. Deep SVDD's degenerate check is decided on train.
MIN-8: the F1-optimal threshold is searched over the observed calibration scores, so RuleBased is not always-fire.
MIN-9: sequence length is searched on 5 to 11 windows, and every model scores the same within-run negatives, the last
10 pre-fault windows of every attempt, tagged in the windows file and copied into every score file."""
from __future__ import annotations

import importlib.util
import json
import os
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

from streamprep.config import load_config
from streamprep.contract import sha256_file
from streamprep.fold_fit import feature_columns

NEW = Path(__file__).resolve().parents[1] / "training" / "train.py"
CFG = {"seed_base": 42, "seed_campaign_multiplier": 1000, "search_campaign_index": 1, "max_epochs": 30,
       "trials": {"deep": 1, "classical": 1, "RF": 1}, "variance_threshold": 0.01, "correlation_threshold": 0.95,
       "fp_budgets": [0.01, 0.05]}


@pytest.fixture(scope="module")
def train_mod():
    os.environ["STREAMPREP_DEVICE"] = "cpu"
    spec = importlib.util.spec_from_file_location("rebuild_train_scoring", NEW)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def runs(full, train_mod, tmp_path_factory):
    tmp = tmp_path_factory.mktemp("train_scoring")
    cfg_path = tmp / "cfg.json"
    cfg_path.write_text(json.dumps(CFG))
    out = {}
    for model in ("IF", "OC_SVM", "RuleBased", "CNN_AE"):
        res = train_mod.main(["--model", model, "--windows", str(full.out / "windows_30s.parquet"),
                              "--folds", str(full.out / "folds.parquet"), "--config", str(cfg_path),
                              "--output", str(tmp / model), "--fold", "1"])
        out[model] = (tmp / model, res)
    df, folds = train_mod.load_inputs(full.out / "windows_30s.parquet", full.out / "folds.parquet")
    d = train_mod.prepare_fold_data(df, folds, 1, feature_columns(df), train_mod.load_training_config(cfg_path))
    return out, d


def _scores(path: Path, model: str) -> pd.DataFrame:
    return pd.read_parquet(path / "scores" / f"{model}_fold1.parquet")


@pytest.mark.parametrize("model", ["IF", "OC_SVM"])
def test_point_model_scores_use_one_train_normal_scaler(runs, model):
    out, d = runs
    path, _res = out[model]
    obj = pickle.loads((path / "models" / f"{model}_fold1.pkl").read_bytes())
    m = obj["model"] if isinstance(obj, dict) else obj
    raw_train = -m.decision_function(d["X_normal"])
    lo, hi = float(raw_train.min()), float(raw_train.max())
    s = _scores(path, model)
    for split, role in (("test", "test"), ("cal", "cal"), ("trailing", "eval_only")):
        expected = (-m.decision_function(d[f"X_{split}"]) - lo) / (hi - lo)
        got = s[s["role"] == role].set_index("window_id")["score"].reindex(d[f"df_{split}"]["window_id"])
        assert np.allclose(got.to_numpy(), expected, rtol=1e-9, atol=1e-12), (model, split)
    # Scoring a set alone or inside a larger set gives the same value for the same window.
    test_scores = s[s["role"] == "test"]["score"].to_numpy()
    assert not (test_scores.min() == 0.0 and test_scores.max() == 1.0), "the test block was min-max scaled on itself"


def test_dagmm_non_finite_energies_take_the_train_maximum(train_mod):
    torch = pytest.importorskip("torch")

    class Stub:
        phi = mu = sigma = None

        def eval(self):
            return self

        def __call__(self, xb):
            return None, xb[:, 0, :], None

        def sample_energy(self, z, phi, mu, sigma):
            return z[:, 0]

    model = Stub()
    train = np.array([[[1.0]], [[2.0]], [[np.inf]], [[3.0]]], dtype=np.float32)
    mean, std, e_max = train_mod.get_training_energy_stats_dagmm(model, train)
    assert e_max == 3.0 and mean == pytest.approx(2.0) and std == pytest.approx(np.std([1, 2, 3]) + 1e-8)
    X = np.array([[[0.5]], [[np.inf]], [[np.nan]]], dtype=np.float32)
    s = train_mod.score_dagmm(model, X, mean, std, e_max)
    ref = train_mod.score_dagmm(model, np.array([[[0.5]], [[3.0]], [[3.0]]], dtype=np.float32), mean, std, e_max)
    assert np.allclose(s, ref)
    # The value no longer depends on the other windows of the scored set.
    alone = train_mod.score_dagmm(model, X[1:2], mean, std, e_max)
    assert np.allclose(alone, s[1:2])
    del torch


def test_deep_svdd_degenerate_check_is_decided_on_train(train_mod):
    torch = pytest.importorskip("torch")

    class Stub:
        def __init__(self, z_fn):
            self.center = torch.zeros(2)
            self.z_fn = z_fn

        def eval(self):
            return self

        def encode(self, xb):
            return self.z_fn(xb)

    spread = Stub(lambda xb: xb[:, 0, :2])
    train = np.random.default_rng(0).normal(size=(50, 3, 2)).astype(np.float32)
    mean, std, degenerate = train_mod.get_training_dist_stats_svdd(spread, train)
    assert degenerate is False
    same = np.zeros((10, 3, 2), dtype=np.float32) + 0.3  # one scoring batch whose encodings are all equal
    s = train_mod.score_deep_svdd(spread, same, mean, std, degenerate)
    assert (s < 1.0).all(), "a constant scoring batch is not a degenerate model"
    flat = Stub(lambda xb: torch.zeros(xb.shape[0], 2) + 0.5)
    m2, s2, deg2 = train_mod.get_training_dist_stats_svdd(flat, train)
    assert deg2 is True
    assert (train_mod.score_deep_svdd(flat, train[:7], m2, s2, deg2) == 1.0).all()


def test_f1_threshold_is_searched_over_observed_scores(train_mod, runs):
    rng = np.random.default_rng(1)
    normal = 1.5 + np.abs(rng.normal(size=1000))  # max |z| scores, almost all above 0.99
    anom = 6.0 + rng.normal(size=50)
    scores = np.r_[normal, anom]
    y = np.r_[np.zeros(1000), np.ones(50)].astype(int)
    t = train_mod.find_optimal_threshold(scores, y)
    assert scores.min() <= t <= scores.max()
    flagged = scores >= t
    assert flagged.mean() < 0.2, "the RuleBased F1 threshold must not be an always-fire threshold"
    assert t == np.min(scores[flagged])  # the threshold is an observed score
    out, _d = runs
    r = pd.read_csv(out["RuleBased"][0] / "model_results.csv").iloc[0]
    assert r["predicted_anomalies"] < r["total_samples"]
    cal = _scores(out["RuleBased"][0], "RuleBased")
    cal = cal[cal["role"] == "cal"]["score"]
    assert cal.min() <= r["threshold"] <= cal.max() + 1e-9


def test_seq_length_is_searched_on_5_to_11(train_mod, runs, full, tmp_path):
    cfg = load_config()
    assert cfg["evaluation"]["seq_length"] == {"min": 5, "max": 11}
    out, _d = runs
    r = pd.read_csv(out["CNN_AE"][0] / "model_results.csv").iloc[0]
    assert 5 <= r["seq_length"] <= 11
    p = tmp_path / "cfg.json"
    p.write_text(json.dumps(CFG))
    with pytest.raises(SystemExit):
        train_mod.main(["--model", "CNN_AE", "--windows", str(full.out / "windows_30s.parquet"),
                        "--folds", str(full.out / "folds.parquet"), "--config", str(p), "--output", str(tmp_path / "x"),
                        "--fold", "1", "--max-seq-length", "20"])


def test_windows_tag_the_last_ten_pre_fault_windows(full):
    w = full.windows[30]
    assert "within_run" in w.columns
    labels = pq.read_table(full.out / "labels_30s.parquet")
    assert "within_run" in labels.column_names
    tagged = w[w["within_run"].notna()]
    assert set(tagged["within_run"]) == {"early", "late"}
    assert set(tagged["segment_type"]) == {"PRE_FAULT"} and set(tagged["label"]) == {"NORMAL"}
    tl = full.timeline
    pre = tl[tl["segment_type"] == "PRE_FAULT"].set_index("attempt_key")
    valid = w[(w["segment_type"] == "PRE_FAULT") & (w["label"] == "NORMAL")]
    assert valid["attempt_key"].nunique() >= 60
    for key, g in valid.groupby("attempt_key"):
        g = g.sort_values("start_at")
        assert len(g) == 20, key
        exp = [None] * 10 + ["early"] * 5 + ["late"] * 5
        assert [x if isinstance(x, str) else None for x in g["within_run"]] == exp, key
        assert g["end_at"].iloc[-1] == pytest.approx(pre.loc[key, "end_at"])  # the last window ends at the planned onset
    assert set(tagged["run_type"]) >= {"F0", "F1", "F4", "F5"}


@pytest.mark.parametrize("model", ["IF", "CNN_AE"])
def test_every_model_scores_the_same_within_run_negatives(runs, full, model):
    out, _d = runs
    s = _scores(out[model][0], model)
    assert "within_run" in s.columns
    w = full.windows[30].set_index("window_id")
    test_ids = w.index[(w["block"] == 1) & w["within_run"].notna() & w["attempt_key"].notna()]
    got = s[(s["role"] == "test") & s["within_run"].notna()]
    assert set(got["window_id"]) == set(test_ids)
    assert (got.set_index("window_id")["within_run"] == w.loc[got["window_id"], "within_run"]).all()
    r = pd.read_csv(out[model][0] / "model_results.csv").iloc[0]
    assert r["within_run_negatives"] == len(test_ids) and bool(r["within_run_complete"])


def test_train_refuses_a_windows_file_whose_tags_disagree_with_the_config(full, train_mod, tmp_path):
    t = pq.read_table(full.out / "windows_30s.parquet")
    df = t.to_pandas()
    i = int(np.flatnonzero(df["within_run"].to_numpy() == "early")[0])
    df.loc[i, "within_run"] = "late"
    import pyarrow as pa
    bad = tmp_path / "windows_30s.parquet"
    pq.write_table(pa.Table.from_pandas(df, schema=t.schema, preserve_index=False), bad)
    (tmp_path / "windows_30s.parquet.sha256").write_text(sha256_file(bad) + "\n")
    p = tmp_path / "cfg.json"
    p.write_text(json.dumps(CFG))
    with pytest.raises(SystemExit, match="within_run"):
        train_mod.main(["--model", "IF", "--windows", str(bad), "--folds", str(full.out / "folds.parquet"),
                        "--config", str(p), "--output", str(tmp_path / "o"), "--fold", "1"])
