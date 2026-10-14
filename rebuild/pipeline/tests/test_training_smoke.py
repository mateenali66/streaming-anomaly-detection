"""Smoke test for training/train.py on the synthetic outputs, plus a check that the model code is the R1 code except
for the seed and the scoring changes of Review 4 (decision 26, MIN-8), whose source is pinned here."""
from __future__ import annotations

import ast
import hashlib
import importlib.util
import json
import os
import time
from pathlib import Path

import pandas as pd
import pytest

PIPE = Path(__file__).resolve().parents[1]
R1 = PIPE.parents[1] / "sagemaker" / "training.py"
NEW = PIPE / "training" / "train.py"

MODEL_CODE = [
    "_empty_gpu_cache", "set_seeds", "LSTMAutoencoder", "TransformerAutoencoder", "CNN1DAutoencoder",
    "LSTMVariationalAutoencoder", "vae_loss", "DeepSVDD", "DAGMM", "train_isolation_forest",
    "score_isolation_forest", "train_ocsvm", "score_ocsvm", "train_random_forest", "score_random_forest",
    "train_rule_based", "score_rule_based", "train_autoencoder", "train_vae", "score_autoencoder", "score_vae",
    "get_training_mse_stats", "get_training_mse_stats_vae", "train_deep_svdd", "score_deep_svdd",
    "get_training_dist_stats_svdd", "train_dagmm", "score_dagmm", "get_training_energy_stats_dagmm",
    "find_optimal_threshold",
]


def _segments(path: Path) -> dict[str, str]:
    src = path.read_text()
    tree = ast.parse(src)
    return {n.name: ast.get_source_segment(src, n) for n in tree.body
            if isinstance(n, (ast.FunctionDef, ast.ClassDef))}


# Functions that differ from R1 by design, with the SHA-256 of their rebuild source. Review 4 CAM-5 (decision 26): one
# train NORMAL scaler for IF and OC-SVM, the train maximum for DAGMM's non-finite energies, the Deep SVDD degenerate
# check on train. MIN-8: the F1 threshold over observed scores. Any further edit fails this test, and the list of
# differences from R1 in the paper and in rebuild/review4_fixes/B_pipeline.md must then change with the pin.
PINNED = {
    "fit_score_scaler": "0086848cb728f3dd37aef789a5dae3b926b99ef2a1ad1a05c08a9be3c0b5d5f3",
    "score_isolation_forest": "b7938bf821cc553fe25bc326649f1093a85f5c7294f9736209ccb8e3b2b09ba5",
    "score_ocsvm": "0253012f285a4af0ecbca7fcfb172588a657d7e1736f6f10357f0605f1944b89",
    "score_deep_svdd": "6cdb12fdacc19de5950bec0858b1419ff2c10885caab34386a7714385c7a8c4f",
    "get_training_dist_stats_svdd": "7ff78a4c1425c685951e8e6eb3f643b94c1dcd3742ef10e718fa3646560507fb",
    "score_dagmm": "a71f8556bf6a10902969c4398d53b7756f771350c2462f1f1be60832df861271",
    "get_training_energy_stats_dagmm": "e5acc13db55de78e28e37158105a99989a0ce543c8d8cb514a15e88b7dfa526b",
    "find_optimal_threshold": "a1593dab1be8a2cea2cf3ee00aa7d764ff65e48ff88e532e6f0058900050b2d1",
}


def test_model_code_is_the_r1_code_or_the_pinned_rebuild_code():
    old, new = _segments(R1), _segments(NEW)
    for name in MODEL_CODE:
        assert name in old and name in new, name
        if name in PINNED:
            assert new[name] != old[name], f"{name} is pinned as changed but equals R1"
            continue
        assert new[name].replace("MODEL_SEED", "42") == old[name], f"{name} differs from R1 beyond the seed"
    changed = [n for n in MODEL_CODE if n not in PINNED and new[n] != old[n]]
    assert sorted(changed) == ["train_isolation_forest", "train_ocsvm", "train_random_forest"]
    for name, sha in PINNED.items():
        got = hashlib.sha256(new[name].encode("utf-8")).hexdigest()
        assert got == sha, f"{name} changed since it was pinned ({got}); update PINNED and the R1 difference list"
    assert "fit_score_scaler" not in old


@pytest.fixture(scope="module")
def train_mod():
    os.environ["STREAMPREP_DEVICE"] = "cpu"
    spec = importlib.util.spec_from_file_location("rebuild_train", NEW)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_trains_isolation_forest_and_an_autoencoder_in_under_a_minute(full, train_mod, tmp_path):
    cfg = {"seed_base": 42, "seed_campaign_multiplier": 1000, "search_campaign_index": 1, "max_epochs": 30,
           "trials": {"deep": 1, "classical": 2, "RF": 2}, "variance_threshold": 0.01,
           "correlation_threshold": 0.95, "fp_budgets": [0.01, 0.05]}
    cfg_path = tmp_path / "cfg.json"
    cfg_path.write_text(json.dumps(cfg))
    win = full.out / "windows_30s.parquet"
    folds = full.out / "folds.parquet"
    t0 = time.time()
    for model in ("IF", "CNN_AE"):
        res = train_mod.main(["--model", model, "--windows", str(win), "--folds", str(folds), "--config", str(cfg_path),
                              "--output", str(tmp_path / model), "--fold", "1", "--max-seq-length", "8"])
        assert res and res[0]["result"]["auc_roc"] > 0
    elapsed = time.time() - t0
    assert elapsed < 60, f"smoke training took {elapsed:.1f} s"
    for model in ("IF", "CNN_AE"):
        out = tmp_path / model
        r = pd.read_csv(out / "model_results.csv").iloc[0]
        assert r["hpo_objective"] == "pooled_auc_roc_hpo_val"
        assert r["seed"] == 1000 * 1 + 42 + 1
        s = pd.read_parquet(out / "scores" / f"{model}_fold1.parquet")
        assert set(s["role"]) == {"test", "cal", "eval_only"}
        w = pd.read_parquet(win).set_index("window_id")
        lab = w.loc[s["window_id"], "label"]
        assert not lab.isin(["EXCLUDED"]).any()
        assert set(lab[s["role"].to_numpy() == "eval_only"]) == {"TRAILING"}
        blocks = w.loc[s.loc[s["role"] == "test", "window_id"], "block"]
        assert set(blocks) == {1}
        ft = json.loads((out / "models" / "fold_transform_fold1.json").read_text())
        assert 0 < len(ft["selected"]) < len([c for c in w.columns if c.startswith("f__")])
    cnn = pd.read_csv(tmp_path / "CNN_AE" / "model_results.csv").iloc[0]
    assert 5 <= cnn["seq_length"] <= 8


def test_pilot_trains_only_with_a_frozen_configuration(pilot, train_mod, tmp_path):
    """The one-block pilot has test and train only (A15), so no search can run. The pilot of 14 October 2026 stopped at
    "no ANOMALOUS windows in hpo_val". It now asks for --frozen-params and then trains and scores its test block."""
    win = pilot.out / "windows_30s.parquet"
    folds = pilot.out / "folds.parquet"
    with pytest.raises(SystemExit, match="one-block pilot"):
        train_mod.main(["--model", "IF", "--windows", str(win), "--folds", str(folds), "--output", str(tmp_path)])
    frozen = tmp_path / "frozen.json"
    frozen.write_text(json.dumps({"IF": {"n_estimators": 50, "contamination": 0.05, "max_features": 1.0,
                                         "max_samples": 1.0}}))
    res = train_mod.main(["--model", "IF", "--windows", str(win), "--folds", str(folds), "--output", str(tmp_path / "o"),
                          "--frozen-params", str(frozen)])
    assert len(res) == 1 and res[0]["result"]["hpo_objective"] == "frozen"
    s = pd.read_parquet(tmp_path / "o" / "scores" / "IF_fold1.parquet")
    assert set(s["role"]) == {"test", "eval_only"}


def test_replicate_campaign_requires_freeze_and_frozen_params(iot2, train_mod, tmp_path):
    win = iot2.out / "windows_30s.parquet"
    folds = iot2.out / "folds.parquet"
    with pytest.raises(SystemExit, match="replicate"):
        train_mod.main(["--model", "IF", "--windows", str(win), "--folds", str(folds), "--output", str(tmp_path),
                        "--fold", "1"])
    frozen = tmp_path / "frozen.json"
    frozen.write_text(json.dumps({"IF": {"n_estimators": 50, "contamination": 0.05, "max_features": 1.0,
                                         "max_samples": 1.0}}))
    res = train_mod.main(["--model", "IF", "--windows", str(win), "--folds", str(folds), "--output", str(tmp_path / "o"),
                          "--fold", "2", "--freeze-marker", str(iot2.freeze_marker), "--frozen-params", str(frozen)])
    assert res[0]["result"]["hpo_objective"] == "frozen" and res[0]["result"]["seed"] == 2000 + 42 + 2
