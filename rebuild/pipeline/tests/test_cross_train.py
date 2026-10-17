"""Cross-workload fits (training/cross_train.py, protocol 4.3.2) on the synthetic e-commerce campaign 1 and IoT
campaign 2: the 4.3.1 source split, the target scaler, every target test block scored, and the freeze gate."""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

PIPE = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def cross_mod():
    os.environ["STREAMPREP_DEVICE"] = "cpu"
    spec = importlib.util.spec_from_file_location("cross_train", PIPE / "training" / "cross_train.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def derived(full, iot2, tmp_path):
    d = tmp_path / "derived"
    d.mkdir()
    for fx in (full, iot2):
        cid = fx.rep["campaign_id"]
        (d / cid).symlink_to(fx.out, target_is_directory=True)
    return d


def test_source_split_is_blocks_1_to_8_train_9_hpo_val_10_cal(full, cross_mod):
    df = full.windows[30]
    role = cross_mod.source_roles(df)
    att = df["attempt_key"].notna()
    by_block = role[att].groupby(df.loc[att, "block"]).unique()
    assert {int(b): list(r) for b, r in by_block.items()} == {**{b: ["train"] for b in range(1, 9)},
                                                             9: ["hpo_val"], 10: ["cal"]}
    assert set(role[df["segment_type"] == "BASELINE"]) == {"train"}
    assert set(role[df["segment_type"] == "TRAILING"]) == {"eval_only"}


def test_target_scaler_centres_the_target_train_normal_windows(full, iot2, cross_mod):
    src = cross_mod.FoldTransform.from_config(full.cfg)
    df = full.windows[30]
    cols = sorted(set(c for c in df.columns if c.startswith("f__")) & set(iot2.windows[30].columns))
    src.fit(df.loc[df["label"] == "NORMAL", cols])
    tgt = iot2.windows[30]
    normal = tgt.loc[tgt["label"] == "NORMAL"]
    t, rep = cross_mod.refit_on_target(src, normal)
    X = t.transform(normal)
    assert t.selected == src.selected and X.shape[1] == len(src.selected)
    assert np.allclose(X.mean(axis=0), 0.0, atol=1e-4)
    assert rep["target_train_normal"] == len(normal)


def test_cross_fit_scores_every_target_test_block_and_respects_the_freeze(full, iot2, derived, cross_mod, tmp_path):
    shared = tmp_path / "shared.json"
    cross_mod.write_shared(derived, shared, expect=2)
    frozen = tmp_path / "frozen.json"
    frozen.write_text(json.dumps({"IF": {"n_estimators": 50, "contamination": 0.05, "max_features": 1.0,
                                         "max_samples": 1.0}}))
    src, tgt = full.rep["campaign_id"], iot2.rep["campaign_id"]
    args = ["--derived", str(derived), "--source", src, "--model", "IF", "--frozen-params", str(frozen),
            "--shared", str(shared), "--output", str(tmp_path / "o")]
    with pytest.raises(SystemExit, match="replicate"):
        cross_mod.main(args)
    cross_mod.main(args + ["--freeze-marker", str(iot2.freeze_marker)])
    auc = pd.read_csv(tmp_path / "o" / "auc.csv")
    assert list(auc["target"].unique()) == [tgt] and sorted(auc["fold"]) == list(range(1, 11))
    assert auc["auc_roc"].between(0, 1).all() and not auc["same_workload"].any()
    s = pd.read_parquet(tmp_path / "o" / "scores" / f"IF_{tgt}.parquet")
    folds = iot2.folds
    for k, part in s.groupby("fold"):
        test_ids = set(folds.loc[(folds["fold"] == k) & (folds["role"] == "test"), "window_id"])
        assert set(part["window_id"]) <= test_ids
    summary = json.loads((tmp_path / "o" / "summary.json").read_text())
    assert summary["seed"] == 1000 * 1 + 42 and 0 < summary["n_selected"] <= summary["n_shared"]
