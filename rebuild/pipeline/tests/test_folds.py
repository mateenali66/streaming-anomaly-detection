"""Protocol 4.2 and contract 11.9: leave one block out with the R1 role rotation, no leakage."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from streamprep.build import build_campaign
from streamprep.config import load_config
from streamprep.contract import BuildError


@pytest.fixture(scope="module")
def joined(full):
    allw = pd.concat(list(full.windows.values()), ignore_index=True)
    return full.folds.merge(allw, on="window_id", how="left", validate="many_to_one")


def test_every_window_has_one_role_per_fold(full):
    allw = pd.concat(list(full.windows.values()), ignore_index=True)
    f = full.folds
    assert set(f["fold"]) == set(range(1, 11))
    assert f["fold"].dtype == np.int32
    counts = f.groupby("window_id").size()
    assert (counts == 10).all() and set(counts.index) == set(allw["window_id"])
    assert not f.duplicated(["window_id", "fold"]).any()
    assert set(f["role"]) == {"train", "hpo_val", "cal", "test", "eval_only"}


def test_role_rotation_matches_r1_table5(joined):
    att = joined[joined["block"] > 0]
    for i in range(1, 11):
        roles = att[att["fold"] == i].groupby("block")["role"].agg(lambda s: set(s))
        assert all(len(v) == 1 for v in roles), "a block holds more than one role"
        r = {b: next(iter(v)) for b, v in roles.items()}
        assert r[i] == "test"
        assert r[i % 10 + 1] == "cal"
        assert r[(i + 1) % 10 + 1] == "hpo_val"
        assert sorted(b for b, v in r.items() if v == "train") == sorted(set(range(1, 11)) - {i, i % 10 + 1, (i + 1) % 10 + 1})


def test_baseline_train_and_trailing_eval_only_in_every_fold(joined):
    assert (joined.loc[joined["segment_type"] == "BASELINE", "role"] == "train").all()
    assert (joined.loc[joined["segment_type"] == "TRAILING", "role"] == "eval_only").all()
    assert set(joined.loc[joined["role"] == "eval_only", "segment_type"]) == {"TRAILING"}


def test_no_fold_leakage(full, joined):
    atts = {a["attempt_key"]: a for a in full.manifest["attempts"]}
    for i in range(1, 11):
        f = joined[joined["fold"] == i]
        test_keys = set(f.loc[f["role"] == "test", "attempt_key"].dropna())
        other = set(f.loc[f["role"].isin(["train", "cal", "hpo_val"]), "attempt_key"].dropna())
        assert test_keys and not (test_keys & other)
        assert (f.loc[f["role"].isin(["train", "cal", "hpo_val"]), "block"] != i).all()
        spans = np.array([(round(atts[k]["started_at"] * 1000), round(atts[k]["ended_at"] * 1000)) for k in test_keys])
        fit = f[f["role"].isin(["train", "cal", "hpo_val"])]
        s = np.round(fit["start_at"].to_numpy() * 1000)
        e = np.round(fit["end_at"].to_numpy() * 1000)
        for a, b in spans:
            assert not ((s < b) & (e > a)).any(), f"fold {i}: a fit window overlaps test attempt time"
        # Every window of a test-block attempt, valid or not, carries the test role.
        all_test_block = set(f.loc[f["block"] == i, "attempt_key"])
        assert all_test_block == test_keys


def test_folds_never_mix_campaigns(full, iot2):
    a = set(full.folds["window_id"].str.split("-").str[:2].str.join("-"))
    b = set(iot2.folds["window_id"].str.split("-").str[:2].str.join("-"))
    assert a == {"ec-a"} and b == {"iot-b"}


def test_pilot_gets_one_fold(pilot):
    f = pilot.folds
    assert set(f["fold"]) == {1}
    w = pilot.windows[30].merge(f, on="window_id")
    assert set(w.loc[w["block"] == 1, "role"]) == {"test"}
    assert set(w.loc[w["segment_type"] == "BASELINE", "role"]) == {"train"}


def test_replicate_campaign_needs_the_freeze_tag(iot2, tmp_path):
    cfg = load_config()
    rep = build_campaign(iot2.campaign, tmp_path / "labels_only", cfg, stages=("labels",))
    assert "labels_30s.parquet" in rep["outputs"]
    for stage in ("features", "folds"):
        with pytest.raises(BuildError, match="freeze"):
            build_campaign(iot2.campaign, tmp_path / stage, cfg, stages=(stage,))
    assert iot2.rep["freeze_marker_sha256"]
