"""Review 4 MIN-6 (validate_data checks every role, not only the test block) and MIN-7 (an Optuna study is never reused
across window sizes, leave-one-fault-out analyses, the derived-feature switch or a different windows file)."""
from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import sqlite3
from pathlib import Path

import pandas as pd
import pytest

NEW = Path(__file__).resolve().parents[1] / "training" / "train.py"


@pytest.fixture(scope="module")
def train_mod():
    os.environ["STREAMPREP_DEVICE"] = "cpu"
    spec = importlib.util.spec_from_file_location("rebuild_train_val", NEW)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _caught(train_mod, win, folds) -> str | None:
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf):
            train_mod.validate_data(win, folds, 1)
    except SystemExit:
        return buf.getvalue()
    return None


def _ids(win, mask) -> set:
    return set(win.loc[mask, "window_id"])


def _set_role(folds, ids, role):
    f = folds.copy()
    f.loc[(f["fold"] == 1) & f["window_id"].isin(ids), "role"] = role
    return f


def test_validate_data_checks_every_role(full, train_mod):
    win = full.windows[30]
    folds = full.folds
    f1 = folds[folds["fold"] == 1].set_index("window_id")["role"]
    role = win["window_id"].map(f1)
    assert _caught(train_mod, win, folds) is None, "the clean folds must pass"
    blk = win["block"]
    cal_anom = win.loc[(role == "cal") & (win["label"] == "ANOMALOUS"), "window_id"].iloc[:1]
    hpo_one = win.loc[(role == "hpo_val") & (win["label"] == "NORMAL"), "window_id"].iloc[:1]
    base = win.loc[win["segment_type"] == "BASELINE", "window_id"]
    trail = win.loc[win["segment_type"] == "TRAILING", "window_id"].iloc[:1]
    train_block = int(blk[(role == "train") & win["attempt_key"].notna()].iloc[0])
    test_pre = win.loc[(role == "test") & (win["segment_type"] == "PRE_FAULT"), "window_id"].iloc[:1]
    hpo_block = int(blk[role == "hpo_val"].iloc[0])
    cases = {
        "a cal attempt's ANOMALOUS window moved to train": _set_role(folds, set(cal_anom), "train"),
        "an hpo_val attempt split across hpo_val and cal": _set_role(folds, set(hpo_one), "cal"),
        "a BASELINE window in test": _set_role(folds, {base.iloc[0]}, "test"),
        "a BASELINE window in cal": _set_role(folds, {base.iloc[1]}, "cal"),
        "a BASELINE window in eval_only": _set_role(folds, {base.iloc[2]}, "eval_only"),
        "a TRAILING window in train": _set_role(folds, set(trail), "train"),
        "two cal blocks": _set_role(folds, _ids(win, blk == train_block), "cal"),
        "two test blocks": _set_role(folds, _ids(win, blk == train_block), "test"),
        "no hpo_val block": _set_role(folds, _ids(win, blk == hpo_block), "train"),
        "a test PRE_FAULT window in train": _set_role(folds, set(test_pre), "train"),
    }
    missed = [name for name, f in cases.items() if _caught(train_mod, win, f) is None]
    assert not missed, missed


def _run(train_mod, full, tmp, w=30, extra=()):
    cfg = {"seed_base": 42, "seed_campaign_multiplier": 1000, "search_campaign_index": 1, "max_epochs": 30,
           "trials": {"deep": 1, "classical": 2, "RF": 2}, "variance_threshold": 0.01,
           "correlation_threshold": 0.95, "fp_budgets": [0.01, 0.05]}
    p = tmp / "cfg.json"
    p.write_text(json.dumps(cfg))
    return train_mod.main(["--model", "IF", "--windows", str(full.out / f"windows_{w}s.parquet"),
                           "--folds", str(full.out / "folds.parquet"), "--config", str(p), "--output", str(tmp / "out"),
                           "--fold", "1", *extra])


def test_optuna_studies_are_never_reused_across_analyses(full, train_mod, tmp_path):
    runs = [(30, ()), (60, ()), (30, ("--exclude-derived",)), (30, ("--lofo-held-out", "F4"))]
    for w, extra in runs:
        _run(train_mod, full, tmp_path, w, extra)
    opt = pd.read_csv(tmp_path / "out" / "optuna_results.csv")
    names = opt["study_name"].tolist()
    assert len(set(names)) == 4, names
    assert "_w30_" in names[0] and "_w60_" in names[1]
    assert "derived-off" in names[2] and "derived-on" in names[0]
    assert "lofo-F4" in names[3] and "lofo-none" in names[0]
    sha30 = (full.out / "windows_30s.parquet.sha256").read_text().strip()
    sha60 = (full.out / "windows_60s.parquet.sha256").read_text().strip()
    assert sha30[:12] in names[0] and sha60[:12] in names[1]
    assert (opt["n_trials"] == 2).all(), "every analysis ran its own search"
    # Rerunning the same analysis resumes the same study and runs no new trial.
    _run(train_mod, full, tmp_path, 30, ())
    again = pd.read_csv(tmp_path / "out" / "optuna_results.csv").iloc[-1]
    assert again["study_name"] == names[0] and again["n_trials"] == 2
    # A stored study whose recorded identity differs from the run is refused, never resumed.
    dbs = sorted((tmp_path / "out" / "optuna_dbs").glob("*w30*.db"))
    assert dbs
    with sqlite3.connect(dbs[0]) as con:
        n = con.execute("UPDATE study_user_attributes SET value_json = ? WHERE key = 'folds_sha256'",
                        (json.dumps("0" * 64),)).rowcount
        con.commit()
    assert n >= 1
    with pytest.raises(SystemExit, match="refus"):
        _run(train_mod, full, tmp_path, 30, ())
