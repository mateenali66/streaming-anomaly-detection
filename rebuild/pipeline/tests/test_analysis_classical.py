"""analysis/classical.py: the copied score functions equal the registered classical_baselines.py, the curated rules on
hand-built signals, and the persistence rule over contiguous stretches."""
from __future__ import annotations

import ast
import hashlib
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

PIPE = Path(__file__).resolve().parents[1]
REPO = PIPE.parents[1]
sys.path.insert(0, str(PIPE))
from analysis import classical as C  # noqa: E402

WT = REPO / "analysis" / "cross_workload" / "classical_baselines.py"
WT_SHA = "ac2de7ee8a9fdae54822927b7af221f05fbb6daa04402aa498527cac5ee545fb"


def _fn(src: str, name: str) -> ast.FunctionDef:
    return next(n for n in ast.parse(src).body if isinstance(n, ast.FunctionDef) and n.name == name)


@pytest.mark.parametrize("name", list(C.DETECTOR_FUNCS.values()))
def test_score_functions_equal_the_registered_commit(name):
    reg = subprocess.run(["git", "-C", str(REPO), "show", "d7521a1:analysis/cross_workload/classical_baselines.py"],
                         capture_output=True, text=True)
    if reg.returncode != 0:
        pytest.skip("registered commit not available")
    assert ast.dump(_fn((PIPE / "analysis" / "classical.py").read_text(), name)) == ast.dump(_fn(reg.stdout, name))


def test_pot_monotone_equals_the_r2_draft_score_pot():
    if not WT.is_file() or hashlib.sha256(WT.read_bytes()).hexdigest() != WT_SHA:
        pytest.skip("R2 draft classical_baselines.py not present at the recorded hash")
    mine = _fn((PIPE / "analysis" / "classical.py").read_text(), "score_pot_monotone")
    mine.name = "score_pot"
    assert ast.dump(mine) == ast.dump(_fn(WT.read_text(), "score_pot"))


def _frame(groups, idx):
    return pd.DataFrame({"seq_group": groups, "seq_index": idx})


def test_previous_fired_stays_inside_a_stretch():
    f = _frame(["a", "a", "a", "b", "b"], [0, 1, 3, 1, 2])
    fired = np.array([1, 1, 1, 1, 1], bool)
    assert C.previous_fired(f, fired).tolist() == [False, True, False, False, True]


def test_rules_fire_as_written():
    f = _frame(["a"] * 5, [0, 1, 2, 3, 4])
    sig = pd.DataFrame({"consumer_lag": [20_000, 20_000, 0, 20_000, 0], "under_repl": [0, 0, 1, 0, 0],
                        "failed_ckpt_rate": [0, 0, 0, 0.1, 0], "backpressure": [150, 0, 150, 150, 0],
                        "offline_part": [0, 0, 0, 0, 2], "rebal_failed": [0, 0, 0, 0, 0],
                        "heap_used": [95, 50, 50, 50, np.nan], "heap_committed": [100, 100, 0, 100, 100]})
    fired = C.fire_rules(f, sig, *C.FIXED)
    assert fired["R1"].tolist() == [0, 1, 0, 0, 0]          # two windows in a row
    assert fired["R2"].tolist() == [0, 0, 1, 0, 0]
    assert fired["R3"].tolist() == [0, 0, 0, 1, 0]
    assert fired["R4"].tolist() == [0, 0, 0, 1, 0]          # the first 150 has no firing predecessor
    assert fired["R5"].tolist() == [0, 0, 0, 0, 1]
    assert fired["R7"].tolist() == [1, 0, 0, 0, 0]          # committed 0 or used missing never fires
    assert fired.sum(axis=1).tolist() == [1, 1, 1, 2, 1]


def test_rule_columns_need_every_signal():
    names = ["f__broker__kafka_server_replicamanager_underreplicatedpartitions__b0__max"]
    with pytest.raises(SystemExit, match="no rebuilt column"):
        C.rule_columns(names)


def test_loda_is_deterministic_once_seeded():
    pytest.importorskip("pyod")
    C._imports()
    rng = np.random.default_rng(0)
    Xt, Xe = rng.normal(size=(300, 8)), rng.normal(size=(50, 8))
    np.random.seed(42)
    a = C.score_loda(Xt, Xe)
    np.random.seed(42)
    b = C.score_loda(Xt, Xe)
    assert np.array_equal(a, b)
