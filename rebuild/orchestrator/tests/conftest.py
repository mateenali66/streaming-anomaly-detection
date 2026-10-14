import json
import logging
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from orchestrator import schema  # noqa: E402
from orchestrator.fakes.harness import Harness  # noqa: E402


@pytest.fixture(autouse=True)
def _quiet():
    logging.getLogger("orchestrator").setLevel(logging.CRITICAL)
    yield


@pytest.fixture
def make(tmp_path):
    """make(workload, index, knobs={run_type: {...}}, world={global knob: value}, overrides={config}) -> Harness"""
    counter = {"n": 0}

    def _make(workload="ecommerce", index=0, knobs=None, world=None, overrides=None):
        counter["n"] += 1
        h = Harness(workload, index, root=tmp_path / f"h{counter['n']}", overrides=overrides)
        for rt, kv in (knobs or {}).items():
            h.world.knobs.setdefault(rt, {}).update(kv)
        h.world.global_knobs.update(world or {})
        return h
    return _make


def attempts(h, run_type=None):
    m = h.manifest()
    return [a for a in m["attempts"] if run_type is None or a["run_type"] == run_type]


def verification(h, a):
    return json.loads(h.s3.get(a["verification_key"]))


def checks_by_name(v):
    out = {}
    for c in v["checks"]:
        out.setdefault(c["name"], []).append(c)
    return out


def assert_manifest_ok(h):
    m = h.manifest()
    schema.validate_manifest(m)
    return m


def first_index_of(seq, pred):
    for i, x in enumerate(seq):
        if pred(x):
            return i
    return -1
