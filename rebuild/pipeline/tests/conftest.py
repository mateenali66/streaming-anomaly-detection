"""Shared synthetic campaigns. Each is generated and built once per test session."""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from streamprep.build import build_campaign  # noqa: E402
from streamprep.config import load_config  # noqa: E402
from streamprep.synthetic import SynthConfig, generate_campaign, rehash_campaign  # noqa: E402


def _make(tmp_path_factory, name: str, synth: SynthConfig, freeze: bool = False, **build_kw) -> SimpleNamespace:
    root = tmp_path_factory.mktemp(name)
    camp = root / "campaign"
    truth_dir = root / "truth"
    truth = generate_campaign(camp, synth, truth_dir)
    cfg = load_config()
    marker = None
    if freeze:
        marker = root / "FREEZE.json"
        marker.write_text(json.dumps({"tag": "rebuild-freeze-test"}))
    rep = build_campaign(camp, root / "out", cfg, freeze_marker=marker, keep_step_columns=True, **build_kw)
    out = rep["_out"]
    sizes = rep["window_sizes"]
    win = {w: pd.read_parquet(out / f"windows_{w}s.parquet") for w in sizes}
    return SimpleNamespace(root=root, campaign=camp, truth=truth, truth_dir=truth_dir, cfg=cfg, rep=rep,
                           out=out, windows=win, folds=pd.read_parquet(out / "folds.parquet"),
                           timeline=pd.read_parquet(out / "timeline.parquet"),
                           manifest=json.loads((camp / "manifest" / "manifest.json").read_text()),
                           result=rep["_result"], tl=rep["_timeline"], freeze_marker=marker)


@pytest.fixture(scope="session")
def full(tmp_path_factory):
    """E-commerce campaign 1 with every scenario of synthetic.full_scenario. Window sizes 15, 30, 60, 120."""
    return _make(tmp_path_factory, "full", SynthConfig(workload="ecommerce", campaign_index=1, scenario="full"))


@pytest.fixture(scope="session")
def iot2(tmp_path_factory):
    """IoT campaign 2 (a replicate), clean scenario, one-hour baseline, built with a freeze marker."""
    return _make(tmp_path_factory, "iot2", SynthConfig(workload="iot", campaign_index=2, scenario="clean",
                                                       baseline_s=3600, seed=11, t0_ms=1_790_500_007_431), freeze=True)


@pytest.fixture(scope="session")
def pilot(tmp_path_factory):
    """Pilot campaign (index 0, one block)."""
    return _make(tmp_path_factory, "pilot", SynthConfig(workload="iot", campaign_index=0, scenario="clean",
                                                        baseline_s=3600, seed=5, t0_ms=1_789_900_001_009))


@pytest.fixture
def mutable(full, tmp_path):
    """A fresh copy of the full campaign directory, for tests that break one rule."""
    dst = tmp_path / "campaign"
    shutil.copytree(full.campaign, dst)
    return dst


def edit_manifest(campaign: Path, fn) -> None:
    p = campaign / "manifest" / "manifest.json"
    m = json.loads(p.read_text())
    fn(m)
    p.write_text(json.dumps(m, indent=1, sort_keys=True) + "\n")
    rehash_campaign(campaign)


def edit_json(campaign: Path, rel: str, fn, rehash: bool = True) -> None:
    p = campaign / rel
    obj = json.loads(p.read_text())
    fn(obj)
    p.write_text(json.dumps(obj, indent=1, sort_keys=True) + "\n")
    if rehash:
        rehash_campaign(campaign)


def resync_verification_hash(campaign: Path, key: str) -> None:
    """After editing a verification record, store its new hash in the manifest and rehash."""
    from streamprep.contract import sha256_file
    sha = sha256_file(campaign / "attempts" / key / "verification.json")

    def fn(m):
        for a in m["attempts"]:
            if a["attempt_key"] == key:
                a["verification_sha256"] = sha
    edit_manifest(campaign, fn)


def attempt_by(manifest: dict, pred) -> dict:
    hits = [a for a in manifest["attempts"] if pred(a)]
    assert hits, "no attempt matches"
    return hits[0]
