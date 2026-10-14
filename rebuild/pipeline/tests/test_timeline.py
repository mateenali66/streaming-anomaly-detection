"""Protocol 3.1: every second belongs to exactly one segment, EXCLUDED is the default label, and
the build fails if any second is unassigned or assigned twice."""
from __future__ import annotations

import numpy as np
import pytest

from conftest import edit_manifest
from streamprep.build import build_campaign
from streamprep.contract import BuildError
from streamprep.timeline import per_second_coverage, per_second_labels


def test_every_second_covered_exactly_once(full):
    tl = full.tl
    cov = per_second_coverage(tl)
    n = len(cov)
    expected = np.full(n, 1000)
    rem = (tl.end_ms - tl.start_ms) % 1000
    if rem:
        expected[-1] = rem
    assert (cov == expected).all()
    assert n == -((-(tl.end_ms - tl.start_ms)) // 1000)


def test_timeline_parquet_chains_from_start_to_end(full):
    t = full.timeline
    m = full.manifest
    assert t["start_at"].iloc[0] == pytest.approx(m["timeline_start_at"], abs=1e-6)
    assert t["end_at"].iloc[-1] == pytest.approx(m["timeline_end_at"], abs=1e-6)
    s = np.round(t["start_at"].to_numpy() * 1000).astype(np.int64)
    e = np.round(t["end_at"].to_numpy() * 1000).astype(np.int64)
    assert (s[1:] == e[:-1]).all(), "a gap or overlap between consecutive segments"
    assert (e >= s).all()
    assert list(t["segment_index"]) == list(range(len(t)))
    assert t["segment_index"].dtype == np.int32


def test_excluded_is_the_default_label(full):
    labels = per_second_labels(full.tl)
    seg = full.tl.segments
    labelled = seg[seg["label"] != "EXCLUDED"]
    assert set(labelled["segment_type"]) <= {"PRE_FAULT", "INJECTION", "RECOVERY", "BASELINE", "TRAILING"}
    for typ in ("WARMUP", "IDLE", "SETTLE", "APPLY", "COOLDOWN", "GATE"):
        assert (seg.loc[seg["segment_type"] == typ, "label"] == "EXCLUDED").all(), typ
    # Seconds that are not EXCLUDED add up to the labelled segments, give or take one partial
    # second at each labelled segment boundary.
    n_labelled_s = int((labels != "EXCLUDED").sum())
    total = float(((labelled["end_ms"] - labelled["start_ms"]) / 1000).sum())
    assert abs(n_labelled_s - total) <= 2 * len(labelled)
    # Every failed attempt contributes only EXCLUDED segments.
    failed = seg[seg["outcome"].isin(["GATE_FAILED", "TELEMETRY_FAILED", "ABORTED"])]
    assert len(failed) > 0 and (failed["label"] == "EXCLUDED").all()


def test_unassigned_second_fails_the_build(mutable, tmp_path):
    def drop_idle(m):
        assert m["campaign_segments"][2]["segment_type"] == "IDLE"
        m["campaign_segments"].pop(2)  # the 45 s between BASELINE and block 1
    edit_manifest(mutable, drop_idle)
    from streamprep.config import load_config
    with pytest.raises(BuildError, match="unassigned time"):
        build_campaign(mutable, tmp_path / "out", load_config(), stages=("labels",))


def test_second_assigned_twice_fails_the_build(mutable, tmp_path):
    def overlap(m):
        idle = [s for s in m["campaign_segments"] if s["segment_type"] == "IDLE"][0]
        idle["start_at"] = round(idle["start_at"] - 1.0, 3)
    edit_manifest(mutable, overlap)
    from streamprep.config import load_config
    with pytest.raises(BuildError, match="assigned twice"):
        build_campaign(mutable, tmp_path / "out", load_config(), stages=("labels",))
