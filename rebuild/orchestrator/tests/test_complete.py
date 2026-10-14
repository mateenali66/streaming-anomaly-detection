"""COMPLETE is written only when every condition of 11.10 holds, once, last, and newer than every object."""
import json

import pytest

from orchestrator.canonical import sha256_hex
from orchestrator.complete import CHECKSUMS_KEY, verify_complete
from orchestrator.schema import validate_complete


@pytest.fixture
def finished(make):
    h = make()
    assert h.run() == 0
    return h


def test_complete_marker_contents(finished):
    h = finished
    marker = json.loads(h.s3.get("COMPLETE"))
    validate_complete(marker)
    assert marker["status"] == "COMPLETE" and all(c["pass"] for c in marker["checks"])
    objs = h.s3.list("")
    newest_other = max(o.last_modified for o in objs if o.key != "COMPLETE")
    assert marker["written_at"] > newest_other
    sums = h.s3.get(CHECKSUMS_KEY).decode()
    lines = sums.strip().split("\n")
    listed = [ln.split("  ", 1)[1] for ln in lines]
    assert listed == sorted(listed)
    assert "COMPLETE" not in listed and CHECKSUMS_KEY not in listed
    assert set(listed) == {o.key for o in objs} - {"COMPLETE", CHECKSUMS_KEY}
    for ln in lines:
        sha, key = ln.split("  ", 1)
        assert sha256_hex(h.s3.get(key)) == sha
    assert marker["object_count"] == len(lines)
    assert marker["total_bytes"] == sum(len(h.s3.get(k)) for k in listed)
    assert marker["checksums_sha256"] == sha256_hex(h.s3.get(CHECKSUMS_KEY))
    assert marker["manifest_sha256"] == sha256_hex(h.s3.get("manifest/manifest.json"))
    assert marker["plan_sha256"] == h.plan_sha
    assert h.s3.put_log[-1] == "COMPLETE"


def test_complete_is_written_only_once(finished):
    rt = finished.runtime()
    assert verify_complete(rt, finished.manifest()) is None


def _reset(h):
    del h.s3.objects["COMPLETE"]
    del h.s3.objects[CHECKSUMS_KEY]


@pytest.mark.parametrize("damage", ["probe_file", "export_hash", "baseline_chunk", "timeline_gap", "running_attempt",
                                    "missing_cell", "verification_record"])
def test_complete_refused_when_a_condition_fails(finished, damage):
    h = finished
    _reset(h)
    m = h.manifest()
    a = m["attempts"][0]
    if damage == "probe_file":
        del h.s3.objects[a["export"]["probes_key"]]
    elif damage == "export_hash":
        data, t = h.s3.objects[a["export"]["features_key"]]
        h.s3.objects[a["export"]["features_key"]] = (data + b"x", t)
    elif damage == "baseline_chunk":
        seg = next(s for s in m["campaign_segments"] if s["segment_type"] == "BASELINE")
        del h.s3.objects[f"baseline/features/chunk-{int(seg['start_at'])}.parquet"]
    elif damage == "timeline_gap":
        m["campaign_segments"] = [s for s in m["campaign_segments"] if s["segment_type"] != "IDLE"][:]
        idle = [s for s in h.manifest()["campaign_segments"] if s["segment_type"] == "IDLE"]
        assert idle
    elif damage == "running_attempt":
        a["status"] = "RUNNING"
    elif damage == "missing_cell":
        m["attempts"] = m["attempts"][:-1]
    elif damage == "verification_record":
        del h.s3.objects[a["verification_key"]]
    before = set(h.s3.objects)
    assert verify_complete(h.runtime(), m) is None
    assert "COMPLETE" not in h.s3.objects
    assert CHECKSUMS_KEY not in h.s3.objects and set(h.s3.objects) == before


def test_complete_waits_until_it_is_newer_than_every_object(finished):
    h = finished
    _reset(h)
    # an object stored "in the future" relative to the clock: verify_complete must wait past it
    data, _ = h.s3.objects["run/versions.json"]
    h.s3.objects["run/versions.json"] = (data, h.clock.t + 5)
    marker = verify_complete(h.runtime(), h.manifest())
    assert marker is not None and marker["written_at"] > h.clock.t - 1 and marker["written_at"] > max(
        o.last_modified for o in h.s3.list("") if o.key != "COMPLETE")


def test_abandoned_campaign_gets_an_abandoned_marker(make):
    h = make(knobs={"F2": {"effect": "none"}})
    assert h.run() == 3
    camp = h.campaign()
    camp.m = h.manifest()
    assert camp.abandon("author confirmed: F2 cannot pass") == 0
    marker = json.loads(h.s3.get("COMPLETE"))
    assert marker["status"] == "ABANDONED"
    m = h.manifest()
    assert m["status"] == "ABANDONED" and m["halts"][-1]["resolution"] == "ABANDONED"
