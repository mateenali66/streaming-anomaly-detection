"""SIGTERM and SIGKILL in every phase, including during a second attempt and after verification. The resumed
orchestrator continues at the same plan position, uses the right attempt number and never creates a duplicate valid
record (2.6.3, 2.7.5, 11.5; Review 2 item 5 and adversarial test B)."""
import json
from collections import defaultdict

import pytest

from orchestrator import gate
from orchestrator.fakes.clock import ProcessDead
from orchestrator.fakes.harness import Harness
from orchestrator.schema import timeline_gaps, validate_manifest


@pytest.fixture(scope="module")
def reference(tmp_path_factory):
    """One uninterrupted pilot with a repeated F4 attempt (attempt 1 fails, attempt 2 passes), for phase times."""
    h = Harness("ecommerce", 0, root=tmp_path_factory.mktemp("ref"))
    h.world.knobs["F4"] = {"fail_first_n": 1}
    assert h.run() == 0
    m = h.manifest()
    times = {}
    segs = m["campaign_segments"]
    times["STARTUP"] = h.world.t0 + 30
    times["WARMUP"] = next(s for s in segs if s["segment_type"] == "WARMUP")["start_at"] + 100
    base = [s for s in segs if s["segment_type"] == "BASELINE"]
    times["BASELINE"] = base[0]["start_at"] + 100
    times["IDLE_AFTER_BASELINE"] = base[-1]["end_at"] + 10
    a = next(x for x in m["attempts"] if x["run_type"] == "F5")
    for s in a["segments"]:
        times[s["segment_type"]] = s["start_at"] + (1 if s["segment_type"] == "APPLY" else 20)
    times["POST_PROCESSING"] = a["ended_at"] + 10
    a2 = next(x for x in m["attempts"] if x["run_type"] == "F4" and x["attempt"] == 2)
    times["SECOND_ATTEMPT_INJECTION"] = next(s for s in a2["segments"] if s["segment_type"] == "INJECTION")["start_at"] + 30
    times["SECOND_ATTEMPT_SETTLE"] = a2["started_at"] + 5
    trailing = [s for s in segs if s["segment_type"] == "TRAILING"]
    times["TRAILING"] = trailing[0]["start_at"] + 60
    return times


def check_final(h, interrupted_running: str | None):
    m = h.manifest()
    validate_manifest(m)
    assert m["status"] == "COMPLETE" and "COMPLETE" in h.s3.objects
    assert timeline_gaps(m) == []
    by_cell = defaultdict(list)
    for a in m["attempts"]:
        by_cell[a["cell_key"]].append(a)
    for blk in h.plan["blocks"]:
        for c in blk["cells"]:
            mine = by_cell[c["cell_key"]]
            assert sum(1 for a in mine if a["outcome"] in ("VALID", "VALID_REMEDIATED")) == 1, c["cell_key"]
            assert [a["attempt"] for a in mine] == list(range(1, len(mine) + 1))
    assert len({a["run_id"] for a in m["attempts"]}) == len(m["attempts"])
    assert all(h_["resolution"] == "RESUMED" for h_ in m["halts"])
    if interrupted_running:
        ab = next(a for a in m["attempts"] if a["attempt_key"] == interrupted_running)
        assert ab["outcome"] == "ABORTED"
        assert all(r["not_found_at"] is not None for r in ab["resources"])
        later = [a for a in by_cell[ab["cell_key"]] if a["attempt"] > ab["attempt"]]
        assert later and later[0]["attempt"] == ab["attempt"] + 1  # the next number, never a reused one
        assert any(a["outcome"] in ("VALID", "VALID_REMEDIATED") for a in later)
        assert all(a["outcome"] == "ABORTED" for a in later[:-1] if a["outcome"] not in ("GATE_FAILED", "TELEMETRY_FAILED"))
    # plan position: cells are completed in plan order
    order = [c["cell_key"] for b in h.plan["blocks"] for c in b["cells"]]
    first_valid = [next(a["started_at"] for a in by_cell[k] if a["outcome"] in ("VALID", "VALID_REMEDIATED")) for k in order]
    assert first_valid == sorted(first_valid)
    # the offline gate reproduces every online result, aborted attempts included
    for a in m["attempts"]:
        online = json.loads(h.s3.get(a["verification_key"]))
        gi = gate.load_inputs(h.s3, m, h.plan, a["attempt_key"], h.th, facts=online)
        offline = gate.verification_record(gi, gate.evaluate(gi), "OFFLINE", "x", h.th and online["thresholds_sha256"], 0.0)
        assert gate.compare(online, offline) == [], a["attempt_key"]
    return m


PHASES = ["STARTUP", "WARMUP", "BASELINE", "IDLE_AFTER_BASELINE", "SETTLE", "PRE_FAULT", "APPLY", "INJECTION",
          "RECOVERY", "COOLDOWN", "GATE", "POST_PROCESSING", "SECOND_ATTEMPT_SETTLE", "SECOND_ATTEMPT_INJECTION", "TRAILING"]


def running_attempt(h):
    p = h.volume / "manifest.json"
    if not p.exists():
        return None
    return next((a["attempt_key"] for a in json.loads(p.read_text())["attempts"] if a["status"] == "RUNNING"), None)


@pytest.mark.parametrize("phase", PHASES)
def test_sigkill_in_every_phase(tmp_path, reference, phase):
    h = Harness("ecommerce", 0, root=tmp_path / "k")
    h.world.knobs["F4"] = {"fail_first_n": 1}
    h.clock.kill_at = reference[phase]
    with pytest.raises(ProcessDead):
        h.run()
    running = running_attempt(h)
    h.clock.restart(45)
    if (h.volume / "manifest.json").exists():
        assert h.run() == 4  # a crash needs the operator's --resume confirmation (2.6.6)
    assert h.run(resume=True) == 0
    m = check_final(h, running)
    if (h.volume / "manifest.json").exists() and phase not in ("STARTUP",):
        assert any(x["reason"].startswith("ORCHESTRATOR_STOPPED") for x in m["halts"])


@pytest.mark.parametrize("phase", PHASES)
def test_sigterm_in_every_phase(tmp_path, reference, phase):
    h = Harness("ecommerce", 0, root=tmp_path / "t")
    h.world.knobs["F4"] = {"fail_first_n": 1}
    h.clock.term_at = reference[phase]
    rc = h.run()
    assert rc in (143, 2), rc
    running = running_attempt(h)
    # SIGTERM cleanup: no chaos resource of the campaign is left behind
    t = h.clock.t
    assert not [c for c in h.world.chaos if c.gone_at is None or c.gone_at > t]
    if (h.volume / "manifest.json").exists():
        m = h.manifest()
        assert m["halts"][-1]["reason"].startswith("ORCHESTRATOR_STOPPED: SIGTERM")
        assert m["halts"][-1]["attempt_key"] == running
    h.clock.restart(45)
    assert h.run(resume=True) == 0
    check_final(h, running)


@pytest.mark.parametrize("after", ["verification.json", "probes.parquet", "features.parquet"])
def test_sigkill_right_after_an_object_is_stored(tmp_path, after):
    """After verification (and during export): the record exists in S3 but the attempt is not FINISHED. Resume must
    finish it as ABORTED and repeat the cell; it must never count it as valid (Review 2 adversarial B3/B4)."""
    h = Harness("ecommerce", 0, root=tmp_path / "v")
    h.s3.kill_after_put = lambda key: key.endswith(after) and "-p2-" in key
    with pytest.raises(ProcessDead):
        h.run()
    running = running_attempt(h)
    assert running and "-p2-a1" in running
    h.clock.restart(45)
    assert h.run(resume=True) == 0
    m = check_final(h, running)
    ab = next(a for a in m["attempts"] if a["attempt_key"] == running)
    assert ab["outcome"] == "ABORTED"


def test_double_crash_during_the_resume_recovery_gate(tmp_path, reference):
    h = Harness("ecommerce", 0, root=tmp_path / "d")
    h.world.knobs["F4"] = {"fail_first_n": 1}
    h.clock.kill_at = reference["INJECTION"]
    with pytest.raises(ProcessDead):
        h.run()
    running = running_attempt(h)
    h.clock.restart(45)
    h.clock.kill_at = h.clock.t + 1000  # shortly after the resume's 300 s wait and recovery gate
    with pytest.raises(ProcessDead):
        h.run(resume=True)
    second = running_attempt(h)
    h.clock.restart(45)
    assert h.run(resume=True) == 0
    m = check_final(h, running)
    assert len([x for x in m["halts"] if x["reason"].startswith("ORCHESTRATOR_STOPPED")]) == 2
    if second:
        assert next(a for a in m["attempts"] if a["attempt_key"] == second)["outcome"] == "ABORTED"


def test_resume_without_confirmation_changes_nothing(tmp_path, reference):
    h = Harness("ecommerce", 0, root=tmp_path / "n")
    h.clock.term_at = reference["INJECTION"]
    assert h.run() == 143
    before = (h.volume / "manifest.json").read_bytes()
    h.clock.restart(45)
    assert h.run() == 4
    assert (h.volume / "manifest.json").read_bytes() == before


def test_campaign_pause_of_two_hours_is_not_resumed(tmp_path):
    """2.6.6: resume only if the pause lasted under two hours (campaigns, not the pilot)."""
    h = Harness("iot", 1, root=tmp_path / "p")
    h.clock.term_at = h.world.t0 + 1500
    assert h.run() == 143
    h.clock.restart(2 * 3600 + 10)
    assert h.run(resume=True) == 4
    assert h.manifest()["status"] == "RUNNING"


def test_campaign_resume_refused_after_a_code_change(tmp_path):
    h = Harness("iot", 1, root=tmp_path / "c")
    h.clock.term_at = h.world.t0 + 1500
    assert h.run() == 143
    h.env.git_commit = "1" * 40
    h.clock.restart(60)
    assert h.run(resume=True) == 4


def test_campaign_resume_under_two_hours_continues(tmp_path):
    h = Harness("iot", 1, root=tmp_path / "r", overrides={"fixed": {"baseline_s": 1200}})
    h.clock.term_at = h.world.t0 + 1500
    assert h.run() == 143
    h.clock.restart(600)
    h.clock.term_at = h.clock.t + 6000  # stop again inside block 1 to keep the test short
    assert h.run(resume=True) == 143
    m = h.manifest()
    validate_manifest(m)
    assert m["halts"][0]["resolution"] == "RESUMED"
    idle = [s for s in m["campaign_segments"] if s["segment_type"] == "IDLE"]
    assert idle  # the paused time is an IDLE segment (2.6.6)
