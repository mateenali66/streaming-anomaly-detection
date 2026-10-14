"""Attempt flow: recovery and cooldown always run once anything was applied, deletion is confirmed and timed, a stuck
resource or a leftover Job blocks the next attempt, validity is set only in the final status, and remediation and
redeploys are recorded and flagged (1.3, 1.6, 2.4, 2.6, 2.7)."""
import json

from conftest import attempts, verification
from orchestrator.manifest import ManifestStore
from orchestrator.schema import validate_manifest


def seg_types(a):
    return [s["segment_type"] for s in a["segments"]]


def seg_len(a, t):
    s = next(s for s in a["segments"] if s["segment_type"] == t)
    return s["end_at"] - s["start_at"]


def test_failure_after_partial_injection_still_runs_recovery_and_cooldown(make):
    """Review 1 A3, Review 1 T1: F5 fails after two kills; the retry must not start a minute after a partial fault."""
    h = make(knobs={"F5": {"apply_only_first_n": 2}})
    h.run(only_fault="F5")
    a1, a2 = attempts(h, "F5")[:2]
    assert a1["applied"] and a1["aborted_at"] is not None and a1["outcome"] == "GATE_FAILED"
    assert seg_types(a1) == ["SETTLE", "PRE_FAULT", "APPLY", "INJECTION", "RECOVERY", "COOLDOWN", "GATE"]
    assert seg_len(a1, "RECOVERY") == 300 and seg_len(a1, "COOLDOWN") == 300
    inj = next(s for s in a1["segments"] if s["segment_type"] == "INJECTION")
    assert abs(inj["end_at"] - a1["aborted_at"]) < 1e-6
    assert all(r["not_found_at"] is not None for r in a1["resources"])
    # the retry's pre-fault starts only after recovery, cooldown, the gate and its own settle gap
    retry_pre = next(s for s in a2["segments"] if s["segment_type"] == "PRE_FAULT")
    assert retry_pre["start_at"] >= inj["end_at"] + 600


def test_fault_on_two_of_three_pods_still_runs_recovery_and_cooldown(make):
    h = make(knobs={"F7": {"missing_record": True}})
    h.run(only_fault="F7")
    a = attempts(h, "F7")[0]
    assert a["outcome"] == "GATE_FAILED" and a["applied"]
    assert "RECOVERY" in seg_types(a) and "COOLDOWN" in seg_types(a)
    assert "onset_verified" in a["failed_checks"]


def test_nothing_applied_has_no_injection_segments(make):
    h = make(world={"ws_base_bytes": 3.4 * 1024 ** 3})
    h.run(only_fault="F8")
    a = attempts(h, "F8")[0]
    assert seg_types(a) == ["SETTLE", "PRE_FAULT", "APPLY", "GATE"]


def test_every_deletion_is_confirmed_not_found_and_timed(make):
    h = make()
    assert h.run() == 0
    for a in h.manifest()["attempts"]:
        for r in a["resources"]:
            assert r["delete_called_at"] is not None and r["not_found_at"] is not None
            assert 0 <= r["not_found_at"] - r["delete_called_at"] <= 60
        v = verification(h, a)
        if a["run_type"] in ("F2", "F4", "F7", "F8"):
            before = [s for s in v["chaos_snapshots"] if s["tag"] == "BEFORE_DELETE"]
            assert before and all(s["conditions"]["AllRecovered"]["status"] == "True" for s in before)
            assert any(c["name"] == "all_recovered_observed" and c["pass"] for c in v["checks"])


def test_stuck_chaos_resource_blocks_the_next_attempt(make):
    """Review 2 item 4: a resource that never reaches NotFound must keep the fault out of the next run."""
    h = make(knobs={"F4": {"stuck": True}})
    rc = h.run(only_fault="F4")
    assert rc == 3
    m = h.manifest()
    mine = attempts(h, "F4")
    assert len(mine) == 1  # no second attempt was started
    assert mine[0]["status"] == "RUNNING" and mine[0]["outcome"] is None
    assert m["halts"][-1]["reason"].startswith("RECOVERY_GATE_FAILED_AFTER_REMEDIATION")
    assert m["halts"][-1]["attempt_key"] == mine[0]["attempt_key"]
    facts = json.loads((h.volume / "state" / "attempts" / mine[0]["attempt_key"] / "facts.json").read_text())
    conds = {c["name"]: c for c in facts["recovery_gate_conditions"]}
    assert not conds["no_campaign_chaos_or_jobs"]["pass"] and not conds["deleted_resources_not_found"]["pass"]
    assert mine[0]["recovery_gate"]["remediation"]["performed"]


def test_leftover_injector_job_blocks_the_health_gate(make):
    h = make()
    assert h.run(only_fault="F0") == 0
    h.world.jobs_k8s["poison-orphan"] = {"labels": {"rebuild/campaign": h.campaign_id}, "done_at": None, "ok": True, "pods": []}
    rc = h.run(only_fault="F4")
    assert rc == 3
    assert h.manifest()["halts"][-1]["reason"] == "HEALTH_GATE_FAILED"
    assert attempts(h, "F4") == []


def test_overrun_between_60_and_120_s_is_flagged_and_above_120_fails(make):
    h = make(knobs={"F4": {"recover_delay_s": 90}})
    assert h.run(only_fault="F4") == 0
    a = attempts(h, "F4")[0]
    assert a["outcome"] == "VALID" and "OVERRUN" in a["flags"] and 60 < a["overrun_s"] <= 120
    assert a["labelled_end_at"] - a["onset_verified_at"] > 300 + 60  # the observed end is the labelled end
    h2 = make(knobs={"F4": {"no_recover": True}})
    h2.run(only_fault="F4")
    b = attempts(h2, "F4")[0]
    assert b["outcome"] == "GATE_FAILED" and b["labelled_end_at"] is None and b["aborted_at"] is not None
    assert "end_overrun" in b["failed_checks"]


def test_validity_is_set_only_in_the_final_status(make, monkeypatch):
    seen = []
    orig = ManifestStore.save

    def spy(self, m, validate=True):
        orig(self, m, validate)
        seen.append(json.loads(json.dumps(m)))
    monkeypatch.setattr(ManifestStore, "save", spy)
    h = make()
    assert h.run() == 0
    assert seen
    for m in seen:
        validate_manifest(m)
        for a in m["attempts"]:
            if a["status"] == "RUNNING":
                assert a["outcome"] is None and a["verification_key"] is None
            else:
                assert a["outcome"] is not None and a["verification_sha256"]
    # an attempt goes from RUNNING (no outcome) straight to FINISHED with its outcome, in one write
    transitions = {}
    for m in seen:
        for a in m["attempts"]:
            transitions.setdefault(a["attempt_key"], []).append((a["status"], a["outcome"]))
    for k, seq in transitions.items():
        firsts = [i for i, s in enumerate(seq) if s[0] == "FINISHED"]
        assert all(s == ("RUNNING", None) for s in seq[:firsts[0]])
        assert len({s for s in seq[firsts[0]:]}) == 1


def test_remediation_resubmits_missing_jobs_from_s3_and_is_flagged(make):
    h = make(world={"f5_restore_fails": True})
    h.run(only_fault="F5")
    a = attempts(h, "F5")[0]
    rem = a["recovery_gate"]["remediation"]
    assert rem["performed"] and "REMEDIATED" in a["flags"]
    assert any(x.startswith("PREFERRED_LEADER_ELECTION") for x in rem["actions"])
    resub = [x for x in rem["actions"] if x.startswith("RESUBMIT ")]
    # the action text enters the manifest, so the checkpoint path is relative to the campaign's scratch prefix (C24)
    assert len(resub) == 3 and all("from=flink-checkpoints/" in x and "dry-run-results" not in x for x in resub)
    assert a["outcome"] == "GATE_FAILED" and "env_jobs_running_after_last_kill" in a["failed_checks"]


def test_full_redeploy_flags_the_next_attempt_and_adds_600_s_settle(make):
    h = make(world={"f5_restore_fails": True, "submit_fails_until_redeploy": True})
    h.run(only_fault="F5")
    a1, a2 = attempts(h, "F5")[:2]
    assert any(x.startswith("FLINK_REDEPLOY") for x in a1["recovery_gate"]["remediation"]["actions"])
    assert "AFTER_REDEPLOY" in a2["flags"]
    reserve = h.plan["reserve_settle_gap_s"][0]
    assert seg_len(a2, "SETTLE") == reserve + 600


def test_valid_remediated_when_the_gate_needed_remediation(make):
    h = make()
    orig = h.world._recover

    def recover_then_fail_a_job(c, t):
        orig(c, t)
        if c.run_type == "F4":
            j = next(j for j in h.world.flink_jobs if j.state(t) == "RUNNING")
            j.states.append((t + 1, "FAILED"))
    h.world._recover = recover_then_fail_a_job
    assert h.run(only_fault="F4") == 0
    a = attempts(h, "F4")[0]
    assert a["outcome"] == "VALID_REMEDIATED" and "REMEDIATED" in a["flags"]


def test_jobs_are_checked_by_name_not_by_count(make):
    """Review 1 A2: an IoT cluster running e-commerce jobs was accepted because three jobs were RUNNING. Here the
    gate rejects them by name, and the scripted remediation (resubmit by name, then a full redeploy) leaves only the
    IoT jobs, before the warm-up starts."""
    from orchestrator.recovery import RecoveryGate
    h = make("iot")
    for j in list(h.world.flink_jobs):
        j.removed_at = h.world.t0 - 10
    for name in ("Event Enrichment Job", "Windowed Aggregation Job", "Fraud Detection Job"):
        h.world.submit_job(name, h.world.t0 - 5000, None)
    rt = h.runtime()
    conds = {c["name"]: c for c in RecoveryGate(rt).flink_conditions(h.clock.t - 600, h.clock.t)}
    assert conds["flink_jobs_running_by_name"]["value"] == 0 and not conds["flink_jobs_running_by_name"]["pass"]
    assert conds["flink_no_extra_jobs"]["value"] == 3 and not conds["flink_no_extra_jobs"]["pass"]
    assert h.run() == 0
    assert h.world.redeploys and h.world.redeploys[0] < h.manifest()["timeline_start_at"]
    live = {j.name for j in h.world.flink_jobs if j.state(h.clock.t) == "RUNNING"}
    assert live == {"IoT Event Enrichment Job", "IoT Per-Location Aggregation Job", "IoT Threshold Alert Job"}


def test_sham_that_sees_a_campaign_resource_halts(make, monkeypatch):
    from orchestrator import injectors
    h = make()
    orig = injectors.Sham.hold

    def hold_with_leftover(self):
        h.kube.create({"kind": "PodChaos", "metadata": {"name": "pk-leftover", "labels": {
            "rebuild/campaign": h.campaign_id, "rebuild/run-type": "F1"}},
            "spec": {"action": "pod-kill", "mode": "all", "selector": {"pods": {"streaming": ["streaming-kafka-brokers-1"]}}}})
        orig(self)
    monkeypatch.setattr(injectors.Sham, "hold", hold_with_leftover)
    assert h.run(only_fault="F0") == 3
    m = h.manifest()
    assert m["halts"][-1]["reason"] == "SHAM_SHOWED_CAMPAIGN_RESOURCE"
    facts = json.loads((h.volume / "state" / "attempts" / m["attempts"][-1]["attempt_key"] / "facts.json").read_text())
    assert any(s["name"] == "pk-leftover" for s in facts["chaos_snapshots"])
