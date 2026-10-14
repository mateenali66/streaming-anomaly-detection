"""Halt rules of protocol 2.6.5: two consecutive failed attempts counted ACROSS faults, more than 8 in total, a node
replaced, the clock check, an S3 upload failing for 15 minutes, a failing export, and the recovery gate failing after
remediation (Review 1 C15, Review 2 item 8 and adversarial test D)."""
import pytest

from conftest import attempts
from orchestrator.campaign import Campaign
from orchestrator.errors import HaltRequested


def fake_campaign(outcomes, halts=()):
    class R:
        thresholds = {"campaign_rules": {"halt_consecutive_failed": 2, "halt_total_failed_gt": 8}}
    c = Campaign.__new__(Campaign)
    c.rt = R()
    c.m = {"attempts": [{"attempt_key": f"k{i}", "status": "FINISHED", "outcome": o, "started_at": float(i)}
                        for i, o in enumerate(outcomes)], "halts": list(halts)}
    return c


@pytest.mark.parametrize("outcomes,halt", [
    (["VALID", "GATE_FAILED", "TELEMETRY_FAILED"], "CONSECUTIVE"),  # two in a row across faults (the R1 per-fault bug)
    (["GATE_FAILED", "VALID", "GATE_FAILED"], None),
    (["GATE_FAILED", "ABORTED", "GATE_FAILED"], "CONSECUTIVE"),    # an abort neither counts nor resets the streak
    (["GATE_FAILED", "VALID"] * 8, None),                          # 8 failures in total: not more than 8
    (["GATE_FAILED", "VALID"] * 8 + ["GATE_FAILED"], "TOTAL"),     # the 9th failure
])
def test_halt_rule_counts(outcomes, halt):
    c = fake_campaign(outcomes)
    if halt is None:
        c.check_halt_rules()
    else:
        with pytest.raises(HaltRequested) as e:
            c.check_halt_rules()
        assert e.value.reason.startswith("CONSECUTIVE" if halt == "CONSECUTIVE" else "TOTAL")


def test_a_resumed_halt_resets_the_streak_but_not_the_total():
    # attempts start at 0, 1, 2; the author resumed at 1.5, so only attempt 2 counts toward a new streak
    fake_campaign(["GATE_FAILED", "GATE_FAILED", "GATE_FAILED"],
                  halts=[{"resolution": "RESUMED", "resumed_at": 1.5}]).check_halt_rules()
    with pytest.raises(HaltRequested, match="CONSECUTIVE"):
        fake_campaign(["GATE_FAILED", "GATE_FAILED", "GATE_FAILED", "GATE_FAILED"],
                      halts=[{"resolution": "RESUMED", "resumed_at": 1.5}]).check_halt_rules()
    with pytest.raises(HaltRequested, match="TOTAL"):
        fake_campaign(["GATE_FAILED", "VALID"] * 8 + ["GATE_FAILED"],
                      halts=[{"resolution": "RESUMED", "resumed_at": 15.5}]).check_halt_rules()


def test_always_failing_fault_halts_after_two_attempts(make):
    h = make(knobs={"F2": {"effect": "none"}})
    assert h.run() == 3
    m = h.manifest()
    f2 = attempts(h, "F2")
    assert [a["attempt"] for a in f2] == [1, 2] and all(a["outcome"] == "GATE_FAILED" for a in f2)
    assert m["halts"][-1]["reason"] == "CONSECUTIVE_FAILED_ATTEMPTS 2"
    assert m["attempts"][-1]["attempt_key"] == f2[-1]["attempt_key"]  # nothing ran after the halt


def test_more_than_eight_failures_in_a_campaign_halt(make):
    """A full ten-block campaign: every F4 cell fails once and then passes. The ninth failure halts it."""
    h = make("ecommerce", 1, knobs={"F4": {"fail_first_n": 99}}, overrides={"trailing_baseline": False})
    # fail only each cell's first attempt: allow the repeat to work by resetting the knob per cell
    orig = h.world._apply

    def apply(c, t):
        n_f4 = sum(1 for x in h.world.chaos if x.run_type == "F4")
        h.world.knobs["F4"]["fail_first_n"] = n_f4 if n_f4 % 2 == 1 else 0
        orig(c, t)
    h.world._apply = apply
    assert h.run() == 3
    m = h.manifest()
    failed = [a for a in m["attempts"] if a["outcome"] == "GATE_FAILED"]
    assert len(failed) == 9
    assert m["halts"][-1]["reason"] == "TOTAL_FAILED_ATTEMPTS 9"
    assert m["status"] == "RUNNING"


def test_node_replaced_halts_at_the_next_health_gate(make):
    h = make()
    h.world.global_knobs["node_replaced_at"] = h.world.t0 + 3000
    assert h.run() == 3
    m = h.manifest()
    assert m["halts"][-1]["reason"] == "NODE_REPLACED"
    assert len(m["node_inventory"]) == 2 and m["node_inventory"][0]["instance_ids"] != m["node_inventory"][1]["instance_ids"]


def test_clock_skew_halts(make):
    h = make()
    assert h.run(only_fault="F0") == 0
    h.world.global_knobs["prom_clock_offset"] = 2.0
    assert h.run(only_fault="F4") == 3
    assert h.manifest()["halts"][-1]["reason"] == "CLOCK_SKEW"


def test_s3_failing_for_15_minutes_halts(make):
    h = make()
    h.s3.fail_put_until = None
    orig_put = h.s3.put
    window = (h.world.t0 + 2500, h.world.t0 + 2500 + 3000)  # an outage inside the baseline, longer than 15 minutes

    def put(key, data, ct="application/octet-stream"):
        if window[0] <= h.clock.t < window[1]:
            from orchestrator.errors import StoreError
            raise StoreError("simulated outage")
        return orig_put(key, data, ct)
    h.s3.put = put
    assert h.run() == 3
    m = h.manifest()  # the volume copy still records the halt
    assert m["halts"][-1]["reason"] == "S3_UPLOAD_FAILING_15_MIN"
    assert "COMPLETE" not in h.s3.objects


def test_export_failing_for_15_minutes_halts_and_blocks_complete(make):
    h = make()
    h.world.global_knobs["export_fail_until"] = h.world.t0 + 10 ** 9
    assert h.run() == 3
    m = h.manifest()
    assert m["halts"][-1]["reason"].startswith("EXPORT_FAILING_15_MIN")
    assert "COMPLETE" not in h.s3.objects


def test_halts_are_alerted(make):
    h = make(knobs={"F2": {"effect": "none"}})
    h.run(only_fault="F2")
    assert h.alerts and "halted" in h.alerts[-1][1]
