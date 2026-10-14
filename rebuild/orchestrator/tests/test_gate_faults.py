"""Every fault's gate passes on a realistic simulated effect and fails when the effect is absent, partial, late or on
the wrong target (task brief; protocol 2.4). Each case runs the full state machine on one cell of the pilot plan."""
import pytest

from conftest import attempts, verification

FAULTS = ["F1", "F2", "F4", "F5", "F6", "F7", "F8"]

# Knobs per scenario. A failing scenario makes every attempt of the fault fail, so the campaign also halts after
# two consecutive failed attempts (2.6.5).
SCENARIOS = {
    "absent": {
        "F1": ({"F1": {"effect": "none"}}, {}),
        "F2": ({"F2": {"effect": "none"}}, {}),
        "F4": ({"F4": {"effect": "none"}}, {}),
        "F5": ({"F5": {"effect": "none"}}, {}),
        "F6": ({}, {"f6_ready": False}),
        "F7": ({"F7": {"effect": "none"}}, {}),
        "F8": ({"F8": {"effect": "none"}}, {}),
    },
    "partial": {
        "F1": ({}, {"broker_ready_delay_s": 0.5}),     # broker back before any scrape or probe sees it gone
        "F2": ({"F2": {"effect": "partial"}}, {}),     # cut off from one of the two other brokers only
        "F4": ({"F4": {"effect": "partial"}}, {}),     # stress lands on one TaskManager of three
        "F5": ({"F5": {"effect": "partial"}}, {}),     # every second kill kills nothing
        "F6": ({}, {"f6_send_frac": 0.5}),             # half the planned records
        "F7": ({"F7": {"effect": "partial"}}, {}),
        "F8": ({"F8": {"effect": "weak"}}, {}),        # working set rises by under half the stressor
    },
    "late": {
        "F1": ({"F1": {"apply_delay_s": 45}}, {}),
        "F2": ({"F2": {"apply_delay_s": 45}}, {}),
        "F4": ({"F4": {"apply_delay_s": 45}}, {}),
        "F5": ({"F5": {"apply_delay_s": 45}}, {}),
        "F6": ({}, {"f6_ack_delay_s": 8.0}),           # first produce ack more than 5 s after the planned onset
        "F7": ({"F7": {"apply_delay_s": 45}}, {}),
        "F8": ({"F8": {"apply_delay_s": 45}}, {}),
    },
    "wrong_target": {
        "F1": ({"F1": {"wrong_target": True}}, {}),    # kills the active controller instead of the follower
        "F2": ({"F2": {"wrong_target": True}}, {}),
        "F4": ({"F4": {"wrong_target": True}}, {}),    # one record names the JobManager
        "F5": ({"F5": {"wrong_target": True}}, {}),
        "F6": ({"F6": {"wrong_target": True}}, {}),    # records carry another attempt's header
        "F7": ({"F7": {"wrong_target": True}}, {}),
        "F8": ({"F8": {"wrong_target": True}}, {}),
    },
}


@pytest.mark.parametrize("fault", FAULTS)
def test_realistic_effect_passes(make, fault):
    h = make()
    assert h.run(only_fault=fault) == 0
    (a,) = attempts(h, fault)
    assert a["outcome"] == "VALID", a["failed_checks"]
    v = verification(h, a)
    assert v["result"] == {k: True for k in ("mechanism", "signature", "envelope", "timing", "telemetry", "gate")}
    own = {s["run_type"]: s["pass"] for s in v["signatures_all_faults"]}
    assert own[fault] is True
    assert not any(p for rt, p in own.items() if rt != fault), own  # other faults' signatures do not fire
    assert a["onset_verified_at"] is not None and a["labelled_end_at"] is not None


@pytest.mark.parametrize("scenario", list(SCENARIOS))
@pytest.mark.parametrize("fault", FAULTS)
def test_degraded_effect_fails(make, fault, scenario):
    knobs, world = SCENARIOS[scenario][fault]
    h = make(knobs=knobs, world=world)
    rc = h.run(only_fault=fault)
    mine = attempts(h, fault)
    assert mine, "no attempt ran"
    first = mine[0]
    assert first["outcome"] in ("GATE_FAILED", "TELEMETRY_FAILED"), (scenario, first["outcome"])
    assert first["failed_checks"], "a failure always names its checks"
    assert not any(a["outcome"] in ("VALID", "VALID_REMEDIATED") for a in mine)
    v = verification(h, first)
    assert v["result"]["gate"] is False
    # It is repeated in place with the next attempt number, then the campaign halts after two in a row.
    assert [a["attempt"] for a in mine] == list(range(1, len(mine) + 1))
    assert rc == 3
    m = h.manifest()
    assert m["halts"] and m["halts"][-1]["reason"].startswith("CONSECUTIVE_FAILED_ATTEMPTS")


def test_f1_never_targets_the_active_controller(make):
    for controller in (0, 1, 2):
        h = make(world={"controller": controller})
        assert h.run(only_fault="F1") == 0
        (a,) = attempts(h, "F1")
        v = verification(h, a)
        assert v["controller"]["node_id_before_apply"] == controller
        target = v["targets_expected"][0]["broker_id"]
        assert target != controller
        followers = sorted(i for i in range(3) if i != controller)
        cell = next(c for b in h.plan["blocks"] for c in b["cells"] if c["cell_key"] == a["cell_key"])
        assert target == followers[cell["follower_rank"]]
        assert a["controller_at_onset"] == {"node_id": controller, "pod": f"streaming-kafka-brokers-{controller}"}


def test_f2_fails_when_clients_lose_the_follower(make):
    h = make(world={"f2_breaks_clients": True})
    h.run(only_fault="F2")
    a = attempts(h, "F2")[0]
    assert a["outcome"] == "GATE_FAILED"
    assert any(n.startswith("env_clients_connected") for n in a["failed_checks"])


def test_f5_fails_when_jobs_cannot_restore_from_s3(make):
    """Review 1 A1: a stateful job that cannot restore after a TaskManager loss is an envelope failure."""
    h = make(world={"checkpoint_scheme": "file"})
    h.run(only_fault="F5")
    a = attempts(h, "F5")[0]
    assert "env_checkpoints_on_s3" in a["failed_checks"]


def test_f5_checkpoints_must_be_under_this_campaigns_scratch_prefix(make):
    """CONTRACT_CHANGES C13: s3://<bucket>/scratch/<campaign_id>/flink-checkpoints/ (11.7). A checkpoint path in another
    campaign's prefix fails the envelope, although it is on S3."""
    h = make(world={"checkpoint_campaign_id": "ec-a"})
    h.run(only_fault="F5")
    a = attempts(h, "F5")[0]
    assert "env_checkpoints_on_s3" in a["failed_checks"]
    ok = make()
    ok.run(only_fault="F5")
    assert attempts(ok, "F5")[0]["outcome"] == "VALID"


def test_f6_records_that_never_reach_raw_events_fail_the_signature(make):
    """CONTRACT_CHANGES C6: the injector reports its acknowledgements, but the verification consumer counts nothing under
    this attempt's rebuild-attempt header, so the signature fails and the attempt is repeated."""
    h = make(knobs={"F6": {"fail_first_n": 1}})
    assert h.run(only_fault="F6") == 0
    a1, a2 = attempts(h, "F6")[:2]
    assert a1["outcome"] == "GATE_FAILED" and a1["failed_checks"] == ["f6_consumer_malformed"]
    v1, v2 = verification(h, a1), verification(h, a2)
    assert v1["f6"]["acked_count"] == v1["f6"]["planned_count"] and v1["f6"]["consumer_count"] == 0
    assert a2["outcome"] == "VALID" and v2["f6"]["consumer_count"] >= 0.9 * v2["f6"]["planned_count"]
    # the second attempt's count is its own: the first attempt's records carry another key
    log = h.s3.get(f"attempts/{a2['attempt_key']}/injector.log").decode()
    assert f'"attempt_key": "{a2["attempt_key"]}"' in log and "INJECTED sent=" in log


def test_f8_sizing_below_256_mib_applies_nothing(make):
    h = make(world={"ws_base_bytes": 3.4 * 1024 ** 3})
    h.run(only_fault="F8")
    a = attempts(h, "F8")[0]
    assert a["outcome"] == "GATE_FAILED" and a["applied"] is False and a["resources"] == []
    assert [s["segment_type"] for s in a["segments"]] == ["SETTLE", "PRE_FAULT", "APPLY", "GATE"]
    assert any(n.startswith("f8_stressor_min") for n in a["failed_checks"])


def test_f8_per_pod_sizing_uses_each_pods_own_working_set(make):
    h = make()
    assert h.run(only_fault="F8") == 0
    v = verification(h, attempts(h, "F8")[0])
    per = v["f8"]["per_pod"]
    assert len(per) == 3
    mib = 1024 * 1024
    for p in per:
        assert p["stressor_bytes"] % mib == 0
        assert 0 <= (0.85 * p["limit_bytes"] - p["ws_pre_bytes"]) - p["stressor_bytes"] < mib + 1
        assert p["increase_bytes"] >= 0.5 * p["stressor_bytes"]
    assert len({p["pod"] for p in per}) == 3


def test_f7_taskmanager_loss_is_flagged_not_failed(make):
    h = make()
    h.world.knobs["F7"] = {}
    # replace a TaskManager while the loss is on: kill one TM 60 s into the F7 injection
    orig = h.world._apply

    def apply_and_kill(c, t):
        orig(c, t)
        if c.run_type == "F7":
            victim = c.targets["."][0]
            h.world.at(t + 60, lambda tt: h.world.kill_pod(victim, tt))
    h.world._apply = apply_and_kill
    h.run(only_fault="F7")
    a = attempts(h, "F7")[0]
    assert "F7_TM_LOSS" in a["flags"]
    assert not any(n.startswith("env_") for n in a["failed_checks"])
