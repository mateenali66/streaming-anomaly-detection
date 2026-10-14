"""Gate rules: shams fail every signature, missing values fail, every target is checked, feature values never
decide anything, and the offline gate reproduces the online result (2.1, 2.4, 2.5, 2.7.4)."""
import copy
import json
import random

import pytest

from conftest import attempts, checks_by_name, verification
from orchestrator import gate
from orchestrator.cli import cmd_gate_offline
from orchestrator.fakes.series import METRIC_TYPES
from orchestrator.gate import make_check


@pytest.mark.parametrize("workload", ["ecommerce", "iot"])
def test_full_pilot_shams_fail_every_signature_and_faults_only_their_own(make, workload):
    h = make(workload)
    assert h.run() == 0
    m = h.manifest()
    assert m["status"] == "COMPLETE"
    for a in m["attempts"]:
        v = verification(h, a)
        sig = {s["run_type"]: s["pass"] for s in v["signatures_all_faults"]}
        if a["run_type"] == "F0":
            assert not any(sig.values()), sig
            assert v["result"]["signature"] is True and "SIGNATURE_FALSE_PASS" not in a["flags"]
            assert v["pseudo_targets"]["f6_planned_count"] > 0
            assert len(v["pseudo_targets"]["tm_pods"]) == 3
        else:
            assert sig[a["run_type"]] and sum(sig.values()) == 1, (a["run_type"], sig)


def test_sham_f6_signature_uses_the_planned_count_not_the_sent_count(make):
    """Review 1 A6: "consumer counts at least 90% of sent" passes when sent is 0. The planned count fails."""
    h = make()
    assert h.run(only_fault="F0") == 0
    v = verification(h, attempts(h, "F0")[0])
    gi = gate.load_inputs(h.s3, h.manifest(), h.plan, v["attempt_key"], h.th, facts=v)
    sig = gate.sig_f6(gate.Ctx(gi), gi.facts["pseudo_targets"]["f6_planned_count"])
    assert sig[0]["pass"] is False and sig[0]["during_value"] == 0 and sig[0]["threshold"] > 0


def test_make_check_missing_values_fail():
    for pre, dur in ((None, 5.0), (5.0, None), (None, None), (float("nan"), 1.0)):
        c = make_check("x", "SIGNATURE", "all", "s", pre, dur, ">=", 0, "u")
        assert c["pass"] is False and c["reason"] == "MISSING"
    c = make_check("y_pre", "SIGNATURE", "all", "s", None, 1.0, ">=", 0, "u")
    assert c["pass"] is False and c["reason"] == "MISSING"
    assert make_check("x", "SIGNATURE", "all", "s", 0.0, 2.0, ">=", 1, "u")["pass"] is True
    assert make_check("x_pre", "SIGNATURE", "all", "s", 0.0, 2.0, ">=", 1, "u")["pass"] is False  # compares pre


def test_missing_pre_fault_probes_fail_the_attempt(make):
    """Review 1 A4: R1 replaced a missing pre value with 1e-9, so every ratio rule passed."""
    h = make()
    plan_f2 = next(c for b in h.plan["blocks"] for c in b["cells"] if c["run_type"] == "F2")
    # Probe service down from the start of the F2 attempt's pre-fault until after the onset.
    h.run(only_fault="F0")  # warm-up, baseline and one sham so the clock is past the baseline
    start = h.clock.t
    h.world.global_knobs["probes_down"] = [(start, start + plan_f2["settle_gap_s"] + 700)]
    h.run(only_fault="F2")
    a = attempts(h, "F2")[0]
    v = verification(h, a)
    pre_checks = [c for c in v["checks"] if c["name"] == "f2_tcp_ok_pre"]
    assert pre_checks and all(c["reason"] == "MISSING" and not c["pass"] for c in pre_checks)
    assert a["outcome"] == "GATE_FAILED"


def test_one_target_never_stands_in_for_another(make):
    h = make(world={"drop_series": [{"metric": "container_cpu_usage_seconds_total", "pod": "streaming-flink-taskmanager-1-2"}]})
    h.run(only_fault="F4")
    a = attempts(h, "F4")[0]
    v = verification(h, a)
    by = checks_by_name(v)
    cpu = {c["target"]: c for c in by["f4_cpu_cores"]}
    assert cpu["streaming-flink-taskmanager-1-2"]["reason"] == "MISSING"
    assert cpu["streaming-flink-taskmanager-1-1"]["pass"] and cpu["streaming-flink-taskmanager-1-3"]["pass"]
    assert a["outcome"] == "GATE_FAILED"


def test_untargeted_feature_source_gap_is_a_series_gap_flag(make):
    # Every consumer-source series of one TaskManager (the Flink Kafka source reader metrics) stops: an untargeted source.
    # Decision 21 (C22): series presence is a recorded value and the SERIES_GAP flag, never a gate. The TaskManager was
    # still scraped, so scrape health passes and the attempt is VALID.
    consumer = [m for m in METRIC_TYPES if m.startswith("flink_taskmanager_job_task_operator_KafkaSourceReader_")]
    h = make()
    h.run(only_fault="F0")
    t = h.clock.t
    h.world.global_knobs["drop_series"] = [{"metric": m, "pod": "streaming-flink-taskmanager-1-1", "start": t, "end": t + 1e9}
                                           for m in consumer]
    h.run(only_fault="F2")
    a = attempts(h, "F2")[0]
    assert a["outcome"] == "VALID", a["failed_checks"]
    assert "SERIES_GAP" in a["flags"]
    v = verification(h, a)
    gaps = [r for r in v["telemetry"] if r["coverage_frac"] < 0.95]
    assert gaps and {r["source"] for r in gaps} == {"consumer:taskmanager"}
    assert v["result"]["telemetry"] is True and all(c["pass"] for c in v["checks"] if c["layer"] == "TELEMETRY")


def test_untargeted_scrape_target_down_is_telemetry_failed(make):
    """Decision 21: the TaskManager's scrape fails (`up` 0) for the rest of the campaign, an untargeted target for F2."""
    h = make()
    h.run(only_fault="F0")
    t = h.clock.t
    h.world.global_knobs["drop_series"] = [{"pod": "streaming-flink-taskmanager-1-1", "job": "flink-metrics", "start": t,
                                            "end": t + 1e9}]
    h.run(only_fault="F2")
    a = attempts(h, "F2")[0]
    assert a["outcome"] == "TELEMETRY_FAILED", a["failed_checks"]
    assert a["failed_checks"] == ["telemetry_scrape_misses"]
    v = verification(h, a)
    bad = [c for c in v["checks"] if not c["pass"]]
    assert bad and all(c["layer"] == "TELEMETRY" and c["target"] == "taskmanager:streaming-flink-taskmanager-1-1" for c in bad)
    assert v["result"]["telemetry"] is False and v["result"]["mechanism"] and v["result"]["signature"]


def test_targeted_source_gap_is_allowed_only_where_3_4_3_says(make):
    """F1: the target broker's JMX may have gaps during INJECTION, and only there."""
    h = make()
    assert h.run(only_fault="F1") == 0
    v = verification(h, attempts(h, "F1")[0])
    tgt = v["targets_expected"][0]["broker_id"]
    gaps = [t for t in v["telemetry"] if t["coverage_frac"] < 0.95]
    assert gaps and all(t["targeted"] and t["segment"] == "INJECTION" and t["source"].endswith(f"broker-{tgt}") for t in gaps)


def test_empty_query_result_is_never_zero(make):
    """Review 2 item 9: failed queries returned [], read as zero. Controller metrics missing must block the start."""
    h = make(world={"drop_series": [{"metric": m} for m in ("kafka_controller_kafkacontroller_fencedbrokercount",
                                                              "kafka_controller_kafkacontroller_activecontrollercount",
                                                              "kafka_controller_kafkacontroller_offlinepartitionscount",
                                                              "kafka_controller_kafkacontroller_preferredreplicaimbalancecount")]})
    assert h.run() == 3
    m_exists = (h.volume / "manifest.json").exists()
    assert not m_exists  # the startup health gate never passed, so no warm-up started


def test_gate_never_reads_feature_values(make):
    """2.1: no symptom feature decides anything. Scrambling every feature value leaves every check unchanged."""
    h = make()
    assert h.run() == 0
    m = h.manifest()
    for a in m["attempts"]:
        v = verification(h, a)
        gi = gate.load_inputs(h.s3, m, h.plan, a["attempt_key"], h.th, facts=v)
        base = gate.evaluate(gi)
        rng = random.Random(7)
        gi2 = copy.copy(gi)
        gi2.feature_rows = [dict(r, value=rng.uniform(-1e9, 1e9)) for r in gi.feature_rows]
        other = gate.evaluate(gi2)
        assert [c["pass"] for c in base.checks] == [c["pass"] for c in other.checks]
        assert base.result == other.result


def test_offline_gate_matches_online(make, tmp_path, capsys):
    h = make()
    assert h.run() == 0
    root = tmp_path / "prefix"
    for key, (data, _) in h.s3.objects.items():
        p = root / key
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    class A:
        store = str(root)
        attempt = None
        out = str(tmp_path / "offline")
    assert cmd_gate_offline(A) == 0
    out = capsys.readouterr().out
    assert out.count("MATCH") == 8 and "DIFFER" not in out
    off = json.loads((tmp_path / "offline" / f"{h.manifest()['attempts'][0]['attempt_key']}.offline.json").read_text())
    assert off["mode"] == "OFFLINE"


def test_offline_gate_detects_a_changed_threshold(make, tmp_path, capsys):
    h = make()
    assert h.run(only_fault="F4") == 0
    root = tmp_path / "prefix"
    for key, (data, _) in h.s3.objects.items():
        p = root / key
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
    th = json.loads((root / "run/thresholds.json").read_text())
    th["gate"]["F4"]["cpu_cores_min"] = 5.0
    (root / "run/thresholds.json").write_text(json.dumps(th))

    class A:
        store = str(root)
        attempt = None
        out = None
    assert cmd_gate_offline(A) == 1
    assert "DIFFER" in capsys.readouterr().out


def _cpu_ctx(rows):
    ctx = object.__new__(gate.Ctx)
    ctx.th = {"F4": {"container": "flink-main-container"}}
    ctx.metric = {(gate.CPU_METRIC, "tm"): rows}
    return ctx


def _cpu_row(ts, value, series_id="s1"):
    return {"ts": ts, "value": value, "series_id": series_id, "container": "flink-main-container"}


def test_cpu_rate_is_not_biased_by_a_counter_that_refreshes_every_15s():
    # C41: a container using exactly 2.0 cores, its counter refreshed every 15 s and read on the 10 s grid (the pilot's
    # cAdvisor pattern). Steps read 0 or a double step. The 60 s samples must give 2.0 within the 15 s quantum.
    rows, t0 = [], 1_000_000
    for t in range(t0, t0 + 600, 10):
        refreshed = t - (t - t0) % 15
        rows.append(_cpu_row(t, 2.0 * (refreshed - t0)))
    rates = _cpu_ctx(rows).cpu_rates("tm", (t0, t0 + 600))
    assert len(rates) == 54
    assert all(1.5 <= r <= 2.5 for r in rates)
    assert abs(gate.median(rates) - 2.0) <= 0.01


def test_cpu_rate_samples_stay_inside_the_window_and_drop_a_new_container():
    t0 = 2_000_000
    rows = [_cpu_row(t, 1.0 * (t - t0)) for t in range(t0 - 100, t0 + 200, 10)]
    rows += [_cpu_row(t, 0.5 * (t - t0 - 150), "s2") for t in range(t0 + 150, t0 + 300, 10)]
    rates = _cpu_ctx(rows).cpu_rates("tm", (t0, t0 + 300))
    # s1 gives t0+60 .. t0+190 at 1.0, s2 gives t0+210 .. t0+290 at 0.5. Nothing reaches back before t0.
    assert sorted(rates) == [0.5] * 9 + [1.0] * 14
    assert _cpu_ctx(rows).cpu_rates("tm", (t0, t0 + 50)) == []
