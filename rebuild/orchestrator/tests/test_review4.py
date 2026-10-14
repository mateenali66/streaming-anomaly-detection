"""Review 4 findings (diagnostics/REVIEW4_*.md), one or more tests each. Each test drives the simulated cluster where it
now behaves like the real system (orchestrator/fakes) and checks the fix. rebuild/review4_fixes/A_orchestrator.md lists
which of these failed on the code before the fixes."""
import copy
import json
import sys
from pathlib import Path

import pytest

from conftest import attempts, checks_by_name, verification
from orchestrator import gate
from orchestrator.cli import cmd_gate_offline, redact_log
from orchestrator.fakes.series import METRIC_TYPES

PIPELINE = Path(__file__).resolve().parents[2] / "pipeline"
TM1 = "streaming-flink-taskmanager-1-1"
CONSUMER = [m for m in METRIC_TYPES if m.startswith("flink_taskmanager_job_task_operator_KafkaSourceReader_")]


def when_attempt(h, run_type, fn, nth=1):
    """Call fn(attempt record) when the nth attempt of run_type first appears in the manifest (its times are known)."""
    seen = {"n": 0}
    orig = h.world.on_manifest

    def hook(m):
        for a in m.get("attempts") or []:
            if a["run_type"] == run_type and a["status"] == "RUNNING" and a["attempt_key"] not in seen:
                seen[a["attempt_key"]] = True
                seen["n"] += 1
                if seen["n"] == nth:
                    fn(a)
        orig(m)
    h.world.on_manifest = hook


def seg(a, t):
    return next(s for s in a["segments"] if s["segment_type"] == t)


def jm_pod(h):
    return next(p.name for p in h.world.pods if p.role == "jobmanager" and p.deleted_at is None)


def dump(h, root):
    for key, (data, _) in h.s3.objects.items():
        p = root / key
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
    return root


# ============================================================================================ PIL-1 (decision 21)
def test_untargeted_series_gap_is_a_flag_not_a_failure(make):
    """A 20 s gap in the consumer series of every TaskManager (a job restart) on an untargeted source used to make F2
    TELEMETRY_FAILED. Series presence is now a value and the SERIES_GAP flag; scrapes were fine, so the attempt is VALID."""
    h = make()

    def gap(a):
        t = a["planned_onset_at"] + 60
        h.world.global_knobs["drop_series"] = [{"metric": m, "start": t, "end": t + 20} for m in CONSUMER]
        h.world.cache.clear()
    when_attempt(h, "F2", gap)
    assert h.run(only_fault="F2") == 0
    (a,) = attempts(h, "F2")
    assert a["outcome"] == "VALID", a["failed_checks"]
    assert "SERIES_GAP" in a["flags"]
    v = verification(h, a)
    low = [t for t in v["telemetry"] if t["source"] == "consumer:taskmanager" and t["segment"] == "INJECTION"]
    assert low and low[0]["coverage_frac"] < 0.95 and not low[0]["targeted"]
    assert not any(c["layer"] == "TELEMETRY" and not c["pass"] for c in v["checks"])


@pytest.mark.parametrize("misses,outcome", [(3, "VALID"), (5, "TELEMETRY_FAILED")])
def test_untargeted_scrape_misses_decide_telemetry(make, misses, outcome):
    """Decision 21: more than 3 missed scrapes of an untargeted target in one segment fail the attempt."""
    h = make()

    def gap(a):
        t0 = int(a["planned_onset_at"] - 300) // 10 * 10 + 1  # inside PRE_FAULT, covering `misses` 10 s steps
        h.world.global_knobs["drop_series"] = [{"pod": jm_pod(h), "job": "flink-metrics", "start": t0,
                                                "end": t0 + 10 * misses}]
        h.world.cache.clear()
    when_attempt(h, "F2", gap)
    h.run(only_fault="F2")
    a = attempts(h, "F2")[0]
    assert a["outcome"] == outcome, a["failed_checks"]
    v = verification(h, a)
    jm_rows = [r for r in v["scrapes"] if r["target"].startswith("jobmanager:") and r["segment"] == "PRE_FAULT"]
    assert jm_rows and jm_rows[0]["missed_count"] == misses and not jm_rows[0]["targeted"]
    if outcome == "TELEMETRY_FAILED":
        assert a["failed_checks"] == ["telemetry_scrape_misses"]


def test_targeted_scrape_misses_are_exempt_in_injection_and_recovery(make):
    """F7: one TaskManager scrape in three fails under packet loss. The fault's own targets are exempt in INJECTION and
    RECOVERY (decision 21), so the attempt stays VALID although the TaskManagers miss far more than 3 scrapes."""
    h = make()
    assert h.run(only_fault="F7") == 0
    (a,) = attempts(h, "F7")
    assert a["outcome"] == "VALID", a["failed_checks"]
    v = verification(h, a)
    inj = [r for r in v["scrapes"] if r["target"].startswith("taskmanager:") and r["segment"] == "INJECTION"]
    assert inj and all(r["targeted"] for r in inj) and sum(r["missed_count"] for r in inj) > 3
    pre = [r for r in v["scrapes"] if r["segment"] == "PRE_FAULT"]
    assert pre and not any(r["targeted"] for r in pre)


def test_f5_job_restore_running_into_recovery_is_not_a_telemetry_failure(make):
    """Flink removes task metrics while a job restarts. With a 100 s restart after the last F5 kill at onset + 240 s, the
    consumer series are gone into RECOVERY. That used to fail telemetry (RECOVERY had no exemption)."""
    h = make(world={"job_restart_s": 100.0})
    assert h.run(only_fault="F5") == 0
    (a,) = attempts(h, "F5")
    assert a["outcome"] == "VALID", a["failed_checks"]
    v = verification(h, a)
    rec = [t for t in v["telemetry"] if t["source"] == "consumer:taskmanager" and t["segment"] == "RECOVERY"]
    assert rec and rec[0]["coverage_frac"] < 0.95 and rec[0]["targeted"]


# ============================================================================================ CAM-1 (decision 22)
def test_sham_false_pass_is_gate_failed_and_repeated(make):
    """2.4: every fault's signature must fail on a sham. Packet loss on the TaskManagers from no campaign fault, during the
    first sham, makes its F7 signature pass. That sham is GATE_FAILED and repeated, never labelled."""
    h = make(world={"sham_disturbance": [1]})
    assert h.run(only_fault="F0") == 0
    s1, s2 = attempts(h, "F0")
    assert s1["outcome"] == "GATE_FAILED" and s1["failed_checks"] == ["f0_signature_false_pass"]
    v1 = verification(h, s1)
    assert {s["run_type"]: s["pass"] for s in v1["signatures_all_faults"]}["F7"] is True
    assert v1["result"]["signature"] is False and v1["result"]["gate"] is False
    assert s2["outcome"] == "VALID" and "SIGNATURE_FALSE_PASS" not in s2["flags"] + s1["flags"]


# ============================================================================================ CAM-2 (decision 23)
def test_failed_controller_read_after_injection_is_a_repeat(make):
    """kafka-metadata-quorum.sh times out in every broker at the end of the first F1. The null node_id_after_injection
    used to pass the gate and stop the build afterwards; now it is a gate check."""
    h = make()

    def fail(a):
        end = a["planned_onset_at"] + 300
        h.world.global_knobs["quorum_fail"] = [(end - 5, end + 120)]
    when_attempt(h, "F1", fail)
    assert h.run(only_fault="F1") == 0
    a1, a2 = attempts(h, "F1")[:2]
    assert a1["outcome"] == "GATE_FAILED" and a1["failed_checks"] == ["controller_read_after_injection"]
    assert verification(h, a1)["controller"]["node_id_after_injection"] is None
    assert a2["outcome"] == "VALID" and verification(h, a2)["controller"]["node_id_after_injection"] is not None


def test_flink_job_list_at_end_is_retried_until_it_answers(make, monkeypatch):
    """The Flink REST call right after the recovery gate fails twice. The list is retried, so the valid attempt never ends
    with an empty flink_jobs_at_end (which the preprocessing refuses)."""
    from orchestrator import recovery
    h = make()
    orig = recovery.RecoveryGate.run

    def run_then_fail(self, *a, **kw):
        out = orig(self, *a, **kw)
        if "on_remediation_start" in kw:  # the recovery gate of an attempt, not the startup or health gate
            h.world.global_knobs["flink_snapshot_fail_next"] = 2
        return out
    monkeypatch.setattr(recovery.RecoveryGate, "run", run_then_fail)
    assert h.run(only_fault="F4") == 0
    (a,) = attempts(h, "F4")
    assert a["outcome"] == "VALID"
    assert sorted(j["name"] for j in a["flink_jobs_at_end"]) == sorted(h.manifest()["flink_jobs_expected"])
    assert h.world.global_knobs["flink_snapshot_fail_next"] == 0


def test_end_more_than_60_s_early_fails_timing(make):
    """2.4: the labelled end must fall within 60 s of onset plus duration. An end 70 s early used to be VALID online and
    refused by the preprocessing."""
    h = make(knobs={"F4": {"recover_delay_s": -70}})
    h.run(only_fault="F4")
    a = attempts(h, "F4")[0]
    assert a["overrun_s"] < -60 and a["outcome"] == "GATE_FAILED" and "end_underrun" in a["failed_checks"]


def test_f6_attempt_stopped_mid_injection_keeps_its_injector_log(make):
    """SIGTERM during an F6 injection. The halt path deletes the injector Job and its pod log with it; the log is stored
    first, so the ABORTED F6 attempt has attempts/<key>/injector.log, which the preprocessing requires."""
    h = make()
    h.run(only_fault="F0")

    def term(a):
        h.clock.term_at = a["planned_onset_at"] + 60
    when_attempt(h, "F6", term)
    assert h.run(only_fault="F6") == 143
    h.clock.restart(30)
    assert h.run(resume=True, only_fault="F6") == 0
    f6 = attempts(h, "F6")
    assert f6[0]["outcome"] == "ABORTED" and f6[0]["applied"]
    for a in f6:
        log = h.s3.get(f"attempts/{a['attempt_key']}/injector.log").decode()
        assert '"event": "orchestrator_observed"' in log
    assert f6[-1]["outcome"] == "VALID"


def test_loader_accepts_every_campaign_the_gate_completes(make, tmp_path):
    """Decision 23: every rule the preprocessing applies to a valid attempt is also a gate check. A pilot with each CAM-2
    defect injected (controller read, job list, early end, F6 halt) completes, and the preprocessing's loader, which runs
    every valid-attempt rule, accepts it."""
    sys.path.insert(0, str(PIPELINE))
    from streamprep.config import load_config
    from streamprep.loader import load_campaign
    h = make()

    def early(a):
        h.world.knobs["F4"] = {"recover_delay_s": -70}  # the first F4 recovers 70 s early

    def on_time(a):
        h.world.knobs["F4"] = {}
    when_attempt(h, "F4", early)
    when_attempt(h, "F4", on_time, nth=2)

    def ctrl(a):
        h.world.global_knobs["quorum_fail"] = [(a["planned_onset_at"] + 295, a["planned_onset_at"] + 420)]
    when_attempt(h, "F1", ctrl)

    def term(a):
        h.clock.term_at = a["planned_onset_at"] + 60
    when_attempt(h, "F6", term)
    rc = h.run()
    for _ in range(3):
        if rc == 0:
            break
        h.clock.restart(30)  # the operator confirms the stop was transient and resumes (2.6.6)
        rc = h.run(resume=True)
    assert rc == 0, h.manifest()["halts"]
    m = h.manifest()
    assert m["status"] == "COMPLETE"
    got = {a["outcome"] for a in m["attempts"]}
    assert "GATE_FAILED" in got and "ABORTED" in got
    c = load_campaign(dump(h, tmp_path / "prefix"), load_config())
    assert len(c.attempts) == len(m["attempts"])


def test_loader_accepts_a_campaign_whose_clock_reads_carry_microseconds(make, tmp_path, monkeypatch):
    """Pilot, 14 October 2026: time.time() carries microseconds, and two verification instants (recovery gate condition
    `at`, F5 `all_jobs_running_at`) were written unrounded, so the preprocessing refused both pilot campaigns (11.1.1). The
    fake clock reads whole simulated seconds and hid it. A fixed offset is not enough, because the orchestrator sleeps
    until planned boundaries. Here every read takes 7.3 microseconds, as a real read does, and every probe sample is
    stamped 0.36 ms into its step, as the real probes stamp time.time()."""
    sys.path.insert(0, str(PIPELINE))
    from streamprep.config import load_config
    from streamprep.loader import load_campaign
    from orchestrator.fakes.clock import FakeClock
    h = make()

    def read():
        h.clock.t += 0.0000073
        return FakeClock.now(h.clock)
    monkeypatch.setattr(h.clock, "now", read)
    rows = h.world.probe_rows
    monkeypatch.setattr(h.world, "probe_rows", lambda t: [dict(r, ts=r["ts"] + 0.0003565) for r in rows(t)])
    assert h.run() == 0, h.manifest()["halts"]
    m = h.manifest()
    assert m["status"] == "COMPLETE"
    c = load_campaign(dump(h, tmp_path / "prefix"), load_config())
    assert len(c.attempts) == len(m["attempts"])


# ============================================================================================ CAM-3 (decision 24)
def test_job_restart_inside_pre_fault_fails_the_envelope(make):
    """A Flink job restart 300 s before the planned onset, inside PRE_FAULT. Those windows would have been NORMAL. The
    PRE_FAULT envelope now fails the attempt, which is repeated."""
    h = make(world={"prefault_job_restart_once": "F4"})
    assert h.run(only_fault="F4") == 0
    a1, a2 = attempts(h, "F4")[:2]
    assert a1["outcome"] == "GATE_FAILED" and a1["failed_checks"] == ["pre_fault_jobs_running_same_id"]
    assert a2["outcome"] == "VALID"


def test_taskmanager_replaced_inside_pre_fault_fails_the_envelope(make):
    h = make()

    def kill(a):
        at = a["planned_onset_at"] - 400
        h.world.at(at, lambda tt: h.world.kill_pod(TM1, tt))
    when_attempt(h, "F2", kill)
    h.run(only_fault="F2")
    a1 = attempts(h, "F2")[0]
    assert a1["outcome"] == "GATE_FAILED"
    assert "pre_fault_no_pod_change" in a1["failed_checks"]
    by = checks_by_name(verification(h, a1))
    assert by["pre_fault_no_pod_change"][0]["during_value"] >= 1


# ============================================================================================ MIN-1
def test_offline_gate_detects_a_moved_labelled_end(make, tmp_path, capsys):
    """Probe 9: the F4 labelled end moved 50 s early, later segments shifted to match. The offline gate recomputes onset and
    end from the stored chaos records and finds the difference."""
    h = make()
    assert h.run(only_fault="F4") == 0
    root = dump(h, tmp_path / "prefix")
    mp = root / "manifest/manifest.json"
    m = json.loads(mp.read_text())
    a = next(x for x in m["attempts"] if x["run_type"] == "F4")
    a["labelled_end_at"] = round(a["labelled_end_at"] - 50, 3)
    a["overrun_s"] = round(a["overrun_s"] - 50, 3)
    for s in a["segments"]:
        if s["segment_type"] == "INJECTION":
            s["end_at"] = a["labelled_end_at"]
        elif s["segment_type"] in ("RECOVERY", "COOLDOWN"):
            s["start_at"] = round(s["start_at"] - 50, 3)
            s["end_at"] = round(s["end_at"] - 50, 3)
        elif s["segment_type"] == "GATE":
            s["start_at"] = round(s["start_at"] - 50, 3)
    mp.write_text(json.dumps(m))

    class A:
        store = str(root)
        attempt = None
        out = None
    assert cmd_gate_offline(A) == 1
    out = capsys.readouterr().out
    assert "end_from_records" in out and "DIFFER" in out


# ============================================================================================ MIN-2
def test_resubmission_flags_and_delays_the_next_attempt(make):
    """A remediation that resubmitted jobs from their checkpoints, without a redeploy, flags the next attempt
    AFTER_RESUBMIT and gives it the 600 s extra settle."""
    h = make(world={"f5_restore_fails": True})
    h.run(only_fault="F5")
    a1, a2 = attempts(h, "F5")[:2]
    acts = a1["recovery_gate"]["remediation"]["actions"]
    assert any(x.startswith("RESUBMIT ") for x in acts) and not any(x.startswith("FLINK_REDEPLOY") for x in acts)
    assert all("from=flink-checkpoints/" in x for x in acts if x.startswith("RESUBMIT "))
    assert "AFTER_RESUBMIT" in a2["flags"]
    assert seg(a2, "SETTLE")["end_at"] - a2["started_at"] == h.plan["reserve_settle_gap_s"][0] + 600


# ============================================================================================ MIN-3
def test_trailing_starts_after_the_health_gate_and_a_fixed_settle(make):
    h = make()
    assert h.run() == 0
    m = h.manifest()
    last_end = max(a["ended_at"] for a in m["attempts"])
    trailing = min(s["start_at"] for s in m["campaign_segments"] if s["segment_type"] == "TRAILING")
    assert trailing - last_end >= h.th["campaign_rules"]["trailing_settle_s"]
    assert all(s["segment_type"] == "IDLE" for s in m["campaign_segments"] if last_end <= s["start_at"] < trailing)


# ============================================================================================ MIN-4
def test_health_events_are_recorded_and_flag_overlapping_attempts(make):
    """An EC2 operational issue opens during the pilot and closes 15 minutes later. It is read from the CloudWatch Logs
    group, recorded once, and every attempt it overlaps is flagged HEALTH_EVENT, also one that finished before the poll.
    The first polls fail; that is logged and retried, never fatal."""
    h = make()
    t0 = h.world.t0
    h.world.global_knobs["health_events"] = [{"offset_s": 4600, "duration_s": 900}]
    h.world.global_knobs["health_poll_fail_calls"] = {2, 3, 4}  # call 1 is the check at the start
    assert h.run() == 0
    m = h.manifest()
    (ev,) = m["health_events"]
    assert ev["status"] == "closed" and ev["end_at"] - ev["start_at"] == 900 and len(ev["event_key"]) == 16
    hit = [a for a in m["attempts"] if a["started_at"] < ev["end_at"] and a["ended_at"] > ev["start_at"]]
    assert hit and all("HEALTH_EVENT" in a["flags"] for a in hit)
    assert not any("HEALTH_EVENT" in a["flags"] for a in m["attempts"] if a not in hit)
    assert m["health_events_polled_until"] is not None and m["health_events_polled_until"] >= m["attempts"][-1]["ended_at"]
    assert h.health.calls >= 5
    import re
    assert not re.search(r"(?<![0-9a-f])000000000000(?![0-9a-f])", json.dumps(m))  # no affectedAccount in the record


# ============================================================================================ MIN-5 (decision 28)
def test_f6_injector_opens_no_connection_before_its_start_time(make):
    h = make()
    assert h.run(only_fault="F6") == 0
    (a,) = attempts(h, "F6")
    (key, at), = h.world.f6_connects
    assert key == a["attempt_key"] and a["planned_onset_at"] <= at <= a["onset_verified_at"]


# ============================================================================================ MIN-10
def test_recovery_gate_checks_every_probe_pair(make):
    """One broker pair at 50% connect success inside 9 healthy pairs pooled to 0.95 and passed. Per pair it fails."""
    from orchestrator.recovery import RecoveryGate
    h = make()
    rt = h.runtime()
    now = h.clock.t
    rows = []
    for i in range(12):
        for p in range(10):
            ok = 0 if (p == 0 and i % 2) else 1
            rows.append({"ts": now - 115 + 10 * i, "probe": "tcp", "src_pod": f"b{p}", "dst": "x:9091", "tried_count": 1,
                         "ok_count": ok, "timeout_s": 0.5, "latency_s": None, "value": None, "detail": None, "error": None})
    rt.probes.samples = lambda start, end: (0.0, rows)
    conds = {c["name"]: c for c in RecoveryGate(rt).probe_conditions(now)}
    assert conds["probe_tcp_ok_frac"]["value"] == 0.5 and not conds["probe_tcp_ok_frac"]["pass"]


# ============================================================================================ L2, L3, L4
def test_pod_kill_records_stay_injected_and_notfound_is_the_recovery_proof(make):
    h = make()
    assert h.run(only_fault="F1") == 0
    (a,) = attempts(h, "F1")
    assert a["outcome"] == "VALID"
    v = verification(h, a)
    snaps = [s for s in v["checks"] if s["name"] == "all_recovered_observed"]
    assert snaps and all(c["pass"] and "NotFound" in c["series"] for c in snaps)
    assert all(r["phase"] == "Injected" for s in v["chaos_snapshots"] for r in s["records"])


def test_dead_letter_rate_needs_no_broker_meter(make):
    """No broker has a dead-letter messages-in series before the first F6 (Kafka creates topic meters lazily). The startup
    gate and the recovery gate read the verification consumer instead, so an F1 on the broker that took the sentinel
    still recovers."""
    h = make(world={"controller": 2})
    assert h.run(only_fault="F1") == 0
    names = {lab.get("pod") for lab, _ in h.world.series_at(int(h.clock.t) // 10 * 10).values_of(
        "kafka_server_brokertopicmetrics_messagesinpersec_total") if lab.get("topic") == "dead-letter"}
    assert "streaming-kafka-brokers-1" not in names and "streaming-kafka-brokers-2" not in names
    (a,) = attempts(h, "F1")
    assert a["outcome"] == "VALID"
    conds = {c["name"]: c for c in verification(h, a)["recovery_gate_conditions"]}
    assert conds["dead_letter_rate_zero"]["pass"] and conds["dead_letter_rate_zero"]["value"] == 0


def test_redeploy_resubmits_before_waiting_for_taskmanagers(make):
    """A native session cluster with no jobs starts no TaskManagers. The redeploy resubmits first and then waits."""
    h = make(world={"f5_restore_fails": True, "submit_fails_until_redeploy": True})
    h.run(only_fault="F5")
    a1, a2 = attempts(h, "F5")[:2]
    acts = a1["recovery_gate"]["remediation"]["actions"]
    i = next(i for i, x in enumerate(acts) if x.startswith("FLINK_REDEPLOY"))
    assert any(x.startswith("RESUBMIT ") for x in acts[i + 1:])
    assert a1["recovery_gate"]["passed_at"] is not None and "AFTER_REDEPLOY" in a2["flags"]


# ============================================================================================ L5
def test_f8_size_comes_from_the_recorded_samples_when_cadvisor_arrives_late(make):
    """Every cAdvisor sample reaches Prometheus 9.9 s after its timestamp, so the online sizing query sees the last step's
    previous value and the export later sees the new one. The gate recomputes the size from the samples the injector
    recorded; the export's size is recorded only."""
    h = make(world={"cadvisor_arrival_delay_s": 9.9})
    assert h.run(only_fault="F8") == 0
    (a,) = attempts(h, "F8")
    assert a["outcome"] == "VALID", a["failed_checks"]
    per = verification(h, a)["f8"]["per_pod"]
    assert all(p["ws_samples"] and p["limit_samples"] and p["export_stressor_bytes"] is not None for p in per)
    assert any(p["export_stressor_bytes"] != p["stressor_bytes"] for p in per)


# ============================================================================================ L6
@pytest.mark.parametrize("fault", ["F4", "F5", "F6", "F7", "F8"])
def test_controller_is_read_before_and_after_every_run_type(make, fault):
    h = make()
    assert h.run(only_fault=fault) == 0
    (a,) = attempts(h, fault)
    assert a["controller_at_onset"] is not None
    c = verification(h, a)["controller"]
    assert c["node_id_before_apply"] is not None and c["node_id_after_injection"] is not None


# ============================================================================================ L7
def test_failed_exec_is_a_missing_sample_not_a_failed_connect(make):
    """The probe service's kubectl exec fails for 60 s inside the F2 PRE_FAULT. Counted as failed connects, the probes
    fell to 90% of pre-fault samples and F2 failed f2_tcp_ok_pre."""
    h = make()

    def down(a):
        t = a["planned_onset_at"] - 200
        h.world.global_knobs["exec_fail"] = [(t, t + 60)]
    when_attempt(h, "F2", down)
    assert h.run(only_fault="F2") == 0
    (a,) = attempts(h, "F2")
    assert a["outcome"] == "VALID", a["failed_checks"]


# ============================================================================================ L8
def test_stressor_killed_by_the_kernel_fails_the_f8_envelope(make):
    h = make(knobs={"F8": {"oom_at_frac": 0.8}})
    h.run(only_fault="F8")
    a = attempts(h, "F8")[0]
    assert a["outcome"] == "GATE_FAILED" and a["failed_checks"] == ["env_no_oom_event"]


# ============================================================================================ C1 (decision 29)
def test_no_bucket_name_or_account_id_in_the_stored_record(make):
    import pyarrow.parquet as pq
    import io
    h = make(world={"f5_restore_fails": True})  # remediation actions with checkpoint paths enter the manifest
    h.run()
    texts = []
    for key, (data, _) in h.s3.objects.items():
        if key.endswith(".parquet"):
            t = pq.read_table(io.BytesIO(data)).to_pylist()
            texts.append(json.dumps(t, default=str))
        elif not key.endswith(".gz"):
            texts.append(data.decode("utf-8", "replace"))
    blob = "\n".join(texts)
    import re
    assert "dry-run-results" not in blob
    assert "amazonaws.com" not in blob
    assert not re.search(r"(?<![0-9a-f])000000000000(?![0-9a-f])", blob)  # the fake's account ID, not inside a hex job ID
    assert "flink-checkpoints/" in blob  # the paths are there, relative to the campaign's scratch prefix


def test_registry_host_stripped_before_hashing_and_collisions_refused():
    from orchestrator.errors import OrchestratorError
    from orchestrator.exporter import finalize, strip_registry_host
    assert strip_registry_host("000000000000.dkr.ecr.us-east-2.amazonaws.com/streaming/flink@sha256:ab") == \
        "streaming/flink@sha256:ab"
    rows = [{"ts": 10, "series_id": "s", "metric": "m", "value": 1.0}, {"ts": 10, "series_id": "s", "metric": "m", "value": 2.0}]
    with pytest.raises(OrchestratorError):
        finalize(rows)


def test_uploaded_log_is_redacted():
    raw = (b"AccessDenied arn:aws:sts::123456789012:assumed-role/x for s3://dry-run-results/campaign/pilot-ec/x\n"
           b"pull 123456789012.dkr.ecr.us-east-2.amazonaws.com/x failed; account 123456789012.\n"
           b"image sha256:ab123456789012cd0000 and epoch 1800000000.123")
    out = redact_log(raw, "dry-run-results")
    assert b"123456789012" not in out.replace(b"ab123456789012cd", b"") and b"dry-run-results" not in out
    assert b"arn:aws" not in out and out.count(b"<account-id>") == 2 and b"sha256:ab123456789012cd0000" in out
    assert b"1800000000.123" in out


# ============================================================================================ B6
def test_run_accepts_resume_fault_and_block_as_the_job_passes_them(monkeypatch):
    from orchestrator import cli
    seen = {}
    monkeypatch.setattr(cli, "cmd_run", lambda a: seen.update(resume=a.resume, fault=a.fault, block=a.block) or 0)
    assert cli.main(["run", "--resume", "--fault", "F4", "--block", "1"]) == 0
    assert seen == {"resume": True, "fault": "F4", "block": 1}
    assert cli.main(["run", "--resume"]) == 0 and seen["resume"] is True and seen["fault"] is None


# ============================================================================================ coordinator requests
INFRA = Path(__file__).resolve().parents[2] / "infra"


def test_remediation_resubmits_with_run_job_sh(make):
    """REVIEW4 L1 (C40): the remediation submits through run-job.sh, the flink-submit Job's path, which passes -p 4 and the
    checkpoint settings. The simulated cluster refuses a plain `flink run`, which would build the JobGraph with the
    distribution's parallelism 1 and no checkpoints."""
    from orchestrator.fakes.world import RUN_JOB_SETTINGS
    from orchestrator.recovery import Remediation
    h = make(world={"f5_restore_fails": True})
    h.run(only_fault="F5")
    a1 = attempts(h, "F5")[0]
    acts = a1["recovery_gate"]["remediation"]["actions"]
    resub = [x for x in acts if x.startswith("RESUBMIT ")]
    assert len(resub) == 3 and all(x.endswith("rc=0") for x in resub) and "JOB_SETTINGS_OK" in acts
    execs = [c for c in h.kube.calls if c[0] == "exec" and "flink" in c[2]]
    assert execs and all(c[2] == Remediation.RUN_JOB for c in execs)
    resubmitted = [j for j in h.world.flink_jobs if j.restored_from]
    assert resubmitted and all(j.settings == RUN_JOB_SETTINGS and j.restored_from.startswith("s3://dry-run-results/scratch/")
                               for j in resubmitted)
    rt = h.runtime()
    assert rt.kube.exec(jm_pod(h), "flink-main-container", ["/opt/flink/bin/flink", "run", "-d", "-c", "x", "y.jar"], 30).rc != 0


def test_job_settings_wrong_at_startup_halt_before_warmup(make):
    """REVIEW4 L1 (C40): jobs submitted with the distribution defaults (parallelism 1, checkpointing off) halt the start."""
    from orchestrator.fakes.world import DISTRIBUTION_DEFAULT_SETTINGS
    h = make()
    for j in h.world.flink_jobs:  # as the flink-submit Job's plain `flink run` would have submitted them
        j.settings = dict(DISTRIBUTION_DEFAULT_SETTINGS)
    assert h.run() == 3
    assert not (h.volume / "manifest.json").exists()
    assert any("FLINK_JOB_SETTINGS_WRONG at STARTUP" in msg for _, _, msg in h.alerts)


def test_job_settings_are_the_ones_run_job_sh_passes():
    """The registered settings equal rebuild/infra/images/flink-jobs/run-job.sh, read with infra's own parser."""
    sys.path.insert(0, str(INFRA / "scripts"))
    import check_flink_jobs
    from orchestrator import thresholds
    from orchestrator.config import load_config
    want = check_flink_jobs.expected_settings((INFRA / "images/flink-jobs/run-job.sh").read_text())
    got = thresholds.build(load_config())["flink_job_settings"]
    assert got == {"parallelism": int(want["parallelism"]),
                   "checkpoint_mode": want["execution.checkpointing.mode"].lower(),
                   "checkpoint_interval_ms": int(want["execution.checkpointing.interval"]),
                   "checkpoint_timeout_ms": int(want["execution.checkpointing.timeout"]),
                   "checkpoint_min_pause_ms": int(want["execution.checkpointing.min-pause"]),
                   "externalization_enabled": True,
                   "delete_on_cancellation": want["execution.checkpointing.externalized-checkpoint-retention"]
                   != "RETAIN_ON_CANCELLATION"}


def test_restore_path_from_the_stored_relative_form(make):
    """C24: stored paths are relative to the campaign's scratch prefix. The remediation rebuilds the full URI."""
    from orchestrator.recovery import Remediation
    h = make()
    r = Remediation(h.runtime())
    assert r.full_path("flink-checkpoints/abc/chk-7") == "s3://dry-run-results/scratch/pilot-ec/flink-checkpoints/abc/chk-7"
    assert r.full_path("s3://dry-run-results/scratch/pilot-ec/flink-checkpoints/abc/chk-7").endswith("chk-7")


@pytest.mark.parametrize("slots_min", [0, 12])
def test_redeploy_works_with_and_without_the_minimum_slots(make, slots_min):
    """REVIEW4 L4: with slotmanager.number-of-slots.min 12 (infra) three TaskManagers come back with no job; with 0 none do
    until the jobs ask for slots. The redeploy resubmits before it waits for TaskManagers, so it works in both cases."""
    h = make(world={"f5_restore_fails": True, "submit_fails_until_redeploy": True, "slots_min": slots_min})
    h.run(only_fault="F5")
    a1, a2 = attempts(h, "F5")[:2]
    acts = a1["recovery_gate"]["remediation"]["actions"]
    assert any(x.startswith("FLINK_REDEPLOY") for x in acts) and "JOB_SETTINGS_OK" in acts
    assert a1["recovery_gate"]["passed_at"] is not None and "AFTER_REDEPLOY" in a2["flags"]


def test_watchdog_pods_and_jobs_count_for_nothing(make):
    """C6 (infra): the watchdog CronJob's pods carry app=campaign-watchdog and no rebuild/* label. Every pod and Job count of
    the orchestrator uses the rebuild/campaign or rebuild/attempt selectors, so they are ignored."""
    h = make()
    h.world.add_pod("campaign-watchdog-29331840-k8x2p", "watchdog", {"app": "campaign-watchdog"}, h.world.t0, 1, "watchdog")
    h.world.jobs_k8s["campaign-watchdog-29331840"] = {"labels": {"app": "campaign-watchdog"}, "done_at": h.world.t0 + 5,
                                                      "ok": True, "pods": []}
    assert h.run(only_fault="F4") == 0
    (a,) = attempts(h, "F4")
    assert a["outcome"] == "VALID"
    conds = {c["name"]: c for c in verification(h, a)["recovery_gate_conditions"]}
    assert conds["no_campaign_chaos_or_jobs"]["value"] == 0


def test_image_references_are_recorded_without_the_registry_host(make):
    """C1 (infra): the orchestrator records image digests only, never an ECR host with the account ID."""
    h = make()
    h.env.orchestrator_image = "000000000000.dkr.ecr.us-east-2.amazonaws.com/streaming/orchestrator@sha256:" + "a" * 64
    h.env.f6_injector_image = "000000000000.dkr.ecr.us-east-2.amazonaws.com/streaming/f6-injector@sha256:" + "b" * 64
    assert h.run(only_fault="F6") == 0
    m = h.manifest()
    assert m["orchestrator"]["image_digest"] == "sha256:" + "a" * 64
    v = verification(h, attempts(h, "F6")[0])
    assert v["f6"]["injector_image_digest"] == "sha256:" + "b" * 64
    assert "amazonaws.com" not in json.dumps(m) + json.dumps(v)


def test_fake_counters_never_decrease_outside_a_restart(make):
    """The simulated exporters' counters are cumulative, like the real ones: within one series and one pod they never fall.
    A broker restart (new pod UID) and a reopened Kafka meter (the series was absent) are the only resets."""
    h = make()
    assert h.run() == 0
    w = h.world
    counters = {k for k, v in METRIC_TYPES.items() if v == "counter"} | {
        f"flink_taskmanager_job_task_{m}" for m in ("numRecordsIn", "numRecordsOut")} | {
        "flink_taskmanager_job_task_operator_KafkaSourceReader_commitsSucceeded",
        "flink_jobmanager_job_numberOfCompletedCheckpoints", "flink_jobmanager_job_numberOfFailedCheckpoints",
        "flink_taskmanager_Status_JVM_CPU_Time", "flink_taskmanager_Status_JVM_GarbageCollector_G1_Young_Generation_Count",
        "jvm_gc_collection_seconds_count", "jvm_gc_collection_seconds_sum"}
    last: dict = {}
    bad, checked = [], 0
    t0 = int(h.manifest()["timeline_start_at"]) // 10 * 10
    for t in range(t0, int(h.clock.t) // 10 * 10, 10):
        step = w.series_at(t)
        seen = set()
        for name, rows in step.by_name.items():
            if name not in counters:
                continue
            for lab, v in rows:
                p = w.pod_by_name(lab.get("pod", ""), t)
                k = (id(lab), p.uid if p else None)
                seen.add(k)
                prev = last.get(k)
                if prev is not None and prev[0] == t - 10:
                    checked += 1
                    if v < prev[1]:
                        bad.append((name, lab.get("pod"), t, prev[1], v))
                last[k] = (t, v)
    assert checked > 100000 and not bad, bad[:5]


def test_plan_tool_resolves_paths_against_the_callers_directory(tmp_path):
    """REVIEW4 M10: plan_tool.sh cd's into rebuild/orchestrator, so a relative --out used to land there. It now resolves
    --out (and --plan) against the caller's working directory first. Uses the pinned tool environment
    (rebuild/integration/.plan-env), which plan_tool.sh builds from PyPI with --require-hashes if it is missing."""
    import subprocess
    tool = Path(__file__).resolve().parents[2] / "integration" / "plan_tool.sh"
    r = subprocess.run(["bash", str(tool), "plan", "--workload", "ecommerce", "--index", "0", "--out", "plans/pilot-ec"],
                       cwd=tmp_path, capture_output=True, text=True, timeout=600)
    assert r.returncode == 0, r.stderr[-2000:]
    sha = r.stdout.strip().splitlines()[-1]
    assert (tmp_path / "plans/pilot-ec/plan.json").is_file()
    assert (tmp_path / "plans/pilot-ec/plan.json.sha256").read_text().strip() == sha
    assert not (Path(__file__).resolve().parents[1] / "plans").exists()
    r = subprocess.run(["bash", str(tool), "verify-plan", "--workload", "ecommerce", "--index", "0",
                        "--plan=plans/pilot-ec/plan.json", "--sha", sha], cwd=tmp_path, capture_output=True, text=True,
                       timeout=600)
    assert r.returncode == 0 and "OK" in r.stdout, r.stdout + r.stderr[-1000:]


def test_unreadable_health_record_halts_the_start_before_warmup(make):
    """C30 with decision 23: the preprocessing refuses a manifest whose Health record was never read, so the start reads the
    log group once, retries for 15 minutes and halts before WARMUP. A read that recovers inside 15 minutes starts normally."""
    h = make()
    h.world.global_knobs["health_poll_fail_until"] = h.world.t0 + 10 ** 6
    assert h.run() == 3
    assert not (h.volume / "manifest.json").exists()
    assert any("HEALTH_RECORD_UNAVAILABLE" in msg for _, _, msg in h.alerts)
    h2 = make()
    h2.world.global_knobs["health_poll_fail_calls"] = {1, 2, 3}
    assert h2.run(only_fault="F4") == 0 and h2.manifest()["health_events_polled_until"] is not None
