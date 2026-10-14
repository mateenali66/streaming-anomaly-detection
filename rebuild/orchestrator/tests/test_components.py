"""Components: the F6 injector, the probe service's parsing, the verification consumer, chaos resource builders."""
import json

from orchestrator import chaos, consumer_service, ids
from orchestrator.f6_injector import parse_log, run
from orchestrator.kube import ExecResult
from orchestrator.probe_service import ProbeRunner, loadgen_status, parse_leader_id, parse_prom_text
from orchestrator.probefile import validate_row


class FakeProducer:
    def __init__(self, clock, delay=0.01):
        self.clock, self.delay, self.pending, self.records = clock, delay, [], []

    def produce(self, topic, value=None, key=None, headers=None, callback=None):
        self.records.append((topic, value, key, headers))
        self.pending.append((self.clock["t"] + self.delay, callback))

    def poll(self, timeout):
        due = [p for p in self.pending if p[0] <= self.clock["t"]]
        self.pending = [p for p in self.pending if p[0] > self.clock["t"]]
        for _, cb in due:
            cb(None, None)

    def flush(self, timeout):
        if self.pending:
            self.clock["t"] = max(d for d, _ in self.pending)
        self.poll(0)
        return 0


def test_f6_injector_sends_exactly_the_planned_count_with_header_and_no_brace():
    clock = {"t": 1000.0}
    prod = FakeProducer(clock)
    lines, events = [], []
    created = []

    def make():  # decision 28: the producer (and its connections) only from START_AT_UNIX on
        created.append(clock["t"])
        return prod
    summ = run(make, "iot-c1-b02-p3-a1", 1010.0, 180, 1800, 7, now=lambda: clock["t"],
               sleep=lambda s: clock.__setitem__("t", clock["t"] + max(s, 1e-4)), ready_file=None,
               emit=lambda e, **kw: events.append((e, kw)), print_line=lines.append)
    assert created == [created[0]] and created[0] >= 1010.0
    assert len(prod.records) == 1800 and summ["acked_count"] == 1800 and summ["sent_count"] == 1800
    for topic, value, key, headers in prod.records:
        assert topic == "raw-events" and b"}" not in value and headers == [("rebuild-attempt", b"iot-c1-b02-p3-a1")]
    assert summ["first_produce_at"] >= 1010.0 and summ["first_produce_at"] - 1010.0 < 0.1
    assert 179 < summ["last_produce_at"] - summ["first_produce_at"] < 181
    parsed = parse_log("\n".join(json.dumps({"event": e, **kw}) for e, kw in events) + "\n" + lines[-1])
    assert parsed["summary"]["acked_count"] == 1800 and parsed["summary"]["planned_count"] == 1800
    assert abs(parsed["summary"]["first_produce_at"] - summ["first_produce_at"]) < 1e-3


def test_parse_log_reads_the_infra_injected_line():
    text = "START t0=1.0\nINJECTED sent=10 delivered=9 failed=1 planned=10 first_epoch=100.500 last_epoch=280.250 payload_sha256=ab\n"
    s = parse_log(text)["summary"]
    assert s == {"sent_count": 10, "acked_count": 9, "failed_count": 1, "planned_count": 10, "first_produce_at": 100.5,
                 "last_produce_at": 280.25}
    assert parse_log("INJECTED sent=0 delivered=0 failed=0 planned=10 first_epoch=0.000 last_epoch=0.000")["summary"][
        "first_produce_at"] is None


LOADGEN_STATUS = {"acked_count": 97, "produced_count": 100, "delivery_failures_count": 3, "produce_errors_count": 0,
                  "commanded_rate_per_s": 1000.0, "cycle_phase_frac": 0.25, "epoch_at": 1.7e9, "started_at": 1.7e9 + 5,
                  "seed": 4242, "workload": "ECOMMERCE", "profile": "LOW", "at": 1.7e9 + 60}


def test_leader_id_and_prom_text_parsing():
    assert parse_leader_id("ClusterId: x\nLeaderId:              3002\nLeaderEpoch: 2\n") == 3002
    m = parse_prom_text("# HELP a\nloadgen_produced_total 5\nloadgen_info{seed=\"1\"} 1\nloadgen_seed 42\n")
    assert m == {"loadgen_produced_total": 5.0, "loadgen_seed": 42.0}


def test_load_generator_status_is_the_acknowledged_count():
    """11.11 and 2.3.9 (CONTRACT_CHANGES C7): /status on 9102, acked_count from delivery acknowledgements."""
    st = loadgen_status(dict(LOADGEN_STATUS))
    assert st["acked_count"] == 97 and st["cycle_phase_frac"] == 0.25 and st["seed"] == 4242
    import pytest
    from orchestrator.errors import QueryError
    for k in ("acked_count", "seed", "cycle_phase_frac"):
        with pytest.raises(QueryError):
            loadgen_status({x: v for x, v in LOADGEN_STATUS.items() if x != k})
    with pytest.raises(QueryError):
        loadgen_status(dict(LOADGEN_STATUS, acked_count="97"))
    assert ids.LOADGEN_STATUS_URL.endswith(":9102/status")


def test_f6_header_is_the_contract_header():
    from orchestrator import f6_injector
    assert f6_injector.HEADER == ids.F6_HEADER == "rebuild-attempt"


def test_malformed_records_are_attributed_by_the_attempt_header():
    """2.3.8 and 11.11 (CONTRACT_CHANGES C6): only records carrying this attempt's rebuild-attempt header count. Another
    attempt's records, or malformed records without the header, inside the same window never count; a consumer restart
    inside the window makes the count missing."""
    from types import SimpleNamespace
    from orchestrator.canonical import canonical_str
    from orchestrator.gate import malformed_delta
    akey = "ecommerce-c1-b02-p3-a1"

    def sample(ts, mine, other, untagged, started=100.0):
        by = {akey: mine, "ecommerce-c1-b02-p2-a1": other} if mine or other else {}
        st = {"started_at": started, "malformed_count": mine + other + untagged, "malformed_by_attempt": by}
        return {"ts": ts, "probe": "consumer", "ok_count": 1, "value": float(st["malformed_count"]), "detail": canonical_str(st)}
    rows = [sample(1000, 0, 0, 0), sample(1010, 0, 40, 5), sample(1020, 300, 40, 9), sample(1030, 900, 40, 9)]
    ctx = SimpleNamespace(by_probe={"consumer": rows}, a={"attempt_key": akey})
    assert malformed_delta(ctx, 1005, 1035) == 900.0  # the other attempt's 40 and the 9 untagged records do not count
    assert malformed_delta(ctx, 1000, 1010) == 0.0
    restarted = rows[:2] + [sample(1020, 300, 0, 0, started=1015.0), sample(1030, 900, 0, 0, started=1015.0)]
    assert malformed_delta(SimpleNamespace(by_probe={"consumer": restarted}, a={"attempt_key": akey}), 1005, 1035) is None
    assert malformed_delta(ctx, None, 1035) is None and malformed_delta(ctx, 900, 1035) is None


class ExecKube:
    def __init__(self, results):
        self.results = results
        self.calls = []

    def exec(self, pod, container, argv, timeout_s):
        self.calls.append((pod, container, argv))
        return self.results(pod, container, argv)


def test_probe_rows_from_exec_output_and_exec_failure():
    def results(pod, container, argv):
        if "probe" in argv:
            return ExecResult(0, "OK streaming-kafka-brokers-1:9091 100.10 100.11\nFAIL streaming-kafka-brokers-2:9091 100.20 100.70\n", "")
        if "burst" in argv:
            return ExecResult(0, "\n".join(f"{'OK' if i % 2 else 'FAIL'} {200 + i} {200 + i + 0.01}" for i in range(10)), "")
        return ExecResult(1, "", "error: container not found")
    pr = ProbeRunner(ExecKube(results), http=None, flink=None, now=lambda: 99.0)
    rows = pr.tcp_from("streaming-kafka-brokers-0", "kafka",
                       [("10.0.0.2", 9091, "streaming-kafka-brokers-1:9091"), ("10.0.0.3", 9091, "streaming-kafka-brokers-2:9091"),
                        (None, 9091, "streaming-kafka-brokers-9:9091")])
    for r in rows:
        validate_row(r)
    by = {r["dst"]: r for r in rows}
    assert by["streaming-kafka-brokers-1:9091"]["ok_count"] == 1 and by["streaming-kafka-brokers-1:9091"]["ts"] == 100.10
    assert by["streaming-kafka-brokers-2:9091"]["ok_count"] == 0
    assert by["streaming-kafka-brokers-9:9091"]["error"] == "NO_POD_IP"
    b = pr.burst_from("streaming-flink-taskmanager-1-1", "10.0.0.9", "jm")
    validate_row(b)
    assert b["tried_count"] == 10 and b["ok_count"] == 5 and b["ts"] == 200.0
    bad = ProbeRunner(ExecKube(lambda *a: ExecResult(1, "", "boom")), http=None, flink=None, now=lambda: 5.0)
    r = bad.burst_from("tm", "10.0.0.9", "jm")
    assert r["ok_count"] == 0 and "rc=1" in r["error"]


def test_consumer_counts_malformed_and_ignores_the_sentinel():
    c = consumer_service.Counters(now=lambda: 1.0)
    c.add("raw-events", b'{"a":1}', None)
    c.add("raw-events", b'{"event_type":"poison","raw":"xyz', [(ids.F6_HEADER, b"k1")])
    c.add("raw-events", b'{"event_type":"poison","raw":"xyz', None)
    c.add("raw-events", b"\xff\xfe", None)
    c.add("dead-letter", b"{}", [(ids.SENTINEL_HEADER, b"x")])
    c.add("dead-letter", b"oops", [(ids.F6_HEADER, b"k1")])
    st = c.status()
    assert st["raw_events_count"] == 4 and st["malformed_count"] == 3 and st["malformed_by_attempt"] == {"k1": 1}
    assert st["dead_letter_count"] == 1
    text = consumer_service.metrics_text(st)
    assert "verify_consumer_records_total 4" in text and "verify_consumer_malformed_total 3" in text


def test_chaos_builders_name_exact_targets_and_carry_labels():
    objs = [chaos.pod_kill("pk-x", "pilot-ec", "k", "F1", "streaming-kafka-brokers-0"),
            chaos.partition("nc-x", "pilot-ec", "k", "streaming-kafka-brokers-0", ["streaming-kafka-brokers-1", "streaming-kafka-brokers-2"], 180),
            chaos.loss("nc-y", "pilot-ec", "k", ["tm-1", "tm-2", "tm-3"], 50, 300),
            chaos.cpu_stress("sc-x", "pilot-ec", "k", ["tm-1"], 4, 90, 300),
            chaos.memory_stress("sc-y", "pilot-ec", "k", "tm-1", 1024, 180)]
    for o in objs:
        assert set(o["spec"]["selector"]) == {"pods"}  # the pod list selector: other selectors are ignored
        assert o["metadata"]["labels"][ids.LABEL_CAMPAIGN] == "pilot-ec"
    assert objs[0]["spec"]["gracePeriod"] == 0 and objs[0]["spec"]["action"] == "pod-kill"
    assert objs[1]["spec"]["direction"] == "both" and objs[1]["spec"]["target"]["selector"]["pods"]["streaming"] == [
        "streaming-kafka-brokers-1", "streaming-kafka-brokers-2"]
    assert objs[2]["spec"]["direction"] == "to" and objs[2]["spec"]["loss"]["loss"] == "50"
    assert objs[3]["spec"]["containerNames"] == [ids.FLINK_CONTAINER]
    assert objs[4]["spec"]["stressors"]["memory"]["size"] == "1024MiB"
    for o in objs[1:]:
        assert o["spec"]["duration"].endswith("s")  # the fault ends by itself if the orchestrator dies


def test_chaos_records_need_every_target_applied_and_recovered():
    obj = {"status": {"conditions": [{"type": "AllRecovered", "status": "True"}],
                      "experiment": {"containerRecords": []}}}
    assert chaos.all_recovered(chaos.records(obj)) is False  # an empty record list never counts as recovered
    obj["status"]["experiment"]["containerRecords"] = [
        {"id": "streaming/tm-1/flink-main-container", "selectorKey": ".", "phase": "Not Injected",
         "events": [{"type": "Succeeded", "operation": "Apply", "timestamp": "2026-10-13T10:00:00Z"},
                    {"type": "Succeeded", "operation": "Recover", "timestamp": "2026-10-13T10:05:00Z"}]}]
    recs = chaos.records(obj)
    assert recs[0]["id"] == "streaming/tm-1" and chaos.all_recovered(recs)
    assert chaos.all_applied(recs, {"streaming/tm-1"}) and not chaos.all_applied(recs, {"streaming/tm-1", "streaming/tm-2"})
