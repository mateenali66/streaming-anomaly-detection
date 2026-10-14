"""F6 poison-pill injector (protocol 1.2 F6, 2.4 F6, 11.11 F6 injector). The ONE implementation (CONTRACT_CHANGES C6).

It runs in two places, always from this file:
  * infra's f6-injector image (rebuild/infra/images/f6-injector/Dockerfile copies this file to /opt/injector/f6_injector.py
    and runs it as a script), which is the image the orchestrator puts in Job poison-<attempt_key>;
  * `python -m orchestrator f6-inject` in the orchestrator image, the same code, for a manual check.
It imports only the standard library at module level, so it runs as a plain script; confluent-kafka is imported in main().

Behaviour. The pod starts before onset (the orchestrator creates the Job 90 s ahead), so it is Ready long before START_AT_UNIX.
It opens no Kafka connection before START_AT_UNIX (decision 28, REVIEW4 MIN-5): the producer is created and the topic's
metadata fetched only then, so F6 pre-fault windows carry no injector traffic that other run types lack. It then sends exactly
round(INJECT_RATE x INJECT_DURATION) records, evenly paced over INJECT_DURATION. R1 sent int(rate*0.1) records per 100 ms tick and under-delivered by about a fifth (Review 2). Every record
carries the header rebuild-attempt=<ATTEMPT_KEY>, so the verification consumer attributes it to this attempt (2.3.8, 11.11).
The value is a JSON fragment that never contains a closing brace, so the enrichment job throws and routes it to dead-letter
(R1's random binary payloads held a brace one time in three), and the verification consumer counts it as malformed.

What it reports. Counts and times come from the delivery callbacks (acks=all, idempotent producer): acked_count is the number
of records the brokers acknowledged, first_produce_at and last_produce_at are the times of the first and last acknowledgement.
Those two instants are the F6 onset and labelled end (2.4 onset table). Send times are logged too, for diagnosis only.

Environment (also --kafka-bootstrap style arguments, which win over the environment):
  KAFKA_BOOTSTRAP         required  host:port
  KAFKA_TOPIC             default raw-events
  INJECT_RATE             required  records per second (the orchestrator passes f6_rate_per_s of 11.12)
  INJECT_DURATION         required  seconds
  INJECT_SEED             required  integer (the plan's f6_payload_seed), so the payload stream is reproducible
  START_AT_UNIX           required  unix seconds of the planned onset; more than LATE_TOLERANCE_SEC late means exit 3
  ATTEMPT_KEY             required  the attempt key of 11.2, the value of the rebuild-attempt header
  LATE_TOLERANCE_SEC      default 5
  MIN_DELIVERED_FRACTION  default 0.9  of the planned count, counted from acknowledgements

Output on stdout: one JSON object per line ("ready", "first_produce_ack", "summary", or "error"), then the final line
  INJECTED sent=N delivered=A failed=F planned=P first_epoch=T0 last_epoch=T1 payload_sha256=H
where delivered is the acknowledged count and T0, T1 are the first and last acknowledgement times (0.000 if none).
Exit codes: 0 ok; 1 shortfall, delivery failure or records left unflushed; 2 bad configuration or no metadata; 3 too late.

confluent-kafka (context7 /confluentinc/confluent-kafka-python): produce(topic, value, key, headers, callback) is
asynchronous and raises BufferError when the local queue is full; the delivery callback is served by poll() or flush();
flush(timeout) returns the number of messages still queued. msg.headers() is not available in the callback, so acks are
counted, not matched by header.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import string
import sys
import time
from pathlib import Path

HEADER = "rebuild-attempt"  # equals orchestrator.ids.F6_HEADER (asserted by tests/test_components.py)
ATTEMPT_KEY_RE = re.compile(r"^(ecommerce|iot)-c[0-5]-b(0[1-9]|10)-p[1-8]-a[1-9]$")
EXIT_OK, EXIT_SHORTFALL, EXIT_CONFIG, EXIT_LATE = 0, 1, 2, 3


class ConfigError(Exception):
    pass


def payload(rng: random.Random, akey: str, seq: int) -> bytes:
    body = "".join(rng.choices(string.ascii_letters + string.digits, k=rng.randint(10, 200)))
    return ('{"event_type":"poison","attempt":"' + akey + '","seq":' + str(seq) + ',"raw":"' + body).encode("utf-8")


def log(event: str, **kw) -> None:
    print(json.dumps({"event": event, **kw}, sort_keys=True), flush=True)


def injected_line(summary: dict) -> str:
    """The final stdout line. delivered, first_epoch and last_epoch are acknowledgements, never send times."""
    return (f"INJECTED sent={summary['sent_count']} delivered={summary['acked_count']} failed={summary['failed_count']} "
            f"planned={summary['planned_count']} first_epoch={summary['first_produce_at'] or 0:.3f} "
            f"last_epoch={summary['last_produce_at'] or 0:.3f} payload_sha256={summary['payload_sha256']}")


def _produce(producer, topic: str, value: bytes, key: bytes, akey: str, callback, sleep) -> None:
    """One produce call. A full local queue (BufferError) is drained with poll() and the record is sent again, so the
    planned count is always met in full or the shortfall shows up in the acknowledgements."""
    while True:
        try:
            producer.produce(topic, value=value, key=key, headers=[(HEADER, akey.encode("utf-8"))], callback=callback)
            return
        except BufferError:
            producer.poll(0.2)
            sleep(0.0)


def run(make_producer, akey: str, start_at: float, duration_s: float, planned_count: int, seed: int,
        topic: str = "raw-events", now=time.time, sleep=time.sleep, ready_file: str | None = "/tmp/ready",
        emit=log, print_line=lambda line: print(line, flush=True), flush_timeout_s: float = 60.0) -> dict:
    """make_producer() creates the producer, which opens the connections. It is called at START_AT_UNIX, never before
    (decision 28). It may raise, and then nothing is sent."""
    if planned_count <= 0 or duration_s <= 0:
        raise ValueError("planned_count and duration_s must be positive")
    if not ATTEMPT_KEY_RE.match(akey):
        raise ValueError(f"ATTEMPT_KEY {akey!r} is not an 11.2 attempt key")
    rng = random.Random(f"{seed}:{akey}")  # string seeds are hashed with SHA-512, so this is reproducible
    digest = hashlib.sha256()
    state = {"acked": 0, "failed": 0, "first_ack": None, "last_ack": None}

    def on_delivery(err, msg):
        t = now()
        if err is None:
            state["acked"] += 1
            if state["first_ack"] is None:
                state["first_ack"] = t
                emit("first_produce_ack", at=t)
            state["last_ack"] = t
        else:
            state["failed"] += 1

    if ready_file:
        Path(ready_file).write_text("ready")
    emit("ready", at=now(), start_at=start_at, planned_count=planned_count, duration_s=duration_s, attempt_key=akey)
    while now() < start_at:
        sleep(min(0.05, max(0.0, start_at - now())))
    producer = make_producer()
    emit("connected", at=now())
    interval = duration_s / planned_count
    sent = 0
    first_send = None
    for i in range(planned_count):
        due = start_at + i * interval
        while now() < due:
            producer.poll(0)
            sleep(min(0.01, max(0.0, due - now())))
        value = payload(rng, akey, i)
        digest.update(value)
        _produce(producer, topic, value, f"poison-{i}".encode(), akey, on_delivery, sleep)
        if first_send is None:
            first_send = now()
        sent += 1
        producer.poll(0)
    last_send = now()
    remaining = producer.flush(flush_timeout_s)
    summary = {"planned_count": planned_count, "sent_count": sent, "acked_count": state["acked"],
               "failed_count": state["failed"], "unflushed_count": remaining, "first_send_at": first_send,
               "last_send_at": last_send, "first_produce_at": state["first_ack"], "last_produce_at": state["last_ack"],
               "payload_sha256": digest.hexdigest()}
    emit("summary", **summary)
    print_line(injected_line(summary))
    return summary


def exit_code_for(summary: dict, min_frac: float) -> int:
    if summary["unflushed_count"] or summary["failed_count"] or summary["acked_count"] < min_frac * summary["planned_count"]:
        return EXIT_SHORTFALL
    return EXIT_OK


INJECTED_RE = re.compile(r"^INJECTED sent=(\d+) delivered=(\d+) failed=(\d+) planned=(\d+) first_epoch=([0-9.]+) "
                         r"last_epoch=([0-9.]+)(?: payload_sha256=([0-9a-f]+))?\s*$")


def parse_log(text: str) -> dict:
    """The orchestrator reads the injector's result back from the pod log (stored as attempts/<key>/injector.log, 11.7).
    The INJECTED line is authoritative; the JSON summary line is the fallback."""
    out = {"ready_at": None, "summary": None, "orchestrator": None}
    injected = None
    for line in text.splitlines():
        line = line.strip()
        m = INJECTED_RE.match(line)
        if m:
            first, last = float(m.group(5)), float(m.group(6))
            injected = {"sent_count": int(m.group(1)), "acked_count": int(m.group(2)), "failed_count": int(m.group(3)),
                        "planned_count": int(m.group(4)), "first_produce_at": first or None, "last_produce_at": last or None}
            continue
        if not line.startswith("{"):
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if obj.get("event") == "ready":
            out["ready_at"] = obj.get("at")
        elif obj.get("event") == "summary" and out["summary"] is None:
            out["summary"] = {k: obj.get(k) for k in ("sent_count", "acked_count", "failed_count", "planned_count",
                                                      "first_produce_at", "last_produce_at")}
        elif obj.get("event") == "orchestrator_observed":
            out["orchestrator"] = obj
    if injected is not None:
        out["summary"] = injected
    return out


ENV_NAMES = ("KAFKA_BOOTSTRAP", "KAFKA_TOPIC", "INJECT_RATE", "INJECT_DURATION", "INJECT_SEED", "START_AT_UNIX",
             "ATTEMPT_KEY", "LATE_TOLERANCE_SEC", "MIN_DELIVERED_FRACTION")


def config_from(env: dict, argv: list[str] | None) -> dict:
    ap = argparse.ArgumentParser(prog="f6_injector", add_help=True)
    for n in ENV_NAMES:
        ap.add_argument("--" + n.lower().replace("_", "-"), dest=n)
    a = ap.parse_args(argv or [])

    def get(name: str, default: str | None = None) -> str:
        v = getattr(a, name) or env.get(name) or default
        if v is None or str(v).strip() == "":
            raise ConfigError(f"{name} is required (no default)")
        return str(v).strip()

    try:
        cfg = {"bootstrap": get("KAFKA_BOOTSTRAP"), "topic": get("KAFKA_TOPIC", "raw-events"),
               "rate": float(get("INJECT_RATE")), "duration": float(get("INJECT_DURATION")), "seed": int(get("INJECT_SEED")),
               "start_at": float(get("START_AT_UNIX")), "akey": get("ATTEMPT_KEY"),
               "tolerance": float(get("LATE_TOLERANCE_SEC", "5")), "min_frac": float(get("MIN_DELIVERED_FRACTION", "0.9"))}
    except ValueError as e:
        raise ConfigError(f"bad number: {e}") from e
    if cfg["rate"] <= 0 or cfg["duration"] <= 0:
        raise ConfigError("INJECT_RATE and INJECT_DURATION must be positive")
    if not ATTEMPT_KEY_RE.match(cfg["akey"]):
        raise ConfigError(f"ATTEMPT_KEY {cfg['akey']!r} is not an 11.2 attempt key")
    cfg["planned"] = int(round(cfg["rate"] * cfg["duration"]))
    if cfg["planned"] <= 0:
        raise ConfigError("INJECT_RATE x INJECT_DURATION rounds to zero records")
    return cfg


def main(argv: list[str] | None = None, env: dict | None = None, producer_factory=None, now=time.time,
         sleep=time.sleep, ready_file: str | None = "/tmp/ready") -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv == ["--self-test"]:
        return self_test()
    try:
        cfg = config_from(os.environ if env is None else env, argv)
    except ConfigError as e:
        log("error", message=f"configuration: {e}")
        return EXIT_CONFIG
    if producer_factory is None:
        from confluent_kafka import Producer  # noqa: PLC0415 - baked into the image, never installed at run time

        def producer_factory(conf):
            return Producer(conf)
    if now() > cfg["start_at"] + cfg["tolerance"]:
        log("error", message=f"started {now() - cfg['start_at']:.1f} s after START_AT_UNIX (tolerance {cfg['tolerance']} s); "
                             "nothing sent")
        return EXIT_LATE

    def make_producer():
        # Called at START_AT_UNIX (decision 28): the connections and the metadata request happen only from onset on.
        producer = producer_factory({"bootstrap.servers": cfg["bootstrap"], "client.id": f"poison-{cfg['akey']}",
                                     "acks": "all", "enable.idempotence": True, "linger.ms": 5, "socket.timeout.ms": 10000})
        try:
            md = producer.list_topics(cfg["topic"], timeout=5)
        except Exception as e:  # noqa: BLE001 - a broker that cannot be reached at onset fails the injection
            raise ConfigError(f"{type(e).__name__}: {e}") from e
        topics = getattr(md, "topics", {}) or {}
        if cfg["topic"] not in topics or getattr(topics[cfg["topic"]], "error", None):
            raise ConfigError(f"{cfg['topic']} not in the cluster metadata")
        return producer

    try:
        summ = run(make_producer, cfg["akey"], cfg["start_at"], cfg["duration"], cfg["planned"], cfg["seed"],
                   topic=cfg["topic"], now=now, sleep=sleep, ready_file=ready_file)
    except ConfigError as e:
        log("error", message=f"metadata: {e}")
        return EXIT_CONFIG
    except Exception as e:  # noqa: BLE001 - never end quietly: an error is a non-zero exit with the reason logged
        log("error", message=f"{type(e).__name__}: {e}")
        return EXIT_SHORTFALL
    rc = exit_code_for(summ, cfg["min_frac"])
    if rc:
        log("error", message=f"shortfall: acked {summ['acked_count']} of {summ['planned_count']}, failed {summ['failed_count']}, "
                             f"unflushed {summ['unflushed_count']}")
    return rc


class _SelfTestProducer:
    def __init__(self, clock: dict) -> None:
        self.clock, self.pending, self.records, self.full_once = clock, [], [], True

    def produce(self, topic, value=None, key=None, headers=None, callback=None):
        if self.full_once and len(self.records) == 3:
            self.full_once = False
            raise BufferError("queue full")
        self.records.append((topic, value, headers))
        self.pending.append((self.clock["t"] + 0.01, callback))

    def poll(self, timeout=0):
        due = [p for p in self.pending if p[0] <= self.clock["t"]]
        self.pending = [p for p in self.pending if p[0] > self.clock["t"]]
        for _, cb in due:
            cb(None, None)

    def flush(self, timeout=0):
        if self.pending:
            self.clock["t"] = max(d for d, _ in self.pending)
        self.poll(0)
        return 0


def self_test() -> int:
    """Run once at image build (rebuild/infra/images/f6-injector/Dockerfile): header on every record, exact count, no brace,
    acknowledgement times, and the INJECTED line parses back. No Kafka needed."""
    clock = {"t": 100.0}
    prod = _SelfTestProducer(clock)
    lines: list[str] = []
    created = []

    def make():
        created.append(clock["t"])
        return prod
    summ = run(make, "ecommerce-c0-b01-p1-a1", 101.0, 10, 50, 3, now=lambda: clock["t"],
               sleep=lambda s: clock.__setitem__("t", clock["t"] + max(s, 1e-4)), ready_file=None,
               emit=lambda e, **kw: None, print_line=lines.append)
    ok = (len(prod.records) == 50 and summ["acked_count"] == 50
          and all(h == [(HEADER, b"ecommerce-c0-b01-p1-a1")] and b"}" not in v for _, v, h in prod.records)
          and summ["first_produce_at"] is not None and summ["first_produce_at"] >= 101.0
          and created == [created[0]] and created[0] >= 101.0
          and parse_log(lines[-1])["summary"]["acked_count"] == 50)
    print("f6_injector self-test", "ok" if ok else "FAILED", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":  # pragma: no cover - the image entry point
    sys.exit(main())
