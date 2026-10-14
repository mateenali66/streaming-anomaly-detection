"""Verification consumer (protocol 2.3 item 8), Deployment verification-consumer, consumer group rebuild-verification.

Reads raw-events and dead-letter in its own consumer group and counts malformed records, in total and per value of the
record header rebuild-attempt that the F6 injector sets on every record (11.11). The gate attributes malformed records to
an attempt by that header (gate.malformed_delta, CONTRACT_CHANGES C6). The orchestrator's sentinel record (header
rebuild-sentinel) is ignored. /status (JSON) and /metrics (Prometheus text) are served on METRICS_PORT, 9103 (C7).

A record on raw-events is malformed when its value is not a UTF-8 JSON object. The F6 payload is a JSON fragment
without its closing brace, so it is malformed by construction. Normal load-generator events are JSON objects.

/status fields, which the probe service stores as the consumer probe's detail (11.8): started_at, last_poll_at,
raw_events_count, malformed_count, malformed_by_attempt (attempt key -> cumulative count), dead_letter_count,
dead_letter_by_attempt.

confluent-kafka (context7 /confluentinc/confluent-kafka-python): Consumer(conf).subscribe([...]); poll/consume
return Message objects with value(), topic(), headers() (a list of (key, bytes) tuples or None) and error().

The counts live in memory. started_at changes on every restart, and the gate reads counts only from consecutive
samples with the same started_at, so a restart shows up as missing data, never as a lower count.
"""
from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import ids


def classify(topic: str, value: bytes | None, headers: list | None) -> tuple[str | None, bool, bool]:
    """Returns (attempt_key, is_sentinel, malformed)."""
    hdr = {k: v for k, v in (headers or [])}
    if ids.SENTINEL_HEADER in hdr:
        return None, True, False
    akey = hdr.get(ids.F6_HEADER)
    akey = akey.decode("utf-8", "replace") if isinstance(akey, (bytes, bytearray)) else akey
    malformed = False
    if topic == "raw-events":
        try:
            obj = json.loads((value or b"").decode("utf-8"))
            malformed = not isinstance(obj, dict)
        except (UnicodeDecodeError, json.JSONDecodeError):
            malformed = True
    return akey, False, malformed


class Counters:
    def __init__(self, now=time.time) -> None:
        self.lock = threading.Lock()
        self.started_at = now()
        self.raw_events_count = 0
        self.malformed_count = 0
        self.malformed_by_attempt: dict[str, int] = {}
        self.dead_letter_count = 0
        self.dead_letter_by_attempt: dict[str, int] = {}
        self.last_poll_at: float | None = None

    def add(self, topic: str, value: bytes | None, headers: list | None) -> None:
        akey, sentinel, malformed = classify(topic, value, headers)
        if sentinel:
            return
        with self.lock:
            if topic == "raw-events":
                self.raw_events_count += 1
                if malformed:
                    self.malformed_count += 1
                    if akey:
                        self.malformed_by_attempt[akey] = self.malformed_by_attempt.get(akey, 0) + 1
            elif topic == "dead-letter":
                self.dead_letter_count += 1
                if akey:
                    self.dead_letter_by_attempt[akey] = self.dead_letter_by_attempt.get(akey, 0) + 1

    def status(self) -> dict:
        with self.lock:
            return {"started_at": self.started_at, "last_poll_at": self.last_poll_at,
                    "raw_events_count": self.raw_events_count, "malformed_count": self.malformed_count,
                    "malformed_by_attempt": dict(self.malformed_by_attempt),
                    "dead_letter_count": self.dead_letter_count,
                    "dead_letter_by_attempt": dict(self.dead_letter_by_attempt)}


def metrics_text(st: dict) -> str:
    """The metric names rebuild/infra/NAMES.md section 6 fixes for this service (job verification-consumer)."""
    return (f"# TYPE verify_consumer_records_total counter\nverify_consumer_records_total {st['raw_events_count']}\n"
            f"# TYPE verify_consumer_malformed_total counter\nverify_consumer_malformed_total {st['malformed_count']}\n"
            f"# TYPE verify_consumer_dead_letter_total counter\nverify_consumer_dead_letter_total {st['dead_letter_count']}\n"
            f"# TYPE verify_consumer_started_at_seconds gauge\nverify_consumer_started_at_seconds {st['started_at']}\n")


def serve() -> None:  # pragma: no cover - needs Kafka
    """Environment of rebuild/infra/k8s/base/loadgen/verification-consumer.yaml: KAFKA_BOOTSTRAP, KAFKA_GROUP_ID
    (rebuild-verification), METRICS_PORT (9103). /status and /metrics are both served on METRICS_PORT. The consumer
    always subscribes raw-events and dead-letter (2.3.8)."""
    import os

    from confluent_kafka import Consumer

    port = int(os.environ.get("METRICS_PORT", str(ids.CONSUMER_STATUS_PORT)))
    group = os.environ.get("KAFKA_GROUP_ID", ids.CONSUMER_GROUP)
    if group != ids.CONSUMER_GROUP:
        raise SystemExit(f"KAFKA_GROUP_ID must be {ids.CONSUMER_GROUP} (11.11), got {group!r}")
    counters = Counters()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            st = counters.status()
            if self.path.startswith("/status"):
                body, ctype, code = json.dumps(st).encode(), "application/json", 200
            elif self.path.startswith("/metrics"):
                body, ctype, code = metrics_text(st).encode(), "text/plain; version=0.0.4", 200
            else:
                body, ctype, code = b"{}", "application/json", 404
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            return

    srv = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    c = Consumer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", ids.KAFKA_BOOTSTRAP), "group.id": group,
                  "auto.offset.reset": "latest", "enable.auto.commit": True, "client.id": "verification-consumer"})
    c.subscribe(["dead-letter", "raw-events"])  # protocol 2.3.8
    while True:
        for msg in c.consume(num_messages=1000, timeout=1.0):
            if msg.error():
                continue  # partition EOF and transient errors; a stall shows up as a flat count
            counters.add(msg.topic(), msg.value(), msg.headers())
        counters.last_poll_at = time.time()
