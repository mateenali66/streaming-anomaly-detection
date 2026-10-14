#!/usr/bin/env python3
"""Entry point of the load-generator image: seed, acknowledged-delivery counts, /status and /metrics, restart-safe load phase.

It wraps streaming-app/load-generator/generator.py WITHOUT modifying it (that tree is not part of this build). The wrapper
imports the generator, replaces four things on the module object, and calls generator.run():

  generator.get_target_rate     computes the target from (now - epoch) instead of the process's own clock, so a restarted pod
                                continues the same 1800 s load cycle, and records the target for Prometheus
  generator.Producer            a thin proxy around confluent_kafka.Producer that wraps every delivery callback, so records
                                the brokers ACKNOWLEDGED are counted (loadgen_acked_total); the generator's own callback
                                still runs
  generator.Metrics.record_*    also bump Prometheus counters
  random.seed(LOADGEN_SEED)     the per-event randomness follows the plan's load_generator_seed. The pools (users, products,
                                devices, locations) are built inside generator.py from a fixed Random(42), so they are
                                identical in every campaign by design.

Environment (all read here; the generator reads the rest through its config.py):
  LOADGEN_SEED          required, integer: the plan's load_generator_seed (scripts/deploy_workload.sh reads it from the plan)
  LOADGEN_EPOCH_UNIX    optional, unix seconds the load cycle is measured from. Default: this process's start time.
  LOADGEN_METRICS_PORT  default 9102

HTTP on LOADGEN_METRICS_PORT (protocol 2.3.9 and 11.11 as recorded in rebuild/CONTRACT_CHANGES.md, item C7):
  /metrics   Prometheus text, see k8s/base/loadgen/load-generator.yaml and NAMES.md section 6
  /status    JSON: acked_count, produced_count, delivery_failures_count, produce_errors_count, commanded_rate_per_s,
             cycle_phase_frac, epoch_at, started_at, seed, workload, profile, at
Requires prometheus_client and confluent_kafka (images/load-generator/requirements.lock).
"""
from __future__ import annotations

import json
import os
import random
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def _require_int(name: str) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        raise SystemExit(f"{name} is required (no default): the load generator must run with a recorded seed")
    try:
        return int(raw)
    except ValueError:
        raise SystemExit(f"{name} must be an integer, got {raw!r}") from None


def main() -> int:
    seed = _require_int("LOADGEN_SEED")
    started = time.time()
    epoch_raw = os.environ.get("LOADGEN_EPOCH_UNIX", "").strip()
    epoch = float(epoch_raw) if epoch_raw else started
    port = int(os.environ.get("LOADGEN_METRICS_PORT", "9102"))

    from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest  # noqa: PLC0415

    import config  # noqa: PLC0415  (streaming-app/load-generator/config.py, next to this file in the image)
    import generator  # noqa: PLC0415

    g_target = Gauge("loadgen_target_rate_per_second", "Target produce rate right now")
    g_actual = Gauge("loadgen_actual_rate_per_second", "Events handed to the producer per second, last 5 s")
    c_prod = Counter("loadgen_produced_total", "Events handed to the producer")
    c_err = Counter("loadgen_produce_errors_total", "Produce calls that raised")
    c_fail = Counter("loadgen_delivery_failures_total", "Delivery failures reported by Kafka")
    c_acked = Counter("loadgen_acked_total", "Records the brokers acknowledged (delivery callback without error)")
    g_start = Gauge("loadgen_start_time_seconds", "Unix time this process started")
    g_epoch = Gauge("loadgen_epoch_seconds", "Unix time the load cycle is measured from")
    g_phase = Gauge("loadgen_cycle_phase_seconds", "(now - epoch) modulo the sinusoid period")
    g_burst = Gauge("loadgen_burst_active", "1 inside an IoT burst, else 0")
    g_seed = Gauge("loadgen_seed", "Seed of the per-event random stream")
    g_info = Gauge("loadgen_info", "Static facts", ["workload", "profile", "seed"])

    g_start.set(started)
    g_epoch.set(epoch)
    g_seed.set(seed)
    g_info.labels(config.WORKLOAD, config.THROUGHPUT_PROFILE, str(seed)).set(1)

    original_rate = generator.get_target_rate

    def get_target_rate(base_rate: int, elapsed_sec: float) -> float:
        # Ignore the generator's own elapsed time. The cycle is anchored at the epoch, so it survives a restart.
        since_epoch = time.time() - epoch
        rate = original_rate(base_rate, since_epoch)
        g_target.set(rate)
        state["target"] = rate
        g_phase.set(since_epoch % config.SINUSOIDAL_PERIOD_SEC)
        if config.WORKLOAD == "IOT":
            g_burst.set(1 if (since_epoch % config.BURST_PERIOD_SEC) < config.BURST_DURATION_SEC else 0)
        return rate

    generator.get_target_rate = get_target_rate

    state = {"target": 0.0, "acked": 0}
    lock = threading.Lock()
    real_producer = generator.Producer

    class CountingProducer:
        """Delegates everything to the real producer; only the delivery callback is wrapped."""

        def __init__(self, conf):
            self._p = real_producer(conf)

        def produce(self, *args, **kwargs):
            key = "callback" if "callback" in kwargs else "on_delivery" if "on_delivery" in kwargs else "callback"
            inner = kwargs.get(key)

            def on_delivery(err, msg):
                if err is None:
                    with lock:
                        state["acked"] += 1
                    c_acked.inc()
                if inner is not None:
                    inner(err, msg)
            kwargs[key] = on_delivery
            return self._p.produce(*args, **kwargs)

        def __getattr__(self, name):
            return getattr(self._p, name)

    generator.Producer = CountingProducer

    orig_sent, orig_err, orig_fail = generator.Metrics.record_sent, generator.Metrics.record_error, generator.Metrics.record_delivery_failure

    def record_sent(self, event_type):
        orig_sent(self, event_type)
        c_prod.inc()

    def record_error(self):
        orig_err(self)
        c_err.inc()

    def record_delivery_failure(self):
        orig_fail(self)
        c_fail.inc()

    generator.Metrics.record_sent = record_sent
    generator.Metrics.record_error = record_error
    generator.Metrics.record_delivery_failure = record_delivery_failure

    def rate_loop() -> None:
        last_t, last_n = time.time(), c_prod._value.get()  # noqa: SLF001
        while True:
            time.sleep(5)
            now, n = time.time(), c_prod._value.get()  # noqa: SLF001
            g_actual.set((n - last_n) / max(now - last_t, 1e-9))
            last_t, last_n = now, n

    def status() -> dict:
        now = time.time()
        with lock:
            acked = state["acked"]
        return {"acked_count": acked, "produced_count": int(c_prod._value.get()),  # noqa: SLF001
                "delivery_failures_count": int(c_fail._value.get()), "produce_errors_count": int(c_err._value.get()),  # noqa: SLF001
                "commanded_rate_per_s": state["target"],
                "cycle_phase_frac": ((now - epoch) % config.SINUSOIDAL_PERIOD_SEC) / config.SINUSOIDAL_PERIOD_SEC,
                "epoch_at": epoch, "started_at": started, "seed": seed, "workload": config.WORKLOAD,
                "profile": config.THROUGHPUT_PROFILE, "at": now}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - http.server API
            if self.path.split("?", 1)[0] == "/status":
                body, ctype, code = json.dumps(status(), sort_keys=True).encode(), "application/json", 200
            elif self.path.split("?", 1)[0] == "/metrics":
                body, ctype, code = generate_latest(), CONTENT_TYPE_LATEST, 200
            else:
                body, ctype, code = b"{}", "application/json", 404
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            return

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    threading.Thread(target=rate_loop, daemon=True).start()

    random.seed(seed)
    generator.log.info("loadgen_entry: seed=%d epoch=%.0f (started %.0f) metrics on :%d", seed, epoch, started, port)
    generator.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
