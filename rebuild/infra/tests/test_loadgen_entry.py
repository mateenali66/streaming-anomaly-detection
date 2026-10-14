#!/usr/bin/env python3
"""Offline test of images/load-generator/loadgen_entry.py against the REAL generator.py and config.py (copied read-only
into a temp dir) with a fake confluent_kafka. Checks: seed is required, the metrics endpoint exposes target rate, actual rate,
start time, epoch and seed, produced_total grows, the IoT burst flag follows the epoch, /status reports ACKNOWLEDGED deliveries
(a fake broker that fails every tenth record: acked + failures = produced, and the generator's own callback still counts the
failures), and a clean shutdown on SIGTERM. Needs prometheus_client (skipped with exit 0 and a message if missing)."""
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

try:
    import prometheus_client  # noqa: F401
except ImportError:
    print("SKIP: prometheus_client not installed")
    sys.exit(0)

ROOT = Path(__file__).resolve().parent.parent
APP = ROOT.parent.parent / "streaming-app" / "load-generator"
assert (APP / "generator.py").exists(), APP

FAKE = '''
class KafkaError: pass
class KafkaException(Exception): pass
class _Meta:
    brokers = {1: 1}
    topics = {"raw-events": 1}
import os as _os
class Producer:
    def __init__(self, conf): self.conf = conf; self.n = 0
    def list_topics(self, timeout=0): return _Meta()
    def produce(self, topic, key=None, value=None, headers=None, callback=None):
        self.n += 1
        every = int(_os.environ.get("FAKE_FAIL_EVERY", "0"))
        if callback: callback(Exception("fake broker refused") if every and self.n % every == 0 else None, None)
    def poll(self, t=0): return 0
    def flush(self, timeout=0): return 0
'''


def run_case(workload: str, extra_env: dict, check):
    d = Path(tempfile.mkdtemp(prefix="lg-test-"))
    try:
        for f in ("generator.py", "config.py"):
            shutil.copy(APP / f, d / f)
        shutil.copy(ROOT / "images/load-generator/loadgen_entry.py", d / "loadgen_entry.py")
        (d / "confluent_kafka.py").write_text(FAKE)
        env = dict(os.environ, WORKLOAD=workload, KAFKA_BOOTSTRAP_SERVERS="x:9092", PYTHONUNBUFFERED="1",
                   LOADGEN_METRICS_PORT="19102", PYTHONPATH=str(d), PYTHONDONTWRITEBYTECODE="1", **extra_env)
        # 1. seed is required
        bad = subprocess.run([sys.executable, str(d / "loadgen_entry.py")], env={k: v for k, v in env.items() if k != "LOADGEN_SEED"},
                             capture_output=True, text=True, cwd=d)
        assert bad.returncode != 0 and "LOADGEN_SEED is required" in (bad.stderr + bad.stdout), bad
        # 2. run, scrape twice, SIGTERM
        p = subprocess.Popen([sys.executable, str(d / "loadgen_entry.py")], env=env, cwd=d, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        try:
            text1 = text2 = ""
            for _ in range(60):
                time.sleep(0.5)
                try:
                    text1 = urllib.request.urlopen("http://127.0.0.1:19102/metrics", timeout=2).read().decode()
                    if "loadgen_target_rate_per_second" in text1 and re.search(r"loadgen_produced_total [1-9]", text1):
                        break
                except Exception:
                    pass
            time.sleep(6)  # one 5 s rate window
            text2 = urllib.request.urlopen("http://127.0.0.1:19102/metrics", timeout=2).read().decode()
            status = json.loads(urllib.request.urlopen("http://127.0.0.1:19102/status", timeout=2).read().decode())
            check(text1, text2)
            check_status(status, text2, extra_env)
        finally:
            p.send_signal(signal.SIGTERM)
            try:
                out, _ = p.communicate(timeout=60)
            except subprocess.TimeoutExpired:
                p.kill()
                out, _ = p.communicate()
        assert p.returncode == 0, (p.returncode, out[-2000:])
        assert "Shutdown Complete" in out, out[-2000:]
    finally:
        shutil.rmtree(d, ignore_errors=True)


def val(text, name):
    m = re.search(rf"^{name}(?:{{[^}}]*}})? ([0-9.eE+-]+)$", text, re.M)
    assert m, f"{name} missing from metrics"
    return float(m.group(1))


def check_status(st, text, extra_env):
    for k in ("acked_count", "produced_count", "delivery_failures_count", "produce_errors_count", "commanded_rate_per_s",
              "cycle_phase_frac", "epoch_at", "started_at", "seed", "workload", "profile", "at"):
        assert k in st, f"/status lacks {k}"
    assert st["seed"] == int(extra_env["LOADGEN_SEED"]) and st["epoch_at"] == float(extra_env["LOADGEN_EPOCH_UNIX"])
    assert 0 <= st["cycle_phase_frac"] < 1 and st["commanded_rate_per_s"] > 0
    assert st["acked_count"] > 0 and st["acked_count"] <= st["produced_count"]
    if extra_env.get("FAKE_FAIL_EVERY"):
        # the counters are read while the generator keeps producing, so allow a handful in flight between the reads
        assert st["delivery_failures_count"] > 0, st
        assert abs(st["produced_count"] - st["acked_count"] - st["delivery_failures_count"]) <= 50, st
        assert abs(st["acked_count"] / st["produced_count"] - 0.9) < 0.02, st
    else:
        assert abs(st["produced_count"] - st["acked_count"]) <= 50, st
    assert val(text, "loadgen_acked_total") > 0


def check_ecom(t1, t2):
    for n in ("loadgen_target_rate_per_second", "loadgen_actual_rate_per_second", "loadgen_start_time_seconds",
              "loadgen_epoch_seconds", "loadgen_cycle_phase_seconds", "loadgen_seed", "loadgen_produced_total"):
        val(t2, n)
    assert val(t2, "loadgen_seed") == 4242
    assert val(t2, "loadgen_epoch_seconds") == 1_700_000_000.0
    assert 800 <= val(t2, "loadgen_target_rate_per_second") <= 1200, "LOW profile is 1000 +/- 20%"
    # wiring check, not a timing benchmark: macOS sleeps overshoot, so the generator itself lands 10 to 30% under target here
    assert 400 <= val(t2, "loadgen_actual_rate_per_second") <= 1400, val(t2, "loadgen_actual_rate_per_second")
    assert val(t2, "loadgen_produced_total") > val(t1, "loadgen_produced_total")
    assert 'loadgen_info{profile="LOW",seed="4242",workload="ECOMMERCE"} 1.0' in t2
    # epoch 1.7e9 is far in the past: the phase must equal (now - epoch) mod 1800, not "seconds since start"
    expect = (time.time() - 1_700_000_000.0) % 1800
    assert abs(val(t2, "loadgen_cycle_phase_seconds") - expect) < 15 or abs(abs(val(t2, "loadgen_cycle_phase_seconds") - expect) - 1800) < 15


def check_iot(t1, t2):
    assert val(t2, "loadgen_seed") == 7
    burst = val(t2, "loadgen_burst_active")
    phase = val(t2, "loadgen_cycle_phase_seconds")
    # burst flag is 1 exactly when (now - epoch) mod 300 < 30
    since = (time.time() - 1_700_000_000.0)
    assert burst in (0.0, 1.0)
    if abs((since % 300) - 30) > 3 and (since % 300) > 3 and (since % 300) < 297:
        assert burst == (1.0 if (since % 300) < 30 else 0.0), (burst, since % 300, phase)
    assert val(t2, "loadgen_target_rate_per_second") >= 7000


if __name__ == "__main__":
    run_case("ECOMMERCE", {"THROUGHPUT_PROFILE": "LOW", "LOADGEN_SEED": "4242", "LOADGEN_EPOCH_UNIX": "1700000000",
                           "FAKE_FAIL_EVERY": "10"}, check_ecom)
    print("ok   ecommerce")
    run_case("IOT", {"THROUGHPUT_PROFILE": "IOT_MEDIUM", "LOADGEN_SEED": "7", "LOADGEN_EPOCH_UNIX": "1700000000", "NUM_DEVICES": "2000", "NUM_LOCATIONS": "20"}, check_iot)
    print("ok   iot")
