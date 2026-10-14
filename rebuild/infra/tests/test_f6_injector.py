#!/usr/bin/env python3
"""Offline test of the F6 injector that images/f6-injector/Dockerfile builds into the image: the ONE implementation,
rebuild/orchestrator/orchestrator/f6_injector.py (rebuild/CONTRACT_CHANGES.md C6), run as a script exactly as the image's
ENTRYPOINT runs it, against a fake confluent_kafka that acknowledges each record 0.3 s after it is produced.

Checks: planned count met exactly; every record carries the header rebuild-attempt=<ATTEMPT_KEY>; no payload contains a
closing brace; delivered and first_epoch/last_epoch are ACKNOWLEDGEMENTS (0.3 s after the sends), not send times; same seed
same payloads, other seed other payloads; a full local queue (BufferError) loses nothing; 500 per second delivers 100%;
delivery failures exit 1; a late start exits 3 with nothing sent; it waits for START_AT_UNIX; missing or bad configuration and
a topic missing from the metadata exit 2; the image's build-time --self-test passes."""
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
INJECTOR = ROOT.parent / "orchestrator" / "orchestrator" / "f6_injector.py"
assert INJECTOR.is_file(), INJECTOR
DOCKERFILE = (ROOT / "images" / "f6-injector" / "Dockerfile").read_text()
FAKE = '''
import json, os, time
RECORDS = os.environ.get("FAKE_RECORDS")
class _Topic:
    error = None
class _Meta:
    def __init__(self): self.topics = {} if os.environ.get("FAKE_NO_TOPIC") else {"raw-events": _Topic()}
class Producer:
    def __init__(self, conf):
        assert conf["acks"] == "all" and conf["enable.idempotence"] is True, conf
        self.pending = []; self.n = 0; self.full_once = bool(os.environ.get("FAKE_BUFFER_FULL_ONCE"))
        self.out = open(RECORDS, "a") if RECORDS else None
    def list_topics(self, topic=None, timeout=0): return _Meta()
    def produce(self, topic, value=None, key=None, headers=None, callback=None):
        if "}" in value.decode(): raise SystemExit("payload with a closing brace")
        if self.full_once and self.n == 5:
            self.full_once = False
            raise BufferError("Local: Queue full")
        self.n += 1
        if self.out:
            self.out.write(json.dumps({"topic": topic, "headers": [[k, v.decode()] for k, v in (headers or [])],
                                       "sent_at": time.time()}) + "\\n")
        self.pending.append((time.time() + 0.3, callback))
    def poll(self, t=0):
        now = time.time(); due = [p for p in self.pending if p[0] <= now]
        self.pending = [p for p in self.pending if p[0] > now]
        for _, cb in due: cb(Exception("boom") if os.environ.get("FAKE_FAIL") else None, None)
        return len(due)
    def flush(self, t=0):
        while self.pending:
            time.sleep(max(0.0, min(d for d, _ in self.pending) - time.time())); self.poll(0)
        if self.out: self.out.close()
        return 0
'''
d = Path(tempfile.mkdtemp())
(d / "confluent_kafka.py").write_text(FAKE)
AKEY = "iot-c1-b02-p3-a1"


def run(env_extra, args=()):
    env = dict(os.environ, PYTHONPATH=str(d), PYTHONDONTWRITEBYTECODE="1", KAFKA_BOOTSTRAP="x:9092", INJECT_RATE="50",
               INJECT_DURATION="2", INJECT_SEED="5", START_AT_UNIX=f"{time.time() + 0.5:.3f}", ATTEMPT_KEY=AKEY)
    for k, v in env_extra.items():
        if v is None:
            env.pop(k, None)
        else:
            env[k] = v
    return subprocess.run([sys.executable, str(INJECTOR), *args], env=env, capture_output=True, text=True, cwd=str(d))


def kv(out):
    line = next(ln for ln in out.splitlines() if ln.startswith("INJECTED "))
    return dict(t.split("=") for t in line.split()[1:])


def summary(out):
    return next(json.loads(ln) for ln in out.splitlines() if ln.startswith("{") and '"summary"' in ln)


bad = 0


def check(name, cond, extra=""):
    global bad
    print(("ok   " if cond else "FAIL ") + name, extra if not cond else "")
    bad += 0 if cond else 1


check("Dockerfile copies the orchestrator's injector", "COPY --from=orchestrator_src orchestrator/f6_injector.py" in DOCKERFILE)
check("no second injector in the infra tree", not (ROOT / "images" / "f6-injector" / "injector.py").exists())
records = d / "records.jsonl"
r1 = run({"FAKE_RECORDS": str(records)}); r2 = run({}); r3 = run({"INJECT_SEED": "6"})
k1, k2, k3 = kv(r1.stdout), kv(r2.stdout), kv(r3.stdout)
check("exit 0", r1.returncode == 0, r1.stdout + r1.stderr)
check("planned met exactly", k1["sent"] == "100" and k1["delivered"] == "100" and k1["failed"] == "0", k1)
sent = [json.loads(ln) for ln in records.read_text().splitlines()]
check("every record carries rebuild-attempt=<ATTEMPT_KEY>", len(sent) == 100 and all(
    s["headers"] == [["rebuild-attempt", AKEY]] and s["topic"] == "raw-events" for s in sent), sent[:2])
s1 = summary(r1.stdout)
check("first_epoch and last_epoch are acknowledgements, not sends",
      abs(float(k1["first_epoch"]) - s1["first_produce_at"]) < 1e-3 and 0.25 < s1["first_produce_at"] - s1["first_send_at"] < 1.0
      and 0.25 < s1["last_produce_at"] - s1["last_send_at"] < 1.0, s1)
check("same seed same payloads", k1["payload_sha256"] == k2["payload_sha256"])
check("other seed other payloads", k1["payload_sha256"] != k3["payload_sha256"])
t0, t1 = float(k1["first_epoch"]), float(k1["last_epoch"])
check("window about 2 s", 1.5 < t1 - t0 < 3.0, (t0, t1))
r4 = run({"INJECT_RATE": "500", "INJECT_DURATION": "1"}); k4 = kv(r4.stdout)
check("500 per second delivers 100%", k4["delivered"] == "500" and r4.returncode == 0, k4)
r5 = run({"FAKE_FAIL": "1"})
check("delivery failures exit 1", r5.returncode == 1 and "shortfall" in r5.stdout, r5.stdout + r5.stderr)
r6 = run({"START_AT_UNIX": str(time.time() - 60)})
check("late start exit 3, nothing sent", r6.returncode == 3 and "INJECTED" not in r6.stdout, r6.stdout + r6.stderr)
t = time.time(); r7 = run({"START_AT_UNIX": str(time.time() + 2), "INJECT_DURATION": "1"})
check("waits for START_AT_UNIX", r7.returncode == 0 and time.time() - t >= 2.5, r7.stdout + r7.stderr)
r8 = run({"FAKE_BUFFER_FULL_ONCE": "1"}); k8 = kv(r8.stdout)
check("a full local queue loses nothing", r8.returncode == 0 and k8["sent"] == "100" and k8["delivered"] == "100", k8)
for name, change in (("missing seed", {"INJECT_SEED": ""}), ("missing START_AT_UNIX", {"START_AT_UNIX": None}),
                     ("missing ATTEMPT_KEY", {"ATTEMPT_KEY": None}), ("bad ATTEMPT_KEY", {"ATTEMPT_KEY": "iot-b"}),
                     ("topic not in metadata", {"FAKE_NO_TOPIC": "1"})):
    r = run(change)
    check(f"{name} exit 2", r.returncode == 2 and "INJECTED" not in r.stdout, r.stdout + r.stderr)
check("no payload contains a brace (fake producer exits on one)", "closing brace" not in r1.stderr + r1.stdout)
rs = subprocess.run([sys.executable, str(INJECTOR), "--self-test"], capture_output=True, text=True, cwd=str(d),
                    env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"))
check("image build self-test passes", rs.returncode == 0 and "self-test ok" in rs.stdout, rs.stdout + rs.stderr)
sys.exit(1 if bad else 0)
