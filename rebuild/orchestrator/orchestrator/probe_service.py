"""Verification-channel probe service (protocol 2.3 items 4 to 9), Deployment verification-probes.

Runs from the start of warm-up to the end of the trailing baseline on the same 10 s schedule in every segment
and run type, so its traffic is part of every NORMAL window and cannot mark an injection (2.3, last paragraph).

  tcp        every 10 s, from inside each broker pod, one connect to each other broker's 9090 and 9091 with a 0.5 s
             timeout; from the load generator pod, one connect to each broker's 9092.
  tcp_burst  every 10 s, from inside each TaskManager's network namespace, ten connects to the JobManager's 6123
             with a 0.5 s timeout each. Loss fraction = failed connects / 10.
  controller every 60 s, kafka-metadata-quorum.sh describe --status in a broker; value = LeaderId
             (Kafka docs, "kafka-metadata-quorum.sh ... describe --status", context7 /apache/kafka).
  flink_jobs every 10 s, Flink REST job names, states and checkpoint facts (detail).
  flink_tms  every 10 s, Flink REST /overview; value = registered TaskManagers, detail = slots.
  loadgen    every 10 s, the load generator's /status JSON (LOADGEN_STATUS_URL, port 9102, CONTRACT_CHANGES C7);
             value = cumulative count of records the brokers acknowledged, detail = the whole status.
  consumer   every 10 s, the verification consumer's /status on port 9103; value = cumulative malformed count, detail =
             the whole status, including malformed_by_attempt, which the F6 gate reads (C6).

Connects run inside the source pod through `kubectl exec`, which reaches the container through the kubelet and the
container runtime, not through the pod network, so a NetworkChaos fault on the pod does not stop the probe from
running; it only makes the connect inside the pod fail, which is the measurement.

Samples are kept in memory, appended to a JSON-lines file on the pod's emptyDir, served at /samples, and exported
as Prometheus gauges at /metrics for the PodMonitor job `rebuild-verification`, which the feature export never reads.
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from . import ids
from .canonical import canonical_str
from .errors import QueryError
from .flinkrest import FlinkRest
from .httpjson import HttpJson
from .kube import Kube, KubectlKube, pod_ip, pod_ready
from .probefile import row

TCP_TIMEOUT_S = 0.5
BURST_TRIES = 10

# One connect per argument "<ip>|<port>|<label>". `timeout 0.5` bounds each connect; bash's /dev/tcp opens the socket.
TCP_SCRIPT = r'''
for a in "$@"; do
  ip="${a%%|*}"; rest="${a#*|}"; port="${rest%%|*}"; label="${rest#*|}"
  s=$EPOCHREALTIME
  if timeout 0.5 bash -c "exec 3<>/dev/tcp/$ip/$port" 2>/dev/null; then r=OK; else r=FAIL; fi
  e=$EPOCHREALTIME
  echo "$r $label $s $e"
done
'''
BURST_SCRIPT = r'''
ip="$1"; port="$2"
for i in 1 2 3 4 5 6 7 8 9 10; do
  s=$EPOCHREALTIME
  if timeout 0.5 bash -c "exec 3<>/dev/tcp/$ip/$port" 2>/dev/null; then r=OK; else r=FAIL; fi
  e=$EPOCHREALTIME
  echo "$r $s $e"
done
'''
QUORUM_CMD = ["/opt/kafka/bin/kafka-metadata-quorum.sh", "--bootstrap-server", "localhost:9092", "describe", "--status"]
LEADER_RE = re.compile(r"^LeaderId:\s+(-?\d+)\s*$", re.M)
PROM_LINE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([-+0-9.eEnaNIf]+)(?:\s+\d+)?$")


def parse_prom_text(text: str) -> dict[str, float]:
    """Unlabelled samples of a Prometheus text exposition (the load generator's /metrics)."""
    out: dict[str, float] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = PROM_LINE.match(line)
        if m and not m.group(2):
            out[m.group(1)] = float(m.group(3))
    return out


LOADGEN_STATUS_FIELDS = ("acked_count", "produced_count", "delivery_failures_count", "produce_errors_count",
                         "commanded_rate_per_s", "cycle_phase_frac", "epoch_at", "started_at", "seed", "workload",
                         "profile", "at")


def loadgen_status(status: dict) -> dict:
    """Check the load generator's /status JSON (rebuild/infra/images/load-generator/loadgen_entry.py, 2.3.9). acked_count
    counts the records the brokers acknowledged (delivery callback without error), so the rate match of 1.6.6 compares the
    broker's messages-in rate with acknowledged deliveries, not with records merely handed to the producer."""
    if not isinstance(status, dict):
        raise QueryError("load generator /status is not a JSON object")
    missing = [k for k in LOADGEN_STATUS_FIELDS if k not in status]
    if missing:
        raise QueryError(f"load generator /status lacks {missing}")
    for k in ("acked_count", "produced_count", "commanded_rate_per_s", "cycle_phase_frac", "seed"):
        if isinstance(status[k], bool) or not isinstance(status[k], (int, float)):
            raise QueryError(f"load generator /status {k} is not a number")
    return {k: status[k] for k in LOADGEN_STATUS_FIELDS}


def parse_leader_id(text: str) -> int:
    m = LEADER_RE.search(text)
    if not m:
        raise QueryError("no LeaderId line in kafka-metadata-quorum.sh output")
    return int(m.group(1))


def _f(s: str, fallback: float) -> float:
    try:
        return float(s.replace(",", "."))
    except ValueError:
        return fallback


class ProbeRunner:
    def __init__(self, kube: Kube, http: HttpJson, flink: FlinkRest, now=time.time,
                 loadgen_status_url: str = ids.LOADGEN_STATUS_URL) -> None:
        self.kube, self.http, self.flink, self.now = kube, http, flink, now
        self.loadgen_status_url = loadgen_status_url
        self.tick_count = 0

    def loadgen(self) -> dict:
        t0 = self.now()
        dst = self.loadgen_status_url.split("//", 1)[-1].split("/", 1)[0]
        try:
            status = loadgen_status(self.http.get(self.loadgen_status_url, timeout_s=3.0))
            return row(t0, "loadgen", dst=dst, tried=1, ok=1, value=float(status["acked_count"]), detail=canonical_str(status))
        except Exception as e:
            return row(t0, "loadgen", dst=dst, tried=1, ok=0, error=f"{type(e).__name__}: {e}"[:300])

    # ---------------------------------------------------------------------------------- individual probes
    def tcp_from(self, src: str, container: str, targets: list[tuple[str | None, int, str]]) -> list[dict]:
        t0 = self.now()
        args, rows = [], []
        for ip, port, label in targets:
            if ip is None:
                rows.append(row(t0, "tcp", src_pod=src, dst=label, tried=1, ok=0, timeout_s=TCP_TIMEOUT_S,
                                error="NO_POD_IP"))
            else:
                args.append(f"{ip}|{port}|{label}")
        if not args:
            return rows
        try:
            res = self.kube.exec(src, container, ["bash", "-c", TCP_SCRIPT, "probe"] + args, timeout_s=8.0)
            lines = [ln.split() for ln in res.stdout.splitlines() if ln.strip()]
            parsed = {ln[1]: ln for ln in lines if len(ln) == 4}
            for a in args:
                label = a.split("|", 2)[2]
                ln = parsed.get(label)
                if res.rc != 0 or ln is None:
                    rows.append(row(t0, "tcp", src_pod=src, dst=label, tried=1, ok=0, timeout_s=TCP_TIMEOUT_S,
                                    error=f"EXEC rc={res.rc} {res.stderr.strip()[:200]}"))
                    continue
                s, e = _f(ln[2], t0), _f(ln[3], t0)
                ok = ln[0] == "OK"
                rows.append(row(s, "tcp", src_pod=src, dst=label, tried=1, ok=1 if ok else 0, timeout_s=TCP_TIMEOUT_S,
                                latency_s=(e - s) if ok else None, error=None if ok else "CONNECT_FAILED"))
        except Exception as e:  # recorded, never swallowed: the failed probe is the record
            for a in args:
                rows.append(row(t0, "tcp", src_pod=src, dst=a.split("|", 2)[2], tried=1, ok=0,
                                timeout_s=TCP_TIMEOUT_S, error=f"EXEC_ERROR {type(e).__name__}: {e}"[:300]))
        return rows

    def burst_from(self, tm: str, jm_ip: str | None, jm_name: str) -> dict:
        t0 = self.now()
        dst = f"{jm_name}:{ids.FLINK_JM_RPC_PORT}"
        if jm_ip is None:
            return row(t0, "tcp_burst", src_pod=tm, dst=dst, tried=BURST_TRIES, ok=0, timeout_s=TCP_TIMEOUT_S,
                       error="NO_JOBMANAGER_IP")
        try:
            res = self.kube.exec(tm, ids.FLINK_CONTAINER, ["bash", "-c", BURST_SCRIPT, "burst", jm_ip,
                                                           str(ids.FLINK_JM_RPC_PORT)], timeout_s=12.0)
            lines = [ln.split() for ln in res.stdout.splitlines() if ln.strip()]
            if res.rc != 0 or len(lines) != BURST_TRIES:
                return row(t0, "tcp_burst", src_pod=tm, dst=dst, tried=BURST_TRIES, ok=0, timeout_s=TCP_TIMEOUT_S,
                           error=f"EXEC rc={res.rc} lines={len(lines)} {res.stderr.strip()[:200]}")
            oks = [(_f(ln[1], t0), _f(ln[2], t0)) for ln in lines if ln[0] == "OK"]
            start = _f(lines[0][1], t0)
            lat = sum(e - s for s, e in oks) / len(oks) if oks else None
            return row(start, "tcp_burst", src_pod=tm, dst=dst, tried=BURST_TRIES, ok=len(oks), timeout_s=TCP_TIMEOUT_S,
                       latency_s=lat, error=None if len(oks) == BURST_TRIES else f"{BURST_TRIES - len(oks)} connects failed")
        except Exception as e:
            return row(t0, "tcp_burst", src_pod=tm, dst=dst, tried=BURST_TRIES, ok=0, timeout_s=TCP_TIMEOUT_S,
                       error=f"EXEC_ERROR {type(e).__name__}: {e}"[:300])

    def controller(self, brokers: list[dict]) -> dict:
        t0 = self.now()
        errors = []
        for b in brokers:
            if not pod_ready(b):
                continue
            name = b["metadata"]["name"]
            try:
                res = self.kube.exec(name, ids.BROKER_CONTAINER, QUORUM_CMD, timeout_s=30.0)
                if res.rc != 0:
                    errors.append(f"{name}: rc={res.rc}")
                    continue
                leader = parse_leader_id(res.stdout)
                return row(t0, "controller", src_pod=name, dst=None, tried=1, ok=1, value=float(leader),
                           detail=canonical_str({"method": "metadata-quorum"}))
            except Exception as e:
                errors.append(f"{name}: {type(e).__name__}: {e}")
        return row(t0, "controller", src_pod=None, dst=None, tried=1, ok=0, error="; ".join(errors)[:300] or "NO_READY_BROKER")

    def http_status(self, probe: str, pods: list[dict], port: int, value_key: str, required: tuple[str, ...]) -> dict:
        t0 = self.now()
        ready = [p for p in pods if pod_ready(p) and pod_ip(p)]
        if not ready:
            return row(t0, probe, dst=None, tried=1, ok=0, error="NO_READY_POD")
        p = ready[0]
        dst = f"{p['metadata']['name']}:{port}"
        try:
            status = self.http.get(f"http://{pod_ip(p)}:{port}/status", timeout_s=3.0)
            missing = [k for k in required if k not in status]
            if missing:
                raise QueryError(f"status lacks {missing}")
            return row(t0, probe, dst=dst, tried=1, ok=1, value=float(status[value_key]), detail=canonical_str(status))
        except Exception as e:
            return row(t0, probe, dst=dst, tried=1, ok=0, error=f"{type(e).__name__}: {e}"[:300])

    def flink_jobs(self) -> dict:
        t0 = self.now()
        try:
            snap = self.flink.snapshot()
            running = sum(1 for j in snap if j["state"] == "RUNNING")
            return row(t0, "flink_jobs", dst="streaming-flink-rest:8081", tried=1, ok=1, value=float(running),
                       detail=canonical_str(snap))
        except Exception as e:
            return row(t0, "flink_jobs", dst="streaming-flink-rest:8081", tried=1, ok=0, error=f"{type(e).__name__}: {e}"[:300])

    def flink_tms(self) -> dict:
        t0 = self.now()
        try:
            ov = self.flink.overview()
            return row(t0, "flink_tms", dst="streaming-flink-rest:8081", tried=1, ok=1, value=float(ov["taskmanagers"]),
                       detail=canonical_str({"slots_total": ov["slots-total"], "slots_available": ov.get("slots-available")}))
        except Exception as e:
            return row(t0, "flink_tms", dst="streaming-flink-rest:8081", tried=1, ok=0, error=f"{type(e).__name__}: {e}"[:300])

    # ---------------------------------------------------------------------------------- one 10 s round
    def tick(self) -> list[dict]:
        self.tick_count += 1
        brokers = sorted(self.kube.list_pods(ids.BROKER_SELECTOR), key=lambda p: p["metadata"]["name"])
        tms = sorted(self.kube.list_pods(ids.TM_SELECTOR), key=lambda p: p["metadata"]["name"])
        jms = self.kube.list_pods(ids.JM_SELECTOR)
        loadgens = self.kube.list_pods(ids.LOADGEN_SELECTOR)
        consumers = self.kube.list_pods(ids.CONSUMER_SELECTOR)
        jm = next((p for p in jms if pod_ready(p)), jms[0] if jms else None)
        jm_ip = pod_ip(jm) if jm else None
        jm_name = jm["metadata"]["name"] if jm else "jobmanager"
        broker_addr = {b["metadata"]["name"]: pod_ip(b) for b in brokers}
        jobs = []
        with ThreadPoolExecutor(max_workers=12) as ex:
            for b in brokers:
                src = b["metadata"]["name"]
                if not pod_ready(b):
                    continue  # a pod that is not Ready cannot run the probe; the other brokers' probes to it are the record
                targets = [(broker_addr[o], port, f"{o}:{port}") for o in broker_addr if o != src
                           for port in (ids.KAFKA_PORT_CONTROLLER, ids.KAFKA_PORT_REPLICATION)]
                jobs.append(ex.submit(self.tcp_from, src, ids.BROKER_CONTAINER, targets))
            lg = next((p for p in loadgens if pod_ready(p)), None)
            if lg is not None:
                targets = [(broker_addr[o], ids.KAFKA_PORT_PLAIN, f"{o}:{ids.KAFKA_PORT_PLAIN}") for o in broker_addr]
                lg_container = (lg.get("spec") or {}).get("containers", [{}])[0].get("name", "load-generator")
                jobs.append(ex.submit(self.tcp_from, lg["metadata"]["name"], lg_container, targets))
            bursts = [ex.submit(self.burst_from, t["metadata"]["name"], jm_ip, jm_name) for t in tms if pod_ready(t)]
            single = [ex.submit(self.flink_jobs), ex.submit(self.flink_tms), ex.submit(self.loadgen),
                      ex.submit(self.http_status, "consumer", consumers, ids.CONSUMER_STATUS_PORT, "malformed_count",
                                ("malformed_count", "malformed_by_attempt", "started_at"))]
            if self.tick_count % 6 == 1:
                single.append(ex.submit(self.controller, brokers))
            rows = []
            for f in jobs:
                rows.extend(f.result())
            rows.extend(f.result() for f in bursts)
            rows.extend(f.result() for f in single)
        return rows


class SampleBuffer:
    def __init__(self, path: str | None, keep_s: float = 48 * 3600) -> None:
        self.rows: deque[dict] = deque()
        self.lock = threading.Lock()
        self.path = path
        self.keep_s = keep_s
        if path and os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                for line in f:
                    try:
                        self.rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue  # a torn last line after a crash

    def add(self, rows: list[dict]) -> None:
        with self.lock:
            self.rows.extend(rows)
            cutoff = time.time() - self.keep_s
            while self.rows and self.rows[0]["ts"] < cutoff:
                self.rows.popleft()
            if self.path:
                with open(self.path, "a", encoding="utf-8") as f:
                    for r in rows:
                        f.write(json.dumps(r, allow_nan=False) + "\n")

    def between(self, start: float, end: float) -> list[dict]:
        with self.lock:
            return [r for r in self.rows if start <= r["ts"] < end]


def metrics_text(last: list[dict], started_at: float) -> str:
    lines = ["# TYPE rebuild_probe_ok gauge"]
    for r in last:
        lab = f'probe="{r["probe"]}",src_pod="{r["src_pod"] or ""}",dst="{r["dst"] or ""}"'
        lines.append(f"rebuild_probe_ok{{{lab}}} {r['ok_count'] / r['tried_count']}")
    lines += ["# TYPE rebuild_probe_started_at gauge", f"rebuild_probe_started_at {started_at}"]
    return "\n".join(lines) + "\n"


def _fetch_text(url: str, timeout_s: float = 3.0) -> str:
    import urllib.request
    try:
        with urllib.request.urlopen(url, timeout=timeout_s) as resp:
            return resp.read().decode("utf-8")
    except Exception as e:  # noqa: BLE001 - reported as a failed probe sample
        raise QueryError(f"GET {url} failed: {e}") from e


def serve(port: int = 8000, data_path: str | None = "/data/probes.jsonl") -> None:  # pragma: no cover - needs a cluster
    import os
    kube, http = KubectlKube(kubectl=os.environ.get("KUBECTL_BIN", "kubectl")), HttpJson()
    runner = ProbeRunner(kube, http, FlinkRest(os.environ.get("FLINK_REST_URL", ids.FLINK_REST), http),
                         loadgen_status_url=os.environ.get("LOADGEN_STATUS_URL", ids.LOADGEN_STATUS_URL))
    buf = SampleBuffer(data_path)
    state = {"started_at": time.time(), "last_tick_at": None, "last": []}

    class Handler(BaseHTTPRequestHandler):
        def _send(self, code: int, body: bytes, ctype: str) -> None:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):  # noqa: N802 - http.server API
            u = urlparse(self.path)
            if u.path == "/status":
                self._send(200, json.dumps({"started_at": state["started_at"], "last_tick_at": state["last_tick_at"],
                                            "rows": len(buf.rows)}).encode(), "application/json")
            elif u.path == "/samples":
                q = parse_qs(u.query)
                rows = buf.between(float(q["start"][0]), float(q["end"][0]))
                self._send(200, json.dumps({"started_at": state["started_at"], "rows": rows}).encode(), "application/json")
            elif u.path == "/metrics":
                self._send(200, metrics_text(state["last"], state["started_at"]).encode(), "text/plain; version=0.0.4")
            else:
                self._send(404, b"{}", "application/json")

        def log_message(self, *a):
            return

    srv = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    next_tick = (int(time.time()) // 10 + 1) * 10
    while True:
        time.sleep(max(0.0, next_tick - time.time()))
        rows = runner.tick()
        buf.add(rows)
        state["last"], state["last_tick_at"] = rows, time.time()
        next_tick += 10
        while next_tick <= time.time():
            next_tick += 10  # a slow round skips a slot rather than drifting off the 10 s schedule


class ProbeClient:
    """Orchestrator side: fetch probe samples for a time span from the service."""

    def __init__(self, base_url: str = ids.PROBES_SERVICE_URL, http: HttpJson | None = None) -> None:
        self.base = base_url.rstrip("/")
        self.http = http or HttpJson()

    def status(self) -> dict:
        return self.http.get(f"{self.base}/status", timeout_s=5.0)

    def samples(self, start: float, end: float) -> tuple[float, list[dict]]:
        data = self.http.get(f"{self.base}/samples?start={start:.3f}&end={end:.3f}", timeout_s=60.0)
        return float(data["started_at"]), data["rows"]
