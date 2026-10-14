"""Flink REST API (Flink 1.19 docs, nightlies.apache.org/flink/flink-docs-release-1.19/docs/ops/rest_api,
context7 /websites/nightlies_apache_flink_flink-docs-release-1_19).

  GET /overview                -> taskmanagers, slots-total, slots-available, jobs-running, ...
  GET /jobs/overview           -> {"jobs": [{"jid", "name", "state", "start-time", ...}]}
  GET /jobs/<jid>/checkpoints  -> {"counts": {"completed", "failed", ...}, "latest": {"completed": {...,
                                  "latest_ack_timestamp", "external_path"}, "failed": {..., "failure_timestamp"}},
                                  "history": [{"status", "latest_ack_timestamp", "trigger_timestamp", ...}]}
  PATCH /jobs/<jid>?mode=cancel -> cancels a job (used only by remediation for a job stuck outside RUNNING)

Job states (same docs): INITIALIZING, CREATED, RUNNING, FAILING, FAILED, CANCELLING, CANCELED, FINISHED,
RESTARTING, SUSPENDED, RECONCILING. Timestamps are epoch milliseconds.
"""
from __future__ import annotations

from .errors import QueryError
from .httpjson import HttpJson

TERMINAL_STATES = {"FAILED", "CANCELED", "FINISHED"}


class FlinkRest:
    def __init__(self, base_url: str, http: HttpJson | None = None) -> None:
        self.base = base_url.rstrip("/")
        self.http = http or HttpJson()

    def overview(self) -> dict:
        data = self.http.get(f"{self.base}/overview")
        for k in ("taskmanagers", "slots-total"):
            if k not in data:
                raise QueryError(f"Flink /overview has no {k}")
        return data

    def jobs(self) -> list[dict]:
        data = self.http.get(f"{self.base}/jobs/overview")
        if "jobs" not in data:
            raise QueryError("Flink /jobs/overview has no jobs field")
        return [{"jid": j["jid"], "name": j["name"], "state": j["state"]} for j in data["jobs"]]

    def checkpoints(self, jid: str) -> dict:
        return self.http.get(f"{self.base}/jobs/{jid}/checkpoints")

    def job_config(self, jid: str) -> dict:
        """GET /jobs/<jid>/config: {"jid", "name", "execution-config": {"job-parallelism", ...}} (Flink 1.19 REST API)."""
        return self.http.get(f"{self.base}/jobs/{jid}/config")

    def checkpoint_config(self, jid: str) -> dict:
        """GET /jobs/<jid>/checkpoints/config: {"mode", "interval", "timeout", "min_pause", "externalization":
        {"enabled", "delete_on_cancellation"}, ...}. Answers 404 when checkpointing is not enabled."""
        return self.http.get(f"{self.base}/jobs/{jid}/checkpoints/config")

    def cancel(self, jid: str) -> None:
        self.http.patch(f"{self.base}/jobs/{jid}?mode=cancel")

    def snapshot(self) -> list[dict]:
        """Job names, states and checkpoint facts for every non-terminal job (the flink_jobs probe detail)."""
        out = []
        for j in self.jobs():
            entry = {"name": j["name"], "job_id": j["jid"], "state": j["state"], "completed_count": None,
                     "failed_count": None, "latest_completed_ack_at": None, "latest_completed_path": None,
                     "latest_failed_at": None, "completed_ack_at": [], "failed_at": []}
            if j["state"] not in TERMINAL_STATES:
                cp = self.checkpoints(j["jid"])
                counts = cp.get("counts") or {}
                latest = cp.get("latest") or {}
                comp = latest.get("completed") or {}
                fail = latest.get("failed") or {}
                entry["completed_count"] = counts.get("completed")
                entry["failed_count"] = counts.get("failed")
                if comp.get("latest_ack_timestamp"):
                    entry["latest_completed_ack_at"] = comp["latest_ack_timestamp"] / 1000.0
                entry["latest_completed_path"] = comp.get("external_path")
                if fail.get("failure_timestamp"):
                    entry["latest_failed_at"] = fail["failure_timestamp"] / 1000.0
                for h in cp.get("history") or []:
                    if h.get("status") == "COMPLETED" and h.get("latest_ack_timestamp"):
                        entry["completed_ack_at"].append(h["latest_ack_timestamp"] / 1000.0)
                    elif h.get("status") == "FAILED":
                        t = h.get("failure_timestamp") or h.get("latest_ack_timestamp") or h.get("trigger_timestamp")
                        if t:
                            entry["failed_at"].append(t / 1000.0)
                entry["completed_ack_at"].sort()
                entry["failed_at"].sort()
            out.append(entry)
        return sorted(out, key=lambda e: (e["name"], e["job_id"]))


def job_settings_problems(flink, expected_jobs: list[str], want: dict) -> list[str]:
    """REVIEW4 L1 (C40): every expected RUNNING job must run with the registered settings: job parallelism, exactly-once
    checkpoints at the registered interval, timeout and minimum pause, externalized and kept on cancel. The REST shapes are
    the ones rebuild/infra/scripts/check_flink_jobs.py reads. A REST failure is a problem too, never a pass."""
    problems: list[str] = []
    try:
        jobs = [j for j in flink.snapshot() if j["state"] == "RUNNING" and j["name"] in expected_jobs]
    except Exception as e:  # noqa: BLE001 - reported, the caller halts
        return [f"job list unavailable: {type(e).__name__}: {e}"]
    for name in expected_jobs:
        if sum(1 for j in jobs if j["name"] == name) != 1:
            problems.append(f"{name}: not exactly one RUNNING job")
    for j in jobs:
        try:
            par = (flink.job_config(j["job_id"]).get("execution-config") or {}).get("job-parallelism")
            cp = flink.checkpoint_config(j["job_id"])
        except Exception as e:  # noqa: BLE001
            problems.append(f"{j['name']}: job or checkpoint config unavailable ({type(e).__name__}: {e})"[:300])
            continue
        ext = cp.get("externalization") or {}
        got = {"parallelism": par, "checkpoint_mode": cp.get("mode"), "checkpoint_interval_ms": cp.get("interval"),
               "checkpoint_timeout_ms": cp.get("timeout"), "checkpoint_min_pause_ms": cp.get("min_pause"),
               "externalization_enabled": ext.get("enabled"), "delete_on_cancellation": ext.get("delete_on_cancellation")}
        for k, v in want.items():
            if got.get(k) != v:
                problems.append(f"{j['name']}: {k} is {got.get(k)!r}, expected {v!r}")
    return problems
