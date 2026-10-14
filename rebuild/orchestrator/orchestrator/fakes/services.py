"""Fake Flink REST, probe service client and verification-consumer side, over the simulated world."""
from __future__ import annotations

import math

from ..errors import QueryError
from .clock import ProcessDead


class FakeFlink:
    def __init__(self, world) -> None:
        self.w = world

    def _t(self) -> float:
        if self.w.dead:
            raise ProcessDead("process is dead")
        self.w.advance(self.w.clock.t)
        return self.w.clock.t

    def overview(self) -> dict:
        t = self._t()
        ov = self.w.flink_overview(t)
        if not ov:
            raise QueryError("Flink REST unavailable (no Ready JobManager)")
        return ov

    def jobs(self) -> list[dict]:
        return [{"jid": j["job_id"], "name": j["name"], "state": j["state"]} for j in self.snapshot()]

    def snapshot(self) -> list[dict]:
        t = self._t()
        if self.w.global_knobs.get("flink_snapshot_fail_next", 0) > 0:
            self.w.global_knobs["flink_snapshot_fail_next"] -= 1
            raise QueryError("simulated Flink REST timeout")
        if not self.w.flink_overview(t):
            raise QueryError("Flink REST unavailable (no Ready JobManager)")
        return self.w.flink_snapshot(t)

    def _job(self, jid: str):
        t = self._t()
        for j in self.w.flink_jobs:
            if j.jid == jid and j.state(t) is not None:
                return j
        raise QueryError(f"404 job {jid} not found")

    def job_config(self, jid: str) -> dict:
        j = self._job(jid)
        return {"jid": jid, "name": j.name, "execution-config": {"job-parallelism": j.settings.get("parallelism"),
                                                                 "execution-mode": "PIPELINED"}}

    def checkpoint_config(self, jid: str) -> dict:
        j = self._job(jid)
        st = j.settings
        if st.get("checkpointing") is False:
            raise QueryError("404 Checkpointing has not been enabled.")
        return {"mode": st["checkpoint_mode"], "interval": st["checkpoint_interval_ms"], "timeout": st["checkpoint_timeout_ms"],
                "min_pause": st["checkpoint_min_pause_ms"], "max_concurrent": 1,
                "externalization": {"enabled": st["externalization_enabled"],
                                    "delete_on_cancellation": st["delete_on_cancellation"]},
                "state_backend": "EmbeddedRocksDBStateBackend", "checkpoint_storage": "FileSystemCheckpointStorage"}

    def cancel(self, jid: str) -> None:
        t = self._t()
        for j in self.w.flink_jobs:
            if j.jid == jid and j.state(t) not in (None, "CANCELED", "FAILED", "FINISHED"):
                j.states.append((t + 1, "CANCELED"))
        self.w.snap_cache.clear()


class FakeProbes:
    """The orchestrator's ProbeClient against the simulated probe service."""

    def __init__(self, world, started_at: float | None = None) -> None:
        self.w = world
        self.started_at = world.t0 - 7200 if started_at is None else started_at
        self.calls = 0

    def _t(self) -> float:
        if self.w.dead:
            raise ProcessDead("process is dead")
        self.w.advance(self.w.clock.t)
        return self.w.clock.t

    def status(self) -> dict:
        t = self._t()
        if any(a <= t < b for a, b in self.w.global_knobs["probes_down"]):
            raise QueryError("probe service down")
        return {"started_at": self.started_at, "last_tick_at": t}

    def samples(self, start: float, end: float):
        now = self._t()
        self.calls += 1
        if any(a <= now < b for a, b in self.w.global_knobs.get("probe_api_down", [])):
            raise QueryError("probe service unreachable")
        rows = []
        t = int(math.floor(start / 10.0) * 10)
        while t < min(end, now):
            for r in self.w.probe_rows(t):
                if start <= r["ts"] < end and r["ts"] <= now:
                    rows.append(r)
            t += 10
        return self.started_at, rows


class FakeHealthLogs:
    """The CloudWatch Logs group of infra's EventBridge rule for aws.health events (C30). global knob health_events holds
    entries {offset_s, duration_s (None: still open), service, event_type_code, status, region}; each is published as an
    EventBridge event at world.t0 + offset_s and, when it has a duration, published again as closed at its end.
    health_poll_fail_until makes every FilterLogEvents call fail before that instant; health_poll_fail_calls (a set of
    1-based call numbers) makes those calls fail."""

    def __init__(self, world) -> None:
        self.w = world
        self.calls = 0

    def _raw(self) -> list[tuple[float, dict]]:
        from email.utils import formatdate
        import time as _time
        out = []
        for i, e in enumerate(self.w.global_knobs["health_events"]):
            start = self.w.t0 + e["offset_s"]
            arn = f"arn:aws:health:{e.get('region', 'us-east-2')}::event/EC2/{e.get('event_type_code', 'AWS_EC2_OPERATIONAL_ISSUE')}/ID{i}"
            versions = [(start, "open", None)]
            if e.get("duration_s") is not None:
                versions.append((start + e["duration_s"], "closed", start + e["duration_s"]))
            for at, status, end in versions:
                detail = {"eventArn": arn, "service": e.get("service", "EC2"),
                          "eventTypeCode": e.get("event_type_code", "AWS_EC2_OPERATIONAL_ISSUE"),
                          "eventTypeCategory": "issue", "eventScopeCode": "PUBLIC", "statusCode": status,
                          "eventRegion": e.get("region", "us-east-2"), "startTime": formatdate(start, usegmt=True),
                          "affectedAccount": "000000000000"}
                if end is not None:
                    detail["endTime"] = formatdate(end, usegmt=True)
                out.append((at, {"version": "0", "id": f"ev-{i}-{status}", "detail-type": "AWS Health Event",
                                 "source": "aws.health", "account": "000000000000",
                                 "time": _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime(at)), "region": "us-east-2",
                                 "resources": [], "detail": detail}))
        return out

    def events(self, start_s: float, end_s: float) -> list[dict]:
        from ..health import parse_event
        if self.w.dead:
            raise ProcessDead("process is dead")
        self.calls += 1
        now = self.w.clock.t
        fail = self.w.global_knobs["health_poll_fail_until"]
        if (fail is not None and now < fail) or self.calls in self.w.global_knobs.get("health_poll_fail_calls", ()):
            raise QueryError("simulated FilterLogEvents failure")
        out = []
        for at, msg in self._raw():
            if start_s <= at <= end_s and at <= now:
                ev = parse_event(msg)
                if ev is not None:
                    out.append(ev)
        return out
