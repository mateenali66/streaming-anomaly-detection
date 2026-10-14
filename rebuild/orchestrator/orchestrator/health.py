"""AWS Health events for the region (protocol 1.7, REVIEW4 MIN-4 and M12, rebuild/CONTRACT_CHANGES.md C30).

The AWS Health API needs a Business or Enterprise support plan, so infra writes the region's `aws.health` events to the
CloudWatch Logs group /streaming-anomaly-detection/health with an EventBridge rule, and grants the orchestrator role
logs:FilterLogEvents on it. The orchestrator polls the group every 10 minutes and once more before COMPLETE, records each
event in the manifest's health_events, and flags HEALTH_EVENT on every attempt whose span overlaps an event. A poll error
is logged and retried, never fatal: the manifest's health_events_polled_until says how far the record reaches.

Event format (AWS Health user guide, "Reference: AWS Health events Amazon EventBridge schema"): the log event message is the
EventBridge event, with `time` (yyyy-mm-ddThh:mm:ssZ) and `detail` holding eventArn, service, eventTypeCode, statusCode
(open, closed, upcoming), eventRegion, startTime and the optional endTime in RFC 1123 form ("Fri, 27 Jan 2023 06:02:51 GMT").
Only those fields are kept. The account ID (`account`, `affectedAccount`) and the affected entities never enter the record,
and the event ARN is kept as a hash.

boto3 (context7 /boto/boto3, botocore service model): logs.get_paginator("filter_log_events") with logGroupName, startTime and
endTime in epoch milliseconds; each event has timestamp, message, ingestionTime and eventId.
"""
from __future__ import annotations

import email.utils
import hashlib
import json
import logging
from datetime import datetime, timezone

from . import ids

log = logging.getLogger("orchestrator.health")
MIN_SLACK_S = 60.0       # a poll runs only inside a sleep with at least this much time left, so it never delays a step
RETRY_S = 60.0           # a failed poll is retried this much later
FINAL_RETRY_S = 30.0
FINAL_GIVE_UP_S = 900.0  # the poll before COMPLETE is retried for 15 minutes, then the campaign goes on without it


def _rfc1123(v: str | None) -> float | None:
    if not v:
        return None
    try:
        return email.utils.parsedate_to_datetime(v).timestamp()
    except (TypeError, ValueError):
        return None


def _iso(v: str | None) -> float | None:
    if not v:
        return None
    try:
        return datetime.strptime(v, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        return None


def parse_event(msg: dict, region: str = ids.REGION) -> dict | None:
    """One EventBridge AWS Health event as a manifest health_events entry, or None if it is not one for this region."""
    if not isinstance(msg, dict) or msg.get("source") != "aws.health":
        return None
    d = msg.get("detail") or {}
    if d.get("eventRegion") not in (None, "", region, "global"):
        return None
    arn, start = d.get("eventArn"), _rfc1123(d.get("startTime"))
    at = _iso(msg.get("time"))
    if not arn or start is None or at is None:
        return None
    return {"at": round(at, 3), "service": str(d.get("service") or ""), "event_type_code": str(d.get("eventTypeCode") or ""),
            "status": str(d.get("statusCode") or ""), "event_key": hashlib.sha256(arn.encode("utf-8")).hexdigest()[:16],
            "start_at": round(start, 3), "end_at": None if _rfc1123(d.get("endTime")) is None else round(_rfc1123(d.get("endTime")), 3)}


class CloudWatchHealthEvents:
    """The real source: FilterLogEvents on the log group, with short timeouts so a poll never hangs."""

    def __init__(self, log_group: str = ids.HEALTH_LOG_GROUP, region: str = ids.REGION) -> None:
        import boto3
        from botocore.config import Config
        self.log_group = log_group
        self.client = boto3.client("logs", region_name=region, config=Config(connect_timeout=5, read_timeout=10,
                                                                             retries={"max_attempts": 2}))

    def events(self, start_s: float, end_s: float) -> list[dict]:
        out = []
        pages = self.client.get_paginator("filter_log_events").paginate(
            logGroupName=self.log_group, startTime=int(start_s * 1000), endTime=int(end_s * 1000))
        for page in pages:
            for ev in page.get("events") or []:
                try:
                    parsed = parse_event(json.loads(ev.get("message") or ""))
                except json.JSONDecodeError:
                    continue
                if parsed is not None:
                    out.append(parsed)
        return out


def overlaps(ev: dict, start: float, end: float) -> bool:
    return ev["start_at"] < end and (ev["end_at"] is None or ev["end_at"] > start)


class HealthPoller:
    """Runs from the clock's sleep hook (runtime.HookedClock) every 10 minutes, and before COMPLETE."""

    def __init__(self, camp) -> None:
        self.camp = camp
        self.next_due: float | None = None
        self.closed = False  # after the poll before COMPLETE the manifest must not change any more

    @property
    def source(self):
        return getattr(self.camp.rt, "health", None)

    def tick(self, now: float, remaining: float) -> float | None:
        """Called inside every sleep. Polls when due and when the sleep has room for it. Returns the next due time."""
        if self.camp.m is None or self.source is None or self.closed:
            return None
        if self.next_due is None:
            self.next_due = now
        if now >= self.next_due and remaining >= MIN_SLACK_S:
            self.poll(now)
        return self.next_due

    def poll(self, now: float) -> bool:
        m = self.camp.m
        try:
            got = self.source.events(m["timeline_start_at"] - ids.HEALTH_LOOKBACK_S, now)
        except Exception as e:  # noqa: BLE001 - a poll error is logged and retried, never fatal (C30)
            log.warning("AWS Health poll failed, retrying in %.0f s: %s", RETRY_S, e)
            self.next_due = now + RETRY_S
            return False
        self.merge(got)
        m["health_events_polled_until"] = round(now, 3)
        self.flag_attempts(now)
        self.camp.save()
        self.next_due = now + ids.HEALTH_POLL_S
        return True

    def merge(self, got: list[dict]) -> None:
        m = self.camp.m
        by = {e["event_key"]: e for e in m["health_events"]}
        for e in sorted(got, key=lambda x: x["at"]):
            if e["end_at"] is not None and e["end_at"] <= m["timeline_start_at"]:
                continue  # ended before the campaign began
            old = by.get(e["event_key"])
            if old is None or e["at"] >= old["at"]:
                by[e["event_key"]] = e
        m["health_events"] = sorted(by.values(), key=lambda x: (x["start_at"], x["event_key"]))

    def flag_attempts(self, now: float | None = None) -> bool:
        """HEALTH_EVENT on every attempt whose span [started_at, ended_at) overlaps an event, set when the event is
        recorded, also on attempts that finished before it was seen."""
        m = self.camp.m
        if m is None or not m["health_events"]:
            return False
        now = self.camp.now() if now is None else now
        changed = False
        for a in m["attempts"]:
            end = a["ended_at"] if a["ended_at"] is not None else now
            if "HEALTH_EVENT" not in a["flags"] and any(overlaps(e, a["started_at"], end) for e in m["health_events"]):
                a["flags"].append("HEALTH_EVENT")
                changed = True
        return changed

    def check_source(self) -> None:
        """At a start, before WARMUP, and at a resume while the record has never been read: one read of the log group must
        succeed, retried every 30 s for 15 minutes, or the start halts with HEALTH_RECORD_UNAVAILABLE. The preprocessing
        refuses a manifest whose health_events_polled_until is null (C30), so a campaign must never complete with the
        record unread (decision 23). Later poll errors are never fatal."""
        from .errors import HaltRequested
        clock = self.camp.rt.clock
        if self.source is None:
            raise HaltRequested("HEALTH_RECORD_UNAVAILABLE: no AWS Health source (CloudWatch Logs client)")
        start = clock.now()
        while True:
            now = clock.now()
            try:
                self.source.events(now - 3600.0, now)
                return
            except Exception as e:  # noqa: BLE001 - retried, then the start halts
                if now - start >= FINAL_GIVE_UP_S:
                    raise HaltRequested(f"HEALTH_RECORD_UNAVAILABLE: {type(e).__name__}: {e}"[:300])
                log.warning("AWS Health log group not readable yet, retrying in %.0f s: %s", FINAL_RETRY_S, e)
            clock.sleep(FINAL_RETRY_S)

    def final_poll(self) -> bool:
        """Before COMPLETE: retried for 15 minutes, then the campaign continues (never fatal)."""
        if self.camp.m is None or self.source is None:
            return False
        clock = self.camp.rt.clock
        start = clock.now()
        self.closed = True  # no hook poll from here on: only this loop polls, then nothing changes the manifest
        while True:
            if self.poll(clock.now()):
                return True
            if clock.now() - start >= FINAL_GIVE_UP_S:
                log.error("AWS Health poll before COMPLETE failed for %.0f s; health_events_polled_until stays at %s",
                          FINAL_GIVE_UP_S, self.camp.m["health_events_polled_until"])
                return False
            clock.sleep(FINAL_RETRY_S)
