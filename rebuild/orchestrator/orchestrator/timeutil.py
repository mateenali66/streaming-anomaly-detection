"""RFC 3339 parsing for Kubernetes and Chaos Mesh timestamps (metav1.Time, second precision)."""
from __future__ import annotations

from datetime import datetime, timezone


def parse_rfc3339(s: str) -> float:
    if not isinstance(s, str) or not s:
        raise ValueError(f"bad timestamp {s!r}")
    t = s.strip()
    if t.endswith("Z"):
        t = t[:-1] + "+00:00"
    dt = datetime.fromisoformat(t)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return round(dt.timestamp(), 3)


def to_rfc3339(t: float) -> str:
    return datetime.fromtimestamp(int(t), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
