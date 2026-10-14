"""Baseline statistics (protocol 11.12), computed when BASELINE ends from the gate's own queries over the whole
BASELINE segment. R1 summed counter values and gave IoT a throughput floor of zero (Review 1 B12); here every
statistic comes from the same rate() and max() PromQL that the recovery gate evaluates.

Standard deviations are population standard deviations (numpy default, ddof=0); percentiles use numpy's default
linear interpolation. f6_planned_count = round(f6_rate_per_s * 180) with Python's round, as written in 11.12.
"""
from __future__ import annotations

import math

import numpy as np

from . import SCHEMA_VERSION, ids
from .clock import ceil10
from .errors import QueryError


def _segment_values(prom, query: str, segments: list[dict]) -> list[float]:
    vals = []
    for s in segments:
        lo, hi = ceil10(s["start_at"]), ceil10(s["end_at"]) - 10
        if hi < lo:
            continue
        for start in range(lo, hi + 1, 6 * 3600):
            end = min(hi, start + 6 * 3600 - 10)
            series = prom.range(query, start, end, 10)
            if len(series) != 1:
                raise QueryError(f"baseline query returned {len(series)} series for {query!r} in {start}..{end}")
            vals += [v for _, v in series[0][1] if math.isfinite(v)]
    return vals


def ws_query(cfg: dict) -> str:
    j = cfg["prometheus_jobs"]
    return (f'container_memory_working_set_bytes{{namespace="{ids.NAMESPACE}",job="{j["kubelet"]}",'
            f'container="{ids.FLINK_CONTAINER}",pod=~"{cfg["metric_names"]["tm_pod_regex"]}"}}')


def compute(prom, cfg: dict, thresholds: dict, campaign_id: str, segments: list[dict], now: float) -> dict:
    q = thresholds["queries"]
    base = [s for s in segments if s["segment_type"] == "BASELINE"]
    if not base:
        raise QueryError("no BASELINE segment")
    rate = _segment_values(prom, q["raw_events_in_per_s"], base)
    lag = _segment_values(prom, q["consumer_lag_max"], base)
    if len(rate) < 10 or len(lag) < 10:
        raise QueryError("baseline statistics need samples; the queries returned almost nothing")
    ws: dict[str, list[float]] = {}
    for s in base:
        lo, hi = ceil10(s["start_at"]), ceil10(s["end_at"]) - 10
        for start in range(lo, hi + 1, 6 * 3600):
            end = min(hi, start + 6 * 3600 - 10)
            for labels, values in prom.range(ws_query(cfg), start, end, 10):
                ws.setdefault(labels.get("pod", ""), []).extend(v for _, v in values if math.isfinite(v))
    r = np.asarray(rate, dtype=float)
    lg = np.asarray(lag, dtype=float)
    mean = float(r.mean())
    if mean <= 0:
        raise QueryError("raw-events baseline rate is zero")
    f6_rate = 0.05 * mean
    return {
        "schema_version": SCHEMA_VERSION,
        "campaign_id": campaign_id,
        "window": {"start_at": base[0]["start_at"], "end_at": base[-1]["end_at"]},
        "queries": {"raw_events_in_per_s": q["raw_events_in_per_s"], "consumer_lag_max": q["consumer_lag_max"],
                    "tm_working_set_bytes": ws_query(cfg)},
        "raw_events_in_per_s": {"mean": mean, "sd": float(r.std()), "p05": float(np.percentile(r, 5)),
                                "p95": float(np.percentile(r, 95))},
        "consumer_lag_max": {"mean": float(lg.mean()), "sd": float(lg.std())},
        "f6_rate_per_s": f6_rate,
        "f6_planned_count": int(round(f6_rate * 180)),
        "tm_working_set_bytes": {pod: {"mean": float(np.mean(v)), "sd": float(np.std(v))} for pod, v in sorted(ws.items()) if v},
        "computed_at": round(now, 3),
    }
