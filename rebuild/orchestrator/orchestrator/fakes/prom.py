"""Fake Prometheus over the simulated world.

Answers (a) selector queries `{__name__=~"...", k="v", k!="v", k=~"a|b"}` and `name{...}` by filtering the world's
series at each 10 s step, and (b) the registered aggregate queries of run/thresholds.json by exact string, with a
handler per query. An unknown query raises QueryError, so a query the orchestrator builds wrongly fails the test.
"""
from __future__ import annotations

import re
import zlib

from ..clock import floor10
from ..errors import QueryError
from ..prom import Prom
from .clock import ProcessDead

MATCHER = re.compile(r'\s*([A-Za-z_][A-Za-z0-9_]*)\s*(=~|!=|!~|=)\s*"([^"]*)"\s*')
SELECTOR = re.compile(r"^\s*([A-Za-z_:][A-Za-z0-9_:]*)?\s*\{(.*)\}\s*$")

from .series import METRIC_TYPES  # noqa: E402  (the metadata API answer, next to the series it describes)


def parse_selector(q: str) -> list[tuple[str, str, re.Pattern | str]] | None:
    m = SELECTOR.match(q)
    if not m:
        return None
    out = []
    if m.group(1):
        out.append(("__name__", "=", m.group(1)))
    body = m.group(2).strip()
    pos = 0
    while pos < len(body):
        mm = MATCHER.match(body, pos)
        if not mm:
            return None
        k, op, v = mm.groups()
        out.append((k, op, re.compile(f"^(?:{v})$") if op in ("=~", "!~") else v))
        pos = mm.end()
        if pos < len(body):
            if body[pos] != ",":
                return None
            pos += 1
    return out


def matches(labels: dict, matchers) -> bool:
    for k, op, v in matchers:
        val = labels.get(k, "")
        if op == "=" and val != v:
            return False
        if op == "!=" and val == v:
            return False
        if op == "=~" and not v.match(val):
            return False
        if op == "!~" and v.match(val):
            return False
    return True


class FakeProm(Prom):
    def __init__(self, world, thresholds: dict) -> None:
        self.w = world
        q = thresholds["queries"]
        w = world
        self.queries_seen: list[str] = []
        self._selectors: dict[str, tuple] = {}

        def by(metric, job, agg):
            def f(t):
                vals = [v for _, v in w.series_at(floor10(t)).values_of(metric, job)]
                if not vals:
                    return []
                return [({}, agg(vals))]
            return f

        def rate(fn):
            def f(t):
                return [({}, (fn(t) - fn(t - 60)) / 60.0)]
            return f

        def active(t):
            return [({"pod": lab["pod"]}, v) for lab, v in
                    w.series_at(floor10(t)).values_of("kafka_controller_kafkacontroller_activecontrollercount")]

        self.handlers = {
            q["under_replicated_sum"]: by("kafka_server_replicamanager_underreplicatedpartitions", "kafka-brokers", sum),
            q["under_min_isr_sum"]: by("kafka_server_replicamanager_underminisrpartitioncount", "kafka-brokers", sum),
            q["offline_partitions_max"]: by("kafka_controller_kafkacontroller_offlinepartitionscount", "kafka-brokers", max),
            q["preferred_imbalance_max"]: by("kafka_controller_kafkacontroller_preferredreplicaimbalancecount", "kafka-brokers", max),
            q["fenced_brokers_max"]: by("kafka_controller_kafkacontroller_fencedbrokercount", "kafka-brokers", max),
            q["active_controller_by_pod"]: active,
            q["consumer_lag_max"]: by("flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max",
                                      "flink-metrics", max),
            q["raw_events_in_per_s"]: rate(w.raw_events_count),
        }

    def _alive(self) -> float:
        if self.w.dead:
            raise ProcessDead("process is dead")
        self.w.advance(self.w.clock.t)
        return self.w.clock.t

    def _compiled(self, query: str):
        """[name matchers, other matchers, matching names, names count when matched, series cache] of a selector."""
        got = self._selectors.get(query)
        if got is None:
            matchers = parse_selector(query)
            if matchers is None:
                raise QueryError(f"fake Prometheus does not know the query {query!r}")
            name_m = [m for m in matchers if m[0] == "__name__"]
            got = [name_m, [m for m in matchers if m[0] != "__name__"], [], -1, {}]
            self._selectors[query] = got
        return got

    def _eval(self, query: str, t: float) -> list[tuple[dict, float]]:
        """Selector evaluation over one step. Metric names are matched once per query (again only when a new name has
        appeared) and series labels once per (query, series): the label dicts of the fake TSDB are shared per series and
        never change."""
        if query in self.handlers:
            return self.handlers[query](t)
        c = self._compiled(query)
        name_m, rest, _, _, series_ok = c
        step = self.w.series_at(floor10(t))
        late = self._late_arrivals(rest, floor10(t))
        names = self.w.series.names
        if c[3] != len(names):
            c[2] = sorted(n for n in names if matches({"__name__": n}, name_m))
            c[3] = len(names)
        out = []
        for name in c[2]:
            rows = step.by_name.get(name)
            if not rows:
                continue
            for labels, v in rows:
                k = id(labels)
                hit = series_ok.get(k)
                if hit is None or hit[0] is not labels:
                    hit = series_ok[k] = (labels, matches(labels, rest))
                if hit[1]:
                    if late is not None:
                        v = late(labels, v)
                        if v is None:
                            continue
                    out.append((labels, v))
        return out

    def _late_arrivals(self, matchers, t: int):
        """kube-prometheus-stack scrapes cAdvisor with honorTimestamps, so a sample carries its own collection time and
        reaches Prometheus up to one scrape interval later (REVIEW4 L5). A query made before a sample has arrived sees the
        series' previous sample at that step. Returns a value mapper for kubelet queries asked within 12 s of the step."""
        if not any(k == "job" and op == "=" and v == "kubelet" for k, op, v in matchers):
            return None
        now = self.w.clock.t
        if now >= t + 12:
            return None
        fixed = self.w.global_knobs.get("cadvisor_arrival_delay_s")
        prev_step = self.w.series_at(t - 10)
        prev = {}
        for rows in prev_step.by_name.values():
            for lab, v in rows:
                prev[id(lab)] = v

        def mapper(labels, v):
            delay = fixed if fixed is not None else ((zlib.crc32(labels.get("pod", "").encode()) + t // 10 * 7) % 12)
            if now >= t + delay:
                return v
            return prev.get(id(labels))
        return mapper

    def instant(self, query: str, at: float | None = None):
        now = self._alive()
        self.queries_seen.append(query)
        t = now if at is None else at
        if t > now:
            raise QueryError("query in the future")
        return self._eval(query, t)

    def scalar(self, query: str, at: float | None = None) -> float:
        now = self._alive()
        if query != "time()":
            raise QueryError(f"fake scalar supports only time(), got {query!r}")
        return (now if at is None else at) - self.w.global_knobs.get("prom_clock_offset", 0.0)

    def range(self, query: str, start: float, end: float, step: int):
        now = self._alive()
        self.queries_seen.append(query)
        if end > now:
            raise QueryError(f"range query ends in the future ({end} > {now})")
        # Selector results carry the TSDB's shared label dicts (one per series), so they group by identity; the aggregate
        # handlers build fresh label dicts at every step, so they group by value.
        by_value = query in self.handlers
        series: dict = {}
        t = start
        while t <= end + 1e-9:
            for labels, v in self._eval(query, t):
                k = tuple(sorted(labels.items())) if by_value else id(labels)
                got = series.get(k)
                if got is None:
                    got = series[k] = (labels, [])
                got[1].append((float(t), float(v)))
            t += step
        return [(dict(labels), values) for labels, values in series.values()]

    def metadata(self) -> dict:
        self._alive()
        return {k: [{"type": v, "help": "", "unit": ""}] for k, v in METRIC_TYPES.items()}

