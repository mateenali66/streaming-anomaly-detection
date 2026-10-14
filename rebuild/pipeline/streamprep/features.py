"""Window features (protocol 4.1, contract 11.9).

Order of operations:
1. Counters become per-second rates at 10 s resolution, per series. A negative step is a reset
   and is dropped. A rate is only taken between two samples exactly one step apart.
2. Series map to feature columns. Broker series stay per broker (b0, b1, b2), the JobManager is
   jm, series with no pod are cluster. TaskManager series are aggregated by role at each step
   into tm_min, tm_max and tm_mean, and the count of TaskManagers reporting is its own feature.
   Names take their labels from the fixed per-metric list labels.naming (Review 4 MIN-11). For each source that
   holds TaskManager series, the share of its TaskManager aggregate columns with no value at the step is a feature
   (f__<source>__tm_aggregates_missing_frac__cluster), so a gap that fold_fit fills with a median is never silent.
   The same holds per broker identity (f__<source>__broker_series_missing_frac__b<n>, broker and JVM sources), and the
   number of brokers reporting is a feature (f__broker__brokers_reporting__cluster).
3. The eight derived features of the R1 design are computed per step. lag_velocity and
   lag_acceleration difference only two steps of the same segment.
4. Six statistics per column over the window's 10 s steps, and the telemetry meta columns.

Verification and probe files are never read here (4.1.6).
"""
from __future__ import annotations

import re
import warnings as _warnings
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from .contract import (
    BROKER_SCOPES, FEATURE_SOURCES, STATS, BuildError, ContractError, canonical_json_bytes, sanitize_label_value,
)
from .metrics_io import LongMetrics
from .timeline import Timeline, segment_index_at

SRC_INDEX = {s: i for i, s in enumerate(FEATURE_SOURCES)}


@dataclass
class ColumnSource:
    source: str
    name: str  # metric part of the feature name, after rate renaming and label suffixes
    scope_class: str  # b0, b1, b2, jm, tm or cluster
    sids: list[int]
    ident: str  # canonical JSON of the naming labels, for collision messages


@dataclass
class FeatureReport:
    n_series: int = 0
    n_counter_series: int = 0
    n_columns_direct: int = 0
    n_tm_groups: int = 0
    collisions_outside_windows: int = 0
    monotone_levels: list[str] = field(default_factory=list)
    reference_units: list[str] = field(default_factory=list)
    telemetry_violations: list[str] = field(default_factory=list)
    baseline_outage_windows: int = 0
    baseline_not_ok_windows: int = 0


# ----------------------------------------------------------------------------------------------
# 1. counters and rates
# ----------------------------------------------------------------------------------------------

def counter_mask(lm: LongMetrics, cfg: dict) -> np.ndarray:
    pats = [re.compile(p) for p in cfg["counters"]["extra_patterns"]]
    out = np.zeros(len(lm.series), dtype=bool)
    for i, (m, t) in enumerate(zip(lm.series.metric, lm.series.metric_type)):
        if t == "counter":
            out[i] = True
        elif t in ("histogram", "summary") and m.endswith(("_bucket", "_sum", "_count")):
            out[i] = True
        elif any(p.search(m) for p in pats):
            out[i] = True
    return out


def rate_values(lm: LongMetrics, is_counter: np.ndarray) -> np.ndarray:
    """Per-row value after conversion. Rows are sorted by (sid, step)."""
    n = len(lm.value)
    rate = np.full(n, np.nan)
    if n > 1:
        same = (lm.sid[1:] == lm.sid[:-1]) & (lm.step[1:] == lm.step[:-1] + 1)
        dv = lm.value[1:] - lm.value[:-1]
        ok = same & (dv >= 0)
        rate[1:][ok] = dv[ok] / 10.0
    return np.where(is_counter[lm.sid], rate, lm.value)


def excluded_mask(lm: LongMetrics, cfg: dict) -> np.ndarray:
    """Series kept out of the direct features and the monotone guard. Derived inputs may still use them.

    counters.exclude_feature_patterns match the metric name (clock-like levels). counters.exclude_series match the metric
    name and one label value each: process-age JVM pools and Kafka meters that open on first use (from the pilot).
    """
    pats = [re.compile(p) for p in cfg["counters"].get("exclude_feature_patterns", [])]
    rules = [(re.compile(r["metric_regex"]), r["label"], re.compile(r["value_regex"]))
             for r in cfg["counters"].get("exclude_series", [])]
    st = lm.series
    return np.array([any(p.search(m) for p in pats)
                     or any(mr.search(m) and lab in st.labels[s] and vr.search(str(st.labels[s][lab]))
                            for mr, lab, vr in rules)
                     for s, m in enumerate(st.metric)], dtype=bool)


def renamed_metric(metric: str, is_counter: bool) -> str:
    if not is_counter:
        return metric
    if metric.endswith("_total"):
        return metric[: -len("_total")] + "_per_s"
    return metric + "_per_s"


# ----------------------------------------------------------------------------------------------
# Monotone level guard: a gauge that only grows over the baseline acts as a clock (Review 2, item 6)
# ----------------------------------------------------------------------------------------------

def monotone_levels(lm: LongMetrics, is_counter: np.ndarray, baseline_steps: np.ndarray,
                    cfg: dict) -> list[str]:
    g = cfg["monotone_guard"]
    allow = [re.compile(p) for p in g["allow_metric_patterns"]]
    rows = baseline_steps[lm.step] & ~is_counter[lm.sid] & ~excluded_mask(lm, cfg)[lm.sid]
    sid, step, val = lm.sid[rows], lm.step[rows], lm.value[rows]
    if len(sid) < 2:
        return []
    n_series = len(lm.series)
    cnt = np.bincount(sid, minlength=n_series)
    same = sid[1:] == sid[:-1]
    nondec = np.bincount(sid[1:][same & (val[1:] >= val[:-1])], minlength=n_series)
    pairs = np.bincount(sid[1:][same], minlength=n_series)
    first_idx = np.searchsorted(sid, np.arange(n_series), side="left")
    last_idx = np.searchsorted(sid, np.arange(n_series), side="right") - 1
    out = []
    for s in np.flatnonzero((cnt >= g["min_points"]) & (pairs > 0)):
        if nondec[s] / pairs[s] < g["min_nondecreasing_frac"]:
            continue
        a, b = first_idx[s], last_idx[s]
        if not val[b] > val[a]:
            continue
        rho = pd.Series(step[a:b + 1]).rank().corr(pd.Series(val[a:b + 1]).rank())
        if rho >= g["min_spearman"]:
            m = lm.series.metric[s]
            if any(p.search(m) for p in allow):
                continue
            out.append(f"{lm.series.source[s]}/{m}{lm.series.labels_str[s]} pod={lm.series.pod[s]} (rho {rho:.3f})")
    return out


# ----------------------------------------------------------------------------------------------
# 2. column mapping
# ----------------------------------------------------------------------------------------------

def scope_class_of(role: str, pod: str, broker_id: int | None) -> str:
    if pod == "":
        return "cluster"
    if role == "broker":
        return f"b{broker_id}"
    if role == "jobmanager":
        return "jm"
    if role == "taskmanager":
        return "tm"
    raise ContractError(f"series of role {role!r} with pod {pod!r} has no scope in 11.9")


def naming_rules(cfg: dict) -> list[tuple[re.Pattern, tuple[str, ...]]]:
    """The fixed per-metric naming list (labels.naming, Review 4 MIN-11), in order. The first match wins."""
    return [(re.compile(r["metric_regex"]), tuple(sorted(r["labels"]))) for r in cfg["labels"]["naming"]]


def naming_labels_for(metric: str, rules: list[tuple[re.Pattern, tuple[str, ...]]]) -> tuple[str, ...] | None:
    for rx, labs in rules:
        if rx.search(metric):
            return labs
    return None


def map_columns(lm: LongMetrics, is_counter: np.ndarray, cfg: dict
                ) -> tuple[list[ColumnSource], np.ndarray, np.ndarray, list[tuple[str, str]]]:
    """Return column sources, gid per series and unit id per series (for telemetry presence).

    The labels that enter a name come from the fixed list in labels.naming, matched on the Prometheus metric name, so
    a series has the same name in every campaign whatever the other series of that campaign carry (MIN-11). A label
    outside the list that is not dropped stays part of the series identity: two series that differ only in such a
    label map to one name and fail the build, never merge silently.
    """
    drop = set(cfg["labels"]["drop_before_naming"])
    tm_drop = set(cfg["labels"]["tm_identity"])
    rules = naming_rules(cfg)
    excluded = excluded_mask(lm, cfg)
    st = lm.series
    n = len(st)
    info = []
    errors = []
    unnamed = set()
    for s in range(n):
        if excluded[s]:
            continue
        try:
            sc = scope_class_of(st.role[s], st.pod[s], st.broker_id[s])
        except ContractError as exc:
            errors.append(str(exc))
            continue
        lab = {k: v for k, v in st.labels[s].items() if k not in drop}
        if sc == "tm":
            lab = {k: v for k, v in lab.items() if k not in tm_drop}
        keys = naming_labels_for(st.metric[s], rules)
        if keys is None:
            unnamed.add(f"{st.source[s]}/{st.metric[s]}")
            continue
        info.append((s, st.source[s], renamed_metric(st.metric[s], bool(is_counter[s])), sc, lab, keys))
    if errors:
        raise ContractError(sorted(set(errors))[:20])
    if unnamed:
        raise BuildError(["feature metrics with no rule in labels.naming of config/pipeline.json. Add a rule before "
                          "the pilot (Review 4 MIN-11):"] + sorted(unnamed)[:20])
    columns: dict[tuple, dict[str, list[int]]] = {}
    for s, src, met, sc, lab, keys in info:
        name = met + "".join(f".{k}-{sanitize_label_value(lab[k])}" for k in keys if k in lab)
        ident = canonical_json_bytes(lab).decode()
        columns.setdefault((src, name, sc), {}).setdefault(ident, []).append(s)
    out: list[ColumnSource] = []
    gid = np.full(n, -1, dtype=np.int64)
    coll = []
    for (src, name, sc) in sorted(columns):
        idents = columns[(src, name, sc)]
        if len(idents) > 1:
            coll.append(f"f__{src}__{name}__{sc}: label sets {sorted(idents)[:3]} map to one name (11.9). A label "
                        "that tells them apart is not in labels.naming")
            continue
        ident, sids = next(iter(idents.items()))
        for s in sids:
            gid[s] = len(out)
        out.append(ColumnSource(src, name, sc, sorted(sids), ident))
    if coll:
        raise BuildError(coll[:20])
    units = sorted({(st.source[s], sc) for s, _src, _m, sc, _l, _k in info})
    unit_index = {u: i for i, u in enumerate(units)}
    unit = np.array([unit_index[(st.source[s], sc)] for s, _src, _m, sc, _l, _k in info], dtype=np.int64)
    unit_full = np.full(n, -1, dtype=np.int64)
    unit_full[[s for s, *_ in info]] = unit
    return out, gid, unit_full, units


# Sources that hold TaskManager series. Each gets a missingness feature (decision 25), present in every campaign.
TM_SOURCES = ("consumer", "flink", "jvm")
# Sources that hold per-broker JMX series (broker metrics and the JMX exporter's JVM collectors), one feature per
# broker identity b0, b1, b2 (decision 25 extended to brokers, author to confirm), present in every campaign.
BROKER_SOURCES = ("broker", "jvm")


def missingness_exclusions(columns: list[ColumnSource], cfg: dict) -> np.ndarray:
    """Columns left out of every missingness share (missingness.exclude_column_regex, matched on the column name).

    These are meters Kafka opens on first use and closes on leadership loss (REVIEW4_LIVE_CONTRACT L3), such as the
    dead-letter topic's, whose absence follows fault history rather than telemetry health. They stay features.
    """
    pats = [re.compile(p) for p in cfg.get("missingness", {}).get("exclude_column_regex", [])]
    return np.array([any(p.search(c.name) for p in pats) for c in columns], dtype=bool)


def _missing_share(columns: list[ColumnSource], gid_of_sid: np.ndarray, lm: LongMetrics, rv: np.ndarray,
                   source: str, scope_class: str, exclude: np.ndarray) -> np.ndarray:
    """Per 10 s step, the share of the expected columns of one source and scope class that have no finite value.

    A column with no finite value at a step is NaN there, and when that holds for every step of a window fold_fit fills
    the window statistic with the train NORMAL median. A column is expected from its first finite value in the campaign
    on. Kafka creates per-topic broker metrics on first use, so a series that first appears mid-campaign (the
    dead-letter topic's, with the first malformed record) is not counted as missing before it exists, which would put a
    step into NORMAL windows at that time. 0 before any column has reported. NaN when the campaign has no such column
    or none of them ever reports.
    """
    n = lm.n_steps
    groups = np.flatnonzero(np.array([c.source == source and c.scope_class == scope_class for c in columns], dtype=bool)
                            & ~exclude)
    if len(groups) == 0:
        return np.full(n, np.nan)
    G = len(groups)
    local = np.full(len(columns), -1, dtype=np.int64)
    local[groups] = np.arange(G)
    gid_rows = gid_of_sid[lm.sid]
    rows = (gid_rows >= 0) & np.isfinite(rv)
    rows[rows] = local[gid_rows[rows]] >= 0
    if not rows.any():
        return np.full(n, np.nan)
    g_loc = local[gid_rows[rows]]
    st = lm.step[rows].astype(np.int64)
    pairs = np.unique(st * G + g_loc)
    present = np.bincount(pairs // G, minlength=n).astype(np.float64)
    first = np.full(G, n, dtype=np.int64)
    np.minimum.at(first, g_loc, st)
    expected = np.cumsum(np.bincount(first[first < n], minlength=n)).astype(np.float64)
    return np.where(expected > 0, 1.0 - present / np.where(expected > 0, expected, 1.0), 0.0)


def tm_missing_fraction(columns: list[ColumnSource], gid_of_sid: np.ndarray, lm: LongMetrics, rv: np.ndarray,
                        exclude: np.ndarray) -> dict[str, np.ndarray]:
    """Per source and 10 s step, the share of that source's TaskManager aggregate columns with no finite value.

    A TaskManager aggregate (tm_min, tm_max, tm_mean) is NaN at a step exactly when no member series has a finite value
    there, and fold_fit then fills the window statistic with the train NORMAL median. This feature makes every such
    gap explicit, for example a job restart that empties the task metrics on every TaskManager (Review 4 CAM-4).
    """
    return {src: _missing_share(columns, gid_of_sid, lm, rv, src, "tm", exclude) for src in TM_SOURCES}


def broker_missing_fraction(columns: list[ColumnSource], gid_of_sid: np.ndarray, lm: LongMetrics, rv: np.ndarray,
                            exclude: np.ndarray) -> dict[tuple[str, str], np.ndarray]:
    """Per source, broker identity and 10 s step, the share of that broker's columns with no finite value.

    F1 removes the target broker's series while it is down, so its b<n> columns would be median-filled with nothing
    to tell the detector. This is the TaskManager rule of decision 25 applied to brokers.
    """
    return {(src, sc): _missing_share(columns, gid_of_sid, lm, rv, src, sc, exclude)
            for src in BROKER_SOURCES for sc in BROKER_SCOPES}


def brokers_reporting(lm: LongMetrics) -> np.ndarray:
    """Per 10 s step, the number of broker identities with any sample of any feature series."""
    bid = np.array([b if (r == "broker" and b is not None) else -1
                    for r, b in zip(lm.series.role, lm.series.broker_id)], dtype=np.int64)
    rows = bid[lm.sid] >= 0
    if not rows.any():
        return np.zeros(lm.n_steps, dtype=np.float64)
    uniq = np.unique(lm.step[rows].astype(np.int64) * 3 + bid[lm.sid[rows]])
    return np.bincount(uniq // 3, minlength=lm.n_steps).astype(np.float64)


# ----------------------------------------------------------------------------------------------
# 4. window statistics
# ----------------------------------------------------------------------------------------------

@dataclass
class WindowSteps:
    idx: np.ndarray  # (n_win, K) step indices, -1 for padding
    valid: np.ndarray  # (n_win, K) bool
    n_steps: np.ndarray  # (n_win,)


def window_steps(windows: pd.DataFrame, t0_ms: int, n_grid: int) -> WindowSteps:
    """Steps whose instant lies in [start, end) of each window."""
    a = windows["start_ms"].to_numpy(np.int64)
    b = windows["end_ms"].to_numpy(np.int64)
    i0 = -((-(a - t0_ms)) // 10_000)
    i1 = -((-(b - t0_ms)) // 10_000)
    k = i1 - i0
    if (k <= 0).any():
        raise BuildError("a window holds no 10 s step")
    if (i0 < 0).any() or (i1 > n_grid).any():
        raise BuildError("a window lies outside the step grid")
    K = int(k.max())
    idx = i0[:, None] + np.arange(K)[None, :]
    valid = np.arange(K)[None, :] < k[:, None]
    return WindowSteps(np.where(valid, idx, -1), valid, k)


def nan_iqr(G: np.ndarray, n: np.ndarray) -> np.ndarray:
    """75th minus 25th percentile along axis 1, ignoring NaN, with numpy's default linear method.

    Same values as np.nanpercentile(G, [75, 25], axis=1), without its per-slice Python loop.
    """
    S = np.sort(G, axis=1)  # NaN sorts last
    K = G.shape[1]
    nn = n.astype(np.int64)

    def q(frac: float) -> np.ndarray:
        pos = frac * np.maximum(nn - 1, 0)
        lo = np.clip(np.floor(pos).astype(np.int64), 0, K - 1)
        hi = np.clip(np.ceil(pos).astype(np.int64), 0, K - 1)
        vlo = np.take_along_axis(S, lo[:, None, :], axis=1)[:, 0, :]
        vhi = np.take_along_axis(S, hi[:, None, :], axis=1)[:, 0, :]
        return vlo + (vhi - vlo) * (pos - lo)

    out = q(0.75) - q(0.25)
    out[nn == 0] = np.nan
    return out


def window_stats(M: np.ndarray, ws: WindowSteps) -> dict[str, np.ndarray]:
    """Six statistics over the present samples of each window. M is (n_steps, C)."""
    G = M[np.where(ws.valid, ws.idx, 0)]  # (n_win, K, C)
    G = np.where(ws.valid[:, :, None], G, np.nan)
    present = ~np.isnan(G)
    n = present.sum(axis=1).astype(np.float64)
    with np.errstate(invalid="ignore", divide="ignore"), _warnings.catch_warnings():
        _warnings.simplefilter("ignore", category=RuntimeWarning)
        s = np.nansum(G, axis=1)
        mean = np.where(n > 0, s / n, np.nan)
        dev = np.where(present, G - mean[:, None, :], 0.0)
        std = np.where(n > 0, np.sqrt((dev ** 2).sum(axis=1) / n), np.nan)
        mn = np.nanmin(G, axis=1)
        mx = np.nanmax(G, axis=1)
        iqr = nan_iqr(G, n)
        x = np.where(ws.valid, ws.idx, 0).astype(np.float64)[:, :, None] * np.ones_like(G)
        x = np.where(present, x, 0.0)
        xm = np.where(n > 0, x.sum(axis=1) / n, 0.0)
        dx = np.where(present, x - xm[:, None, :], 0.0)
        sxx = (dx ** 2).sum(axis=1)
        sxy = (dx * dev).sum(axis=1)
        slope = np.where((n >= 2) & (sxx > 0), sxy / np.where(sxx > 0, sxx, 1.0), np.nan)
    empty = n == 0
    for arr in (mn, mx, iqr):
        arr[empty] = np.nan
    return {"mean": mean, "std": std, "min": mn, "max": mx, "slope": slope, "iqr": iqr}


# ----------------------------------------------------------------------------------------------
# 3. derived features
# ----------------------------------------------------------------------------------------------

def _reduce_per_step(step: np.ndarray, v: np.ndarray, n_steps: int, how: str) -> np.ndarray:
    ok = np.isfinite(v)
    step, v = step[ok], v[ok]
    out = np.full(n_steps, np.nan)
    if len(v) == 0:
        return out
    if how == "sum":
        cnt = np.bincount(step, minlength=n_steps)
        s = np.bincount(step, weights=v, minlength=n_steps)
        out[cnt > 0] = s[cnt > 0]
        return out
    order = np.argsort(step, kind="stable")
    step, v = step[order], v[order]
    starts = np.flatnonzero(np.r_[True, step[1:] != step[:-1]])
    red = (np.minimum if how == "min" else np.maximum).reduceat(v, starts)
    out[step[starts]] = red
    return out


def derived_step_columns(lm: LongMetrics, rv: np.ndarray, seg_of_step: np.ndarray, cfg: dict
                         ) -> dict[str, np.ndarray]:
    st = lm.series
    n = lm.n_steps
    inputs = {}
    for name, spec in cfg["derived"]["inputs"].items():
        rx = re.compile(spec["metric_regex"])
        match = np.array([st.source[s] == spec["source"] and st.role[s] in spec["roles"] and bool(rx.search(st.metric[s]))
                          for s in range(len(st))], dtype=bool)
        rows = match[lm.sid] if len(st) else np.zeros(0, bool)
        src_vals = lm.value if spec.get("value", "auto") == "raw" else rv
        inputs[name] = _reduce_per_step(lm.step[rows], src_vals[rows], n, spec["reduce"])
    same_seg = np.zeros(n, dtype=bool)
    same_seg[1:] = (seg_of_step[1:] == seg_of_step[:-1]) & (seg_of_step[1:] >= 0)

    def diff10(x: np.ndarray) -> np.ndarray:
        out = np.full(n, np.nan)
        out[1:] = (x[1:] - x[:-1]) / 10.0
        out[~same_seg] = np.nan
        return out

    with np.errstate(invalid="ignore", divide="ignore"):
        lag = inputs["lag"]
        vel = diff10(lag)
        acc = diff10(vel)
        rin, rout = inputs["records_in"], inputs["records_out"]
        thr = np.where(rin > 0, rout / np.where(rin > 0, rin, 1.0), np.nan)
        cpo = inputs["checkpoint_duration_ms"] / float(cfg["derived"]["checkpoint_interval_ms"])
        pcount = inputs["partition_count"]
        denom = 2.0 * np.where(pcount == 0, 1.0, pcount)
        isr = 1.0 - (inputs["isr_shrinks_per_s"] + inputs["isr_expands_per_s"]) * 10.0 / denom
        reb = inputs["rebalances_per_s"]
        bp, busy, idle = inputs["backpressured_ms"], inputs["busy_ms"], inputs["idle_ms"]
        tot = bp + busy + idle
        bpr = np.where(tot > 0, bp / np.where(tot > 0, tot, 1.0), np.nan)
        wm = inputs["watermark_ms"]
        wml = np.where(wm > 0, lm.instants_ms().astype(np.float64) - wm, np.nan)
    return {
        "lag_velocity": vel, "lag_acceleration": acc, "throughput_ratio": thr,
        "checkpoint_overhead_ratio": cpo, "isr_stability": isr, "rebalance_frequency": reb,
        "backpressure_ratio": bpr, "watermark_lag": wml,
    }


# ----------------------------------------------------------------------------------------------
# Main entry
# ----------------------------------------------------------------------------------------------

@dataclass
class FeatureResult:
    per_w: dict[int, dict[str, np.ndarray]]  # column name -> values aligned with windows rows
    report: FeatureReport
    unit_presence: np.ndarray  # (n_steps, U) bool
    units: list[tuple[str, str]]
    step_columns: dict[str, np.ndarray] | None = None  # only when keep_step_columns is set
    tm_count: np.ndarray | None = None
    src_presence: np.ndarray | None = None


@dataclass
class FeatureContext:
    """Everything that does not depend on the window size, computed once per campaign."""
    lm: LongMetrics
    cfg: dict
    report: FeatureReport
    columns: list[ColumnSource]
    windowed_steps: np.ndarray
    src_presence: np.ndarray
    unit_presence: np.ndarray
    units: list[tuple[str, str]]
    tm_count: np.ndarray
    derived: dict[str, np.ndarray]
    tm_missing: dict[str, np.ndarray]
    broker_missing: dict[tuple[str, str], np.ndarray]
    broker_count: np.ndarray
    g_sorted: np.ndarray
    st_sorted: np.ndarray
    v_sorted: np.ndarray
    bounds: np.ndarray


def prepare_features(lm: LongMetrics, tl: Timeline, cfg: dict) -> FeatureContext:
    report = FeatureReport(n_series=len(lm.series))
    n = lm.n_steps
    instants = lm.instants_ms()
    seg_pos = segment_index_at(tl, instants)
    pos = tl.positive()
    seg_types = pos["segment_type"].to_numpy()
    seg_of_step = np.where(seg_pos >= 0, pos["segment_index"].to_numpy()[np.clip(seg_pos, 0, None)], -1)
    in_type = lambda types: (seg_pos >= 0) & np.isin(seg_types[np.clip(seg_pos, 0, None)], types)
    baseline_steps = in_type(["BASELINE"])
    windowed_steps = in_type(["PRE_FAULT", "INJECTION", "RECOVERY", "BASELINE", "TRAILING"])

    is_counter = counter_mask(lm, cfg)
    report.n_counter_series = int(is_counter.sum())
    rv = rate_values(lm, is_counter)

    report.monotone_levels = monotone_levels(lm, is_counter, baseline_steps, cfg)
    if report.monotone_levels and cfg["monotone_guard"]["policy"] == "error":
        raise BuildError(["gauges that only grow over the BASELINE would act as clocks. Add them to "
                          "counters.extra_patterns or monotone_guard.allow_metric_patterns:"]
                         + report.monotone_levels[:20])

    columns, gid_of_sid, unit_of_sid, units = map_columns(lm, is_counter, cfg)
    report.n_tm_groups = sum(1 for c in columns if c.scope_class == "tm")
    report.n_columns_direct = len(columns) - report.n_tm_groups

    # Raw presence per source, per telemetry unit and TaskManagers reporting, per step.
    src_idx = np.array([SRC_INDEX[s] for s in lm.series.source], dtype=np.int64)
    src_presence = np.zeros((n, len(FEATURE_SOURCES)), dtype=bool)
    src_presence[lm.step, src_idx[lm.sid]] = True
    U = len(units)
    unit_presence = np.zeros((n, U), dtype=bool)
    urow = unit_of_sid[lm.sid]
    unit_presence[lm.step[urow >= 0], urow[urow >= 0]] = True
    tm_series = np.array([r == "taskmanager" and p != "" for r, p in zip(lm.series.role, lm.series.pod)], dtype=bool)
    pods = sorted({p for p, t in zip(lm.series.pod, tm_series) if t})
    pod_idx = {p: i for i, p in enumerate(pods)}
    pod_of_sid = np.array([pod_idx.get(p, -1) if t else -1 for p, t in zip(lm.series.pod, tm_series)], dtype=np.int64)
    rows_tm = tm_series[lm.sid]
    tm_count = np.zeros(n, dtype=np.float64)
    if rows_tm.any() and pods:
        flat = lm.step[rows_tm].astype(np.int64) * len(pods) + pod_of_sid[lm.sid[rows_tm]]
        uniq = np.unique(flat)
        tm_count = np.bincount(uniq // len(pods), minlength=n).astype(np.float64)
    derived = derived_step_columns(lm, rv, seg_of_step, cfg)
    miss_excl = missingness_exclusions(columns, cfg)
    tm_missing = tm_missing_fraction(columns, gid_of_sid, lm, rv, miss_excl)
    broker_missing = broker_missing_fraction(columns, gid_of_sid, lm, rv, miss_excl)
    broker_count = brokers_reporting(lm)
    gid_rows = gid_of_sid[lm.sid]
    order = np.argsort(gid_rows, kind="stable")
    g_sorted, st_sorted, v_sorted = gid_rows[order], lm.step[order].astype(np.int64), rv[order]
    bounds = np.searchsorted(g_sorted, np.arange(len(columns) + 1))
    return FeatureContext(lm, cfg, report, columns, windowed_steps, src_presence, unit_presence, units,
                          tm_count, derived, tm_missing, broker_missing, broker_count, g_sorted, st_sorted, v_sorted,
                          bounds)


def compute_features(lm: LongMetrics, tl: Timeline, windows_by_w: dict[int, pd.DataFrame], cfg: dict,
                     keep_step_columns: bool = False, ctx: FeatureContext | None = None) -> FeatureResult:
    ctx = ctx or prepare_features(lm, tl, cfg)
    report, columns, windowed_steps = ctx.report, ctx.columns, ctx.windowed_steps
    report.collisions_outside_windows = 0  # counted again on every pass, so reset
    src_presence, unit_presence, units, tm_count = ctx.src_presence, ctx.unit_presence, ctx.units, ctx.tm_count
    n = lm.n_steps
    wsteps = {w: window_steps(df, lm.t0_ms, n) for w, df in windows_by_w.items()}
    per_w: dict[int, dict[str, np.ndarray]] = {w: {} for w in windows_by_w}
    step_cols: dict[str, np.ndarray] | None = {} if keep_step_columns else None

    def emit(names: list[str], M: np.ndarray) -> None:
        for w, ws in wsteps.items():
            stats = window_stats(M, ws)
            for j, base in enumerate(names):
                for stat in STATS:
                    per_w[w][f"{base}__{stat}"] = stats[stat][:, j]
        if step_cols is not None:
            for j, base in enumerate(names):
                step_cols[base] = M[:, j].copy()

    # Rows grouped by column source.
    g_sorted, st_sorted, v_sorted, bounds = ctx.g_sorted, ctx.st_sorted, ctx.v_sorted, ctx.bounds
    G = len(columns)
    chunk = max(1, int(cfg["feature_chunk_columns"]))
    for g0 in range(0, G, chunk):
        g1 = min(G, g0 + chunk)
        r0, r1 = bounds[g0], bounds[g1]
        local = g_sorted[r0:r1] - g0
        stp = st_sorted[r0:r1]
        val = v_sorted[r0:r1]
        C = g1 - g0
        flat = stp * C + local
        cnt = np.bincount(flat, minlength=n * C).reshape(n, C)
        names: list[str] = []
        mats: list[np.ndarray] = []
        direct = np.full((n, C), np.nan)
        direct.reshape(-1)[flat] = val
        fin = np.isfinite(val)
        ff, fv = flat[fin], val[fin]
        o = np.argsort(ff, kind="stable")
        ff, fv = ff[o], fv[o]
        tmin = np.full(n * C, np.nan)
        tmax = np.full(n * C, np.nan)
        tsum = np.bincount(ff, weights=fv, minlength=n * C)
        tn = np.bincount(ff, minlength=n * C)
        if len(ff):
            starts = np.flatnonzero(np.r_[True, ff[1:] != ff[:-1]])
            tmin[ff[starts]] = np.minimum.reduceat(fv, starts)
            tmax[ff[starts]] = np.maximum.reduceat(fv, starts)
        tmean = np.where(tn > 0, tsum / np.where(tn > 0, tn, 1), np.nan)
        tmin, tmax, tmean = tmin.reshape(n, C), tmax.reshape(n, C), tmean.reshape(n, C)
        for j in range(C):
            col = columns[g0 + j]
            if col.scope_class == "tm":
                for scope, M in (("tm_min", tmin), ("tm_max", tmax), ("tm_mean", tmean)):
                    names.append(f"f__{col.source}__{col.name}__{scope}")
                    mats.append(M[:, j])
            else:
                coll = cnt[:, j] > 1
                if coll.any():
                    if (coll & windowed_steps).any():
                        bad = np.flatnonzero(coll & windowed_steps)[:3]
                        raise BuildError(
                            f"f__{col.source}__{col.name}__{col.scope_class}: series "
                            f"{[lm.series.series_id[s] for s in col.sids][:4]} have samples at the same "
                            f"step inside windowed segments (ts {[(lm.t0_ms + int(b) * 10_000) // 1000 for b in bad]}). "
                            "Two series mapping to one name fail the build (11.9)")
                    report.collisions_outside_windows += int(coll.sum())
                    direct[coll, j] = np.nan
                names.append(f"f__{col.source}__{col.name}__{col.scope_class}")
                mats.append(direct[:, j])
        emit(names, np.column_stack(mats))

    # TaskManagers and brokers reporting, their missingness features (decision 25, extended to brokers) and the
    # derived features.
    derived = ctx.derived
    bkeys = list(ctx.broker_missing)
    extra_names = ["f__flink__taskmanagers_reporting__cluster", "f__broker__brokers_reporting__cluster"] + \
        [f"f__{src}__tm_aggregates_missing_frac__cluster" for src in TM_SOURCES] + \
        [f"f__{src}__broker_series_missing_frac__{sc}" for src, sc in bkeys] + \
        [f"f__derived__{k}__cluster" for k in derived]
    emit(extra_names, np.column_stack([tm_count, ctx.broker_count] + [ctx.tm_missing[src] for src in TM_SOURCES]
                                      + [ctx.broker_missing[k] for k in bkeys] + list(derived.values())))

    # Telemetry meta columns.
    num, den = cfg["telemetry"]["window_min_steps_num"], cfg["telemetry"]["window_min_steps_den"]
    for w, ws in wsteps.items():
        safe = np.where(ws.valid, ws.idx, 0)
        k = ws.n_steps
        tmc = np.where(ws.valid, tm_count[safe], np.nan)
        per_w[w]["meta__tm_reporting_min"] = np.nanmin(tmc, axis=1)
        per_w[w]["meta__tm_reporting_mean"] = np.nanmean(tmc, axis=1)
        ok = np.ones(len(k), dtype=bool)
        for si, sname in enumerate(FEATURE_SOURCES):
            samples = (src_presence[safe, si] & ws.valid).sum(axis=1).astype(np.int32)
            per_w[w][f"meta__samples__{sname}"] = samples
            ok &= samples.astype(np.int64) * den >= num * k
        per_w[w]["meta__telemetry_ok"] = ok
    return FeatureResult(per_w, report, unit_presence, units, step_cols,
                         tm_count if keep_step_columns else None,
                         src_presence if keep_step_columns else None)
