"""Protocol 4.1: rates, role aggregation, derived features inside segments, telemetry flags, and
no verification or probe series among the features. Expectations are recomputed here from the raw
exported rows with pandas, independently of streamprep.features."""
from __future__ import annotations

import json
import re

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

from conftest import edit_manifest
from streamprep.build import build_campaign
from streamprep.config import load_config
from streamprep.contract import BuildError, ceil10, floor10, sha256_file
from streamprep.features import counter_mask, excluded_mask, rate_values
from streamprep.loader import load_campaign
from streamprep.metrics_io import LongMetrics, SeriesTable, read_feature_files
from streamprep.timeline import segment_index_at

FEATURE_RE = re.compile(r"^f__(broker|consumer|flink|jvm|derived)__.+__(b0|b1|b2|jm|tm_min|tm_max|tm_mean|cluster)__"
                        r"(mean|std|min|max|slope|iqr)$")


@pytest.fixture(scope="module")
def raw(full):
    """Every exported feature row, deduplicated, as pandas."""
    files = sorted((full.campaign / "baseline" / "features").glob("*.parquet")) + \
        sorted((full.campaign / "export" / "attempts").glob("*/features.parquet")) + \
        sorted((full.campaign / "export" / "trailing" / "features").glob("*.parquet"))
    df = pd.concat([pq.read_table(f).to_pandas() for f in files], ignore_index=True)
    return df.drop_duplicates(["ts", "series_id"]).reset_index(drop=True)


@pytest.fixture(scope="module")
def grid(full):
    t0 = floor10(full.tl.start_ms)
    n = (ceil10(full.tl.end_ms) - t0) // 10_000 + 1
    return t0, n


@pytest.fixture(scope="module")
def lm(full, grid):
    c = load_campaign(full.campaign, full.cfg)
    return read_feature_files(c, grid[0], grid[1], full.cfg)


def _step(ts_s, t0):
    return (np.asarray(ts_s, dtype=np.int64) * 1000 - t0) // 10_000


def test_counter_rates_match_truth_across_resets(full, lm, grid):
    is_c = counter_mask(lm, full.cfg)
    rv = rate_values(lm, is_c)
    truth = pq.read_table(full.truth_dir / "counter_truth.parquet").to_pandas()
    counter_ids = {lm.series.series_id[s] for s in np.flatnonzero(is_c)}
    assert counter_ids == set(truth["series_id"]), "counter detection differs from the generated counters"
    sid_of = {s: i for i, s in enumerate(lm.series.series_id)}
    key = lm.sid.astype(np.int64) * grid[1] + lm.step
    tkey = truth["series_id"].map(sid_of).to_numpy(np.int64) * grid[1] + _step(truth["ts"], grid[0])
    pos = np.searchsorted(key, tkey)
    assert (key[pos] == tkey).all()
    got, exp = rv[pos], truth["expected_rate"].to_numpy()
    assert (np.isnan(got) == np.isnan(exp)).all()
    ok = ~np.isnan(exp)
    assert np.allclose(got[ok], exp[ok], rtol=1e-9, atol=1e-6)
    finite = np.isfinite(rv) & is_c[lm.sid]
    assert (rv[finite] >= 0).all(), "a negative rate survived"
    # The in-place reset in the baseline: that step is dropped, the next one is the true rate.
    r = full.truth["baseline_counter_reset"]
    sel = [s for s in range(len(lm.series)) if lm.series.metric[s] == r["metric"] and lm.series.broker_id[s] == r["broker"]
           and lm.series.labels[s].get("topic") == r["topic"]]
    assert len(sel) == 1
    st = (ceil10(r["at_ms"]) - grid[0]) // 10_000
    rows = np.flatnonzero((lm.sid == sel[0]) & ((lm.step == st) | (lm.step == st + 1) | (lm.step == st - 1)))
    vals = dict(zip(lm.step[rows], rv[rows]))
    raw_vals = dict(zip(lm.step[rows], lm.value[rows]))
    assert raw_vals[st] < raw_vals[st - 1], "the generator did not reset the counter"
    assert np.isnan(vals[st]) and np.isfinite(vals[st + 1]) and vals[st + 1] > 0
    # Broker restarts during F1: no rate across the gap, then rates resume.
    assert full.truth["f1_targets"]


def test_counter_names_become_per_s(full):
    cols = [c for c in full.windows[30].columns if c.startswith("f__")]
    assert all(FEATURE_RE.match(c) for c in cols), [c for c in cols if not FEATURE_RE.match(c)][:3]
    assert not any("_total" in c for c in cols)
    assert "f__broker__kafka_server_brokertopicmetrics_messagesinpersec_per_s__b0__mean" in cols
    assert any(c.startswith("f__flink__flink_taskmanager_job_task_numRecordsIn_per_s.") for c in cols)
    # The gc label is in the fixed naming list (MIN-11), so it is in the name even though it is constant here.
    assert any(c.startswith("f__jvm__jvm_gc_collection_seconds_count_per_s.gc-G1_Young_Generation__b1__") for c in cols)
    assert not any("currentInputWatermark" in c for c in cols), "a clock-like level is a direct feature"
    for name in ("lag_velocity", "lag_acceleration", "throughput_ratio", "checkpoint_overhead_ratio",
                 "isr_stability", "rebalance_frequency", "backpressure_ratio", "watermark_lag"):
        for stat in ("mean", "std", "min", "max", "slope", "iqr"):
            assert f"f__derived__{name}__cluster__{stat}" in cols


def test_role_aggregation_matches_raw_min_max_mean(full, raw, grid):
    sc = full.result.step_columns
    t0, _n = grid
    for job in ("Event Enrichment Job", "Fraud Detection Job"):
        sub = raw[(raw["metric"] == "flink_taskmanager_job_task_busyTimeMsPerSecond") & (raw["role"] == "taskmanager")]
        sub = sub[sub["labels"].map(lambda s: json.loads(s)["job_name"]) == job]
        agg = sub.groupby("ts")["value"].agg(["min", "max", "mean", "count"])
        stem = [k for k in sc if k.startswith("f__flink__flink_taskmanager_job_task_busyTimeMsPerSecond.")
                and f"job_name-{job.replace(' ', '_')}." in k and k.endswith("__tm_min")]
        assert len(stem) == 1, stem
        base = stem[0][: -len("__tm_min")]
        idx = _step(agg.index.to_numpy(), t0)
        for stat, scope in (("min", "tm_min"), ("max", "tm_max"), ("mean", "tm_mean")):
            assert np.allclose(sc[f"{base}__{scope}"][idx], agg[stat].to_numpy())
        assert agg["count"].max() == 3 and agg["count"].min() >= 1


def test_taskmanagers_reporting_counts_pods(full, raw, grid):
    t0, n = grid
    cnt = full.result.step_columns["f__flink__taskmanagers_reporting__cluster"]
    tm = raw[raw["role"] == "taskmanager"].groupby("ts")["pod"].nunique()
    exp = np.zeros(n)
    exp[_step(tm.index.to_numpy(), t0)] = tm.to_numpy()
    assert (cnt == exp).all()
    kills = full.truth["kills"]
    assert len(kills) >= 50
    for k in kills:
        steps = np.arange(ceil10(k["at_ms"] + 1_000), k["at_ms"] + 25_000, 10_000)
        assert len(steps) >= 2
        assert (cnt[(steps - t0) // 10_000] == 2).all(), k
    cols = [c for c in full.windows[30].columns if c.startswith("f__")]
    for token in ("taskmanager-1-", "taskmanager-2-", "task_attempt", "job_id", "subtask_index", "node-", "ip_10"):
        assert not any(token in c for c in cols), token
    assert full.rep["features"]["n_tm_groups"] == 30  # 3 jobs x 9 task metrics + 3 JVM metrics


def test_window_stats_match_independent_computation(full, raw):
    win = full.windows[30]
    sub = raw[(raw["metric"] == "jvm_memory_used_bytes") & (raw["broker_id"] == 0)]
    sub = sub[sub["labels"].map(lambda s: json.loads(s)["area"]) == "heap"].set_index("ts")["value"]
    base = "f__jvm__jvm_memory_used_bytes.area-heap__b0"
    rng = np.random.default_rng(3)
    for i in rng.choice(len(win), 300, replace=False):
        a, b = win["start_at"].iloc[i], win["end_at"].iloc[i]
        steps = np.arange(np.ceil(a / 10) * 10, b, 10).astype(np.int64)
        v = sub.reindex(steps).dropna()
        x = v.index.to_numpy() / 10.0
        y = v.to_numpy()
        got = {s: win[f"{base}__{s}"].iloc[i] for s in ("mean", "std", "min", "max", "slope", "iqr")}
        if len(y) == 0:
            assert all(pd.isna(g) for g in got.values())
            continue
        assert got["mean"] == pytest.approx(y.mean(), rel=1e-12)
        assert got["std"] == pytest.approx(y.std(), rel=1e-9, abs=1e-6)
        assert got["min"] == y.min() and got["max"] == y.max()
        assert got["iqr"] == pytest.approx(np.percentile(y, 75) - np.percentile(y, 25), rel=1e-9, abs=1e-6)
        if len(y) >= 2:
            assert got["slope"] == pytest.approx(np.polyfit(x - x[0], y, 1)[0], rel=1e-9, abs=1e-6)
        else:
            assert pd.isna(got["slope"])


def test_derived_features_never_difference_across_segments(full, raw, grid):
    t0, n = grid
    sc = full.result.step_columns
    vel = sc["f__derived__lag_velocity__cluster"]
    acc = sc["f__derived__lag_acceleration__cluster"]
    lagrows = raw[(raw["metric"].str.endswith("KafkaConsumer_records_lag_max")) & (raw["role"] == "taskmanager")]
    lag = np.full(n, np.nan)
    g = lagrows.groupby("ts")["value"].max()
    lag[_step(g.index.to_numpy(), t0)] = g.to_numpy()
    instants = t0 + np.arange(n, dtype=np.int64) * 10_000
    seg = segment_index_at(full.tl, instants)
    same = np.zeros(n, dtype=bool)
    same[1:] = (seg[1:] == seg[:-1]) & (seg[1:] >= 0)
    exp = np.full(n, np.nan)
    exp[1:] = (lag[1:] - lag[:-1]) / 10.0
    exp[~same] = np.nan
    assert np.array_equal(np.isnan(vel), np.isnan(exp))
    ok = ~np.isnan(exp)
    assert np.allclose(vel[ok], exp[ok])
    first_of_segment = np.flatnonzero(~same)
    assert np.isnan(vel[first_of_segment]).all() and np.isnan(acc[first_of_segment]).all()
    assert np.isnan(acc[np.minimum(first_of_segment + 1, n - 1)]).all()
    # The F4 onset jump (lag 500 to 20,000) would give a velocity near 1,950 if it crossed APPLY.
    pos = full.tl.positive()
    for r in pos[(pos["segment_type"] == "INJECTION") & (pos["run_type"] == "F4")].itertuples():
        i0 = (ceil10(r.start_ms) - t0) // 10_000
        i1 = (ceil10(r.end_ms) - t0) // 10_000
        v = vel[i0:i1]
        assert np.isnan(v[0]) and np.nanmax(np.abs(v)) < 100.0, r.attempt_key


def test_telemetry_meta_matches_sample_presence(full, raw):
    win = full.windows[30]
    present = {s: set(raw.loc[raw["source"] == s, "ts"]) for s in ("broker", "consumer", "flink", "jvm")}
    starts = win["start_at"].to_numpy()
    ends = win["end_at"].to_numpy()
    for i in range(len(win)):
        steps = np.arange(np.ceil(starts[i] / 10) * 10, ends[i], 10).astype(np.int64)
        k = len(steps)
        ok = True
        for s, ts in present.items():
            n_s = sum(int(t) in ts for t in steps)
            assert win[f"meta__samples__{s}"].iloc[i] == n_s
            ok &= 3 * n_s >= 2 * k
        assert bool(win["meta__telemetry_ok"].iloc[i]) == ok


def test_telemetry_flags_set_during_gaps(full):
    win = full.windows[30]
    f5 = win[(win["run_type"] == "F5") & (win["segment_type"] == "INJECTION") & win["outcome"].isin(["VALID"])]
    assert (~f5["meta__telemetry_ok"]).sum() >= 10, "restart gaps after TaskManager kills are not flagged"
    assert (f5["meta__tm_reporting_min"] == 2).sum() >= 20
    base = win[win["segment_type"] == "BASELINE"]
    gap = [g for g in full.truth["gaps"] if g["scope"] == "all"][0]
    hit = base[(base["start_at"] * 1000 < gap["end_ms"]) & (base["end_at"] * 1000 > gap["start_ms"])]
    assert len(hit) >= 1 and (hit["meta__samples__broker"] < 3).all()
    assert (~hit["meta__telemetry_ok"]).any()
    clean = win[(win["segment_type"] == "PRE_FAULT") & (win["run_type"] == "F2")]
    assert clean["meta__telemetry_ok"].all() and (clean["meta__tm_reporting_min"] == 3).all()
    assert full.rep["features"]["telemetry_violations"] == []


def test_verification_and_probe_series_never_reach_features(full, mutable, tmp_path):
    cols = [c for c in full.windows[30].columns if c.startswith("f__")]
    assert not any("activecontrollercount" in c or "container_" in c for c in cols)
    # Rewrite every verification-metric and probe file with different values; features must not move.
    import pyarrow as pa
    m = json.loads((mutable / "manifest" / "manifest.json").read_text())
    for p in list(mutable.rglob("verification.parquet")) + list((mutable / "baseline" / "verification").glob("*.parquet")) \
            + list((mutable / "export" / "trailing" / "verification").glob("*.parquet")):
        t = pq.read_table(p)
        t = t.set_column(t.schema.get_field_index("value"), "value", pa.array(t.column("value").to_numpy() * 1000 + 7))
        pq.write_table(t, p, compression="zstd")
    for p in list(mutable.rglob("probes.parquet")) + list((mutable / "probes").rglob("*.parquet")):
        t = pq.read_table(p)
        t = t.set_column(t.schema.get_field_index("ok_count"), "ok_count", pa.array(np.zeros(t.num_rows, np.int32)))
        pq.write_table(t, p, compression="zstd")

    def fix(mm):
        for a in mm["attempts"]:
            k = a["attempt_key"]
            a["export"]["verification_metrics_sha256"] = sha256_file(mutable / "export" / "attempts" / k / "verification.parquet")
            a["export"]["probes_sha256"] = sha256_file(mutable / "attempts" / k / "probes.parquet")
    edit_manifest(mutable, fix)
    del m
    # The verification channel decides validity and the NORMAL-time exclusions (decisions 21 and 24, CONTRACT_CHANGES C22
    # and C26), never a feature: so the scrape recompute is reported instead of stopping the build, labels may change, and
    # every feature and meta column must stay the same.
    cfg = load_config(overrides={"telemetry": {"feature_recompute": "warn"}})
    rep = build_campaign(mutable, tmp_path / "out", cfg, stages=("features",), window_sizes=[30])
    got = pd.read_parquet(rep["_out"] / "windows_30s.parquet")
    want = full.windows[30]
    keep = ["window_id"] + [c for c in want.columns if c.startswith("f__") or c.startswith("meta__")]
    assert list(got.columns) == list(want.columns)
    pd.testing.assert_frame_equal(got[keep].reset_index(drop=True), want[keep].reset_index(drop=True))


def test_clock_like_gauge_stops_the_build(full, tmp_path):
    cfg = load_config(overrides={"counters": {"exclude_feature_patterns": []}})
    with pytest.raises(BuildError, match="clocks"):
        build_campaign(full.campaign, tmp_path / "out", cfg, stages=("features",), window_sizes=[30])


def test_pilot_exclusions_cover_process_age_pools_and_first_use_meters():
    """Decision 38, from the pilot of 14 October 2026: the JVM pools that grow with process age, the dead-letter topic's
    broker meters and request types off the data path are kept out of the features. Their neighbours stay."""
    rows = [
        ("jvm_memory_used_bytes", {"area": "nonheap"}, True),
        ("jvm_memory_used_bytes", {"area": "heap"}, False),
        ("jvm_memory_pool_used_bytes", {"pool": "Metaspace"}, True),
        ("jvm_memory_pool_committed_bytes", {"pool": "CodeHeap 'non-profiled nmethods'"}, True),
        ("jvm_memory_pool_used_bytes", {"pool": "G1 Old Gen"}, True),
        ("jvm_memory_pool_used_bytes", {"pool": "G1 Eden Space"}, False),
        ("flink_taskmanager_Status_JVM_Memory_Metaspace_Used", {}, True),
        ("flink_taskmanager_Status_JVM_Memory_Heap_Used", {}, False),
        ("kafka_server_brokertopicmetrics_bytesinpersec_total", {"topic": "dead-letter"}, True),
        ("kafka_server_brokertopicmetrics_bytesinpersec_total", {"topic": "raw-events"}, False),
        ("kafka_network_requestmetrics_requestspersec_total", {"request": "FindCoordinator, version=4"}, True),
        ("kafka_network_requestmetrics_requestspersec_total", {"request": "OffsetCommit, version=9"}, True),
        ("kafka_network_requestmetrics_requestspersec_total", {"request": "Produce, version=10"}, False),
        ("kafka_network_requestmetrics_requestspersec_total", {"request": "Fetch, version=17"}, False),
        ("kafka_network_requestmetrics_requestspersec_total", {"request": "FetchConsumer, version=16"}, False),
        ("kafka_network_requestmetrics_requestspersec_total", {"request": "FetchFollower, version=17"}, False),
        ("kafka_network_requestmetrics_requestspersec_total", {"request": "Metadata, version=12"}, False),
    ]
    st = SeriesTable(series_id=[str(i) for i in range(len(rows))], metric=[m for m, _, _ in rows],
                     labels=[lab for _, lab, _ in rows])
    lm = LongMetrics(t0_ms=0, n_steps=0, step=np.array([], dtype=np.int32), sid=np.array([], dtype=np.int32),
                     value=np.array([]), series=st, series_id_form="dict")
    got = excluded_mask(lm, load_config())
    assert got.tolist() == [want for _, _, want in rows]


def test_nonheap_gauge_is_no_feature_and_heap_stays(full):
    cols = full.windows[30].columns
    assert not any("jvm_memory_used_bytes.area-nonheap" in c for c in cols)
    assert any("jvm_memory_used_bytes.area-heap" in c for c in cols)


def test_two_series_on_one_name_stop_the_build(full, tmp_path):
    cfg = load_config()
    cfg["labels"]["drop_before_naming"] = cfg["labels"]["drop_before_naming"] + ["topic"]
    with pytest.raises(BuildError, match="one name"):
        build_campaign(full.campaign, tmp_path / "out", cfg, stages=("features",), window_sizes=[30])


def test_jobmanager_series_keep_one_column_across_the_redeploy(full):
    win = full.windows[30]
    cols = [c for c in win.columns if "lastCheckpointDuration" in c]
    assert len(cols) == 3 * 6  # three jobs, six statistics, no second set after the new JobManager
    red = [a for a in full.manifest["attempts"] if "AFTER_REDEPLOY" in a["flags"]][0]
    after = win[win["start_at"] >= red["started_at"]]
    before = win[win["end_at"] <= red["started_at"]]
    for c in [x for x in cols if x.endswith("__mean")]:
        assert after[c].notna().mean() > 0.9 and before[c].notna().mean() > 0.9, c
