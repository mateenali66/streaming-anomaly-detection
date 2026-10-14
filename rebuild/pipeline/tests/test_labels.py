"""Protocol 3.2 and 3.3: per-segment grids, window counts and labels."""
from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from streamprep.contract import VALID_OUTCOMES

MS = lambda x: int(round(x * 1000))  # noqa: E731


def _attempts(full):
    return {a["attempt_key"]: a for a in full.manifest["attempts"]}


@pytest.mark.parametrize("w", [15, 30, 60, 120])
def test_exact_window_counts_per_segment(full, w):
    win = full.windows[w]
    seg = full.tl.segments
    windowed = seg[seg["segment_type"].isin(["PRE_FAULT", "INJECTION", "RECOVERY", "BASELINE", "TRAILING"])]
    for r in windowed.itertuples(index=False):
        expected = (r.end_ms - r.start_ms) // (w * 1000)
        inside = win[(np.round(win["start_at"] * 1000) >= r.start_ms) & (np.round(win["end_at"] * 1000) <= r.end_ms)
                     & (win["segment_type"] == r.segment_type)]
        assert len(inside) == expected, (r.segment_type, r.attempt_key, len(inside), expected)


def test_known_counts_at_30s(full):
    win = full.windows[30]
    att = _attempts(full)
    for key, a in att.items():
        g = win[win["attempt_key"] == key]
        assert (g["segment_type"] == "PRE_FAULT").sum() == 20, key  # 600 s pre-fault
        if a["applied"] or a["run_type"] == "F0":
            if a["outcome"] != "ABORTED":
                assert (g["segment_type"] == "RECOVERY").sum() == 10, key  # 300 s
    assert (win["segment_type"] == "BASELINE").sum() == 7200 // 30
    assert (win["segment_type"] == "TRAILING").sum() == 3600 // 30


def test_anomalous_windows_equal_span_over_30(full):
    win = full.windows[30]
    seen_types = set()
    for key, a in _attempts(full).items():
        if a["outcome"] not in VALID_OUTCOMES or a["run_type"] == "F0":
            continue
        span_s = (MS(a["labelled_end_at"]) - MS(a["onset_verified_at"])) / 1000
        n = int(((win["attempt_key"] == key) & (win["label"] == "ANOMALOUS")).sum())
        assert n == int(span_s // 30), (key, n, span_s)
        if a["configured_duration_s"] <= span_s < a["configured_duration_s"] + 30:
            assert n == a["configured_duration_s"] // 30, key  # duration/30 when the span is on time
            seen_types.add(a["run_type"])
    assert {"F1", "F2", "F4", "F5", "F7", "F8"} <= seen_types


def test_overrun_extends_injection_and_never_leaks_into_recovery(full):
    win = full.windows[30]
    a = [x for x in full.manifest["attempts"] if "OVERRUN" in x["flags"]][0]
    assert a["outcome"] == "VALID" and a["overrun_s"] == pytest.approx(90.0, abs=0.01)
    g = win[win["attempt_key"] == a["attempt_key"]]
    assert (g["label"] == "ANOMALOUS").sum() == (180 + 90) // 30
    rec = g[g["segment_type"] == "RECOVERY"]
    assert MS(rec["start_at"].min()) == MS(a["labelled_end_at"])
    stuck = [x for x in full.manifest["attempts"] if x["failed_checks"] and "F7_timing_end" in x["failed_checks"]][0]
    assert (win.loc[win["attempt_key"] == stuck["attempt_key"], "label"] == "EXCLUDED").all()


def test_normal_only_from_baseline_and_valid_prefault(full):
    for w, win in full.windows.items():
        normal = win[win["label"] == "NORMAL"]
        ok = (normal["segment_type"] == "BASELINE") | (
            (normal["segment_type"] == "PRE_FAULT") & normal["outcome"].isin(VALID_OUTCOMES))
        assert ok.all(), w
        assert (win.loc[win["segment_type"] == "BASELINE", "label"] == "NORMAL").all()
        valid_pre = (win["segment_type"] == "PRE_FAULT") & win["outcome"].isin(VALID_OUTCOMES)
        assert (win.loc[valid_pre, "label"] == "NORMAL").all()


def test_trailing_is_its_own_label(full):
    for win in full.windows.values():
        tr = win[win["segment_type"] == "TRAILING"]
        assert len(tr) > 0 and (tr["label"] == "TRAILING").all()
        assert (tr["run_type"] == "TRAILING").all() and tr["attempt_key"].isna().all()
        assert (tr["block"] == 0).all() and (tr["position"] == 0).all()


def test_sham_windows_get_their_own_labels(full):
    win = full.windows[30]
    sham = win[win["run_type"] == "F0"]
    valid = sham[sham["outcome"].isin(VALID_OUTCOMES)]
    assert set(valid.loc[valid["segment_type"] == "INJECTION", "label"]) == {"SHAM"}
    assert set(valid.loc[valid["segment_type"] == "RECOVERY", "label"]) == {"SHAM_POST"}
    assert set(valid.loc[valid["segment_type"] == "PRE_FAULT", "label"]) == {"NORMAL"}
    assert not sham["label"].isin(["ANOMALOUS", "RECOVERY"]).any()
    assert valid["attempt_key"].nunique() == 10


@pytest.mark.parametrize("outcome", ["GATE_FAILED", "TELEMETRY_FAILED", "ABORTED"])
def test_failed_attempts_excluded_whole(full, outcome):
    win = full.windows[30]
    keys = [a["attempt_key"] for a in full.manifest["attempts"] if a["outcome"] == outcome]
    assert keys
    for k in keys:
        g = win[win["attempt_key"] == k]
        assert len(g) > 0 and (g["label"] == "EXCLUDED").all(), k


def test_retried_attempt_excluded_and_retry_labelled(full):
    win = full.windows[30]
    by_cell = {}
    for a in full.manifest["attempts"]:
        by_cell.setdefault(a["cell_key"], []).append(a)
    retried = {c: v for c, v in by_cell.items() if len(v) > 1}
    assert len(retried) == 5
    for cell, atts in retried.items():
        for a in atts[:-1]:
            assert (win.loc[win["attempt_key"] == a["attempt_key"], "label"] == "EXCLUDED").all()
        last = atts[-1]
        assert last["outcome"] in VALID_OUTCOMES
        assert (win.loc[(win["attempt_key"] == last["attempt_key"]) & (win["segment_type"] == "PRE_FAULT"),
                        "label"] == "NORMAL").all()


@pytest.mark.parametrize("w", [15, 30, 60, 120])
def test_grids_anchor_at_their_segment_and_never_straddle(full, w):
    win = full.windows[w]
    seg = full.tl.segments
    wms = w * 1000
    s = np.round(win["start_at"].to_numpy() * 1000).astype(np.int64)
    e = np.round(win["end_at"].to_numpy() * 1000).astype(np.int64)
    assert ((e - s) == wms).all()
    pos = seg[seg["end_ms"] > seg["start_ms"]]
    starts, ends = pos["start_ms"].to_numpy(), pos["end_ms"].to_numpy()
    i = np.searchsorted(starts, s, side="right") - 1
    j = np.searchsorted(starts, e - 1, side="right") - 1
    assert (i == j).all(), "a window straddles a segment boundary"
    assert (e <= ends[i]).all() and (s >= starts[i]).all()
    assert (pos["segment_type"].to_numpy()[i] == win["segment_type"].to_numpy()).all()
    for key, a in _attempts(full).items():
        g = win[win["attempt_key"] == key]
        gs = np.round(g["start_at"].to_numpy() * 1000).astype(np.int64)
        ge = np.round(g["end_at"].to_numpy() * 1000).astype(np.int64)
        st = g["segment_type"].to_numpy()
        if a["onset_verified_at"] is not None:
            inj = gs[st == "INJECTION"]
            assert ((inj - MS(a["onset_verified_at"])) % wms == 0).all(), key
            if len(inj):
                assert inj.min() == MS(a["onset_verified_at"]), key
        pre_end = ge[st == "PRE_FAULT"]
        if len(pre_end):
            assert pre_end.max() == MS(a["planned_onset_at"]), key
            assert ((MS(a["planned_onset_at"]) - pre_end) % wms == 0).all(), key
        if a["labelled_end_at"] is not None:
            rec = gs[st == "RECOVERY"]
            if len(rec):
                assert rec.min() == MS(a["labelled_end_at"]) and ((rec - rec.min()) % wms == 0).all(), key
    for typ in ("BASELINE", "TRAILING"):
        sg = seg[seg["segment_type"] == typ].iloc[0]
        g = s[win["segment_type"].to_numpy() == typ]
        assert g.min() == sg.start_ms and ((g - sg.start_ms) % wms == 0).all()


def test_window_meta_columns(full):
    win = full.windows[30]
    assert win["window_id"].is_unique
    ms = np.round(win["start_at"] * 1000).astype(np.int64).astype(str)
    assert (win["window_id"] == "ec-a-30-" + ms).all()
    att = win[win["attempt_key"].notna()]
    assert (att["seq_group"] == att["attempt_key"]).all()
    assert (att["cell_key"] == att["attempt_key"].str.rsplit("-a", n=1).str[0]).all()
    base = win[win["segment_type"] == "BASELINE"]
    assert (base["seq_group"] == "baseline-0").all() and base["run_id"].isna().all()
    assert (base["flags"] == "[]").all() and base["outcome"].isna().all()
    for g, grp in win.groupby("seq_group"):
        assert list(grp["seq_index"]) == list(range(len(grp))), g
    flagged = win[win["flags"] != "[]"]
    assert set(pd.Series([f for x in flagged["flags"] for f in json.loads(x)])) >= {
        "OVERRUN", "REMEDIATED", "AFTER_REDEPLOY", "HEALTH_EVENT", "F7_TM_LOSS"}


# ---------------------------------------------------------------------------------------------------------------------
# Review 4: the NORMAL-time health rule (decision 24, CONTRACT_CHANGES C26) and the analysis's false-pass flag (C23)
def _edit_probe_chunk(campaign, prefix, fn):
    """Rewrite the probe chunk of `prefix` (probes/baseline or probes/trailing) that holds the most samples, then rehash."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from streamprep.synthetic import rehash_campaign
    path = max((campaign / prefix).glob("chunk-*.parquet"), key=lambda p: pq.read_metadata(p).num_rows)
    t = pq.read_table(path).to_pylist()
    t, at = fn(t)
    pq.write_table(pa.Table.from_pylist(t, schema=pq.read_schema(path)), path, compression="zstd")
    rehash_campaign(campaign)
    return at


def _build_labels(campaign, tmp_path):
    from streamprep.build import build_campaign
    from streamprep.config import load_config
    rep = build_campaign(campaign, tmp_path / "out", load_config(), stages=("labels",), window_sizes=[30])
    return rep, pd.read_parquet(rep["_out"] / "labels_30s.parquet")


def test_full_campaign_baseline_and_trailing_are_healthy(full):
    rep = full.rep["normal_time_health"]["30"]
    assert rep["BASELINE"]["excluded"] == 0 and rep["TRAILING"]["excluded"] == 0 and rep["BASELINE"]["windows"] > 0


def test_baseline_window_with_a_job_restart_is_excluded(mutable, tmp_path):
    """A Flink job RESTARTING in two probe samples of the BASELINE: the windows whose evidence (the window plus 10 s on each
    side) holds those samples are EXCLUDED, every other BASELINE window stays NORMAL, and the count is reported."""
    def restart(rows):
        jobs = sorted((r for r in rows if r["probe"] == "flink_jobs"), key=lambda r: r["ts"])
        mid = jobs[len(jobs) // 2]["ts"]
        for r in rows:
            if r["probe"] == "flink_jobs" and mid <= r["ts"] < mid + 15:
                js = json.loads(r["detail"])
                js[0]["state"] = "RESTARTING"
                r["detail"] = json.dumps(js)
        return rows, mid
    at = _edit_probe_chunk(mutable, "probes/baseline", restart)
    rep, lab = _build_labels(mutable, tmp_path)
    base = lab[lab["segment_type"] == "BASELINE"]
    ex = base[base["label"] == "EXCLUDED"]
    assert 1 <= len(ex) <= 3
    assert ((ex["start_at"] - 10 <= at + 15) & (ex["end_at"] + 10 > at)).all()
    assert (base.drop(ex.index)["label"] == "NORMAL").all()
    h = rep["normal_time_health"]["30"]["BASELINE"]
    assert h["excluded"] == len(ex) and h["failed_conditions"] == {"jobs_running_same_id": len(ex)}


def test_trailing_window_with_a_failed_connect_is_excluded(mutable, tmp_path):
    """One failed TCP connect of one broker pair in the TRAILING: per pair (1.6.8 at window grain) the window falls below
    95% and is EXCLUDED. A failed kubectl exec in another sample is a missing sample and excludes nothing (REVIEW4 L7)."""
    def fail(rows):
        tcp = sorted((r for r in rows if r["probe"] == "tcp" and r["dst"].endswith(":9091")), key=lambda r: r["ts"])
        hit, other = tcp[len(tcp) // 2], tcp[len(tcp) // 4]
        hit["ok_count"], hit["error"] = 0, "CONNECT_FAILED"
        other["ok_count"], other["error"] = 0, "EXEC rc=1 error: unable to upgrade connection"
        return rows, hit["ts"]
    at = _edit_probe_chunk(mutable, "probes/trailing", fail)
    rep, lab = _build_labels(mutable, tmp_path)
    tr = lab[lab["segment_type"] == "TRAILING"]
    ex = tr[tr["label"] == "EXCLUDED"]
    assert 1 <= len(ex) <= 2 and ((ex["start_at"] - 10 <= at) & (ex["end_at"] + 10 > at)).all()
    assert (tr.drop(ex.index)["label"] == "TRAILING").all()
    assert rep["normal_time_health"]["30"]["TRAILING"]["failed_conditions"] == {"tcp_ok": len(ex)}


def test_baseline_window_with_a_replaced_pod_is_excluded(mutable, tmp_path):
    """kube-state shows a TaskManager with another UID at one step: a pod replaced inside the BASELINE."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from streamprep.synthetic import rehash_campaign
    path = max((mutable / "baseline" / "verification").glob("chunk-*.parquet"), key=lambda p: pq.read_metadata(p).num_rows)
    rows = pq.read_table(path).to_pylist()
    info = sorted((r for r in rows if r["metric"] == "kube_pod_info" and r["role"] == "taskmanager"), key=lambda r: r["ts"])
    at = info[len(info) // 2]["ts"]
    for r in rows:
        if r["metric"] == "kube_pod_info" and r["pod"] == info[0]["pod"] and r["ts"] == at:
            r["pod_uid"] = "00000000-0000-4000-8000-000000000001"
    pq.write_table(pa.Table.from_pylist(rows, schema=pq.read_schema(path)), path, compression="zstd")
    rehash_campaign(mutable)
    rep, lab = _build_labels(mutable, tmp_path)
    base = lab[lab["segment_type"] == "BASELINE"]
    ex = base[base["label"] == "EXCLUDED"]
    assert 1 <= len(ex) <= 3 and rep["normal_time_health"]["30"]["BASELINE"]["failed_conditions"] == {"no_pod_change": len(ex)}


@pytest.mark.parametrize("n_false_passes,flagged", [(1, False), (2, True)])
def test_fault_runs_flagged_after_two_sham_false_passes(full, n_false_passes, flagged):
    """2.5 with decision 22 (C23): the analysis counts false passes over every F0 attempt that has a record, and flags every
    window of the fault's runs after two. The orchestrator never writes the flag."""
    import copy
    from streamprep.loader import load_campaign
    from streamprep.timeline import build_timeline
    from streamprep.windows import build_windows
    c = load_campaign(full.campaign, full.cfg)
    shams = [a["attempt_key"] for a in c.attempts if a["run_type"] == "F0"][:n_false_passes]
    c.verifications = copy.deepcopy(c.verifications)
    for k in shams:
        next(s for s in c.verifications[k]["signatures_all_faults"] if s["run_type"] == "F7")["pass"] = True
    win = build_windows(build_timeline(c), c.campaign_id, c.workload, 30)
    f7 = win[win["run_type"] == "F7"]
    others = win[win["run_type"] != "F7"]
    assert all(("SIGNATURE_FALSE_PASS" in json.loads(x)) == flagged for x in f7["flags"])
    assert not any("SIGNATURE_FALSE_PASS" in json.loads(x) for x in others["flags"])
