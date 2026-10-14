"""Review 4 CAM-4 and decision 25: the per-fold filters never remove the TaskManager count, the derived features or
the named event counters, constant columns keep unit scale, and TaskManager aggregates carry explicit missingness
features, so a lost TaskManager is never median-imputed silently (protocol 3.4.4, 4.1.3, 4.1.5)."""
from __future__ import annotations

import importlib.util
import os
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from streamprep.config import load_config
from streamprep.fold_fit import FoldTransform, feature_columns

NEW = Path(__file__).resolve().parents[1] / "training" / "train.py"
DERIVED = ("lag_velocity", "lag_acceleration", "throughput_ratio", "checkpoint_overhead_ratio", "isr_stability",
           "rebalance_frequency", "backpressure_ratio", "watermark_lag")
STATS = ("mean", "std", "min", "max", "slope", "iqr")


@pytest.fixture(scope="module")
def train_mod():
    os.environ["STREAMPREP_DEVICE"] = "cpu"
    spec = importlib.util.spec_from_file_location("rebuild_train_sel", NEW)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def fold1(full, train_mod):
    """Fold 1 prepared exactly as train.py prepares it, with the default training configuration."""
    w = full.windows[30].sort_values("start_at", kind="stable").reset_index(drop=True)
    cfg = train_mod.load_training_config(None)
    cols = feature_columns(w)
    return w, cols, train_mod.prepare_fold_data(w, full.folds, 1, cols, cfg)


def test_tm_count_derived_and_event_counters_survive_selection(fold1):
    w, cols, d = fold1
    sel = set(d["transform"].selected)
    tm = [f"f__flink__taskmanagers_reporting__cluster__{s}" for s in STATS]
    assert set(tm) <= sel, sorted(set(tm) - sel)
    derived = [f"f__derived__{n}__cluster__{s}" for n in DERIVED for s in STATS]
    normal = w.loc[d["masks"]["normal"]]
    all_nan = {c for c in derived if normal[c].isna().all()}
    assert not all_nan, sorted(all_nan)
    assert set(derived) <= sel, sorted(set(derived) - sel)
    events = [c for c in cols if re.match(r"^f__broker__kafka_server_replicamanager_(underreplicatedpartitions|"
                                          r"isrshrinks_per_s|isrexpands_per_s)__b[012]__", c)]
    assert len(events) == 3 * 3 * 6
    assert set(events) <= sel, sorted(set(events) - sel)[:5]
    # The review's probe: these columns are constant or nearly so on train NORMAL, and the R1 filter dropped them.
    assert normal["f__flink__taskmanagers_reporting__cluster__max"].var() < 0.01
    rep = d["transform"].report
    assert rep["protected_kept"] >= len(tm) + len(derived) + len(events)
    # Patterns for series the synthetic campaign does not export are listed, never silently ignored.
    assert isinstance(rep["protected_patterns_unmatched"], list)
    assert "^f__derived__" not in rep["protected_patterns_unmatched"]


def test_constant_columns_keep_unit_scale(fold1):
    w, cols, d = fold1
    t = d["transform"]
    c = "f__flink__taskmanagers_reporting__cluster__max"
    j = t.selected.index(c)
    normal = w.loc[d["masks"]["normal"], c]
    assert normal.nunique() == 1 and normal.iloc[0] == 3.0
    assert t.scales[j] == 1.0 and t.means[j] == 3.0
    probe = w.iloc[:3][t.selected].copy()
    probe[c] = [3.0, 2.0, 0.0]
    X = t.transform(probe)
    assert X[:, j].tolist() == [0.0, -1.0, -3.0]
    # Every column that is constant on train NORMAL has unit scale and maps train NORMAL to exactly 0, so rounding in
    # the mean can never turn a constant into a division by 1e-17.
    normal_all = w.loc[d["masks"]["normal"], t.selected]
    exact = [k for k, c in enumerate(t.selected) if normal_all[c].dropna().nunique() == 1]
    assert j in exact and len(exact) >= 20
    Xn = t.transform(normal_all)
    assert all(t.scales[k] == 1.0 for k in exact)
    assert np.array_equal(Xn[:, exact], np.zeros((len(Xn), len(exact)), dtype=np.float32))


def test_tm_missingness_is_a_feature_and_never_silent(full, fold1):
    w, cols, d = fold1
    sel = set(d["transform"].selected)
    for src in ("consumer", "flink", "jvm"):
        for s in STATS:
            assert f"f__{src}__tm_aggregates_missing_frac__cluster__{s}" in cols
            assert f"f__{src}__tm_aggregates_missing_frac__cluster__{s}" in sel
    # Every window where a TaskManager aggregate column of a source is entirely missing (so it is median-imputed)
    # carries a positive missingness value for that source.
    for src in ("consumer", "flink", "jvm"):
        tmcols = [c for c in cols if c.startswith(f"f__{src}__") and re.search(r"__tm_(min|max|mean)__mean$", c)]
        assert tmcols, src
        imputed = w[tmcols].isna().any(axis=1).to_numpy()
        miss = w[f"f__{src}__tm_aggregates_missing_frac__cluster__max"].to_numpy()
        assert (miss[imputed] > 0).all(), (src, int((miss[imputed] <= 0).sum()))
    # F5 restarts empty the task metrics: those windows exist, and the detector sees the missingness after scaling.
    f5 = (w["run_type"] == "F5") & (w["label"] == "ANOMALOUS")
    hit = f5 & w[[c for c in cols if c.startswith("f__flink__flink_taskmanager_job_task_") and c.endswith("__tm_mean__mean")]
                  ].isna().any(axis=1)
    assert hit.sum() >= 5
    t = d["transform"]
    j = t.selected.index("f__flink__tm_aggregates_missing_frac__cluster__max")
    X = t.transform(w.loc[hit])
    assert (X[:, j] > 0).all()
    # In healthy pre-fault windows of a clean run nothing is missing.
    clean = w[(w["segment_type"] == "PRE_FAULT") & (w["run_type"] == "F2")]
    assert (clean["f__flink__tm_aggregates_missing_frac__cluster__max"] == 0).all()


def test_protected_columns_bypass_both_filters_on_a_small_table():
    rng = np.random.default_rng(4)
    a = rng.normal(0, 10, 300)
    df = pd.DataFrame({
        "f__x__a__b0__mean": a,
        "f__derived__lag_velocity__cluster__mean": a * 3 + 0.001 * rng.normal(size=300),  # correlated, protected
        "f__flink__taskmanagers_reporting__cluster__min": np.full(300, 3.0),  # constant, protected
        "f__x__flat__b0__mean": np.full(300, 3.0),  # constant, unprotected: still removed
    })
    pats = [p["regex"] for p in load_config()["fold_fit"]["protected"]["patterns"]]
    t = FoldTransform(protected_patterns=pats).fit(df)
    assert "f__derived__lag_velocity__cluster__mean" in t.selected
    assert "f__x__a__b0__mean" in t.selected  # the correlation filter does not use a protected column against others
    assert "f__flink__taskmanagers_reporting__cluster__min" in t.selected
    assert "f__x__flat__b0__mean" not in t.selected
    j = t.selected.index("f__flink__taskmanagers_reporting__cluster__min")
    assert t.scales[j] == 1.0
    # With no protection the R1 rules apply unchanged.
    r1 = FoldTransform().fit(df)
    assert "f__flink__taskmanagers_reporting__cluster__min" not in r1.selected


def test_down_broker_carries_missingness_and_it_survives_both_filters(full, fold1):
    """Decision 25 extended to brokers (coordinator follow-up, author to confirm): F1 removes the target broker's series,
    and fold_fit would fill its b<n> columns with train medians. The per-broker missingness features and the count of
    brokers reporting make that explicit, and they are protected."""
    w, cols, d = fold1
    t = d["transform"]
    sel = set(t.selected)
    names = [f"f__{src}__broker_series_missing_frac__{b}__{s}" for src in ("broker", "jvm") for b in ("b0", "b1", "b2")
             for s in STATS] + [f"f__broker__brokers_reporting__cluster__{s}" for s in STATS]
    assert set(names) <= set(cols), sorted(set(names) - set(cols))[:4]
    assert set(names) <= sel, sorted(set(names) - sel)[:4]
    gaps = [g for g in full.truth["gaps"] if g["scope"].startswith("broker-") and g.get("attempt") in full.truth["f1_targets"]]
    valid = set(w.loc[w["outcome"].isin(["VALID", "VALID_REMEDIATED"]), "attempt_key"])
    checked = 0
    for g in gaps:
        if g["attempt"] not in valid:
            continue
        b = int(g["scope"].split("-")[1])
        hit = (w["attempt_key"] == g["attempt"]) & (w["label"] == "ANOMALOUS") & \
            (w["start_at"] * 1000 < g["end_ms"]) & (w["end_at"] * 1000 > g["start_ms"])
        assert hit.sum() >= 2, g
        for src in ("broker", "jvm"):
            assert (w.loc[hit, f"f__{src}__broker_series_missing_frac__b{b}__max"] > 0).all(), (g, src)
            for other in {0, 1, 2} - {b}:  # the other brokers kept reporting
                assert (w.loc[hit, f"f__{src}__broker_series_missing_frac__b{other}__max"] == 0).all(), (g, src, other)
        # A window that ends just after the gap can hold the broker's first sample again (count 3) while its counter
        # rates are still undefined (missingness above 0), so the count drops in most hit windows, never all.
        cnt = w.loc[hit, "f__broker__brokers_reporting__cluster__min"]
        assert (cnt >= 2).all() and (cnt == 2).sum() >= 2, g
        # The down broker's own columns are median-filled in these windows, and the detector sees the missingness.
        own = [c for c in t.selected if c.startswith("f__broker__") and f"__b{b}__" in c
               and "_missing_frac__" not in c]
        assert w.loc[hit, own].isna().all(axis=1).any(), g
        j = t.selected.index(f"f__broker__broker_series_missing_frac__b{b}__max")
        assert (t.transform(w.loc[hit])[:, j] > 0).all(), g
        checked += 1
    assert checked >= 5
    clean = w[(w["segment_type"] == "PRE_FAULT") & (w["run_type"] == "F2") & (w["label"] == "NORMAL")]
    assert (clean["f__broker__brokers_reporting__cluster__min"] == 3).all()
    assert (clean[[f"f__broker__broker_series_missing_frac__b{b}__max" for b in range(3)]] == 0).all().all()


def test_a_series_that_first_appears_mid_campaign_is_not_missing_before_it_exists():
    """A column counts as expected only from its first finite value on. Kafka creates per-topic broker metrics on first
    use (the simulated cluster's dead-letter series appear with the first F6 record), so counting them as missing
    before then would make the missingness level step at that time in NORMAL windows."""
    import numpy as np
    from streamprep.features import ColumnSource, _missing_share
    from streamprep.metrics_io import LongMetrics, SeriesTable
    st = SeriesTable()
    for i, m in enumerate(("a", "late")):
        st.series_id.append(f"{i:016x}"); st.metric.append(m); st.metric_type.append("gauge"); st.source.append("broker")
        st.role.append("broker"); st.pod.append("streaming-kafka-brokers-0"); st.broker_id.append(0)
        st.container.append(None); st.labels.append({}); st.labels_str.append("{}")
    # Series 0 reports at steps 0..9 except 6 and 7 (a broker outage). Series 1 first appears at step 4.
    steps = [s for s in range(10) if s not in (6, 7)]
    step = np.array(steps + [s for s in range(4, 10) if s not in (6, 7)], dtype=np.int32)
    sid = np.array([0] * len(steps) + [1] * (len(step) - len(steps)), dtype=np.int32)
    lm = LongMetrics(0, 10, step, sid, np.ones(len(step)), st, "string")
    cols = [ColumnSource("broker", "a", "b0", [0], "{}"), ColumnSource("broker", "late", "b0", [1], "{}")]
    share = _missing_share(cols, np.array([0, 1]), lm, np.ones(len(step)), "broker", "b0", np.zeros(2, bool))
    assert share.tolist() == [0, 0, 0, 0, 0, 0, 1, 1, 0, 0]


def test_lazily_created_meters_do_not_enter_the_missingness_share():
    """Kafka opens a topic's broker meters at the first append to a partition the broker leads and closes them when it
    loses leadership (REVIEW4_LIVE_CONTRACT L3). The dead-letter topic is written only during F6, so after an F1 or F2
    its meters on the restarted broker stay closed until the next F6. Counting them would carry fault history into later
    NORMAL windows. Columns matching missingness.exclude_column_regex are left out of the share."""
    import numpy as np
    from streamprep.features import ColumnSource, _missing_share, missingness_exclusions
    from streamprep.metrics_io import LongMetrics, SeriesTable
    cfg = load_config()
    assert any("dead_letter" in p for p in cfg["missingness"]["exclude_column_regex"])
    st = SeriesTable()
    for i in range(2):
        st.series_id.append(f"{i:016x}"); st.metric.append("m"); st.metric_type.append("gauge"); st.source.append("broker")
        st.role.append("broker"); st.pod.append("streaming-kafka-brokers-0"); st.broker_id.append(0)
        st.container.append(None); st.labels.append({}); st.labels_str.append("{}")
    # Series 0 reports throughout. Series 1 (a dead-letter meter) reports at steps 0..2 and closes at step 3.
    step = np.array(list(range(8)) + [0, 1, 2], dtype=np.int32)
    sid = np.array([0] * 8 + [1] * 3, dtype=np.int32)
    lm = LongMetrics(0, 8, step, sid, np.ones(len(step)), st, "string")
    cols = [ColumnSource("broker", "kafka_server_replicamanager_partitioncount", "b0", [0], "{}"),
            ColumnSource("broker", "kafka_server_brokertopicmetrics_messagesinpersec_per_s.topic-dead_letter", "b0", [1], "{}")]
    excl = missingness_exclusions(cols, cfg)
    assert excl.tolist() == [False, True]
    share = _missing_share(cols, np.array([0, 1]), lm, np.ones(len(step)), "broker", "b0", excl)
    assert share.tolist() == [0.0] * 8
    share_all = _missing_share(cols, np.array([0, 1]), lm, np.ones(len(step)), "broker", "b0", np.zeros(2, bool))
    assert share_all.tolist() == [0, 0, 0, 0.5, 0.5, 0.5, 0.5, 0.5]
