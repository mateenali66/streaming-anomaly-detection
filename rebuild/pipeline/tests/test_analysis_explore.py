"""analysis/explore.py and analysis/cost.py helpers on hand-built cases (protocol 1.5.4, 4.5, 5.9)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from sklearn.metrics import roc_auc_score

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis import explore as X  # noqa: E402


def test_alert_episodes_join_across_at_most_120_s_of_quiet_time():
    start = np.arange(12) * 30.0
    end = start + 30
    al = np.array([1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0], bool)   # 120 s of quiet between windows 1 and 6: joined
    assert [e.tolist() for e in X.alert_episodes(start, end, al)] == [[0, 1, 6, 7]]
    al2 = np.array([1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1], bool)  # gaps of 120 s and 150 s
    assert [e.tolist() for e in X.alert_episodes(start, end, al2)] == [[0, 5], [11]]


def test_replay_classifies_true_recovery_sham_and_false_episodes():
    lab = ["NORMAL"] * 4 + ["ANOMALOUS"] * 3 + ["RECOVERY"] * 3
    f = pd.DataFrame({"start_at": np.arange(10) * 30.0, "label": lab, "attempt_key": ["a"] * 10})
    f["end_at"] = f["start_at"] + 30
    sham = pd.DataFrame({"start_at": 10000 + np.arange(4) * 30.0, "label": ["NORMAL", "SHAM", "SHAM", "SHAM_POST"],
                         "attempt_key": ["s"] * 4})
    sham["end_at"] = sham["start_at"] + 30
    late = pd.DataFrame({"start_at": [20000.0, 30000.0], "label": ["NORMAL", "RECOVERY"], "attempt_key": ["a", "a"]})
    late["end_at"] = late["start_at"] + 30
    fr = pd.concat([f, sham, late], ignore_index=True)
    al = np.zeros(len(fr), bool)
    al[[5, 6, 7, 8]] = True       # inside the fault, running on into recovery
    al[[11]] = True               # sham window
    al[[14]] = True               # an isolated NORMAL window
    al[[15]] = True               # an isolated RECOVERY window
    camp = type("C", (), {"attempts": pd.DataFrame({"onset": [120.0]}, index=["a"])})()
    r = X.replay_block(fr, al, camp)
    assert (r["n_true"], r["n_sham"], r["n_false"], r["n_recovery"]) == (1, 1, 1, 1)
    assert r["fault_runs_detected"] == 1 and r["median_lag_s"] == 30.0
    assert r["monitored_h"] == pytest.approx(len(fr) * 30 / 3600)


def test_rayleigh_matches_a_monte_carlo_null():
    rng = np.random.default_rng(0)
    n = 30
    theta = rng.vonmises(0.0, 0.5, n)
    R, Z, p = X.rayleigh(theta)
    null = np.array([X.rayleigh(rng.uniform(0, 2 * np.pi, n))[1] for _ in range(20000)])
    assert p == pytest.approx(np.mean(null >= Z), abs=0.01)
    assert X.rayleigh(np.linspace(0, 2 * np.pi, 40, endpoint=False))[2] == pytest.approx(1.0)


def test_f1_at_prevalence_matches_a_direct_confusion_matrix():
    tpr, fpr, p, n = 0.8, 0.1, 0.05, 1_000_000
    tp, fp = tpr * p * n, fpr * (1 - p) * n
    fn = (1 - tpr) * p * n
    assert X.f1_at(tpr, fpr, p) == pytest.approx(2 * tp / (2 * tp + fp + fn))


def _block():
    rows = []
    for key, rt, labs in (("a", "F1", ["NORMAL"] * 5 + ["ANOMALOUS"] * 3 + ["RECOVERY"] * 2),
                          ("b", "F4", ["NORMAL"] * 5 + ["ANOMALOUS"] * 3 + ["RECOVERY"] * 2),
                          ("s", "F0", ["NORMAL"] * 5 + ["SHAM"] * 3 + ["SHAM_POST"] * 2)):
        for i, l_ in enumerate(labs):
            rows.append({"attempt_key": key, "run_type": rt, "label": l_, "start_at": float(i)})
    return pd.DataFrame(rows)


def test_three_state_regimes_count_the_right_windows():
    t = _block()
    t["score"] = np.where(t["label"] == "ANOMALOUS", 3.0, np.where(t["label"] == "RECOVERY", 2.0, 1.0))
    r = {x["regime"]: x for x in X.three_state_rows(t, t.copy())}
    assert r["binary"]["n"] == 30 and r["binary"]["prevalence"] == pytest.approx(6 / 30)
    assert r["active"]["n"] == 26 and r["active"]["auc"] == 1.0
    assert r["recovery"]["prevalence"] == pytest.approx(4 / 24) and r["recovery"]["auc"] == 1.0
    assert r["hybrid"]["prevalence"] == pytest.approx(10 / 30)


def test_lofo_refits_the_threshold_without_the_held_out_fault_attempts():
    t = _block()
    t["score"] = 1.0
    t.loc[(t["run_type"] == "F1") & (t["label"] == "ANOMALOUS"), "score"] = 5.0
    t.loc[(t["run_type"] == "F4") & (t["label"] == "ANOMALOUS"), "score"] = 2.0
    cal = t.copy()
    rows = {r["held_out"]: r for r in X.lofo_rows(t, cal, thr_all=2.0)}
    assert rows["F1"]["threshold_lofo"] == 2.0 and rows["F1"]["recall_lofo"] == 1.0
    assert rows["F4"]["threshold_lofo"] == 5.0 and rows["F4"]["recall_lofo"] == 0.0
    assert rows["F4"]["recall_all"] == 1.0


def test_univariate_auc_equals_sklearn():
    rng = np.random.default_rng(1)
    A, B = rng.normal(0, 1, (200, 3)), rng.normal(0.5, 1, (150, 3))
    A[:5, 1] = np.nan
    u = X.univariate_auc(A, B)
    for j in range(3):
        a, b = A[:, j][np.isfinite(A[:, j])], B[:, j]
        v = roc_auc_score(np.r_[np.zeros(len(a)), np.ones(len(b))], np.r_[a, b])
        assert u[j] == pytest.approx(max(v, 1 - v))


def test_drift_auc_separates_shifted_data_and_not_identical_data():
    rng = np.random.default_rng(2)
    A = rng.normal(0, 1, (200, 5))
    assert np.mean(X.drift_auc(A, rng.normal(3, 1, (200, 5)))) > 0.99
    assert 0.3 < np.mean(X.drift_auc(A, rng.normal(0, 1, (200, 5)))) < 0.7


def test_load_epoch_reads_one_epoch_and_reconstructs_the_phase(tmp_path):
    base = tmp_path / "ec-z-final" / "attempts"
    for i in range(3):
        d = base / f"a{i}"
        d.mkdir(parents=True)
        at = 1000.0 + 700 * i
        detail = [json.dumps({"at": at + k, "epoch_at": 100.0, "cycle_phase_frac": ((at + k - 100.0) % 1800) / 1800})
                  for k in (0, 10)]
        pd.DataFrame({"probe": ["loadgen", "loadgen", "tcp"], "detail": detail + [None]}).to_parquet(d / "probes.parquet")
    ep, err = X.load_epoch(tmp_path, "ec-z")
    assert ep == 100.0 and err < 1e-12
    d = base / "a9"
    d.mkdir()
    pd.DataFrame({"probe": ["loadgen"], "detail": [json.dumps({"at": 5.0, "epoch_at": 7.0, "cycle_phase_frac": 0.0})]}) \
        .to_parquet(d / "probes.parquet")
    with pytest.raises(SystemExit, match="cycle epochs"):
        X.load_epoch(tmp_path, "ec-z")


def test_cost_reads_the_ec2_train_times(tmp_path):
    from analysis import cost
    for cid in ["ec-a", "ec-b", "ec-c", "iot-a", "iot-b", "iot-c"]:
        for m, dev in (("IF", "cpu"), ("LSTM_AE", "cuda")):
            for k in (1, 2):
                d = tmp_path / cid / "w30-frozen" / m / f"fold{k}"
                d.mkdir(parents=True)
                (d / f"summary_{m}.json").write_text(json.dumps({"device": dev, "per_fold": [{"train_time_s": k}]}))
        (tmp_path / cid / "w30-frozen" / "IF_jvm").mkdir()
    t = cost.ec2_train_times(tmp_path)
    assert set(t["model"]) == {"IF", "LSTM_AE"} and (t["n_fits"] == 6).all() and (t["median_s"] == 1.5).all()
