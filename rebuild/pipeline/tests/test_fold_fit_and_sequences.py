"""Protocol 4.1.4 (selection, imputation and scaling fitted per fold on train NORMAL only) and
4.1.7 (contiguous sequences)."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from streamprep.fold_fit import FoldTransform, feature_columns, train_normal_mask
from streamprep.sequences import check_contiguous, sequence_index


@pytest.fixture(scope="module")
def w30(full):
    return full.windows[30]


def _roles(full, w, fold):
    f = full.folds[full.folds["fold"] == fold].set_index("window_id")["role"]
    return w["window_id"].map(f)


def test_selection_uses_only_train_normal(full, w30):
    cols = feature_columns(w30)
    role = _roles(full, w30, 1)
    m = train_normal_mask(role, w30["label"])
    t1 = FoldTransform().fit(w30.loc[m, cols])
    # Overwrite every other window with noise. The fitted transform must not change.
    noisy = w30.copy()
    rng = np.random.default_rng(0)
    other = ~m
    noisy.loc[other, cols] = rng.normal(0, 1e6, size=(int(other.sum()), len(cols)))
    t2 = FoldTransform().fit(noisy.loc[m, cols])
    assert t1.selected == t2.selected
    assert np.array_equal(t1.medians, t2.medians) and np.array_equal(t1.means, t2.means)
    assert np.array_equal(t1.scales, t2.scales)
    assert set(w30.loc[m, "label"]) == {"NORMAL"} and set(role[m]) == {"train"}
    assert set(w30.loc[m, "segment_type"]) == {"BASELINE", "PRE_FAULT"}


def test_selection_is_refitted_per_fold(full, w30):
    cols = feature_columns(w30)
    fits = []
    for fold in (1, 2, 3):
        role = _roles(full, w30, fold)
        m = train_normal_mask(role, w30["label"])
        fits.append(FoldTransform().fit(w30.loc[m, cols]))
    assert not np.array_equal(fits[0].means[:5], fits[1].means[:5]) or fits[0].selected != fits[1].selected


def test_selection_rules_on_a_small_table():
    rng = np.random.default_rng(1)
    a = rng.normal(0, 10, 200)
    df = pd.DataFrame({"f__x__a__b0__mean": a, "f__x__dup__b0__mean": a * 2 + 0.001 * rng.normal(size=200),
                       "f__x__flat__b0__mean": np.full(200, 3.0), "f__x__nan__b0__mean": np.nan,
                       "f__x__b__b0__mean": rng.normal(0, 5, 200)})
    t = FoldTransform().fit(df)
    assert "f__x__flat__b0__mean" not in t.selected and "f__x__nan__b0__mean" not in t.selected
    assert ("f__x__a__b0__mean" in t.selected) != ("f__x__dup__b0__mean" in t.selected)
    assert "f__x__dup__b0__mean" in t.selected  # the higher-variance member of the pair stays
    X = t.transform(df)
    assert np.allclose(X.mean(axis=0), 0, atol=1e-5)


@pytest.mark.parametrize("length", [5, 11, 20])
def test_training_sequences_are_contiguous_normal_windows(full, w30, length):
    role = _roles(full, w30, 1)
    m = train_normal_mask(role, w30["label"])
    sub = w30[m].reset_index(drop=True)
    members, ends = sequence_index(sub["seq_group"].to_numpy(), sub["seq_index"].to_numpy(),
                                   np.ones(len(sub), bool), np.ones(len(sub), bool), length)
    assert len(members) > 0
    check_contiguous(sub["seq_group"].to_numpy(), sub["seq_index"].to_numpy(), members)
    s = sub["start_at"].to_numpy()[members]
    assert np.allclose(np.diff(s, axis=1), 30.0), "a training sequence crosses a gap"
    assert set(sub["segment_type"].to_numpy()[members].ravel()) <= {"BASELINE", "PRE_FAULT"}
    # A PRE_FAULT stretch has 20 windows, so a length-L run fits 21 - L sequences per attempt.
    per = pd.Series(sub["seq_group"].to_numpy()[ends]).value_counts()
    attempts = per[~per.index.str.startswith("baseline")]
    assert (attempts == 21 - length).all()


def test_eval_sequences_cross_only_the_apply_gap(full, w30):
    role = _roles(full, w30, 1)
    m = (role == "test") & w30["label"].isin(["NORMAL", "ANOMALOUS", "RECOVERY", "SHAM", "SHAM_POST"])
    sub = w30[m].reset_index(drop=True)
    members, ends = sequence_index(sub["seq_group"].to_numpy(), sub["seq_index"].to_numpy(),
                                   np.ones(len(sub), bool), np.ones(len(sub), bool), 20)
    check_contiguous(sub["seq_group"].to_numpy(), sub["seq_index"].to_numpy(), members)
    seg = sub["segment_type"].to_numpy()[members]
    gap = sub["start_at"].to_numpy()[members][:, 1:] - sub["end_at"].to_numpy()[members][:, :-1]
    pair = np.char.add(np.char.add(seg[:, :-1].astype(str), ">"), seg[:, 1:].astype(str))
    assert (gap >= -1e-6).all() and (gap <= 30.0 + 1e-6).all()
    crossing = gap > 1e-6
    assert set(np.unique(pair[crossing])) <= {"PRE_FAULT>INJECTION", "INJECTION>RECOVERY"}
    assert (gap[pair == "PRE_FAULT>INJECTION"] <= 30.0).all()  # the APPLY gap, at most apply_max
    # With a 600 s pre-fault, length 20 reaches the first ANOMALOUS window of every valid fault run.
    labels = sub["label"].to_numpy()
    first_anom = sub[sub["label"] == "ANOMALOUS"].groupby("attempt_key")["seq_index"].min()
    end_keys = set(zip(sub["attempt_key"].to_numpy()[ends], sub["seq_index"].to_numpy()[ends]))
    assert all((k, i) in end_keys for k, i in first_anom.items())
    assert labels[ends].tolist().count("ANOMALOUS") == (labels == "ANOMALOUS").sum()


def test_sequences_respect_the_member_mask(w30):
    groups = w30["seq_group"].to_numpy()
    idx = w30["seq_index"].to_numpy()
    member = (w30["label"] != "EXCLUDED").to_numpy()
    members, ends = sequence_index(groups, idx, member, np.ones(len(w30), bool), 5)
    assert member[members].all()
    check_contiguous(groups, idx, members)
    assert not (w30["label"].to_numpy()[members] == "EXCLUDED").any()
