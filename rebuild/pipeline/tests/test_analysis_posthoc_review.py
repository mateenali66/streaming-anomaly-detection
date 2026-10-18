"""Tests for the post hoc sensitivity checks added after the internal review (D6, item 10)."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
pytest.importorskip("scipy")
from analysis import posthoc_review as P  # noqa: E402


def test_critical_rho_matches_the_published_table_for_seven_ranks():
    # One detector: the null is Spearman's rho over the 5,040 orderings of seven ranks, whose one-sided 5% critical
    # value is 0.714 (1 - 6 * 16 / 336) in the standard tables.
    r = np.arange(7, dtype=float)[None, :]
    crit, n = P.critical_median_rho(r, r.copy())
    assert n == 5040
    assert crit == pytest.approx(1 - 6 * 16 / 336)


def test_critical_rho_is_the_smallest_value_at_or_below_alpha():
    rng = np.random.default_rng(0)
    E, I = rng.normal(size=(3, 7)), rng.normal(size=(3, 7))
    crit, _ = P.critical_median_rho(E, I)
    crit10, _ = P.critical_median_rho(E, I, alpha=0.10)
    assert -1 <= crit10 <= crit <= 1


def test_false_alert_share_sums_counts_before_dividing():
    rows = pd.DataFrame([
        {"workload": "ecommerce", "detector": "IF", "budget": 0.01, "set": "normal", "n_false": 1},
        {"workload": "ecommerce", "detector": "IF", "budget": 0.01, "set": "recovery", "n_false": 3},
        {"workload": "ecommerce", "detector": "IF", "budget": 0.01, "set": "normal", "n_false": 4},
        {"workload": "ecommerce", "detector": "IF", "budget": 0.01, "set": "recovery", "n_false": 2},
    ])
    s = P.false_alert_share(rows).iloc[0]
    assert (s["n_false_normal"], s["n_false_recovery"]) == (5, 5)
    assert s["recovery_share"] == pytest.approx(0.5)
