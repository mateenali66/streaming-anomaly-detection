"""analysis/stats.py against hand computations, scipy and statsmodels (protocol 5.2 to 5.7)."""
from __future__ import annotations

import itertools
import math
import sys
from pathlib import Path

import numpy as np
import pytest
from scipy import stats as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis import stats as S  # noqa: E402


def test_exact_wilcoxon_smallest_p_is_2_over_1024():
    p, t_plus, rb = S.wilcoxon_exact(np.arange(1, 11) / 100)
    assert p == pytest.approx(2 / 1024) and t_plus == 55 and rb == 1.0


def test_exact_wilcoxon_matches_scipy_without_ties():
    rng = np.random.default_rng(3)
    for _ in range(20):
        d = rng.normal(0.01, 0.02, 10)
        p, _, _ = S.wilcoxon_exact(d)
        assert p == pytest.approx(st.wilcoxon(d, method="exact").pvalue, abs=1e-12)


def test_exact_wilcoxon_with_ties_and_zeros_enumerates_mid_ranks():
    d = np.array([0.0, 0.01, -0.01, 0.02, 0.02, 0.03])
    nz = d[d != 0]
    r = st.rankdata(np.abs(nz))
    obs = r[nz > 0].sum()
    null = [sum(ri for ri, s in zip(r, sg) if s) for sg in itertools.product([0, 1], repeat=len(nz))]
    e = r.sum() / 2
    want = np.mean([abs(x - e) >= abs(obs - e) - 1e-9 for x in null])
    p, t_plus, rb = S.wilcoxon_exact(d)
    assert p == pytest.approx(want) and t_plus == obs
    assert rb == pytest.approx((obs - (r.sum() - obs)) / r.sum())


def test_within_campaign_t_interval_matches_scipy():
    d = np.array([0.02, 0.01, 0.03, -0.01, 0.00, 0.02, 0.04, 0.01, 0.02, 0.03])
    w = S.within_campaign(d)
    ci = st.ttest_1samp(d, 0).confidence_interval(0.95)
    assert (w["ci95_lo"], w["ci95_hi"]) == pytest.approx((ci.low, ci.high))
    assert w["p_t"] == pytest.approx(st.ttest_1samp(d, 0).pvalue)
    assert w["upper95_one_sided"] == pytest.approx(d.mean() + st.t.ppf(0.95, 9) * d.std(ddof=1) / math.sqrt(10))
    assert w["bca95_lo"] < d.mean() < w["bca95_hi"]


def test_pooled_is_the_mean_of_campaign_means_with_summed_df():
    a = np.array([0.01, 0.02, 0.03, 0.02, 0.01, 0.00, 0.02, 0.03, 0.01, 0.02])
    b = a + 0.05
    p = S.pooled({"x": a, "y": b})
    assert p["mean"] == pytest.approx((a.mean() + b.mean()) / 2)
    assert p["se"] == pytest.approx(math.sqrt(a.var(ddof=1) / 10 + b.var(ddof=1) / 10) / 2)
    assert p["df"] == 18
    q = st.t.ppf(0.975, 18)
    assert p["ci95_lo"] == pytest.approx(p["mean"] - q * p["se"])


def test_reml_closed_form_matches_statsmodels():
    sm = pytest.importorskip("statsmodels.formula.api")
    import pandas as pd
    rng = np.random.default_rng(7)
    for tau in (0.03, 0.0):
        groups = [rng.normal(rng.normal(0, tau) if tau else 0.0, 0.02, 10) for _ in range(3)]
        r = S.anova_reml(groups)
        df = pd.DataFrame({"y": np.concatenate(groups), "g": np.repeat([0, 1, 2], 10)})
        fit = sm.mixedlm("y ~ 1", df, groups=df["g"]).fit(reml=True)
        assert r["mu"] == pytest.approx(float(fit.params["Intercept"]), abs=1e-9)
        assert r["tau2"] == pytest.approx(float(fit.cov_re.iloc[0, 0]), abs=2e-6)
        assert r["sigma2"] == pytest.approx(float(fit.scale), rel=2e-3)


def test_sign_flip_exact_matches_brute_force_and_random_is_close():
    rng = np.random.default_rng(1)
    ds = {"a": rng.normal(0.01, 0.02, 4), "b": rng.normal(0.01, 0.02, 4)}
    d = np.concatenate([ds["a"], ds["b"]])
    obs = (ds["a"].mean() + ds["b"].mean()) / 2
    null = [(np.dot(s[:4], ds["a"]) / 4 + np.dot(s[4:], ds["b"]) / 4) / 2
            for s in itertools.product([-1, 1], repeat=8)]
    want = np.mean(np.abs(null) >= abs(obs) - 1e-12)
    r = S.sign_flip_p(ds)
    assert r["exact"] and r["p_sign_flip"] == pytest.approx(want)
    three = {k: rng.normal(0.005, 0.02, 10) for k in "abc"}
    r3 = S.sign_flip_p(three, n_random=200_000)
    w = np.concatenate([three[k] for k in "abc"]) / 30
    obs3 = w.sum()
    flips = rng.choice([-1.0, 1.0], size=(200_000, 30)) @ w
    assert not r3["exact"] and r3["n_vectors"] == 200_000
    assert r3["p_sign_flip"] == pytest.approx(np.mean(np.abs(flips) >= abs(obs3)), abs=0.01)


def test_sign_flip_two_campaigns_of_ten_is_exact_over_2_to_the_20():
    r = S.sign_flip_p({"a": np.full(10, 0.01), "b": np.full(10, 0.02)})
    assert r["exact"] and r["n_vectors"] == 2 ** 20 and r["p_sign_flip"] == pytest.approx(2 / 2 ** 20)


def test_holm():
    adj = S.holm({"a": 0.01, "b": 0.04, "c": 0.03})
    assert adj == pytest.approx({"a": 0.03, "c": 0.06, "b": 0.06})


def _est(mean, lo95, hi95, lo90, hi90, up, p):
    return {"mean": mean, "ci95_lo": lo95, "ci95_hi": hi95, "ci90_lo": lo90, "ci90_hi": hi90,
            "upper95_one_sided": up, "p_t": p}


def test_decide_outcomes():
    assert S.decide(_est(0.05, 0.02, 0.08, 0.025, 0.075, 0.075, 0.001), 0.03)["outcome"] == "superior"
    assert S.decide(_est(0.05, 0.02, 0.08, 0.025, 0.075, 0.075, 0.001), 0.03, p_adj=0.2)["outcome"] == "inconclusive"
    assert S.decide(_est(0.0, -0.02, 0.02, -0.015, 0.015, 0.015, 0.9), 0.03)["outcome"] == "equivalent"
    assert S.decide(_est(-0.05, -0.08, -0.02, -0.075, -0.025, -0.025, 0.001), 0.03)["outcome"] == "not superior"
    d = S.decide(_est(-0.05, -0.08, -0.02, -0.075, -0.025, -0.025, 0.001), 0.03, two_sided=True)
    assert d["outcome"] == "inferior" and d["not_superior"]
    d = S.decide(_est(-0.02, -0.06, 0.02, -0.05, 0.01, 0.01, 0.3), 0.03)
    assert d["outcome"] == "not superior" and not d["equivalent"]


def test_replication_statuses():
    c = lambda m, lo, hi: {"mean": m, "ci95_lo": lo, "ci95_hi": hi}
    r = S.replication("superior", True, {"b": c(0.04, 0.01, 0.07), "c": c(0.03, -0.01, 0.07)},
                      {"b": True, "c": False}, 0.03)
    assert r["status"] == "replicated" and not r["replicated_in_each_campaign"]
    r = S.replication("superior", False, {"b": c(0.04, 0.01, 0.07), "c": c(0.01, -0.03, 0.05)},
                      {"b": True, "c": False}, 0.03)
    assert r["status"] == "direction consistent"
    r = S.replication("superior", True, {"b": c(0.06, 0.03, 0.09), "c": c(-0.04, -0.07, -0.01)},
                      {"b": True, "c": False}, 0.03)
    assert r["status"] == "not replicated"
    r = S.replication("not superior", True, {"b": c(0.0, -0.02, 0.02), "c": c(0.04, 0.035, 0.05)},
                      {"b": True, "c": False}, 0.03)
    assert r["status"] == "not replicated" and r["campaign_excludes_claim"]
