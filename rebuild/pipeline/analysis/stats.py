"""Statistics of protocol section 5 (make stats, 7.1 step 8).

Added on 18 October 2026, after registration. The choices section 5 leaves open are recorded as D4 in
protocol/DEVIATIONS.md. The fold is the unit of estimation and the campaign the unit of replication (5.1).

within_campaign(d)   5.2: mean of the paired fold differences, t-based 95% and 90% intervals with n - 1 degrees of
                     freedom, the t-test p, the exact two-sided Wilcoxon signed-rank p (all 2^n sign vectors, zeros
                     dropped, tied magnitudes given mid-ranks), the matched-pairs rank-biserial correlation, and a BCa
                     interval that resamples test blocks
pooled(ds)           5.3.1: the mean of the campaign means, standard error sqrt(sum of per-campaign variances of the
                     mean) / K, a t reference with the summed within-campaign degrees of freedom
random_effects(ds)   5.3.2: d = mu + u(campaign) + e by REML. With equal folds per campaign REML has the closed form of
                     the one-way ANOVA estimator, truncated at zero; tests/test_analysis_stats.py checks it against
                     statsmodels MixedLM
variance_parts(xs)   5.3.3 for absolute AUC, the same estimator
sign_flip_p(ds)      5.3.4: stratified sign flips of the fold differences, exact over all 2^N vectors up to 2^20,
                     random beyond with a fixed seed
decide(...)          5.4 with margin delta: superior (lower 95% bound above 0, the p after Holm below alpha), inferior
                     (the same in the other direction, for two-sided contrasts), not superior (one-sided 95% upper bound
                     below delta), equivalent (90% interval inside -delta to delta)
replication(...)     5.5
holm(ps)             5.7
"""
from __future__ import annotations

import itertools
import math

import numpy as np
from scipy import stats as st

BOOT_N = 10000
BOOT_SEED = 20261018
FLIP_N = 1_000_000
FLIP_SEED = 20261018
EXACT_FLIP_MAX = 20


def _t_interval(mean, se, df, level):
    if not np.isfinite(se) or se == 0 or df <= 0:
        return (mean, mean)
    q = st.t.ppf(0.5 + level / 2, df)
    return (mean - q * se, mean + q * se)


def _t_p(mean, se, df):
    if se == 0:
        return 0.0 if mean != 0 else 1.0
    return float(2 * st.t.sf(abs(mean / se), df))


def wilcoxon_exact(d) -> tuple[float, float, float]:
    """Exact two-sided signed-rank p over all sign vectors, zeros dropped (Wilcoxon), tied |d| given mid-ranks.
    Returns (p, T+, matched-pairs rank-biserial r = (T+ - T-) / (T+ + T-))."""
    d = np.asarray(d, float)
    d = d[d != 0]
    n = len(d)
    if n == 0:
        return 1.0, 0.0, 0.0
    r = st.rankdata(np.abs(d))
    t_plus = float(r[d > 0].sum())
    total = float(r.sum())
    rb = (2 * t_plus - total) / total
    signs = np.array(list(itertools.product([0, 1], repeat=n)), dtype=float) if n <= 20 else None
    if signs is None:
        raise ValueError("exact Wilcoxon is enumerated only up to 20 non-zero differences")
    null = signs @ r
    e = total / 2
    obs = abs(t_plus - e)
    p = float(np.mean(np.abs(null - e) >= obs - 1e-9))
    return p, t_plus, float(rb)


def bca_interval(d, level=0.95, n=BOOT_N, seed=BOOT_SEED) -> tuple[float, float]:
    d = np.asarray(d, float)
    if len(d) < 3 or np.ptp(d) == 0:
        return (float(d.mean()), float(d.mean()))
    res = st.bootstrap((d,), np.mean, n_resamples=n, confidence_level=level, method="BCa",
                       random_state=np.random.default_rng(seed))
    return (float(res.confidence_interval.low), float(res.confidence_interval.high))


def within_campaign(d) -> dict:
    d = np.asarray(d, float)
    n = len(d)
    mean = float(d.mean())
    sd = float(d.std(ddof=1)) if n > 1 else float("nan")
    se = sd / math.sqrt(n) if n > 1 else float("nan")
    p_w, t_plus, rb = wilcoxon_exact(d)
    lo95, hi95 = _t_interval(mean, se, n - 1, 0.95)
    lo90, hi90 = _t_interval(mean, se, n - 1, 0.90)
    b_lo, b_hi = bca_interval(d)
    return {"n": n, "mean": mean, "sd": sd, "se": se, "df": n - 1, "ci95_lo": lo95, "ci95_hi": hi95,
            "ci90_lo": lo90, "ci90_hi": hi90, "upper95_one_sided": _one_sided_upper(mean, se, n - 1),
            "p_t": _t_p(mean, se, n - 1), "p_wilcoxon": p_w, "wilcoxon_t_plus": t_plus, "rank_biserial": rb,
            "bca95_lo": b_lo, "bca95_hi": b_hi}


def _one_sided_upper(mean, se, df, level=0.95):
    if not np.isfinite(se) or se == 0:
        return mean
    return float(mean + st.t.ppf(level, df) * se)


def pooled(ds: dict) -> dict:
    """ds: campaign -> fold differences. Stratified pooled estimate of 5.3.1."""
    keys = sorted(ds)
    arrs = [np.asarray(ds[k], float) for k in keys]
    K = len(arrs)
    means = [a.mean() for a in arrs]
    var_of_mean = [a.var(ddof=1) / len(a) for a in arrs]
    est = float(np.mean(means))
    se = float(math.sqrt(sum(var_of_mean)) / K)
    df = int(sum(len(a) - 1 for a in arrs))
    lo95, hi95 = _t_interval(est, se, df, 0.95)
    lo90, hi90 = _t_interval(est, se, df, 0.90)
    return {"campaigns": keys, "K": K, "n_folds": int(sum(len(a) for a in arrs)), "mean": est, "se": se, "df": df,
            "ci95_lo": lo95, "ci95_hi": hi95, "ci90_lo": lo90, "ci90_hi": hi90,
            "upper95_one_sided": _one_sided_upper(est, se, df), "lower95_one_sided": est - (_one_sided_upper(est, se, df) - est),
            "p_t": _t_p(est, se, df), "campaign_means": dict(zip(keys, map(float, means)))}


def anova_reml(groups: list) -> dict:
    """One-way random effects, equal group sizes: REML equals the ANOVA estimator when it is not negative, and
    otherwise tau^2 = 0 with sigma^2 the total mean square."""
    arrs = [np.asarray(g, float) for g in groups]
    K, n = len(arrs), len(arrs[0])
    if any(len(a) != n for a in arrs):
        raise ValueError("anova_reml needs equal group sizes")
    allv = np.concatenate(arrs)
    gm = float(allv.mean())
    msb = n * sum((a.mean() - gm) ** 2 for a in arrs) / (K - 1) if K > 1 else 0.0
    msw = sum(((a - a.mean()) ** 2).sum() for a in arrs) / (K * (n - 1))
    tau2 = (msb - msw) / n
    singular = tau2 <= 0
    if singular:
        tau2 = 0.0
        sigma2 = float(((allv - gm) ** 2).sum() / (K * n - 1))
    else:
        sigma2 = float(msw)
    se_mu = math.sqrt((sigma2 + n * tau2) / (K * n))
    icc = tau2 / (tau2 + sigma2) if (tau2 + sigma2) > 0 else float("nan")
    return {"mu": gm, "se_mu": se_mu, "tau": math.sqrt(tau2), "tau2": tau2, "sigma2": sigma2, "icc": icc,
            "singular": bool(singular), "K": K, "n_per_campaign": n}


def random_effects(ds: dict) -> dict:
    return anova_reml([ds[k] for k in sorted(ds)])


def variance_parts(xs: dict) -> dict:
    r = anova_reml([xs[k] for k in sorted(xs)])
    tot = r["tau2"] + r["sigma2"]
    return {"between_campaign_var": r["tau2"], "between_fold_var": r["sigma2"],
            "between_campaign_share": r["tau2"] / tot if tot > 0 else float("nan"), "singular": r["singular"]}


def sign_flip_p(ds: dict, n_random=FLIP_N, seed=FLIP_SEED) -> dict:
    """Two-sided p of the stratified pooled mean (mean of campaign means) under sign flips of fold differences."""
    keys = sorted(ds)
    arrs = [np.asarray(ds[k], float) for k in keys]
    K = len(arrs)
    w = np.concatenate([np.full(len(a), 1.0 / (K * len(a))) for a in arrs])
    d = np.concatenate(arrs)
    obs = float(w @ d)
    N = len(d)
    if N <= EXACT_FLIP_MAX:
        idx = np.arange(2 ** N, dtype=np.int64)
        bits = ((idx[:, None] >> np.arange(N)) & 1).astype(np.int8)
        signs = 1 - 2 * bits
        null = signs @ (w * d)
        exact = True
    else:
        rng = np.random.default_rng(seed)
        null = np.empty(n_random)
        step = 100_000
        for i in range(0, n_random, step):
            m = min(step, n_random - i)
            signs = rng.choice(np.array([-1.0, 1.0]), size=(m, N))
            null[i:i + m] = signs @ (w * d)
        exact = False
    p = float(np.mean(np.abs(null) >= abs(obs) - 1e-12))
    return {"p_sign_flip": p, "exact": exact, "n_vectors": int(len(null)), "statistic": obs}


def holm(ps: dict) -> dict:
    """Holm step-down adjusted p-values."""
    items = sorted(((float(v), k) for k, v in ps.items() if v is not None and np.isfinite(v)))
    m = len(items)
    out, running = {}, 0.0
    for i, (p, k) in enumerate(items):
        running = max(running, min(1.0, (m - i) * p))
        out[k] = running
    for k, v in ps.items():
        out.setdefault(k, float("nan"))
    return out


def decide(est: dict, delta: float, p_adj: float | None = None, alpha: float = 0.05, two_sided: bool = False) -> dict:
    """5.4 flags on one estimate (pooled or one campaign). p_adj is the p after Holm for the superiority test; when it
    is None the estimate's own p is used."""
    p = est["p_t"] if p_adj is None else p_adj
    sup = est["ci95_lo"] > 0 and p < alpha
    inf = two_sided and est["ci95_hi"] < 0 and p < alpha
    nonsup = est["upper95_one_sided"] < delta
    equiv = est["ci90_lo"] > -delta and est["ci90_hi"] < delta
    if sup:
        outcome = "superior"
    elif inf:
        outcome = "inferior"
    elif equiv:
        outcome = "equivalent"
    elif nonsup:
        outcome = "not superior"
    else:
        outcome = "inconclusive"
    return {"superior": bool(sup), "inferior": bool(inf), "not_superior": bool(nonsup), "equivalent": bool(equiv),
            "outcome": outcome, "p_decision": float(p)}


def _claim_side(claim: str, x: float, delta: float) -> bool:
    return {"superior": x > 0, "inferior": x < 0, "not superior": x < delta, "equivalent": -delta < x < delta}[claim]


def _wholly_other_side(claim: str, lo: float, hi: float, delta: float) -> bool:
    return {"superior": hi < 0, "inferior": lo > 0, "not superior": lo > delta,
            "equivalent": lo > delta or hi < -delta}[claim]


def replication(claim: str, pooled_holds: bool, per_campaign: dict, per_campaign_holds: dict, delta: float) -> dict:
    """5.5. per_campaign: campaign -> within_campaign() of a confirmatory campaign. per_campaign_holds: campaign ->
    whether the decision rule holds in that campaign alone."""
    ests = {k: v["mean"] for k, v in per_campaign.items()}
    on_side = all(_claim_side(claim, x, delta) for x in ests.values()) if claim != "inconclusive" else False
    excl = any(_wholly_other_side(claim, v["ci95_lo"], v["ci95_hi"], delta) for v in per_campaign.values()) \
        if claim != "inconclusive" else False
    signs = {np.sign(x) for x in ests.values()}
    same_sign = len(signs) == 1 and 0 not in signs
    if claim != "inconclusive" and pooled_holds and on_side and not excl:
        status = "replicated"
    elif excl or not same_sign:
        status = "not replicated"
    else:
        status = "direction consistent"
    return {"status": status, "replicated_in_each_campaign": bool(claim != "inconclusive" and
                                                                  all(per_campaign_holds.values())),
            "same_sign": bool(same_sign), "campaign_excludes_claim": bool(excl)}


def cliffs_delta(a, b) -> float:
    a, b = np.asarray(a, float), np.asarray(b, float)
    gt = (a[:, None] > b[None, :]).sum()
    lt = (a[:, None] < b[None, :]).sum()
    return float((gt - lt) / (len(a) * len(b)))
