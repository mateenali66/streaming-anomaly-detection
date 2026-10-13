#!/usr/bin/env python3
"""
Statistical significance tests across the model panel.

Tests performed:
  1. Friedman test across all 8 models (non-parametric repeated measures)
  2. Nemenyi post-hoc pairwise comparisons (if Friedman rejects H0)
  3. Wilcoxon signed-rank for RQ2: best point-in-time vs best sequence model
  4. Holm-Bonferroni correction for multiple comparisons
  5. Effect sizes: Cohen's d and Cliff's delta for all pairwise comparisons

Usage:
    python analysis/statistical_tests.py --results results/merged --output results/statistical_tests
"""

from __future__ import annotations

import argparse
import json
import sys
from itertools import combinations
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

ALPHA = 0.05

POINT_MODELS = ["IF", "OC_SVM"]
SEQUENCE_MODELS = ["LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE"]
BASELINES = ["RF", "RuleBased"]
ALL_MODELS = POINT_MODELS + SEQUENCE_MODELS + BASELINES


# ---------------------------------------------------------------------------
# Effect size measures
# ---------------------------------------------------------------------------

def cohens_d(a: np.ndarray, b: np.ndarray) -> float:
    """Cohen's d (pooled standard deviation)."""
    na, nb = len(a), len(b)
    if na < 2 or nb < 2:
        return 0.0
    pooled_std = np.sqrt(((na - 1) * np.var(a, ddof=1) + (nb - 1) * np.var(b, ddof=1))
                         / (na + nb - 2))
    if pooled_std < 1e-10:
        return 0.0
    return float((np.mean(a) - np.mean(b)) / pooled_std)


def cliffs_delta(a: np.ndarray, b: np.ndarray) -> tuple[float, str]:
    """
    Cliff's delta: non-parametric effect size.
    Returns (delta, magnitude) where magnitude is negligible/small/medium/large.
    """
    n_a, n_b = len(a), len(b)
    if n_a == 0 or n_b == 0:
        return 0.0, "negligible"

    # Count dominance
    more = sum(1 for ai in a for bi in b if ai > bi)
    less = sum(1 for ai in a for bi in b if ai < bi)
    delta = (more - less) / (n_a * n_b)

    # Magnitude thresholds (Romano et al., 2006)
    abs_d = abs(delta)
    if abs_d < 0.147:
        mag = "negligible"
    elif abs_d < 0.33:
        mag = "small"
    elif abs_d < 0.474:
        mag = "medium"
    else:
        mag = "large"

    return float(delta), mag


# ---------------------------------------------------------------------------
# Holm-Bonferroni correction
# ---------------------------------------------------------------------------

def holm_bonferroni(p_values: list[tuple[str, float]]) -> list[tuple[str, float, bool]]:
    """
    Apply Holm-Bonferroni correction.
    Input: list of (label, p_value)
    Output: list of (label, adjusted_p, significant)
    """
    sorted_pvals = sorted(p_values, key=lambda x: x[1])
    m = len(sorted_pvals)
    results = []
    for i, (label, p) in enumerate(sorted_pvals):
        adjusted = min(p * (m - i), 1.0)
        results.append((label, adjusted, adjusted < ALPHA))
    # Enforce monotonicity: adjusted p can't decrease
    for i in range(1, len(results)):
        if results[i][1] < results[i - 1][1]:
            results[i] = (results[i][0], results[i - 1][1], results[i][2])
    return results


# ---------------------------------------------------------------------------
# Nemenyi test (critical difference)
# ---------------------------------------------------------------------------

def nemenyi_cd(k: int, n: int, alpha: float = 0.05) -> float:
    """
    Nemenyi critical difference.
    k = number of groups, n = number of blocks (folds).
    CD = q_alpha * sqrt(k*(k+1) / (6*n))

    q_alpha values from Studentized Range / sqrt(2) for alpha=0.05.
    """
    # q_alpha values for alpha=0.05 (from statistical tables, k groups)
    q_table = {
        2: 1.960, 3: 2.343, 4: 2.569, 5: 2.728,
        6: 2.850, 7: 2.949, 8: 3.031, 9: 3.102, 10: 3.164,
    }
    q = q_table.get(k, 3.031)  # default to k=8
    return q * np.sqrt(k * (k + 1) / (6 * n))


# ---------------------------------------------------------------------------
# Main analysis functions
# ---------------------------------------------------------------------------

def run_friedman_test(df: pd.DataFrame, metric: str = "f1_score") -> dict:
    """
    Friedman test: are there significant differences among models?
    Each fold is a block.
    """
    print(f"\n{'='*70}")
    print(f"FRIEDMAN TEST ({metric})")
    print(f"{'='*70}")

    pivot = df.pivot_table(index="fold", columns="model", values=metric)
    models = sorted(pivot.columns)
    pivot = pivot[models]

    print(f"  Models: {models}")
    print(f"  Folds: {list(pivot.index)}")
    print(f"\n  Per-model {metric} across folds:")
    for m in models:
        vals = pivot[m].values
        print(f"    {m:12s}: {np.mean(vals):.4f} +/- {np.std(vals):.4f}  "
              f"({', '.join(f'{v:.4f}' for v in vals)})")

    # Friedman test
    groups = [pivot[m].values for m in models]
    stat, p_value = stats.friedmanchisquare(*groups)

    print(f"\n  Friedman chi-squared = {stat:.4f}")
    print(f"  p-value = {p_value:.6f}")
    print(f"  Significant at alpha={ALPHA}: {'YES' if p_value < ALPHA else 'NO'}")

    # Average ranks
    ranks = pivot.rank(axis=1, ascending=False)  # higher metric = rank 1
    avg_ranks = ranks.mean()
    print(f"\n  Average ranks (1=best):")
    for m in avg_ranks.sort_values().index:
        print(f"    {m:12s}: {avg_ranks[m]:.2f}")

    return {
        "test": "friedman",
        "metric": metric,
        "statistic": round(float(stat), 4),
        "p_value": round(float(p_value), 6),
        "significant": bool(p_value < ALPHA),
        "n_models": len(models),
        "n_folds": len(pivot),
        "average_ranks": {m: round(float(avg_ranks[m]), 4) for m in models},
    }


def run_nemenyi_posthoc(df: pd.DataFrame, metric: str = "f1_score") -> list[dict]:
    """
    Nemenyi post-hoc test: pairwise comparisons with critical difference.
    """
    print(f"\n{'='*70}")
    print(f"NEMENYI POST-HOC ({metric})")
    print(f"{'='*70}")

    pivot = df.pivot_table(index="fold", columns="model", values=metric)
    models = sorted(pivot.columns)
    k = len(models)
    n = len(pivot)

    ranks = pivot.rank(axis=1, ascending=False)
    avg_ranks = ranks.mean()

    cd = nemenyi_cd(k, n, ALPHA)
    print(f"  Critical difference (CD) at alpha={ALPHA}: {cd:.4f}")
    print(f"  k={k} models, n={n} folds")

    results = []
    print(f"\n  Pairwise rank differences (significant if > {cd:.4f}):")
    for m1, m2 in combinations(models, 2):
        diff = abs(avg_ranks[m1] - avg_ranks[m2])
        sig = diff > cd
        print(f"    {m1:12s} vs {m2:12s}: |{avg_ranks[m1]:.2f} - {avg_ranks[m2]:.2f}| = {diff:.4f}  "
              f"{'*** SIGNIFICANT' if sig else ''}")
        results.append({
            "test": "nemenyi",
            "metric": metric,
            "model_a": m1,
            "model_b": m2,
            "rank_a": round(float(avg_ranks[m1]), 4),
            "rank_b": round(float(avg_ranks[m2]), 4),
            "rank_diff": round(float(diff), 4),
            "critical_difference": round(float(cd), 4),
            "significant": bool(sig),
        })

    return results


def run_wilcoxon_rq2(df: pd.DataFrame, metric: str = "f1_score") -> list[dict]:
    """
    RQ2: Wilcoxon signed-rank test comparing point-in-time vs sequence models.
    Tests: best point (by mean F1) vs best sequence, and all pairwise point vs sequence.
    """
    print(f"\n{'='*70}")
    print(f"WILCOXON SIGNED-RANK: RQ2 Point vs Sequence ({metric})")
    print(f"{'='*70}")

    pivot = df.pivot_table(index="fold", columns="model", values=metric)

    # Identify best point and best sequence by mean metric
    point_means = {m: pivot[m].mean() for m in POINT_MODELS if m in pivot.columns}
    seq_means = {m: pivot[m].mean() for m in SEQUENCE_MODELS if m in pivot.columns}

    best_point = max(point_means, key=point_means.get) if point_means else None
    best_seq = max(seq_means, key=seq_means.get) if seq_means else None

    print(f"  Point models:    {point_means}")
    print(f"  Sequence models: {seq_means}")
    print(f"  Best point:    {best_point} (mean {metric}={point_means.get(best_point, 'N/A'):.4f})")
    print(f"  Best sequence: {best_seq} (mean {metric}={seq_means.get(best_seq, 'N/A'):.4f})")

    results = []
    raw_pvals = []

    # All pairwise: each point vs each sequence
    for pm in POINT_MODELS:
        for sm in SEQUENCE_MODELS:
            if pm not in pivot.columns or sm not in pivot.columns:
                continue
            a = pivot[pm].values
            b = pivot[sm].values
            try:
                stat, p = stats.wilcoxon(a, b, alternative="two-sided")
            except ValueError:
                stat, p = 0.0, 1.0

            cd, cd_mag = cliffs_delta(a, b)
            d = cohens_d(a, b)
            label = f"{pm} vs {sm}"
            raw_pvals.append((label, p))

            results.append({
                "test": "wilcoxon",
                "metric": metric,
                "comparison": "rq2_point_vs_sequence",
                "model_a": pm,
                "model_b": sm,
                "mean_a": round(float(np.mean(a)), 4),
                "mean_b": round(float(np.mean(b)), 4),
                "statistic": round(float(stat), 4),
                "p_value": round(float(p), 6),
                "cohens_d": round(d, 4),
                "cliffs_delta": round(cd, 4),
                "cliffs_magnitude": cd_mag,
            })

            is_main = (pm == best_point and sm == best_seq)
            marker = " <<<< MAIN COMPARISON" if is_main else ""
            print(f"    {label:25s}: W={stat:.1f}, p={p:.6f}, d={d:+.4f}, "
                  f"delta={cd:+.4f} ({cd_mag}){marker}")

    # Apply Holm-Bonferroni
    if raw_pvals:
        corrected = holm_bonferroni(raw_pvals)
        print(f"\n  Holm-Bonferroni corrected p-values:")
        for label, adj_p, sig in corrected:
            print(f"    {label:25s}: p_adj={adj_p:.6f}  {'*** SIGNIFICANT' if sig else ''}")
            # Update results with adjusted p
            for r in results:
                if f"{r['model_a']} vs {r['model_b']}" == label:
                    r["p_adjusted"] = round(float(adj_p), 6)
                    r["significant_corrected"] = bool(sig)

    return results


def run_pairwise_wilcoxon(df: pd.DataFrame, metric: str = "f1_score") -> list[dict]:
    """
    All pairwise Wilcoxon tests with Holm-Bonferroni correction and effect sizes.
    """
    print(f"\n{'='*70}")
    print(f"ALL PAIRWISE WILCOXON + EFFECT SIZES ({metric})")
    print(f"{'='*70}")

    pivot = df.pivot_table(index="fold", columns="model", values=metric)
    models = sorted(pivot.columns)

    results = []
    raw_pvals = []

    for m1, m2 in combinations(models, 2):
        a = pivot[m1].values
        b = pivot[m2].values

        try:
            stat, p = stats.wilcoxon(a, b, alternative="two-sided")
        except ValueError:
            stat, p = 0.0, 1.0

        d = cohens_d(a, b)
        cd, cd_mag = cliffs_delta(a, b)
        label = f"{m1} vs {m2}"
        raw_pvals.append((label, p))

        results.append({
            "test": "wilcoxon_pairwise",
            "metric": metric,
            "model_a": m1,
            "model_b": m2,
            "mean_a": round(float(np.mean(a)), 4),
            "mean_b": round(float(np.mean(b)), 4),
            "statistic": round(float(stat), 4),
            "p_value": round(float(p), 6),
            "cohens_d": round(d, 4),
            "cliffs_delta": round(cd, 4),
            "cliffs_magnitude": cd_mag,
        })

    # Holm-Bonferroni
    if raw_pvals:
        corrected = holm_bonferroni(raw_pvals)
        for label, adj_p, sig in corrected:
            for r in results:
                if f"{r['model_a']} vs {r['model_b']}" == label:
                    r["p_adjusted"] = round(float(adj_p), 6)
                    r["significant_corrected"] = bool(sig)

    # Print summary
    sig_count = sum(1 for r in results if r.get("significant_corrected", False))
    print(f"  {len(results)} pairwise comparisons, {sig_count} significant after correction")

    for r in sorted(results, key=lambda x: x["p_value"]):
        sig_marker = "***" if r.get("significant_corrected") else "   "
        print(f"  {sig_marker} {r['model_a']:12s} vs {r['model_b']:12s}: "
              f"p={r['p_value']:.6f} (adj={r.get('p_adjusted', 'N/A')}), "
              f"d={r['cohens_d']:+.4f}, delta={r['cliffs_delta']:+.4f} ({r['cliffs_magnitude']})")

    return results


def run_rq2_grouped(df: pd.DataFrame, metric: str = "f1_score") -> dict:
    """
    RQ2 aggregate: pool all point-in-time vs all sequence F1 scores.
    """
    print(f"\n{'='*70}")
    print(f"RQ2 GROUPED: All Point vs All Sequence ({metric})")
    print(f"{'='*70}")

    point_vals = df[df["model"].isin(POINT_MODELS)][metric].values
    seq_vals = df[df["model"].isin(SEQUENCE_MODELS)][metric].values

    print(f"  Point (n={len(point_vals)}): mean={np.mean(point_vals):.4f} +/- {np.std(point_vals):.4f}")
    print(f"  Sequence (n={len(seq_vals)}): mean={np.mean(seq_vals):.4f} +/- {np.std(seq_vals):.4f}")

    # Mann-Whitney U (unpooled, different sample sizes)
    stat, p = stats.mannwhitneyu(point_vals, seq_vals, alternative="two-sided")
    d = cohens_d(point_vals, seq_vals)
    cd, cd_mag = cliffs_delta(point_vals, seq_vals)

    print(f"  Mann-Whitney U = {stat:.1f}, p = {p:.6f}")
    print(f"  Cohen's d = {d:+.4f}")
    print(f"  Cliff's delta = {cd:+.4f} ({cd_mag})")
    print(f"  Significant at alpha={ALPHA}: {'YES' if p < ALPHA else 'NO'}")

    return {
        "test": "mann_whitney_rq2_grouped",
        "metric": metric,
        "point_mean": round(float(np.mean(point_vals)), 4),
        "point_std": round(float(np.std(point_vals)), 4),
        "sequence_mean": round(float(np.mean(seq_vals)), 4),
        "sequence_std": round(float(np.std(seq_vals)), 4),
        "statistic": round(float(stat), 4),
        "p_value": round(float(p), 6),
        "cohens_d": round(d, 4),
        "cliffs_delta": round(cd, 4),
        "cliffs_magnitude": cd_mag,
        "significant": bool(p < ALPHA),
    }


def run_rq2_per_fold_family(df: pd.DataFrame, metric: str = "f1_score") -> dict:
    """
    RQ2: Compare per-fold family averages (paired Wilcoxon).
    For each fold, compute mean metric across point models and sequence models,
    then run a paired test on the 5 fold-level pairs.
    This avoids pseudoreplication from pooling heterogeneous models.
    """
    print(f"\n{'='*70}")
    print(f"RQ2 PER-FOLD FAMILY AVERAGE ({metric})")
    print(f"{'='*70}")

    point_means = []
    seq_means = []

    for fold_id in sorted(df["fold"].unique()):
        fold_df = df[df["fold"] == fold_id]
        pm = fold_df[fold_df["model"].isin(POINT_MODELS)][metric].mean()
        sm = fold_df[fold_df["model"].isin(SEQUENCE_MODELS)][metric].mean()
        point_means.append(pm)
        seq_means.append(sm)
        print(f"  Fold {fold_id}: point={pm:.4f}  seq={sm:.4f}  diff={sm-pm:+.4f}")

    point_arr = np.array(point_means)
    seq_arr = np.array(seq_means)

    try:
        stat, p = stats.wilcoxon(point_arr, seq_arr, alternative="two-sided")
    except ValueError:
        stat, p = 0.0, 1.0

    d = cohens_d(point_arr, seq_arr)
    cd, cd_mag = cliffs_delta(point_arr, seq_arr)

    print(f"\n  Paired Wilcoxon: W={stat:.1f}, p={p:.6f}")
    print(f"  Point family mean: {np.mean(point_arr):.4f} +/- {np.std(point_arr):.4f}")
    print(f"  Seq family mean:   {np.mean(seq_arr):.4f} +/- {np.std(seq_arr):.4f}")
    print(f"  Cohen's d = {d:+.4f}, Cliff's delta = {cd:+.4f} ({cd_mag})")
    print(f"  Significant at alpha={ALPHA}: {'YES' if p < ALPHA else 'NO'}")

    return {
        "test": "wilcoxon_rq2_per_fold_family",
        "metric": metric,
        "point_fold_means": [round(float(x), 4) for x in point_means],
        "sequence_fold_means": [round(float(x), 4) for x in seq_means],
        "point_mean": round(float(np.mean(point_arr)), 4),
        "point_std": round(float(np.std(point_arr)), 4),
        "sequence_mean": round(float(np.mean(seq_arr)), 4),
        "sequence_std": round(float(np.std(seq_arr)), 4),
        "statistic": round(float(stat), 4),
        "p_value": round(float(p), 6),
        "cohens_d": round(d, 4),
        "cliffs_delta": round(cd, 4),
        "cliffs_magnitude": cd_mag,
        "significant": bool(p < ALPHA),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Statistical significance tests across the model panel")
    parser.add_argument("--results", default="results/merged",
                        help="Path to merged results directory")
    parser.add_argument("--output", default="results/statistical_tests",
                        help="Output directory")
    args = parser.parse_args()

    results_dir = Path(args.results)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load data
    model_results = pd.read_csv(results_dir / "model_results.csv")
    print(f"Loaded model_results.csv: {len(model_results)} rows")
    print(f"Models: {sorted(model_results['model'].unique())}")
    print(f"Folds: {sorted(model_results['fold'].unique())}")

    all_results = []

    # -----------------------------------------------------------------------
    # 1. Friedman test (F1 and AUC-ROC)
    # -----------------------------------------------------------------------
    for metric in ["f1_score", "auc_roc"]:
        friedman = run_friedman_test(model_results, metric)
        all_results.append(friedman)

        # 2. Nemenyi post-hoc (if Friedman significant)
        if friedman["significant"]:
            nemenyi = run_nemenyi_posthoc(model_results, metric)
            all_results.extend(nemenyi)
        else:
            print(f"  Friedman not significant for {metric}, skipping Nemenyi")

    # -----------------------------------------------------------------------
    # 3. RQ2: Wilcoxon signed-rank (point vs sequence)
    # -----------------------------------------------------------------------
    for metric in ["f1_score", "auc_roc"]:
        rq2_results = run_wilcoxon_rq2(model_results, metric)
        all_results.extend(rq2_results)

    # RQ2 grouped comparison (supplementary, kept for reference)
    for metric in ["f1_score", "auc_roc"]:
        rq2_grouped = run_rq2_grouped(model_results, metric)
        all_results.append(rq2_grouped)

    # RQ2 per-fold family average (primary, avoids pseudoreplication)
    for metric in ["f1_score", "auc_roc"]:
        rq2_family = run_rq2_per_fold_family(model_results, metric)
        all_results.append(rq2_family)

    # -----------------------------------------------------------------------
    # 4. All pairwise Wilcoxon + effect sizes
    # -----------------------------------------------------------------------
    for metric in ["f1_score", "auc_roc"]:
        pairwise = run_pairwise_wilcoxon(model_results, metric)
        all_results.extend(pairwise)

    # -----------------------------------------------------------------------
    # Save results
    # -----------------------------------------------------------------------
    # Save as JSON (full detail)
    with open(output_dir / "statistical_tests.json", "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\nSaved: {output_dir / 'statistical_tests.json'} ({len(all_results)} entries)")

    # Save flat CSV for the key pairwise comparisons
    pairwise_rows = [r for r in all_results if "model_a" in r]
    if pairwise_rows:
        df_pw = pd.DataFrame(pairwise_rows)
        df_pw.to_csv(output_dir / "pairwise_comparisons.csv", index=False)
        print(f"Saved: {output_dir / 'pairwise_comparisons.csv'} ({len(df_pw)} rows)")

    # Save Friedman results as CSV
    friedman_rows = [r for r in all_results if r.get("test") == "friedman"]
    if friedman_rows:
        df_fr = pd.DataFrame(friedman_rows)
        df_fr.to_csv(output_dir / "friedman_results.csv", index=False)

    # Save grouped RQ2 as CSV
    rq2_rows = [r for r in all_results if "rq2_grouped" in r.get("test", "")]
    if rq2_rows:
        df_rq2 = pd.DataFrame(rq2_rows)
        df_rq2.to_csv(output_dir / "rq2_grouped.csv", index=False)

    print(f"\nAll statistical tests complete. Results in: {output_dir}")


if __name__ == "__main__":
    main()
