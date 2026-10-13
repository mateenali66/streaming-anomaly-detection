"""EWMA in the main model comparison and statistical analysis.

Places the EWMA baseline, evaluated in classical_baselines.py, in the main model comparison
and the statistical analysis.

EWMA was already evaluated on the same 10 folds and the same two workloads as the panel
(classical_baselines_per_fold.csv), so adding it is a join rather than a new experiment.

Under leave-one-rep-out, fold i's TEST set is exactly repetition i, so the ten test
partitions are disjoint and no evaluation window is scored twice. The dependence is in the
training sets: each holds seven of the ten repetitions and overlaps the others, so the ten
fitted models are correlated.

That makes the fold the unit of analysis rather than the window. Every model is scored on
the same ten held-out repetitions as EWMA, so each contrast is a paired fold-level
difference. We report the mean difference with a fold-clustered SE, an exact Wilcoxon
signed-rank test over the ten paired differences (no distributional assumption), and a
bootstrap CI resampling repetitions. p-values are Holm-corrected across the per-workload
family of EWMA-versus-model contrasts.

A linear mixed model with a random intercept for fold was tried first and is not reported:
the between-fold variance estimates to zero on this panel, the covariance is singular, and
the model collapses to OLS, which would overstate precision.

Output:
  rq1_panel_with_ewma.csv     unified per-fold panel, 11 models
  rq1_with_ewma_stats.txt     paired fold-level contrasts, EWMA vs each model
  rq1_with_ewma_table.tex     RQ1 table rows including EWMA
"""
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import wilcoxon
from statsmodels.stats.multitest import multipletests

OUT = Path(__file__).resolve().parent
PANEL = OUT / "panel_overall.csv"
CLASSICAL = OUT / "classical_baselines_per_fold.csv"
ANCHOR_WINDOW = 30
PROMOTE = ["EWMA"]

MODEL_LABEL = {
    "IF": "Isolation Forest", "OC_SVM": "OC-SVM", "LSTM_AE": "LSTM-AE",
    "Trans_AE": "Transformer-AE", "CNN_AE": "CNN-AE", "LSTM_VAE": "LSTM-VAE",
    "DeepSVDD": "Deep SVDD", "DAGMM": "DAGMM", "RF": "Random Forest (supervised)",
    "RuleBased": "Statistical max-z", "EWMA": "EWMA (classical)",
}


def load_panel():
    p = pd.read_csv(PANEL)
    p = p[(p["window_s"] == ANCHOR_WINDOW) & (p["category"] == "all")]
    p = p[["workload", "fold", "model", "precision", "recall", "f1_score", "auc_roc"]]

    c = pd.read_csv(CLASSICAL)
    c = c[c["model"].isin(PROMOTE)]
    c = c.rename(columns={"f1": "f1_score"})
    c = c[["workload", "fold", "model", "precision", "recall", "f1_score", "auc_roc"]]

    both = pd.concat([p, c], ignore_index=True)
    both["source"] = np.where(both["model"].isin(PROMOTE), "classical", "panel")
    return both


def cluster_se(g, col):
    fm = g.groupby("fold")[col].mean()
    n = fm.notna().sum()
    return float(fm.std(ddof=1) / np.sqrt(n)) if n > 1 else float("nan")


def bootstrap_ci(diffs, n_boot=10000, seed=42, alpha=0.05):
    """Percentile CI for the mean paired difference, resampling repetitions."""
    rng = np.random.default_rng(seed)
    d = np.asarray(diffs, dtype=float)
    d = d[~np.isnan(d)]
    if d.size < 2:
        return float("nan"), float("nan")
    idx = rng.integers(0, d.size, size=(n_boot, d.size))
    means = d[idx].mean(axis=1)
    return (float(np.percentile(means, 100 * alpha / 2)),
            float(np.percentile(means, 100 * (1 - alpha / 2))))


def contrast_vs_ewma(df, workload, metric, lines):
    sub = df[df["workload"] == workload]
    ewma = sub[sub["model"] == "EWMA"].set_index("fold")[metric]
    others = sorted(m for m in sub["model"].unique() if m not in PROMOTE)
    rows = []
    for model in others:
        mm = sub[sub["model"] == model].set_index("fold")[metric]
        folds = ewma.index.intersection(mm.index)
        if len(folds) < 3:
            continue
        d = (mm.loc[folds] - ewma.loc[folds]).astype(float)
        d_clean = d.dropna()
        if d_clean.size < 3:
            continue
        # exact Wilcoxon signed-rank over paired fold differences
        if np.allclose(d_clean.values, 0):
            pval, stat = 1.0, float("nan")
        else:
            try:
                stat, pval = wilcoxon(d_clean.values, alternative="two-sided",
                                      zero_method="wilcox")
            except ValueError:
                stat, pval = float("nan"), 1.0
        lo, hi = bootstrap_ci(d_clean.values)
        rows.append(dict(workload=workload, metric=metric, model=model,
                         delta_vs_ewma=float(d_clean.mean()),
                         se=float(d_clean.std(ddof=1) / np.sqrt(d_clean.size)),
                         ci_lo=lo, ci_hi=hi, wilcoxon_stat=float(stat),
                         pval=float(pval), n_folds=int(d_clean.size)))
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    rej, padj, _, _ = multipletests(out["pval"], method="holm")
    out["p_holm"] = padj
    out["sig_holm_05"] = rej
    lines += [f"\n=== {workload}, {metric}: model minus EWMA, paired over the 10 "
              f"held-out repetitions (Wilcoxon signed-rank, Holm-corrected) ===",
              out.sort_values("delta_vs_ewma", ascending=False)
                 .round({"delta_vs_ewma": 4, "se": 4, "ci_lo": 4, "ci_hi": 4,
                         "pval": 5, "p_holm": 5})
                 .to_string(index=False)]
    return out


def main():
    df = load_panel()
    df.to_csv(OUT / "rq1_panel_with_ewma.csv", index=False)

    lines = ["RQ1 MODEL COMPARISON WITH EWMA PROMOTED INTO THE MAIN PANEL", "",
             f"Anchor window: {ANCHOR_WINDOW}s. Unit of analysis is the fold: under",
             "leave-one-rep-out each fold tests exactly one held-out repetition, so the ten",
             "test partitions are disjoint. Training sets overlap (each repetition appears in",
             "seven of ten), so contrasts are paired over folds rather than pooled.", ""]

    summary = []
    for (wl, model), g in df.groupby(["workload", "model"]):
        row = dict(workload=wl, model=model, n_folds=int(g["fold"].nunique()))
        for m in ("f1_score", "auc_roc", "precision", "recall"):
            row[f"{m}_mean"] = g[m].mean()
            row[f"{m}_clusterse"] = cluster_se(g, m)
        summary.append(row)
    s = pd.DataFrame(summary)

    for wl in sorted(df["workload"].unique()):
        sw = s[s["workload"] == wl].sort_values("auc_roc_mean", ascending=False)
        lines += [f"\n=== {wl}: mean (fold-clustered SE), ranked by AUC-ROC ==="]
        disp = sw[["model", "f1_score_mean", "f1_score_clusterse",
                   "auc_roc_mean", "auc_roc_clusterse"]].round(4)
        lines.append(disp.to_string(index=False))

    contrasts = []
    for wl in sorted(df["workload"].unique()):
        for metric in ("f1_score", "auc_roc"):
            c = contrast_vs_ewma(df, wl, metric, lines)
            if not c.empty:
                contrasts.append(c)
    if contrasts:
        pd.concat(contrasts).to_csv(OUT / "rq1_with_ewma_contrasts.csv", index=False)

    text = "\n".join(lines)
    (OUT / "rq1_with_ewma_stats.txt").write_text(text)
    print(text)

    # LaTeX: one block per workload, EWMA inline with the panel
    tex = [r"\begin{tabular}{lcc}", r"\toprule",
           r"Model & F1 & AUC-ROC \\", r"\midrule"]
    for wl in sorted(df["workload"].unique()):
        tex.append(rf"\multicolumn{{3}}{{l}}{{\textit{{{wl}}}}} \\")
        sw = s[s["workload"] == wl].sort_values("auc_roc_mean", ascending=False)
        for _, r in sw.iterrows():
            label = MODEL_LABEL.get(r["model"], r["model"])
            if r["model"] in PROMOTE:
                label = rf"\textbf{{{label}}}"
            tex.append(f"{label} & {r['f1_score_mean']:.3f} ({r['f1_score_clusterse']:.3f})"
                       f" & {r['auc_roc_mean']:.3f} ({r['auc_roc_clusterse']:.3f}) \\\\")
        tex.append(r"\midrule")
    tex[-1] = r"\bottomrule"
    tex.append(r"\end{tabular}")
    (OUT / "rq1_with_ewma_table.tex").write_text("\n".join(tex))

    s.to_csv(OUT / "rq1_with_ewma_summary.csv", index=False)
    print(f"\nSaved: {OUT}/rq1_panel_with_ewma.csv")
    print(f"Saved: {OUT}/rq1_with_ewma_stats.txt")
    print(f"Saved: {OUT}/rq1_with_ewma_table.tex")


if __name__ == "__main__":
    main()
