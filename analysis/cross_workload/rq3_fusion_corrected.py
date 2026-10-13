"""RQ3 category fusion on a matched evaluation population.

Scores four fusion strategies of the per-category IF scores on the full test fold, so every
strategy is scored on identical windows, with F1 and AUC-ROC. Weights for the weighted
strategy, and every F1 threshold, are fitted on the other nine folds, never on the fold being
evaluated. Majority vote alerts when at least two of the four categories exceed their
thresholds. Per-category scores come from the retained category-ablation runs. The
single-model IF baseline is the RQ1 result on the same windows.

Output:
  rq3_fusion_corrected_per_fold.csv
  rq3_fusion_corrected_summary.csv
  rq3_fusion_corrected.txt
"""
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score, roc_auc_score

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
SCORES = ROOT / "data" / "results_v3" / "merged" / "ablation_category" / "raw_scores"
CATEGORIES = ["broker", "consumer", "flink", "jvm"]
MODEL = "IF"
N_FOLDS = 10


def load_fold(fold):
    """Per-category score matrix and labels for one fold, or None if incomplete."""
    cols, labels = [], None
    for c in CATEGORIES:
        s = SCORES / f"{MODEL}_{c}_fold{fold}_scores.npy"
        l = SCORES / f"{MODEL}_{c}_fold{fold}_labels.npy"
        if not (s.exists() and l.exists()):
            return None, None
        sc, lb = np.load(s), np.load(l)
        if labels is None:
            labels = lb
        elif len(lb) != len(labels) or not np.array_equal(lb, labels):
            return None, None
        cols.append(sc)
    n = min(len(c) for c in cols)
    return np.column_stack([c[:n] for c in cols]), labels[:n]


def f1_optimal(scores, y):
    order = np.unique(scores)
    if order.size > 400:
        order = np.quantile(scores, np.linspace(0, 1, 400))
    best, bt = -1.0, float(np.median(scores))
    for t in order:
        pred = scores >= t
        if pred.sum() == 0:
            continue
        f = f1_score(y, pred.astype(int), zero_division=0)
        if f > best:
            best, bt = f, float(t)
    return bt


def metrics(scores, y, t):
    """F1 at threshold t, fitted elsewhere, and threshold-free AUC-ROC."""
    pred = (scores >= t).astype(int)
    p = float(np.mean(y))
    return dict(f1=float(f1_score(y, pred, zero_division=0)),
                auc=float(roc_auc_score(y, scores)) if len(np.unique(y)) > 1 else np.nan,
                n=len(y), prevalence=p, always_fire_f1=2 * p / (1 + p),
                fire_rate=float(pred.mean()))


def main():
    folds = {}
    for f in range(1, N_FOLDS + 1):
        X, y = load_fold(f)
        if X is not None:
            folds[f] = (X, y)
    print(f"loaded {len(folds)} folds with complete per-category scores")
    if not folds:
        return

    rows = []
    for f, (X, y) in folds.items():
        # Every threshold and weight is fitted on the other nine folds, never on fold f.
        others = [o for o in folds if o != f]
        Xtr = np.vstack([folds[o][0] for o in others])
        ytr = np.concatenate([folds[o][1] for o in others])
        for name, fuse in (("max", lambda m: m.max(axis=1)), ("mean", lambda m: m.mean(axis=1))):
            rows.append(dict(fold=f, strategy=name,
                             **metrics(fuse(X), y, f1_optimal(fuse(Xtr), ytr))))
        # majority vote: each category's threshold from the other folds, alert when >= 2 of 4 fire
        cat_t = [f1_optimal(Xtr[:, j], ytr) for j in range(X.shape[1])]
        votes = np.mean([(X[:, j] >= cat_t[j]) for j in range(X.shape[1])], axis=0)
        rows.append(dict(fold=f, strategy="majority_vote", **metrics(votes, y, 0.5)))
        # weighted: logistic regression on the other folds, threshold on its training scores
        lr = LogisticRegression(class_weight="balanced", random_state=42,
                                max_iter=1000).fit(Xtr, ytr)
        t_w = f1_optimal(lr.predict_proba(Xtr)[:, 1], ytr)
        rows.append(dict(fold=f, strategy="weighted_loo",
                         **metrics(lr.predict_proba(X)[:, 1], y, t_w)))

    d = pd.DataFrame(rows)
    d.to_csv(OUT / "rq3_fusion_corrected_per_fold.csv", index=False)

    def cse(s):
        return s.std(ddof=1) / np.sqrt(s.notna().sum())

    g = d.groupby("strategy").agg(
        n_folds=("fold", "nunique"), f1=("f1", "mean"), f1_se=("f1", cse),
        auc=("auc", "mean"), auc_se=("auc", cse),
        prevalence=("prevalence", "mean"), always_fire=("always_fire_f1", "mean"),
        n_windows=("n", "mean")).reset_index()
    g["f1_over_always_fire"] = g["f1"] - g["always_fire"]
    g = g.sort_values("auc", ascending=False)
    g.to_csv(OUT / "rq3_fusion_corrected_summary.csv", index=False)

    lines = ["RQ3 CATEGORY FUSION", "",
             "All strategies are scored on the identical full test fold. Weights and every",
             "threshold are fitted on the other nine folds, never on the evaluated fold.",
             "F1 is reported against the always-fire baseline 2p/(1+p) at that population's",
             "prevalence, because F1 alone is not interpretable here.", "",
             g.round(4).to_string(index=False)]
    text = "\n".join(lines)
    (OUT / "rq3_fusion_corrected.txt").write_text(text)
    print(text)


if __name__ == "__main__":
    main()
