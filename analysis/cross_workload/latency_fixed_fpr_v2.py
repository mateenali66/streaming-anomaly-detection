"""Episode-level detection latency at fixed FPR budgets, full ten-model panel.

Reads the raw per-window scores of the run behind the RQ1 tables, and of the Deep SVDD and
DAGMM run on the same folds. The threshold for each (model, test fold) is the (1 - budget)
quantile of normal-window scores pooled across the other nine folds, so no test-fold
information sets the threshold. The realised FPR on the held-out fold is reported
separately rather than being forced to meet the budget.

Output:
  latency_fixed_fpr_v2_per_fold.csv
  latency_fixed_fpr_v2_summary.csv
  latency_fixed_fpr_v2.txt
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(OUT))


SCORE_TREES = [ROOT / "data" / "results_v3" / "merged" / "raw_scores",
               ROOT / "data" / "v3" / "results" / "ecom-gapfill" / "raw_scores"]
MODELS = ["IF", "OC_SVM", "LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE",
          "DeepSVDD", "DAGMM", "RF", "RuleBased"]
FPR_BUDGETS = [0.01, 0.05]
WINDOW_SEC = 30  # latency is reported in seconds at the 30-second window


def episode_runs(y):
    runs, in_run, start = [], False, 0
    for i, v in enumerate(y):
        if v == 1 and not in_run:
            start = i
            in_run = True
        elif v == 0 and in_run:
            runs.append((start, i - 1))
            in_run = False
    if in_run:
        runs.append((start, len(y) - 1))
    return runs


def episode_metrics(scores, y, threshold):
    pred = (scores >= threshold).astype(int)
    runs = episode_runs(y)
    detected, latencies = 0, []
    for s, e in runs:
        ep_pred = pred[s:e + 1]
        if ep_pred.any():
            detected += 1
            latencies.append(int(np.argmax(ep_pred)) * WINDOW_SEC)
    n_ep = len(runs)
    fpr_test = float(((scores >= threshold) & (y == 0)).sum()) / max(int((y == 0).sum()), 1)
    return {
        "n_episodes": n_ep,
        "n_detected": detected,
        "detection_rate": detected / max(n_ep, 1),
        "median_latency_s": float(np.median(latencies)) if latencies else float("nan"),
        "p95_latency_s": float(np.quantile(latencies, 0.95)) if latencies else float("nan"),
        "fpr_test": fpr_test,
    }


def loo_threshold(model_data, held_out_fold, fpr_budget):
    """Threshold = (1 - fpr_budget) quantile of pooled normal scores from
    every fold except held_out_fold."""
    pool = []
    for fold, (sc, y) in model_data.items():
        if fold == held_out_fold:
            continue
        pool.append(sc[y == 0])
    if not pool:
        return None
    pooled = np.concatenate(pool)
    if len(pooled) == 0:
        return None
    return float(np.quantile(pooled, 1.0 - fpr_budget))


def load_all():
    data = {m: {} for m in MODELS}
    for tree in SCORE_TREES:
        if not tree.exists():
            continue
        for f in sorted(tree.glob("*_scores.npy")):
            base = f.name.replace("_scores.npy", "")
            model, fold_str = base.rsplit("_fold", 1)
            if model not in MODELS:
                continue
            fold = int(fold_str)
            if fold in data[model]:
                continue          # first tree wins, merged before gap-fill
            data[model][fold] = (np.load(f), np.load(tree / f"{base}_labels.npy"))
    return data


def main():
    data = load_all()
    missing = {m: sorted(set(range(1, 11)) - set(f)) for m, f in data.items()}
    for m, folds in missing.items():
        if folds:
            print(f"  WARNING {m}: missing folds {folds}")

    rows = []
    for model, folds in data.items():
        for fold, (scores, y) in folds.items():
            for budget in FPR_BUDGETS:
                t = loo_threshold(folds, fold, budget)
                if t is None:
                    continue
                rows.append({"model": model, "fold": fold, "fpr_budget": budget,
                             "threshold": t, **episode_metrics(scores, y, t)})
    df = pd.DataFrame(rows)
    if df.empty:
        print("No scores found.")
        return
    df.to_csv(OUT / "latency_fixed_fpr_v2_per_fold.csv", index=False)

    def cluster_se(s):
        n = s.notna().sum()
        return s.std(ddof=1) / np.sqrt(n) if n > 1 else np.nan

    summary = df.groupby(["model", "fpr_budget"]).agg(
        n_folds=("fold", "nunique"),
        det_rate_mean=("detection_rate", "mean"),
        det_rate_se=("detection_rate", cluster_se),
        median_latency_s=("median_latency_s", "mean"),
        p95_latency_s=("p95_latency_s", "mean"),
        fpr_realised=("fpr_test", "mean"),
        fpr_realised_se=("fpr_test", cluster_se),
    ).round(3).reset_index()
    summary.to_csv(OUT / "latency_fixed_fpr_v2_summary.csv", index=False)

    lines = ["DETECTION LATENCY AT FIXED FPR BUDGETS, FULL PANEL", "",
             "e-commerce, 30s windows, published run tree, all ten models.",
             "Threshold per (model, test fold) is the (1 - budget) quantile of normal-window",
             "scores pooled over the other nine folds. Realised FPR is measured on the",
             "held-out fold and is not forced to meet the budget.", ""]
    for b in FPR_BUDGETS:
        sub = summary[summary["fpr_budget"] == b].sort_values(
            "det_rate_mean", ascending=False)
        lines += [f"=== FPR budget {b:.0%} ===", sub.to_string(index=False), ""]
    text = "\n".join(lines)
    (OUT / "latency_fixed_fpr_v2.txt").write_text(text)
    print(text)


if __name__ == "__main__":
    main()
