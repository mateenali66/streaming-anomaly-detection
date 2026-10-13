"""AUC-ROC under the four three-state labelling regimes, on the primary run's scores.

The regimes change the evaluated population: RECOVERY windows are dropped, or moved to the
positive class, so prevalence differs between regimes and F1 is not comparable across them.
AUC-ROC does not depend on prevalence. This script scores every panel model, plus the EWMA
baseline, with AUC-ROC under each regime on the published test folds.

The scores are the retained per-window test scores of the primary run (the source of the
RQ1 tables), aligned to the feature parquet so that each window keeps its phase label. A
model-fold is used only if its retained binary labels equal the parquet labels on the
aligned rows.

Regimes:
  active    ANOMALOUS vs NORMAL                   RECOVERY excluded
  recovery  RECOVERY vs NORMAL                    ANOMALOUS excluded
  hybrid    ANOMALOUS and RECOVERY vs NORMAL
  binary    ANOMALOUS vs NORMAL and RECOVERY      the headline regime

Run:  python analysis/cross_workload/three_state_auc.py
Output: three_state_auc_per_fold.csv, three_state_auc_summary.csv, three_state_auc.txt
"""
from pathlib import Path
import sys

import numpy as np
import pandas as pd
from scipy.stats import wilcoxon
from sklearn.metrics import roc_auc_score

OUT = Path(__file__).parent
sys.path.insert(0, str(OUT))
from ewma_mechanism import (WORKLOADS, PANEL, feat_cols, fold_data, load_aligned,  # noqa: E402
                            score_variant)

REGIMES = {
    "active": (["NORMAL", "ANOMALOUS"], ["ANOMALOUS"]),
    "recovery": (["NORMAL", "RECOVERY"], ["RECOVERY"]),
    "hybrid": (["NORMAL", "ANOMALOUS", "RECOVERY"], ["ANOMALOUS", "RECOVERY"]),
    "binary": (["NORMAL", "ANOMALOUS", "RECOVERY"], ["ANOMALOUS"]),
}


def regime_aucs(scores, phases):
    out = {}
    for reg, (keep, pos) in REGIMES.items():
        m = np.isin(phases, keep)
        y = np.isin(phases[m], pos).astype(int)
        if 0 < y.sum() < len(y):
            out[reg] = (roc_auc_score(y, scores[m]), float(y.mean()), int(m.sum()))
    return out


def main():
    rows, skipped = [], []
    for wl, path in WORKLOADS.items():
        df = pd.read_parquet(path)
        cols = feat_cols(df)
        for fold in range(1, 11):
            tr, ca, te, y_ca, y_te, cal, test = fold_data(df, cols, fold)
            phases = test["label"].to_numpy()
            for model in PANEL:
                s, off = load_aligned(wl, model, fold, y_te)
                if s is None:
                    skipped.append(f"{wl}/{model}/fold{fold}")
                    continue
                for reg, (auc, p, n) in regime_aucs(s, phases[off:]).items():
                    rows.append(dict(workload=wl, model=model, fold=fold, regime=reg, auc=auc,
                                     prevalence=p, n=n))
            s = score_variant("published", tr, te, None)
            for reg, (auc, p, n) in regime_aucs(s, phases).items():
                rows.append(dict(workload=wl, model="EWMA", fold=fold, regime=reg, auc=auc,
                                 prevalence=p, n=n))
    a = pd.DataFrame(rows)
    a.to_csv(OUT / "three_state_auc_per_fold.csv", index=False)

    summ = []
    for (wl, model), g in a.groupby(["workload", "model"]):
        w = g.pivot(index="fold", columns="regime", values="auc")
        rec = dict(workload=wl, model=model, **{r: w[r].mean() for r in REGIMES})
        d = (w["active"] - w["binary"]).dropna()
        rec["active_minus_binary"] = d.mean()
        rec["p_active_vs_binary"] = wilcoxon(d).pvalue if d.abs().sum() else 1.0
        summ.append(rec)
    s = pd.DataFrame(summ)
    s.round(6).to_csv(OUT / "three_state_auc_summary.csv", index=False)
    prev = a.groupby(["workload", "regime"]).prevalence.mean().unstack().round(3)
    txt = ("THREE-STATE REGIMES ON AUC-ROC, PRIMARY-RUN SCORES (mean over 10 folds)\n\n"
           + s.round(3).to_string(index=False)
           + "\n\nmean prevalence by regime\n" + prev.to_string()
           + f"\n\nmodel-folds skipped (alignment failed): {len(skipped)} {skipped}\n")
    (OUT / "three_state_auc.txt").write_text(txt)
    print(txt)


if __name__ == "__main__":
    main()
