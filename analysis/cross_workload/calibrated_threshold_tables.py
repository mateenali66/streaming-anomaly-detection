"""Recovery-window exclusion, three-state regimes and threshold-only leave-one-fault-out,
at calibrated operating points on the primary run's scores.

All three analyses use the retained per-window test scores of the primary run (the source
of the RQ1 tables), aligned to the feature parquet so that each window keeps its phase
label and fault id. No model is retrained or re-scored.

1. Regimes at the calibrated threshold. Each model-fold is thresholded at the F1-optimal
   threshold stored with the primary run, which was fitted on the cal split. Applied to the
   retained scores it reproduces every published per-fold F1 exactly (checked below). The
   evaluated population then changes with the regime:
     active    ANOMALOUS vs NORMAL                  RECOVERY excluded
     recovery  RECOVERY vs NORMAL                   ANOMALOUS excluded
     hybrid    ANOMALOUS and RECOVERY vs NORMAL
     binary    ANOMALOUS vs NORMAL and RECOVERY     the headline regime
   Prevalence differs between regimes, so each F1 is reported next to the F1 of a detector
   that flags every window, 2p/(1+p), and next to AUC-ROC, which does not depend on it.
   "active" against "binary" is the recovery-window exclusion. The EWMA baseline is
   included at its own cal-fitted threshold from classical_baselines_per_fold.csv.

2. Threshold-only leave-one-fault-out. For held-out fault h and test fold i, the threshold
   is refitted with fault h's anomalous windows removed. The retained scores cover test
   folds only, so the refit pools the other nine folds' windows, never fold i's. The search
   is the training pipeline's own (99 thresholds from 0.01 to 0.99, first best F1). Fold i
   is then evaluated on its NORMAL and RECOVERY windows plus fault h's anomalous windows.
   The same pooled search with no fault held out is reported against the published F1, as
   a check on pooling. Random Forest is trained with labels, so its row is not a held-out
   result for training, only for the threshold.

Run:  python analysis/cross_workload/calibrated_threshold_tables.py
Outputs (this directory):
  calibrated_regimes_per_fold.csv     1, per workload x model x fold x regime
  calibrated_regimes_summary.csv      1, means over folds, active minus binary, Wilcoxon p
  fault_holdout_threshold_per_fold.csv 2, per workload x model x fold x held-out fault
  fault_holdout_threshold_summary.csv  2, means over folds
  fault_holdout_pooling_check.csv     2, pooled search with no fault held out, against published F1
  calibrated_threshold_tables.txt     readable report
"""
from pathlib import Path
import sys

import numpy as np
import pandas as pd
from scipy.stats import wilcoxon
from sklearn.metrics import f1_score, roc_auc_score

OUT = Path(__file__).parent
sys.path.insert(0, str(OUT))
from ewma_mechanism import (PANEL, SCORE_TREES, WORKLOADS, feat_cols, fold_data,  # noqa: E402
                            load_aligned, score_variant)
from three_state_auc import REGIMES  # noqa: E402

GRID = np.linspace(0.01, 0.99, 99)   # training.find_optimal_threshold


def pipeline_threshold(scores, y):
    """training.find_optimal_threshold, vectorised: first grid point with the highest F1."""
    pred = scores[None, :] >= GRID[:, None]
    tp = (pred & (y == 1)).sum(1)
    fp = (pred & (y == 0)).sum(1)
    fn = ((~pred) & (y == 1)).sum(1)
    den = 2 * tp + fp + fn
    f1 = np.where(den > 0, 2 * tp / np.maximum(den, 1), 0.0)
    if f1.max() <= 0:
        return 0.5
    return float(GRID[int(np.argmax(f1))])


def always_fire_f1(y):
    p = float(np.mean(y))
    return 2 * p / (1 + p) if p > 0 else 0.0


def stored_thresholds():
    """Calibrated threshold per (workload, model, fold), from the run that produced the
    retained scores."""
    out = {}
    for wl, trees in SCORE_TREES.items():
        for tree in trees:
            mr = pd.read_csv(tree.parent / "model_results.csv")
            for _, r in mr[mr["category"] == "all"].iterrows():
                out[(wl, r["model"], int(r["fold"]))] = (float(r["threshold"]),
                                                        float(r["f1_score"]))
    cb = pd.read_csv(OUT / "classical_baselines_per_fold.csv")
    for _, r in cb[cb["model"] == "EWMA"].iterrows():
        out[(r["workload"], "EWMA", int(r["fold"]))] = (float(r["threshold"]), float(r["f1"]))
    return out


def load_workload(wl, thr):
    """Aligned scores, phases and fault ids for every model-fold of one workload."""
    df = pd.read_parquet(WORKLOADS[wl])
    data, skipped = {}, []
    for fold in range(1, 11):
        test = df[df[f"fold_{fold}_split"] == "test"]
        y_te = (test["label"] == "ANOMALOUS").astype(int).to_numpy()
        for model in PANEL:
            s, off = load_aligned(wl, model, fold, y_te)
            if s is None or (wl, model, fold) not in thr:
                skipped.append(f"{wl}/{model}/fold{fold}")
                continue
            data[(model, fold)] = dict(s=s, phase=test["label"].to_numpy()[off:],
                                       fault=test["fault_id"].astype(str).to_numpy()[off:])
        # the EWMA baseline, recomputed exactly as published (ewma_mechanism.py checks this)
        tr, _, te, _, _, _, _ = fold_data(df, feat_cols(df), fold)
        data[("EWMA", fold)] = dict(s=score_variant("published", tr, te, None),
                                    phase=test["label"].to_numpy(),
                                    fault=test["fault_id"].astype(str).to_numpy())
    return data, skipped


def regimes(wl, data, thr):
    rows, mismatches = [], []
    for (model, fold), d in data.items():
        t, f1_pub = thr[(wl, model, fold)]
        pred = (d["s"] >= t).astype(int)
        for reg, (keep, pos) in REGIMES.items():
            m = np.isin(d["phase"], keep)
            y = np.isin(d["phase"][m], pos).astype(int)
            if not 0 < y.sum() < len(y):
                continue
            f1 = f1_score(y, pred[m], zero_division=0)
            if reg == "binary" and abs(f1 - f1_pub) > 6e-4:
                mismatches.append((wl, model, fold, f1, f1_pub))
            rows.append(dict(workload=wl, model=model, fold=fold, regime=reg, threshold=t,
                             f1=f1, auc=roc_auc_score(y, d["s"][m]),
                             always_fire_f1=always_fire_f1(y), prevalence=float(y.mean()),
                             n=int(m.sum())))
    return rows, mismatches


def fault_holdout(wl, data, thr):
    rows, pooled_check = [], []
    # EWMA scores are unbounded, so the pipeline's 0.01-0.99 search does not apply to it
    models = sorted({m for m, _ in data} - {"EWMA"})
    for model in models:
        folds = sorted(f for m, f in data if m == model)
        for i in folds:
            others = [data[(model, j)] for j in folds if j != i]
            ps = np.concatenate([o["s"] for o in others])
            pph = np.concatenate([o["phase"] for o in others])
            pfa = np.concatenate([o["fault"] for o in others])
            py = (pph == "ANOMALOUS").astype(int)
            d = data[(model, i)]
            y_i = (d["phase"] == "ANOMALOUS").astype(int)
            # pooling check: no fault held out, full binary test fold
            t_all = pipeline_threshold(ps, py)
            pooled_check.append(dict(workload=wl, model=model, fold=i,
                                     f1_pooled=f1_score(y_i, (d["s"] >= t_all).astype(int),
                                                        zero_division=0),
                                     f1_published=thr[(wl, model, i)][1]))
            for h in sorted(set(d["fault"][y_i == 1])):
                keep_cal = ~((pph == "ANOMALOUS") & (pfa == h))
                t_h = pipeline_threshold(ps[keep_cal], py[keep_cal])
                ev = (d["phase"] != "ANOMALOUS") | (d["fault"] == h)
                y = y_i[ev]
                s = d["s"][ev]
                t_cal = thr[(wl, model, i)][0]
                rows.append(dict(workload=wl, model=model, fold=i, fault=h, threshold=t_h,
                                 threshold_all=t_all,
                                 f1=f1_score(y, (s >= t_h).astype(int), zero_division=0),
                                 f1_all_faults=f1_score(y, (s >= t_all).astype(int),
                                                        zero_division=0),
                                 f1_calibrated=f1_score(y, (s >= t_cal).astype(int),
                                                        zero_division=0),
                                 auc=roc_auc_score(y, s), always_fire_f1=always_fire_f1(y),
                                 n_pos=int(y.sum()), n=int(len(y))))
    return rows, pooled_check


def main():
    thr = stored_thresholds()
    reg_rows, lofo_rows, checks, skipped, mism = [], [], [], [], []
    for wl in WORKLOADS:
        data, sk = load_workload(wl, thr)
        skipped += sk
        r, mm = regimes(wl, data, thr)
        reg_rows += r
        mism += mm
        lr, pc = fault_holdout(wl, data, thr)
        lofo_rows += lr
        checks += pc
    if mism:
        raise SystemExit(f"stored thresholds do not reproduce the published F1: {mism[:5]}")

    a = pd.DataFrame(reg_rows)
    a.round(6).to_csv(OUT / "calibrated_regimes_per_fold.csv", index=False)
    summ = []
    for (wl, model), g in a.groupby(["workload", "model"]):
        rec = dict(workload=wl, model=model)
        for metric in ["f1", "auc", "always_fire_f1"]:
            w = g.pivot(index="fold", columns="regime", values=metric)
            for reg in REGIMES:
                rec[f"{metric}_{reg}"] = w[reg].mean()
        g = g.assign(f1_over_always_fire=g["f1"] - g["always_fire_f1"])
        w = g.pivot(index="fold", columns="regime", values="f1_over_always_fire")
        for reg in REGIMES:
            rec[f"f1_over_always_fire_{reg}"] = w[reg].mean()
        w = g.pivot(index="fold", columns="regime", values="f1")
        d = (w["active"] - w["binary"]).dropna()
        rec["f1_active_minus_binary"] = d.mean()
        rec["p_f1_active_vs_binary"] = wilcoxon(d).pvalue if d.abs().sum() else 1.0
        summ.append(rec)
    s = pd.DataFrame(summ)
    s.round(6).to_csv(OUT / "calibrated_regimes_summary.csv", index=False)

    lf = pd.DataFrame(lofo_rows)
    lf.round(6).to_csv(OUT / "fault_holdout_threshold_per_fold.csv", index=False)
    ls = (lf.groupby(["workload", "model", "fault"])
            [["f1", "f1_all_faults", "f1_calibrated", "auc", "always_fire_f1", "threshold",
               "threshold_all"]].mean()
            .reset_index())
    ls.round(6).to_csv(OUT / "fault_holdout_threshold_summary.csv", index=False)
    pc = pd.DataFrame(checks)
    pc.round(6).to_csv(OUT / "fault_holdout_pooling_check.csv", index=False)
    pc["abs_diff"] = (pc["f1_pooled"] - pc["f1_published"]).abs()
    lf["holdout_effect"] = lf["f1"] - lf["f1_all_faults"]
    lf["threshold_moved"] = (lf["threshold"] - lf["threshold_all"]).abs() > 1e-9

    show = s[["workload", "model"] + [f"f1_{r}" for r in REGIMES] + [f"auc_{r}" for r in REGIMES]
             + ["f1_active_minus_binary", "p_f1_active_vs_binary"]]
    af = a.groupby(["workload", "regime"]).always_fire_f1.mean().unstack()
    f1w = ls.pivot_table(index=["workload", "model"], columns="fault", values="f1")
    aucw = ls.pivot_table(index=["workload", "model"], columns="fault", values="auc")
    afw = ls.groupby(["workload", "fault"]).always_fire_f1.mean().unstack()
    txt = ("REGIMES AT THE CALIBRATED THRESHOLD (mean over 10 folds)\n\n"
           + show.round(3).to_string(index=False)
           + "\n\nalways-fire F1 by regime\n" + af.round(3).to_string()
           + "\n\nTHRESHOLD-ONLY LEAVE-ONE-FAULT-OUT, F1 (mean over folds)\n"
           + f1w.round(3).to_string()
           + "\n\nAUC-ROC on the same populations\n" + aucw.round(3).to_string()
           + "\n\nalways-fire F1 per held-out fault\n" + afw.round(3).to_string()
           + "\n\nholding the fault out of calibration: F1 minus F1 with all faults, same windows\n"
           + lf.groupby(["workload", "model"]).holdout_effect.agg(["mean", "min", "max"])
               .round(3).to_string()
           + "\n\nshare of model-fold-faults whose threshold moved: "
           + f"{lf.threshold_moved.mean():.3f}"
           + "\n\npooling check, no fault held out: mean |F1 pooled - F1 published| by workload\n"
           + pc.groupby("workload").abs_diff.agg(["mean", "max"]).round(3).to_string()
           + f"\n\nmodel-folds skipped (alignment failed): {len(skipped)} {skipped}\n")
    (OUT / "calibrated_threshold_tables.txt").write_text(txt)
    print(txt)


if __name__ == "__main__":
    main()
