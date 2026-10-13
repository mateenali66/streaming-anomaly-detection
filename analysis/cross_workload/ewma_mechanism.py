"""Sources of the EWMA baseline's lead over the deep panel.

Tests whether EWMA's lead comes from the telemetry, where faults appear as direct magnitude
shifts in a few metrics, or from limits of the deep models such as score saturation and
feature representation. Five analyses, all on the
published feature parquets, fold layout and retained per-window scores. No model is
retrained.

A. Saturation bound. For each deep model, AUC-ROC if every tie at the saturated maximum
   score were broken in the model's favour. This is the most saturation can be costing.
B. EWMA ablation. Change one component of the published EWMA at a time: its centre, its
   scale, pooling across features (max against mean squared, the autoencoders' pooling),
   a sigmoid squash identical to the autoencoders' score function, and a running centre
   that tracks each contiguous test run (the moving-average behaviour the name implies).
C. Per-fault AUC-ROC, EWMA against every panel model. Positives are one fault's ANOMALOUS
   windows, negatives are every non-anomalous test window of the fold.
D. For fault windows EWMA flags, the telemetry category of the feature that sets its
   maximum, against that category's share of the feature space.
E. The re-anchored comparison: each deep model against the static max-z diagnostic
   (RuleBased in the panel files), paired over folds, Holm-corrected over the six deep
   models, from rq1_panel_with_ewma.csv.

Two robustness variants join the ablation in B: a floor on each feature's scale at 0.1 of
its training standard deviation, and removal of the three features that most often set
the published maximum on the cal split. "modal_tie" is the share of test windows that
share the single most common score value.

The published EWMA (classical_baselines.py, score_ewma) scores every window against a
fixed centre, the last exponentially smoothed value of the training-normal sequence, with
a per-feature scale equal to the time-average of the training EWMA standard deviation,
and takes the maximum over features. Variant "published" must reproduce
classical_baselines_per_fold.csv exactly; the script asserts it.

Run:  python analysis/cross_workload/ewma_mechanism.py
Outputs (this directory):
  ewma_mechanism_saturation.csv        A, per workload x model
  ewma_mechanism_ablation_per_fold.csv B, per workload x variant x fold
  ewma_mechanism_ablation_summary.csv  B, means, fold-clustered SE, paired test vs published
  ewma_mechanism_per_fault_auc.csv     C, per workload x model x fault (mean over folds)
  ewma_mechanism_argmax_category.csv   D, per workload x fault x category
  ewma_mechanism_maxz_contrasts.csv    E, deep model minus max-z, per workload x metric
  ewma_mechanism.txt                   readable report
"""
from pathlib import Path
import warnings

import numpy as np
import pandas as pd
from scipy.special import expit
from scipy.stats import wilcoxon
from sklearn.metrics import f1_score, roc_auc_score
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore")

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).parent
N_FOLDS = 10
ALPHA = 0.3            # published EWMA smoothing constant
GAP_S = 45             # a jump in window_start larger than this starts a new contiguous run

WORKLOADS = {
    "ecommerce": ROOT / "data/features_v3/features_30s.parquet",
    "iot": ROOT / "data/v3/iot/features/features_30s.parquet",
}
SCORE_TREES = {
    "ecommerce": [ROOT / "data/results_v3/merged/raw_scores",
                  ROOT / "data/v3/results/ecom-gapfill/raw_scores"],
    "iot": [ROOT / "data/v3/iot/results/gpu-1/raw_scores",
            ROOT / "data/v3/iot/results/gpu-2/raw_scores",
            ROOT / "data/v3/iot/results/cpu-1/raw_scores"],
}
DEEP = ["LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE", "DeepSVDD", "DAGMM"]
PANEL = DEEP + ["IF", "OC_SVM", "RF", "RuleBased"]   # RuleBased is the max-z diagnostic
EXCLUDE = {"label", "fault_id", "fault_category", "repetition", "window_start",
           "window_end", "window_mid", "n_samples", "run_number", "run_source"}


def feat_cols(df):
    return [c for c in df.columns
            if c not in EXCLUDE and not c.startswith("fold_") and df[c].dtype.kind in "fi"]


# identical to classical_baselines.find_optimal_threshold
def find_optimal_threshold(scores, y):
    if y.sum() == 0 or y.sum() == len(y):
        return float(np.nanmax(scores))
    best_t, best_f1 = float(np.nanmax(scores)), -1.0
    cand = np.unique(scores)
    if len(cand) > 200:
        cand = np.quantile(scores, np.linspace(0, 1, 201))
    for t in cand:
        f = f1_score(y, (scores >= t).astype(int), zero_division=0)
        if f > best_f1:
            best_f1, best_t = f, float(t)
    return best_t


# ---------------------------------------------------------------- EWMA components
def ewma_reference(X_train):
    """Published centre (last smoothed training value) and scale (mean EWMA std)."""
    mu = pd.DataFrame(X_train).ewm(alpha=ALPHA, adjust=False).mean().to_numpy()
    sd = pd.DataFrame(X_train).ewm(alpha=ALPHA, adjust=False).std().to_numpy()
    sd_global = np.nanmean(sd, axis=0)
    sd_global = np.where(sd_global > 1e-9, sd_global, 1.0)
    return mu[-1], sd_global


def running_centres(X_eval, start, segments):
    """Causal EWMA centre for each eval row: the smoothed value of the preceding rows of
    the same contiguous run, restarted at `start` at each run boundary."""
    c = np.empty_like(X_eval)
    cur = start.copy()
    for i in range(len(X_eval)):
        if segments[i]:
            cur = start.copy()
        c[i] = cur
        cur = ALPHA * X_eval[i] + (1 - ALPHA) * cur
    return c


def score_variant(variant, X_train, X_eval, seg_eval, drop=None):
    mu_last, sd_ewma = ewma_reference(X_train)
    centre, scale, pool = mu_last, sd_ewma, "max"
    if variant == "centre_train_mean":
        centre = X_train.mean(axis=0)
    elif variant == "scale_global_sd":
        scale = np.where(X_train.std(axis=0) > 1e-9, X_train.std(axis=0), 1.0)
    elif variant == "static_z":
        centre = X_train.mean(axis=0)
        scale = np.where(X_train.std(axis=0) > 1e-9, X_train.std(axis=0), 1.0)
    elif variant == "mean_sq_pool":
        pool = "mean_sq"
    elif variant == "mean_sq_centre_train_mean":
        pool = "mean_sq"
        centre = X_train.mean(axis=0)
    elif variant == "running_centre":
        centre = running_centres(X_eval, mu_last, seg_eval)
    elif variant == "scale_floor":
        # floor each feature's scale at 0.1 of its training standard deviation
        scale = np.maximum(sd_ewma, 0.1 * np.where(X_train.std(axis=0) > 1e-9, X_train.std(axis=0), 1.0))
    z = np.abs(X_eval - centre) / scale
    if variant == "drop_dominant" and drop is not None:
        z[:, drop] = 0.0
    s = np.nanmax(z, axis=1) if pool == "max" else np.nanmean(z ** 2, axis=1)
    if variant == "sigmoid_squash":
        # the autoencoders' score function applied to the published EWMA score:
        # sigmoid of the score standardised by its training distribution, kept in float32
        z_tr = np.abs(X_train - mu_last) / sd_ewma
        s_tr = np.nanmax(z_tr, axis=1)
        s = expit((s - s_tr.mean()) / (s_tr.std() + 1e-8)).astype(np.float32)
    return s


CAT_SHARE = {}
VARIANTS = ["published", "centre_train_mean", "running_centre", "scale_global_sd",
            "static_z", "mean_sq_pool", "mean_sq_centre_train_mean", "sigmoid_squash",
            "scale_floor", "drop_dominant"]


def segment_starts(frame):
    ws = pd.to_datetime(frame["window_start"]).to_numpy()
    gap = np.diff(ws).astype("timedelta64[s]").astype(float) > GAP_S
    run = frame["run_number"].astype(str).to_numpy()
    starts = np.r_[True, gap | (run[1:] != run[:-1])]
    return starts


def fold_data(df, cols, fold):
    col = f"fold_{fold}_split"
    train = df[df[col] == "train"]
    train_norm = train[train["label"] == "NORMAL"]
    cal, test = df[df[col] == "cal"], df[df[col] == "test"]
    scaler = StandardScaler().fit(train_norm[cols].fillna(0).to_numpy())
    tr = scaler.transform(train_norm[cols].fillna(0).to_numpy())
    ca = scaler.transform(cal[cols].fillna(0).to_numpy())
    te = scaler.transform(test[cols].fillna(0).to_numpy())
    y_ca = (cal["label"] == "ANOMALOUS").astype(int).to_numpy()
    y_te = (test["label"] == "ANOMALOUS").astype(int).to_numpy()
    return tr, ca, te, y_ca, y_te, cal, test


# ---------------------------------------------------------------- A. saturation bound
def saturation_bound(workload):
    rows = []
    for tree in SCORE_TREES[workload]:
        for f in sorted(tree.glob("*_scores.npy")):
            model, fold = f.name.replace("_scores.npy", "").rsplit("_fold", 1)
            s = np.load(f).astype(np.float64)
            y = np.load(tree / f.name.replace("_scores", "_labels")).astype(int)
            if y.min() == y.max():
                continue
            auc = roc_auc_score(y, s)
            tied = s == s.max()
            npos, nneg = y.sum(), (1 - y).sum()
            upper = auc + 0.5 * (tied & (y == 1)).sum() * (tied & (y == 0)).sum() / (npos * nneg)
            rows.append(dict(workload=workload, model=model, fold=int(fold), auc=auc,
                             auc_best_case=upper, frac_at_max=tied.mean(),
                             frac_normal_at_max=(tied & (y == 0)).sum() / nneg))
    return pd.DataFrame(rows)


# ---------------------------------------------------------------- C. aligned panel scores
def load_aligned(workload, model, fold, y_test):
    """Retained scores aligned to test rows. Point models score all rows; sequence models
    drop the first rows of the split. Alignment is accepted only if the retained labels
    equal the test labels on the aligned rows."""
    for tree in SCORE_TREES[workload]:
        f = tree / f"{model}_fold{fold}_scores.npy"
        if f.exists():
            s = np.load(f).astype(np.float64)
            y = np.load(tree / f"{model}_fold{fold}_labels.npy").astype(int)
            off = len(y_test) - len(s)
            if off >= 0 and np.array_equal(y, y_test[off:]):
                return s, off
            return None, None
    return None, None


def per_fault_auc(scores, y, faults):
    out = {}
    neg = y == 0
    for fid in sorted(set(faults[y == 1])):
        pos = (y == 1) & (faults == fid)
        if pos.sum() == 0 or neg.sum() == 0:
            continue
        m = pos | neg
        out[fid] = roc_auc_score(y[m], scores[m])
    return out


# ---------------------------------------------------------------- driver
def main():
    sat, abl, pf, arg = [], [], [], []
    align_report = []
    published_check = pd.read_csv(OUT / "classical_baselines_per_fold.csv")
    published_check = published_check[published_check.model == "EWMA"]

    for workload, path in WORKLOADS.items():
        sat.append(saturation_bound(workload))
        df = pd.read_parquet(path)
        cols = feat_cols(df)
        cat_of = np.array([c.split("_")[0] for c in cols])
        cat_share = pd.Series(cat_of).value_counts(normalize=True).to_dict()

        for fold in range(1, N_FOLDS + 1):
            tr, ca, te, y_ca, y_te, cal, test = fold_data(df, cols, fold)
            seg_ca, seg_te = segment_starts(cal), segment_starts(test)
            faults = test["fault_id"].astype(str).to_numpy()

            # B. ablation. The three features that most often set the published maximum on
            # the cal split are the ones "drop_dominant" removes; the test split is not used.
            mu_last, sd_ewma = ewma_reference(tr)
            am_cal = np.nanargmax(np.abs(ca - mu_last) / sd_ewma, axis=1)
            dominant = list(np.argsort(-np.bincount(am_cal, minlength=len(cols)))[:3])
            for v in VARIANTS:
                s_ca = score_variant(v, tr, ca, seg_ca, drop=dominant)
                s_te = score_variant(v, tr, te, seg_te, drop=dominant)
                t = find_optimal_threshold(s_ca, y_ca)
                pred = (s_te >= t).astype(int)
                row = dict(workload=workload, fold=fold, variant=v,
                           f1=f1_score(y_te, pred, zero_division=0),
                           auc=roc_auc_score(y_te, s_te),
                           modal_tie=float(np.unique(s_te, return_counts=True)[1].max() / len(s_te)),
                           alert_rate=float(pred.mean()))
                abl.append(row)
                if v == "published":
                    ref = published_check[(published_check.workload == workload)
                                          & (published_check.fold == fold)]
                    assert len(ref) == 1, (workload, fold)
                    assert abs(ref.f1.iloc[0] - row["f1"]) < 1e-9, ("F1 drift", workload, fold)
                    assert abs(ref.auc_roc.iloc[0] - row["auc"]) < 1e-9, ("AUC drift", workload, fold)
                    # C. EWMA per fault, D. arg-max category on flagged fault windows
                    for fid, a in per_fault_auc(s_te, y_te, faults).items():
                        pf.append(dict(workload=workload, model="EWMA", fold=fold, fault_id=fid, auc=a))
                    z = np.abs(te - mu_last) / sd_ewma
                    top = cat_of[np.nanargmax(z, axis=1)]
                    for i in np.where((y_te == 1) & (pred == 1))[0]:
                        arg.append(dict(workload=workload, fold=fold, fault_id=faults[i],
                                        category=top[i], kind="flagged_fault"))
                    for i in np.where((y_te == 0) & (pred == 1))[0]:
                        arg.append(dict(workload=workload, fold=fold, fault_id="(normal)",
                                        category=top[i], kind="false_alarm"))

            # C. panel per fault
            for model in PANEL:
                s, off = load_aligned(workload, model, fold, y_te)
                align_report.append(dict(workload=workload, model=model, fold=fold,
                                         aligned=s is not None, offset=off))
                if s is None:
                    continue
                for fid, a in per_fault_auc(s, y_te[off:], faults[off:]).items():
                    pf.append(dict(workload=workload, model=model, fold=fold, fault_id=fid, auc=a))
            print(f"{workload} fold {fold} done", flush=True)

        CAT_SHARE[workload] = cat_share

    sat = pd.concat(sat, ignore_index=True)
    abl = pd.DataFrame(abl)
    pf = pd.DataFrame(pf)
    arg = pd.DataFrame(arg)
    align = pd.DataFrame(align_report)

    # ---- summaries
    sat_sum = sat.groupby(["workload", "model"]).agg(
        auc=("auc", "mean"), auc_best_case=("auc_best_case", "mean"),
        frac_at_max=("frac_at_max", "mean"),
        frac_normal_at_max=("frac_normal_at_max", "mean"), folds=("fold", "count")).reset_index()
    sat_sum.round(6).to_csv(OUT / "ewma_mechanism_saturation.csv", index=False)

    abl.to_csv(OUT / "ewma_mechanism_ablation_per_fold.csv", index=False)
    rows = []
    for (wl, v), g in abl.groupby(["workload", "variant"], sort=False):
        base = abl[(abl.workload == wl) & (abl.variant == "published")].set_index("fold")
        g = g.set_index("fold")
        rec = dict(workload=wl, variant=v, f1=g.f1.mean(), f1_se=g.f1.std(ddof=1) / np.sqrt(len(g)),
                   auc=g.auc.mean(), auc_se=g.auc.std(ddof=1) / np.sqrt(len(g)),
                   modal_tie=g.modal_tie.mean(), alert_rate=g.alert_rate.mean())
        if v != "published":
            d_auc = g.auc - base.auc
            d_f1 = g.f1 - base.f1
            rec.update(d_auc=d_auc.mean(), p_auc=wilcoxon(d_auc).pvalue if d_auc.abs().sum() else 1.0,
                       d_f1=d_f1.mean(), p_f1=wilcoxon(d_f1).pvalue if d_f1.abs().sum() else 1.0)
        rows.append(rec)
    abl_sum = pd.DataFrame(rows)
    abl_sum.round(6).to_csv(OUT / "ewma_mechanism_ablation_summary.csv", index=False)

    pf_sum = pf.groupby(["workload", "model", "fault_id"]).auc.agg(["mean", "count"]).reset_index()
    pf_sum.round(6).to_csv(OUT / "ewma_mechanism_per_fault_auc.csv", index=False)

    arg_sum = (arg.groupby(["workload", "kind", "fault_id", "category"]).size()
               .rename("n").reset_index())
    arg_sum["share"] = arg_sum.n / arg_sum.groupby(["workload", "kind", "fault_id"]).n.transform("sum")
    arg_sum.round(4).to_csv(OUT / "ewma_mechanism_argmax_category.csv", index=False)

    # E. deep models against the static max-z diagnostic
    panel = pd.read_csv(OUT / "rq1_panel_with_ewma.csv")
    con = []
    for wl in WORKLOADS:
        for met in ("auc_roc", "f1_score"):
            base = panel[(panel.workload == wl) & (panel.model == "RuleBased")].set_index("fold")[met]
            block = []
            for m in DEEP:
                x = panel[(panel.workload == wl) & (panel.model == m)].set_index("fold")[met]
                d = (x - base).dropna()
                block.append(dict(workload=wl, metric=met, model=m, model_mean=x.mean(),
                                  maxz_mean=base.mean(), delta=d.mean(),
                                  se=d.std(ddof=1) / np.sqrt(len(d)),
                                  p=wilcoxon(d).pvalue if d.abs().sum() else 1.0))
            ps = np.array([b["p"] for b in block])
            order, running = np.argsort(ps), 0.0
            for k, i in enumerate(order):
                running = max(running, min(1.0, (len(ps) - k) * ps[i]))
                block[i]["p_holm"] = running
            con += block
    con = pd.DataFrame(con)
    con.round(6).to_csv(OUT / "ewma_mechanism_maxz_contrasts.csv", index=False)

    # ---- report
    L = []
    p = L.append
    p("WHERE DOES THE EWMA BASELINE'S LEAD COME FROM?\n")
    p("Published EWMA reproduced exactly on all 20 workload-folds (asserted against "
      "classical_baselines_per_fold.csv).\n")
    p("Score alignment for per-fault AUC: "
      f"{int(align.aligned.sum())}/{len(align)} model-folds aligned by label match.")
    bad = align[~align.aligned]
    if len(bad):
        p("  not aligned (excluded from C): " + ", ".join(
            f"{r.workload}/{r.model}/f{r.fold}" for r in bad.itertuples()))
    p("\n=== A. Saturation bound (mean over folds) ===")
    p(sat_sum.round(3).to_string(index=False))
    ew = abl_sum[abl_sum.variant == "published"].set_index("workload").auc
    for wl in WORKLOADS:
        mx = sat_sum[(sat_sum.workload == wl) & sat_sum.model.isin(DEEP)].auc_best_case.max()
        p(f"  {wl}: best deep-model AUC with every saturated tie broken in its favour = "
          f"{mx:.3f}; published EWMA AUC = {ew[wl]:.3f}")
    p("\n=== B. EWMA ablation (mean over 10 folds; d = variant minus published, paired Wilcoxon) ===")
    p(abl_sum.round(4).to_string(index=False))
    p("\n=== C. Per-fault AUC-ROC (mean over folds) ===")
    wide = pf_sum.pivot_table(index=["workload", "fault_id"], columns="model", values="mean")
    p(wide.round(3).to_string())
    p("\n=== D. Category of EWMA's arg-max feature on flagged fault windows and false alarms ===")
    for wl in WORKLOADS:
        share = CAT_SHARE[wl]
        p(f"  {wl} feature-space share: " + ", ".join(f"{k} {v:.2f}" for k, v in sorted(share.items())))
    p(arg_sum.pivot_table(index=["workload", "kind", "fault_id"], columns="category",
                          values="share", fill_value=0).round(2).to_string())
    p("\n=== E. Deep models minus the static max-z diagnostic (paired over folds, Holm over six) ===")
    p(con.round(4).to_string(index=False))
    (OUT / "ewma_mechanism.txt").write_text("\n".join(L) + "\n")
    print("\n".join(L))


if __name__ == "__main__":
    main()
