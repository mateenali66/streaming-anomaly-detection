"""Cluster-aware statistics for the fold-structured cross-workload transfer experiment.

Consumes subexp2_cross_train_folds_raw.csv (one row per scaling x case x model x fold x
seed) and answers two questions:

  Scaler effect    How much of the apparent transfer degradation was the source-fit scaler rather than
                   model transfer? Fit AUC ~ scaling on the cross-workload cells only, with
                   fold as the clustering unit. The scaling coefficient is the artifact size.

  Transfer effect  Are cross-workload and within-workload performance different once folds
                   are treated as clusters rather than independent observations? Fit AUC ~
                   transfer under target_fit only, per direction, with random intercepts for
                   fold and model.

Folds are the clustering unit throughout: the 10 folds share repetitions by construction,
so per-fold rows are not independent. Seeds are nested inside folds and enter as replicate
observations, not as an extra clustering level.

DAGMM is reported but excluded from the models by default: it produces constant scores in
every cell (documented non-convergence on the 193-feature shared subset), so it contributes
no variance and would only dilute the estimates.

Output:
  subexp2_transfer_cells.csv        per-cell means with fold-clustered SE
  subexp2_transfer_stats.txt        mixed-model results, human readable
  subexp2_transfer_table.tex        LaTeX source of tab:cross-train
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf

OUT = Path(__file__).resolve().parent
RAW = OUT / "subexp2_cross_train_folds_raw.csv"

WITHIN = {"ecom": "ecom_to_ecom", "iot": "iot_to_iot"}
CROSS = {"ecom": "iot_to_ecom", "iot": "ecom_to_iot"}
EXCLUDE_MODELS = {"DAGMM"}

MODEL_LABEL = {
    "IF": "Isolation Forest", "OC_SVM": "OC-SVM", "LSTM_AE": "LSTM-AE",
    "Trans_AE": "Transformer-AE", "CNN_AE": "CNN-AE", "LSTM_VAE": "LSTM-VAE",
    "DeepSVDD": "Deep SVDD", "DAGMM": "DAGMM",
}


def cluster_se(df, value, cluster="fold"):
    """SE of the mean with folds as clusters: SD of fold means / sqrt(n_folds)."""
    fold_means = df.groupby(cluster)[value].mean()
    n = fold_means.notna().sum()
    if n < 2:
        return float("nan")
    return float(fold_means.std(ddof=1) / np.sqrt(n))


def per_cell(df):
    rows = []
    for (scaling, case, model), g in df.groupby(["scaling", "case", "model"]):
        for metric in ("auc", "f1"):
            rows.append(dict(
                scaling=scaling, case=case, model=model, metric=metric,
                mean=g[metric].mean(), sd=g[metric].std(ddof=1),
                cluster_se=cluster_se(g, metric),
                n_obs=int(g[metric].notna().sum()),
                n_folds=int(g["fold"].nunique()),
                degenerate_frac=float(g["constant_scores"].mean()),
                mean_fire_rate=float(g["fire_rate"].mean()),
            ))
    return pd.DataFrame(rows)


def fit_mixed(df, formula, groups, vc_formula, label, term, lines):
    try:
        md = smf.mixedlm(formula, data=df, groups=df[groups], vc_formula=vc_formula)
        res = md.fit(reml=True, method="lbfgs")
    except Exception as exc:  # noqa: BLE001
        lines.append(f"\n=== {label} ===\n  FAILED: {type(exc).__name__}: {exc}")
        return None
    if term not in res.params.index:
        lines.append(f"\n=== {label} ===\n  term {term!r} absent; "
                     f"available: {list(res.params.index)}")
        return None
    coef, se = res.params[term], res.bse[term]
    lo, hi = res.conf_int().loc[term].tolist()
    lines += [
        f"\n=== {label} ===",
        f"  N = {len(df)} obs, {df['fold'].nunique()} folds, {df['model'].nunique()} models",
        f"  effect ({term})  = {coef:+.4f}",
        f"  SE               = {se:.4f}",
        f"  95% CI           = [{lo:+.4f}, {hi:+.4f}]",
        f"  z                = {coef/se:+.3f}",
        f"  p                = {res.pvalues[term]:.4g}",
    ]
    return dict(label=label, term=term, coef=coef, se=se, ci_lo=lo, ci_hi=hi,
                pval=float(res.pvalues[term]), n_obs=len(df))


def main():
    if not RAW.exists():
        sys.exit(f"missing {RAW}; run subexp2_cross_train_folds.py first")
    raw = pd.read_csv(RAW)
    raw = raw[~raw["model"].isin(EXCLUDE_MODELS)].copy()

    cells = per_cell(raw)
    cells.to_csv(OUT / "subexp2_transfer_cells.csv", index=False)

    lines = ["CROSS-WORKLOAD TRANSFER: CLUSTER-AWARE ANALYSIS", "",
             "Clustering unit: fold (folds share repetitions by construction).",
             "Seeds are replicates nested within folds.",
             f"DAGMM excluded from models: {sorted(EXCLUDE_MODELS)}", ""]

    # ---- scaler effect: how much of the degradation was the scaler? -------
    cross_cells = raw[raw["case"].isin(CROSS.values())].copy()
    cross_cells["scaling"] = pd.Categorical(
        cross_cells["scaling"], categories=["source_fit", "target_fit"])
    q6 = []
    for direction in sorted(cross_cells["case"].unique()):
        sub = cross_cells[cross_cells["case"] == direction]
        r = fit_mixed(sub, "auc ~ scaling", "fold",
                      {"model": "0 + C(model)"},
                      f"Scaler effect, {direction} (AUC)",
                      "scaling[T.target_fit]", lines)
        if r:
            r["case"] = direction
            q6.append(r)

    # ---- transfer effect: cross vs within, target-fit scaling only ----------
    tf = raw[raw["scaling"] == "target_fit"].copy()
    q1 = []
    for tgt, within_case in WITHIN.items():
        cross_case = CROSS[tgt]
        sub = tf[tf["case"].isin([within_case, cross_case])].copy()
        sub["transfer"] = np.where(sub["case"] == cross_case, "cross", "within")
        sub["transfer"] = pd.Categorical(sub["transfer"], categories=["within", "cross"])
        r = fit_mixed(sub, "auc ~ transfer", "fold",
                      {"model": "0 + C(model)"},
                      f"Transfer effect on target={tgt} (AUC, target_fit)",
                      "transfer[T.cross]", lines)
        if r:
            r["target"] = tgt
            q1.append(r)

    # ---- per-model paired deltas, fold-clustered ---------------------------
    lines += ["", "=== Per-model cross-minus-within AUC (target_fit, paired by fold) ==="]
    deltas = []
    for tgt, within_case in WITHIN.items():
        cross_case = CROSS[tgt]
        for model in sorted(tf["model"].unique()):
            w = tf[(tf["case"] == within_case) & (tf["model"] == model)]
            c = tf[(tf["case"] == cross_case) & (tf["model"] == model)]
            wf = w.groupby("fold")["auc"].mean()
            cf = c.groupby("fold")["auc"].mean()
            common = wf.index.intersection(cf.index)
            if len(common) < 2:
                continue
            d = (cf.loc[common] - wf.loc[common])
            se = d.std(ddof=1) / np.sqrt(len(d))
            deltas.append(dict(target=tgt, model=model, within=wf.loc[common].mean(),
                               cross=cf.loc[common].mean(), delta=d.mean(),
                               se=se, n_folds=len(d)))
    dd = pd.DataFrame(deltas)
    if not dd.empty:
        lines.append(dd.round(4).to_string(index=False))
        dd.to_csv(OUT / "subexp2_transfer_deltas.csv", index=False)

    # ---- degeneracy tally --------------------------------------------------
    lines += ["", "=== Degenerate (constant-score) run fraction by scaling and case ==="]
    deg = raw.pivot_table(index="case", columns="scaling", values="constant_scores",
                          aggfunc="mean")
    lines.append(deg.round(3).to_string())

    text = "\n".join(lines)
    (OUT / "subexp2_transfer_stats.txt").write_text(text)
    print(text)

    # ---- LaTeX replacement table ------------------------------------------
    auc = cells[(cells.metric == "auc") & (cells.scaling == "target_fit")]
    piv_m = auc.pivot_table(index="model", columns="case", values="mean")
    piv_s = auc.pivot_table(index="model", columns="case", values="cluster_se")
    order = [c for c in ["ecom_to_ecom", "ecom_to_iot", "iot_to_iot", "iot_to_ecom"]
             if c in piv_m.columns]
    tex = [r"\begin{tabular}{lcccc}", r"\toprule",
           r"Model & \multicolumn{2}{c}{Target: e-commerce} & "
           r"\multicolumn{2}{c}{Target: IoT} \\",
           r"\cmidrule(lr){2-3}\cmidrule(lr){4-5}",
           r" & within & from IoT & within & from e-com \\", r"\midrule"]
    col_map = [("ecom_to_ecom",), ("iot_to_ecom",), ("iot_to_iot",), ("ecom_to_iot",)]
    for model in piv_m.index:
        cellstr = []
        for (c,) in col_map:
            if c in order:
                cellstr.append(f"{piv_m.loc[model, c]:.3f} ({piv_s.loc[model, c]:.3f})")
            else:
                cellstr.append("--")
        tex.append(f"{MODEL_LABEL.get(model, model)} & " + " & ".join(cellstr) + r" \\")
    tex += [r"\bottomrule", r"\end{tabular}"]
    (OUT / "subexp2_transfer_table.tex").write_text("\n".join(tex))

    print(f"\nSaved: {OUT}/subexp2_transfer_cells.csv")
    print(f"Saved: {OUT}/subexp2_transfer_stats.txt")
    print(f"Saved: {OUT}/subexp2_transfer_table.tex")


if __name__ == "__main__":
    main()
