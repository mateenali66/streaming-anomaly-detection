"""
RQ2 mixed model: point vs sequence model families.

A linear mixed model avoids treating the models within a family as independent replicates.
Fixed effect: family (point vs sequence). Random intercept: model identity, which absorbs
differences such as IF against OC-SVM or LSTM-AE against Trans-AE. There is no fold term,
because with ten exchangeable folds a fold variance component estimates to near zero.

Output: coefficient with SE, 95% CI, p-value for "sequence - point" effect on
F1 and AUC-ROC; also reported pooled across workloads.
"""
import pandas as pd
import statsmodels.formula.api as smf

POINT_MODELS = {"IF", "OC_SVM"}
SEQ_MODELS = {"LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE"}


def fit_one(df, outcome, label):
    df = df.copy()
    df["family"] = df["model"].map(
        lambda m: "point" if m in POINT_MODELS else ("sequence" if m in SEQ_MODELS else None)
    )
    df = df[df["family"].notna()].reset_index(drop=True)
    df["family"] = pd.Categorical(df["family"], categories=["point", "sequence"], ordered=False)

    md = smf.mixedlm(f"{outcome} ~ family", data=df, groups=df["model"])
    res = md.fit(reml=True, method="lbfgs")

    name = "family[T.sequence]"
    coef = res.params[name]
    se = res.bse[name]
    z = coef / se
    pval = res.pvalues[name]
    ci_lo, ci_hi = res.conf_int().loc[name].tolist()

    n_obs = len(df)
    n_models = df["model"].nunique()
    n_folds = df["fold"].nunique()
    sigma_model = float(res.cov_re.iloc[0, 0]) ** 0.5
    sigma_resid = float(res.scale) ** 0.5
    icc = res.cov_re.iloc[0, 0] / (res.cov_re.iloc[0, 0] + res.scale)

    print(f"\n=== {label}: {outcome} ===")
    print(f"  N = {n_obs} obs ({n_models} models x {n_folds} folds)")
    print(f"  fixed effect (sequence - point) = {coef:+.4f}")
    print(f"  SE                              = {se:.4f}")
    print(f"  95% CI                          = [{ci_lo:+.4f}, {ci_hi:+.4f}]")
    print(f"  z                               = {z:+.3f}")
    print(f"  p-value                         = {pval:.4g}")
    print(f"  sigma_model (between-model)     = {sigma_model:.4f}")
    print(f"  sigma_resid (within-model)      = {sigma_resid:.4f}")
    print(f"  ICC (between-model var fraction) = {icc:.3f}")
    return dict(
        scope=label, outcome=outcome,
        n_obs=n_obs, n_models=n_models, n_folds=n_folds,
        coef=round(coef, 4), se=round(se, 4),
        ci_lo=round(ci_lo, 4), ci_hi=round(ci_hi, 4),
        z=round(z, 3), pval=pval,
        sigma_model=round(sigma_model, 4),
        sigma_resid=round(sigma_resid, 4),
        icc=round(icc, 3),
    )


def main():
    panel = pd.read_csv("panel_overall.csv")

    rows = []
    ec = panel[(panel["workload"] == "ecommerce") & (panel["window_s"] == 30)]
    iot = panel[(panel["workload"] == "iot") & (panel["window_s"] == 30)]
    pool = panel[panel["window_s"] == 30]

    for label, sub in [
        ("e-commerce 30s", ec),
        ("IoT 30s", iot),
        ("pooled (both workloads, 30s)", pool),
    ]:
        for outcome in ["f1_score", "auc_roc"]:
            rows.append(fit_one(sub, outcome, label))

    out = pd.DataFrame(rows)
    out.to_csv("rq2_mixedeffects.csv", index=False)
    print("\nWritten rq2_mixedeffects.csv")


if __name__ == "__main__":
    main()
