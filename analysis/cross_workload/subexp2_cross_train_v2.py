"""Sub-experiment 2 (v2): Cross-workload training transfer, normalization-corrected.

Separates genuine cross-workload transfer degradation from the
effect of standardising the target workload with source-workload statistics.

The v1 experiment (subexp2_cross_train.py) standardized the TARGET workload using the
SOURCE workload's NORMAL mean/sigma. Because the two testbeds run at different absolute
scales, that produces |z| up to 2.1e6 on the target, concentrated in 8 of 193 shared
features (99.996% of total squared z). Autoencoder reconstruction error then sits so far
above train_mse_mean that sigmoid((mse - mu)/sigma) saturates to 1.0 on every window.
The result is a constant score vector: AUC exactly 0.500 and F1 exactly 2p/(1+p), the
always-fire baseline. v1's transfer numbers measure unit mismatch, not model transfer.

v2 runs both scaling regimes so the two effects can be separated:

  source_fit  - v1 behaviour (scaler fit on source NORMAL, applied to target).
  target_fit  - scaler fit on TARGET NORMAL, applied to target; source-trained model
                weights unchanged. This is what a practitioner does when deploying an
                existing detector to a new cluster: recalibrate on the new cluster's
                normal traffic. It isolates whether the learned normality manifold
                transfers from whether the two workloads share units.

Degenerate scores are detected and reported rather than silently producing a number:
n_unique scores, constant-score flag, and the always-fire F1 reference for the target
prevalence are emitted per cell.

Output:
  subexp2_cross_train_v2_results.csv
  subexp2_cross_train_v2_summary.txt
"""
import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import f1_score, roc_auc_score

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "sagemaker"))

from training import (  # noqa: E402
    LSTMAutoencoder, TransformerAutoencoder, CNN1DAutoencoder,
    LSTMVariationalAutoencoder, DeepSVDD, DAGMM,
    train_autoencoder, score_autoencoder, get_training_mse_stats,
    train_vae, score_vae, get_training_mse_stats_vae,
    train_deep_svdd, score_deep_svdd, get_training_dist_stats_svdd,
    train_dagmm, score_dagmm, get_training_energy_stats_dagmm,
    train_isolation_forest, score_isolation_forest,
    train_ocsvm, score_ocsvm,
    create_sequences, find_optimal_threshold,
    set_seeds,
)

OUT = ROOT / "analysis" / "cross_workload"
SEED = 42

ECOM_PARQUET = ROOT / "data" / "features_v3" / "features_30s.parquet"
IOT_PARQUET = ROOT / "data" / "v3" / "iot" / "features" / "features_30s.parquet"

META = {"window_start", "window_end", "window_mid", "n_samples", "label",
        "fault_id", "fault_category", "run_number", "repetition", "run_source"}
META |= {f"fold_{i}_split" for i in range(1, 11)}

DEEP_DEFAULTS = dict(hidden_dim=64, n_layers=2, lr=1e-3, dropout=0.1,
                     epochs=30, seq_length=8)

CLASSICAL = ["IF", "OC_SVM"]
DEEP = ["LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE", "DeepSVDD", "DAGMM"]


def fit_scaler(X_normal):
    mu = X_normal.mean(axis=0)
    sigma = X_normal.std(axis=0)
    sigma = np.where(sigma < 1e-8, 1.0, sigma)
    return mu, sigma


def apply_scaler(X, mu, sigma):
    return ((X - mu) / sigma).astype(np.float32)


def evaluate(scores, y_true):
    """F1 at the score-optimal threshold, AUC, plus degeneracy diagnostics."""
    scores = np.asarray(scores, dtype=np.float64)
    n_unique = int(np.unique(np.round(scores, 12)).size)
    constant = n_unique <= 1
    thr = find_optimal_threshold(scores, y_true)
    y_pred = (scores >= thr).astype(int)
    f1v = float(f1_score(y_true, y_pred, zero_division=0))
    try:
        auc = float(roc_auc_score(y_true, scores))
    except Exception:
        auc = float("nan")
    p = float(np.mean(y_true))
    return dict(f1=f1v, auc=auc, threshold=float(thr),
                n_unique_scores=n_unique, constant_scores=constant,
                fire_rate=float(y_pred.mean()),
                target_prevalence=p, always_fire_f1=2 * p / (1 + p))


def run_classical(model_name, X_train_normal, X_eval, y_eval):
    set_seeds(SEED)
    if model_name == "IF":
        model = train_isolation_forest(X_train_normal, n_estimators=200,
                                       contamination=0.05)
        scores = score_isolation_forest(model, X_eval)
    elif model_name == "OC_SVM":
        model = train_ocsvm(X_train_normal, nu=0.05, kernel="rbf", gamma="scale")
        scores = score_ocsvm(model, X_eval)
    else:
        raise ValueError(model_name)
    return evaluate(scores, y_eval)


def run_deep(model_name, X_train_normal, X_eval, y_eval, n_features):
    set_seeds(SEED)
    seq_len = DEEP_DEFAULTS["seq_length"]
    X_train_seq = create_sequences(X_train_normal, seq_len)
    X_eval_seq = create_sequences(X_eval, seq_len)
    if X_train_seq.shape[0] < 32 or X_eval_seq.shape[0] < 32:
        return None
    y_eval_seq = y_eval[seq_len - 1:]

    hd = DEEP_DEFAULTS["hidden_dim"]
    nl = DEEP_DEFAULTS["n_layers"]
    dr = DEEP_DEFAULTS["dropout"]
    lr = DEEP_DEFAULTS["lr"]
    ep = DEEP_DEFAULTS["epochs"]

    if model_name in ("LSTM_AE", "Trans_AE", "CNN_AE"):
        if model_name == "LSTM_AE":
            m = LSTMAutoencoder(n_features, hidden_dim=hd, n_layers=nl, dropout=dr)
        elif model_name == "Trans_AE":
            m = TransformerAutoencoder(n_features, d_model=hd, nhead=4,
                                       n_layers=nl, dropout=dr, seq_len=seq_len)
        else:
            m = CNN1DAutoencoder(n_features, filters=hd, kernel_size=5,
                                 n_layers=nl, dropout=dr)
        model, _ = train_autoencoder(m, X_train_seq, lr=lr, epochs=ep)
        mu, sigma = get_training_mse_stats(model, X_train_seq)
        scores = score_autoencoder(model, X_eval_seq, mu, sigma)
    elif model_name == "LSTM_VAE":
        m = LSTMVariationalAutoencoder(n_features, hidden_dim=hd, n_layers=nl,
                                       latent_dim=32, dropout=dr)
        model, _ = train_vae(m, X_train_seq, lr=lr, epochs=ep)
        mu, sigma = get_training_mse_stats_vae(model, X_train_seq)
        scores = score_vae(model, X_eval_seq, mu, sigma)
    elif model_name == "DeepSVDD":
        m = DeepSVDD(n_features, hidden_dim=hd, n_layers=nl, latent_dim=32, dropout=dr)
        model, _ = train_deep_svdd(m, X_train_seq, lr=lr,
                                   pretrain_epochs=15, finetune_epochs=15)
        mu, sigma = get_training_dist_stats_svdd(model, X_train_seq)
        scores = score_deep_svdd(model, X_eval_seq, mu, sigma)
    elif model_name == "DAGMM":
        m = DAGMM(n_features, hidden_dim=hd, n_layers=nl, latent_dim=8, n_gmm=4,
                  dropout=dr)
        model, _ = train_dagmm(m, X_train_seq, lr=lr, epochs=ep)
        mu, sigma = get_training_energy_stats_dagmm(model, X_train_seq)
        scores = score_dagmm(model, X_eval_seq, mu, sigma)
    else:
        raise ValueError(model_name)
    return evaluate(scores, y_eval_seq)


def load_shared():
    ecom = pd.read_parquet(ECOM_PARQUET)
    iot = pd.read_parquet(IOT_PARQUET)
    ecom_feat = [c for c in ecom.columns if c not in META]
    iot_feat = [c for c in iot.columns if c not in META]
    feat_cols = sorted(set(ecom_feat) & set(iot_feat))

    def arrays(df):
        X = df[feat_cols].fillna(0.0).to_numpy(dtype=np.float32)
        anom = (df["label"] == "ANOMALOUS").to_numpy()
        norm = (df["label"] == "NORMAL").to_numpy()
        keep = (df["label"] != "EXCLUDED").to_numpy()
        return X, anom, norm, keep

    return feat_cols, arrays(ecom), arrays(iot)


def scaler_diagnostics(X_src_normal, X_tgt, label):
    """Quantify how badly a source-fit scaler distorts the target workload."""
    mu, sigma = fit_scaler(X_src_normal)
    z = np.abs(apply_scaler(X_tgt, mu, sigma).astype(np.float64))
    per_feat = z.mean(axis=0)
    sq = z ** 2
    top8 = np.argsort(-per_feat)[:8]
    return dict(
        case=label,
        mean_abs_z=float(z.mean()),
        median_abs_z=float(np.median(z)),
        p99_abs_z=float(np.percentile(z, 99)),
        max_abs_z=float(z.max()),
        frac_abs_z_gt_10=float((z > 10).mean()),
        n_features_mean_z_gt_10=int((per_feat > 10).sum()),
        top8_share_of_squared_z=float(sq[:, top8].sum() / sq.sum()),
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--models", nargs="*", default=CLASSICAL + DEEP)
    ap.add_argument("--scaling", nargs="*", default=["source_fit", "target_fit"])
    args = ap.parse_args()

    set_seeds(SEED)
    feat_cols, (Xe, e_anom, e_norm, e_keep), (Xi, i_anom, i_norm, i_keep) = load_shared()
    n_features = len(feat_cols)
    print(f"Shared features: {n_features}")

    # Scaler distortion diagnostics: evidence for the confound
    diag = [
        scaler_diagnostics(Xe[e_norm], Xe[e_keep], "ecom_to_ecom"),
        scaler_diagnostics(Xe[e_norm], Xi[i_keep], "ecom_to_iot"),
        scaler_diagnostics(Xi[i_norm], Xi[i_keep], "iot_to_iot"),
        scaler_diagnostics(Xi[i_norm], Xe[e_keep], "iot_to_ecom"),
    ]
    diag_df = pd.DataFrame(diag)
    diag_df.to_csv(OUT / "subexp2_scaler_distortion.csv", index=False)
    print("\n=== Source-fit scaler distortion on the target workload ===")
    print(diag_df.round(4).to_string(index=False))

    # (case, source normal, target X, target y, target normal)
    cases = [
        ("ecom_to_iot", Xe[e_norm], Xi[i_keep], i_anom[i_keep], Xi[i_norm]),
        ("iot_to_ecom", Xi[i_norm], Xe[e_keep], e_anom[e_keep], Xe[e_norm]),
        ("ecom_to_ecom", Xe[e_norm], Xe[e_keep], e_anom[e_keep], Xe[e_norm]),
        ("iot_to_iot", Xi[i_norm], Xi[i_keep], i_anom[i_keep], Xi[i_norm]),
    ]

    rows = []
    for scaling in args.scaling:
        for case_name, X_src_normal, X_tgt, y_tgt, X_tgt_normal in cases:
            # Model always trains on the SOURCE workload, standardized against itself.
            mu_s, sig_s = fit_scaler(X_src_normal)
            X_train = apply_scaler(X_src_normal, mu_s, sig_s)

            # Only the target-side scaler changes between regimes.
            if scaling == "source_fit":
                mu_t, sig_t = mu_s, sig_s
            elif scaling == "target_fit":
                mu_t, sig_t = fit_scaler(X_tgt_normal)
            else:
                raise ValueError(scaling)
            X_eval = apply_scaler(X_tgt, mu_t, sig_t)

            print(f"\n=== [{scaling}] {case_name} === train {X_train.shape} "
                  f"eval {X_eval.shape} (prevalence {y_tgt.mean():.3f})")

            for model_name in args.models:
                print(f"  {model_name}...", end=" ", flush=True)
                try:
                    if model_name in CLASSICAL:
                        res = run_classical(model_name, X_train, X_eval, y_tgt)
                    else:
                        res = run_deep(model_name, X_train, X_eval, y_tgt, n_features)
                    if res is None:
                        print("SKIPPED (too few sequences)")
                        continue
                    err = ""
                except Exception as exc:  # noqa: BLE001
                    print(f"FAILED: {type(exc).__name__}: {exc}")
                    res = dict(f1=np.nan, auc=np.nan, threshold=np.nan,
                               n_unique_scores=0, constant_scores=True,
                               fire_rate=np.nan, target_prevalence=float(y_tgt.mean()),
                               always_fire_f1=np.nan)
                    err = f"{type(exc).__name__}: {exc}"
                else:
                    flag = " [DEGENERATE: constant scores]" if res["constant_scores"] else ""
                    print(f"F1={res['f1']:.3f} AUC={res['auc']:.3f} "
                          f"uniq={res['n_unique_scores']}{flag}")
                rows.append(dict(scaling=scaling, case=case_name,
                                 model=model_name, error=err, **res))

    df = pd.DataFrame(rows)
    df.to_csv(OUT / "subexp2_cross_train_v2_results.csv", index=False)

    lines = ["CROSS-WORKLOAD TRANSFER v2 (193-feature shared subset, default hyperparams)",
             "",
             "Model weights always trained on the SOURCE workload. Only the target-side",
             "scaler differs: source_fit reuses the source NORMAL mean/sigma (v1 behaviour),",
             "target_fit refits on the target NORMAL windows (deployment-realistic).",
             ""]
    for scaling in df["scaling"].unique():
        sub = df[df["scaling"] == scaling]
        for metric in ("auc", "f1"):
            piv = sub.pivot_table(index="model", columns="case", values=metric,
                                  aggfunc="first")
            order = [c for c in ["ecom_to_ecom", "ecom_to_iot", "iot_to_iot",
                                 "iot_to_ecom"] if c in piv.columns]
            piv = piv[order]
            lines += [f"=== [{scaling}] {metric.upper()} ===",
                      piv.round(3).to_string(), ""]
        n_degen = int(sub["constant_scores"].sum())
        lines += [f"[{scaling}] degenerate (constant-score) cells: {n_degen}/{len(sub)}", ""]

    ref = df.groupby("case")["always_fire_f1"].first().round(3)
    lines += ["Always-fire F1 reference at target prevalence (2p/(1+p)):",
              ref.to_string(), ""]

    text = "\n".join(lines)
    (OUT / "subexp2_cross_train_v2_summary.txt").write_text(text)
    print("\n" + text)
    print(f"Saved: {OUT}/subexp2_cross_train_v2_results.csv")
    print(f"Saved: {OUT}/subexp2_scaler_distortion.csv")


if __name__ == "__main__":
    main()
