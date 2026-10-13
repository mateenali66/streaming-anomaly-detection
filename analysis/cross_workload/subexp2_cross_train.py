"""Sub-experiment 2: Cross-workload training transfer.

Train each model on workload A's shared-feature subset, evaluate on workload B's
shared-feature subset. Compares F1 against within-workload baseline (also on
shared features) to isolate the transfer effect.

It uses default hyperparameters, not the full HPO, and measures how much detection
performance changes when a model trained on e-commerce telemetry scores IoT telemetry,
and the reverse. subexp2_cross_train_folds.py is the fold-structured version the paper
reports.

Output:
  analysis/cross_workload/subexp2_cross_train_results.csv
  analysis/cross_workload/subexp2_cross_train_summary.txt
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import torch
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


def load_workload(parquet_path, shared_cols):
    df = pd.read_parquet(parquet_path)
    feat_cols = [c for c in df.columns if c not in META and c in shared_cols]
    feat_cols = sorted(feat_cols)
    return df, feat_cols


def prepare_arrays(df, feat_cols):
    X = df[feat_cols].fillna(0.0).to_numpy(dtype=np.float32)
    is_anomaly = (df["label"] == "ANOMALOUS").to_numpy()
    is_normal = (df["label"] == "NORMAL").to_numpy()
    is_excluded = (df["label"] == "EXCLUDED").to_numpy()
    keep = ~is_excluded
    return X, is_anomaly, is_normal, keep


def standardize_against_normal(X, X_normal):
    mu = X_normal.mean(axis=0)
    sigma = X_normal.std(axis=0)
    sigma = np.where(sigma < 1e-8, 1.0, sigma)
    return (X - mu) / sigma, mu, sigma


def evaluate(scores, y_true):
    """Compute F1 at threshold optimal on these scores + AUC."""
    thr = find_optimal_threshold(scores, y_true)
    y_pred = (scores >= thr).astype(int)
    f1v = f1_score(y_true, y_pred)
    try:
        auc = roc_auc_score(y_true, scores)
    except Exception:
        auc = float("nan")
    return float(f1v), float(auc), float(thr)


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
        return float("nan"), float("nan"), float("nan")

    # Sequence labels: anomaly if any timestep in window is anomalous.
    # Conventional: label sequence by last-timestep label.
    y_eval_seq = y_eval[seq_len - 1:]

    if model_name == "LSTM_AE":
        m = LSTMAutoencoder(n_features, hidden_dim=DEEP_DEFAULTS["hidden_dim"],
                             n_layers=DEEP_DEFAULTS["n_layers"],
                             dropout=DEEP_DEFAULTS["dropout"])
        model, _ = train_autoencoder(m, X_train_seq, lr=DEEP_DEFAULTS["lr"],
                                       epochs=DEEP_DEFAULTS["epochs"])
        mu, sigma = get_training_mse_stats(model, X_train_seq)
        scores = score_autoencoder(model, X_eval_seq, mu, sigma)
    elif model_name == "Trans_AE":
        m = TransformerAutoencoder(n_features, d_model=DEEP_DEFAULTS["hidden_dim"],
                                    nhead=4, n_layers=DEEP_DEFAULTS["n_layers"],
                                    dropout=DEEP_DEFAULTS["dropout"],
                                    seq_len=DEEP_DEFAULTS["seq_length"])
        model, _ = train_autoencoder(m, X_train_seq, lr=DEEP_DEFAULTS["lr"],
                                       epochs=DEEP_DEFAULTS["epochs"])
        mu, sigma = get_training_mse_stats(model, X_train_seq)
        scores = score_autoencoder(model, X_eval_seq, mu, sigma)
    elif model_name == "CNN_AE":
        m = CNN1DAutoencoder(n_features, filters=DEEP_DEFAULTS["hidden_dim"],
                              kernel_size=5, n_layers=DEEP_DEFAULTS["n_layers"],
                              dropout=DEEP_DEFAULTS["dropout"])
        model, _ = train_autoencoder(m, X_train_seq, lr=DEEP_DEFAULTS["lr"],
                                       epochs=DEEP_DEFAULTS["epochs"])
        mu, sigma = get_training_mse_stats(model, X_train_seq)
        scores = score_autoencoder(model, X_eval_seq, mu, sigma)
    elif model_name == "LSTM_VAE":
        m = LSTMVariationalAutoencoder(n_features, hidden_dim=DEEP_DEFAULTS["hidden_dim"],
                                        n_layers=DEEP_DEFAULTS["n_layers"],
                                        latent_dim=32,
                                        dropout=DEEP_DEFAULTS["dropout"])
        model, _ = train_vae(m, X_train_seq, lr=DEEP_DEFAULTS["lr"],
                              epochs=DEEP_DEFAULTS["epochs"])
        mu, sigma = get_training_mse_stats_vae(model, X_train_seq)
        scores = score_vae(model, X_eval_seq, mu, sigma)
    elif model_name == "DeepSVDD":
        m = DeepSVDD(n_features, hidden_dim=DEEP_DEFAULTS["hidden_dim"],
                      n_layers=DEEP_DEFAULTS["n_layers"], latent_dim=32,
                      dropout=DEEP_DEFAULTS["dropout"])
        model, _ = train_deep_svdd(m, X_train_seq, lr=DEEP_DEFAULTS["lr"],
                                     pretrain_epochs=15, finetune_epochs=15)
        mu, sigma = get_training_dist_stats_svdd(model, X_train_seq)
        scores = score_deep_svdd(model, X_eval_seq, mu, sigma)
    elif model_name == "DAGMM":
        m = DAGMM(n_features, hidden_dim=DEEP_DEFAULTS["hidden_dim"],
                   n_layers=DEEP_DEFAULTS["n_layers"], latent_dim=8, n_gmm=4,
                   dropout=DEEP_DEFAULTS["dropout"])
        model, _ = train_dagmm(m, X_train_seq, lr=DEEP_DEFAULTS["lr"],
                                epochs=DEEP_DEFAULTS["epochs"])
        mu, sigma = get_training_energy_stats_dagmm(model, X_train_seq)
        scores = score_dagmm(model, X_eval_seq, mu, sigma)
    else:
        raise ValueError(model_name)
    return evaluate(scores, y_eval_seq)


def main():
    set_seeds(SEED)

    print("Loading shared feature schema...")
    ecom_all = pd.read_parquet(ECOM_PARQUET, columns=None)
    iot_all = pd.read_parquet(IOT_PARQUET, columns=None)
    ecom_feat = [c for c in ecom_all.columns if c not in META]
    iot_feat = [c for c in iot_all.columns if c not in META]
    shared = sorted(set(ecom_feat) & set(iot_feat))
    print(f"Shared features: {len(shared)} (ecom {len(ecom_feat)}, iot {len(iot_feat)})")

    # Reload with only needed columns
    ecom_df, ecom_cols = load_workload(ECOM_PARQUET, set(shared))
    iot_df, iot_cols = load_workload(IOT_PARQUET, set(shared))
    feat_cols = sorted(set(ecom_cols) & set(iot_cols))
    n_features = len(feat_cols)
    print(f"Aligned to {n_features} shared columns")

    Xe, ye_anom, ye_norm, ke = prepare_arrays(ecom_df, feat_cols)
    Xi, yi_anom, yi_norm, ki = prepare_arrays(iot_df, feat_cols)

    # Train standardization is fitted on the SOURCE workload's NORMAL data
    Xe_norm_only = Xe[ye_norm]
    Xi_norm_only = Xi[yi_norm]

    # Two-direction transfer: each direction has its own standardization
    cases = [
        ("ecom_to_iot", Xe_norm_only, Xe[ke], ye_anom[ke], Xi[ki], yi_anom[ki]),
        ("iot_to_ecom", Xi_norm_only, Xi[ki], yi_anom[ki], Xe[ke], ye_anom[ke]),
        # Within-workload baselines on shared features only
        ("ecom_to_ecom", Xe_norm_only, Xe[ke], ye_anom[ke], Xe[ke], ye_anom[ke]),
        ("iot_to_iot", Xi_norm_only, Xi[ki], yi_anom[ki], Xi[ki], yi_anom[ki]),
    ]

    rows = []
    classical = ["IF", "OC_SVM"]
    deep = ["LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE", "DeepSVDD", "DAGMM"]
    all_models = classical + deep

    for case_name, X_normal_src, X_src, y_src, X_tgt, y_tgt in cases:
        # Standardize using source-NORMAL stats (transfer is meaningful only with same scaling)
        X_normal_std, mu, sigma = standardize_against_normal(X_normal_src, X_normal_src)
        X_tgt_std = (X_tgt - mu) / np.where(sigma < 1e-8, 1.0, sigma)

        print(f"\n=== {case_name} === source normal: {X_normal_std.shape}, target: {X_tgt_std.shape} (anom rate: {y_tgt.mean():.3f})")

        for model_name in all_models:
            print(f"  {model_name}...", end=" ", flush=True)
            try:
                if model_name in classical:
                    f1v, auc, thr = run_classical(model_name, X_normal_std, X_tgt_std, y_tgt)
                else:
                    f1v, auc, thr = run_deep(model_name, X_normal_std, X_tgt_std, y_tgt, n_features)
            except Exception as e:
                print(f"FAILED: {type(e).__name__}: {e}")
                f1v, auc, thr = float("nan"), float("nan"), float("nan")
                rows.append({"case": case_name, "model": model_name, "f1": f1v,
                             "auc": auc, "threshold": thr, "error": str(e)})
                continue
            print(f"F1={f1v:.3f} AUC={auc:.3f}")
            rows.append({"case": case_name, "model": model_name, "f1": f1v,
                         "auc": auc, "threshold": thr, "error": ""})

    df = pd.DataFrame(rows)
    df.to_csv(OUT / "subexp2_cross_train_results.csv", index=False)

    print("\n" + "=" * 80)
    print("CROSS-WORKLOAD TRANSFER F1 (shared-feature subset, train→eval)")
    print("=" * 80)
    pivot = df.pivot_table(index="model", columns="case", values="f1", aggfunc="first")
    col_order = ["ecom_to_ecom", "ecom_to_iot", "iot_to_iot", "iot_to_ecom"]
    pivot = pivot[[c for c in col_order if c in pivot.columns]]
    pivot["transfer_drop_e2i"] = pivot["ecom_to_ecom"] - pivot["ecom_to_iot"]
    pivot["transfer_drop_i2e"] = pivot["iot_to_iot"] - pivot["iot_to_ecom"]
    print(pivot.round(3).to_string())

    with open(OUT / "subexp2_cross_train_summary.txt", "w") as f:
        f.write("CROSS-WORKLOAD TRANSFER (shared-feature subset, F1)\n\n")
        f.write(pivot.round(3).to_string() + "\n")

    print(f"\nSaved: {OUT}/subexp2_cross_train_results.csv")
    print(f"Saved: {OUT}/subexp2_cross_train_summary.txt")


if __name__ == "__main__":
    main()
