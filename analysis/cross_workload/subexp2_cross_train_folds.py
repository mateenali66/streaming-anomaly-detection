"""Sub-experiment 2 (fold-structured): cross-workload transfer with cluster-aware design.

Covers both the clustering of folds and the transfer scaling.

subexp2_cross_train_v2.py established that the v1 transfer numbers were an artifact of
applying the source workload's scaler to the target: scores saturated and the reported cells
were exactly the always-fire baseline. Overlapping folds also must not be treated as
independent observations.

This script re-runs the transfer experiment with the SAME fold structure as the paper's
main tables, so the transfer result gets the same statistical treatment as everything else
and the cross-vs-within delta is computed on identical target test sets.

Design, for source workload S, target workload T, fold i in 1..10, seed s:

  train   = S rows with fold_i_split == "train"  and label == NORMAL
  scaler  = source_fit: fit on the train rows above (v1 behaviour)
            target_fit: fit on T rows with fold_i_split == "train" and label == NORMAL
  eval    = T rows with fold_i_split == "test", label != EXCLUDED
  y       = (label == "ANOMALOUS"); RECOVERY counts as negative, matching the paper's
            binary mapping in the main tables.

When S == T this reduces exactly to the paper's own protocol, so the within-workload
columns are directly comparable to the main results. When S != T, source fold i's training
subset is paired with target fold i's test subset; the two come from different workloads so
the pairing carries no leakage, it just gives 10 matched observations per cell.

The clustering unit for downstream statistics is the fold. Per-fold, per-seed rows are
written out; aggregation and the mixed model live in subexp2_transfer_stats.py.

Sequence construction matches training.py, which does not reset at run boundaries. Kept
deliberately consistent with the main pipeline rather than silently improved, so these
numbers stay comparable to the published tables.

Output:
  subexp2_cross_train_folds_raw.csv        one row per (scaling, case, model, fold, seed)
"""
import argparse
import sys
import time
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
RAW_CSV = OUT / "subexp2_cross_train_folds_raw.csv"

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
    return mu, np.where(sigma < 1e-8, 1.0, sigma)


def apply_scaler(X, mu, sigma):
    return ((X - mu) / sigma).astype(np.float32)


def evaluate(scores, y_true):
    scores = np.asarray(scores, dtype=np.float64)
    n_unique = int(np.unique(np.round(scores, 12)).size)
    thr = find_optimal_threshold(scores, y_true)
    y_pred = (scores >= thr).astype(int)
    p = float(np.mean(y_true))
    try:
        auc = float(roc_auc_score(y_true, scores))
    except Exception:
        auc = float("nan")
    return dict(
        f1=float(f1_score(y_true, y_pred, zero_division=0)),
        auc=auc,
        threshold=float(thr),
        n_unique_scores=n_unique,
        constant_scores=bool(n_unique <= 1),
        fire_rate=float(y_pred.mean()),
        n_eval=int(len(y_true)),
        target_prevalence=p,
        always_fire_f1=2 * p / (1 + p) if p > 0 else float("nan"),
    )


def run_model(model_name, X_train, X_eval, y_eval, n_features, seed):
    set_seeds(seed)
    if model_name == "IF":
        # training.py hardcodes random_state=42, so IF is seed-invariant here. Left as-is
        # rather than patched, to keep this identical to the published training path.
        m = train_isolation_forest(X_train, n_estimators=200, contamination=0.05)
        return evaluate(score_isolation_forest(m, X_eval), y_eval)
    if model_name == "OC_SVM":
        m = train_ocsvm(X_train, nu=0.05, kernel="rbf", gamma="scale")
        return evaluate(score_ocsvm(m, X_eval), y_eval)

    seq_len = DEEP_DEFAULTS["seq_length"]
    X_train_seq = create_sequences(X_train, seq_len)
    X_eval_seq = create_sequences(X_eval, seq_len)
    if X_train_seq.shape[0] < 32 or X_eval_seq.shape[0] < 32:
        return None
    y_eval_seq = y_eval[seq_len - 1:]
    if y_eval_seq.sum() == 0 or y_eval_seq.sum() == len(y_eval_seq):
        return None

    hd, nl, dr = DEEP_DEFAULTS["hidden_dim"], DEEP_DEFAULTS["n_layers"], DEEP_DEFAULTS["dropout"]
    lr, ep = DEEP_DEFAULTS["lr"], DEEP_DEFAULTS["epochs"]

    if model_name in ("LSTM_AE", "Trans_AE", "CNN_AE"):
        if model_name == "LSTM_AE":
            m = LSTMAutoencoder(n_features, hidden_dim=hd, n_layers=nl, dropout=dr)
        elif model_name == "Trans_AE":
            m = TransformerAutoencoder(n_features, d_model=hd, nhead=4, n_layers=nl,
                                       dropout=dr, seq_len=seq_len)
        else:
            m = CNN1DAutoencoder(n_features, filters=hd, kernel_size=5, n_layers=nl,
                                 dropout=dr)
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
        model, _ = train_deep_svdd(m, X_train_seq, lr=lr, pretrain_epochs=15,
                                   finetune_epochs=15)
        mu, sigma = get_training_dist_stats_svdd(model, X_train_seq)
        scores = score_deep_svdd(model, X_eval_seq, mu, sigma)
    elif model_name == "DAGMM":
        m = DAGMM(n_features, hidden_dim=hd, n_layers=nl, latent_dim=8, n_gmm=4, dropout=dr)
        model, _ = train_dagmm(m, X_train_seq, lr=lr, epochs=ep)
        mu, sigma = get_training_energy_stats_dagmm(model, X_train_seq)
        scores = score_dagmm(model, X_eval_seq, mu, sigma)
    else:
        raise ValueError(model_name)
    return evaluate(scores, y_eval_seq)


def load():
    ecom = pd.read_parquet(ECOM_PARQUET)
    iot = pd.read_parquet(IOT_PARQUET)
    feat_cols = sorted(set(c for c in ecom.columns if c not in META)
                       & set(c for c in iot.columns if c not in META))
    return feat_cols, {"ecom": ecom, "iot": iot}


def subset(df, feat_cols, fold, split, normal_only=False):
    col = f"fold_{fold}_split"
    mask = df[col] == split
    if normal_only:
        mask &= df["label"] == "NORMAL"
    else:
        mask &= df["label"] != "EXCLUDED"
    sub = df.loc[mask]
    X = sub[feat_cols].fillna(0.0).to_numpy(dtype=np.float32)
    y = (sub["label"] == "ANOMALOUS").to_numpy()
    return X, y


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--models", nargs="*", default=CLASSICAL + DEEP)
    ap.add_argument("--scaling", nargs="*", default=["source_fit", "target_fit"])
    ap.add_argument("--folds", type=int, nargs="*", default=list(range(1, 11)))
    ap.add_argument("--seeds", type=int, nargs="*", default=[42, 43, 44])
    args = ap.parse_args()

    feat_cols, wl = load()
    n_features = len(feat_cols)
    print(f"Shared features: {n_features}", flush=True)

    cases = [("ecom_to_iot", "ecom", "iot"), ("iot_to_ecom", "iot", "ecom"),
             ("ecom_to_ecom", "ecom", "ecom"), ("iot_to_iot", "iot", "iot")]

    rows = []
    t0 = time.time()
    total = (len(args.scaling) * len(cases) * len(args.folds)
             * len(args.seeds) * len(args.models))
    done = 0

    for scaling in args.scaling:
        for case_name, src, tgt in cases:
            for fold in args.folds:
                X_tr, _ = subset(wl[src], feat_cols, fold, "train", normal_only=True)
                X_ev, y_ev = subset(wl[tgt], feat_cols, fold, "test")

                mu_s, sig_s = fit_scaler(X_tr)
                X_train = apply_scaler(X_tr, mu_s, sig_s)
                if scaling == "source_fit":
                    mu_t, sig_t = mu_s, sig_s
                else:
                    X_tgt_tr, _ = subset(wl[tgt], feat_cols, fold, "train", normal_only=True)
                    mu_t, sig_t = fit_scaler(X_tgt_tr)
                X_eval = apply_scaler(X_ev, mu_t, sig_t)

                for seed in args.seeds:
                    for model_name in args.models:
                        try:
                            res = run_model(model_name, X_train, X_eval, y_ev,
                                            n_features, seed)
                            err = ""
                        except Exception as exc:  # noqa: BLE001
                            res, err = None, f"{type(exc).__name__}: {exc}"
                        done += 1
                        if res is None:
                            res = dict(f1=np.nan, auc=np.nan, threshold=np.nan,
                                       n_unique_scores=0, constant_scores=True,
                                       fire_rate=np.nan, n_eval=len(y_ev),
                                       target_prevalence=float(y_ev.mean()),
                                       always_fire_f1=np.nan)
                        rows.append(dict(scaling=scaling, case=case_name, source=src,
                                         target=tgt, model=model_name, fold=fold,
                                         seed=seed, n_train=len(X_train),
                                         error=err, **res))
                    if done % 40 < len(args.models):
                        el = time.time() - t0
                        eta = el / max(done, 1) * (total - done)
                        print(f"  {done}/{total} elapsed {el/60:.1f}m eta {eta/60:.1f}m",
                              flush=True)
                        pd.DataFrame(rows).to_csv(RAW_CSV, index=False)
                print(f"[{scaling}] {case_name} fold {fold} done "
                      f"(train {len(X_train)}, eval {len(X_eval)}, "
                      f"prev {y_ev.mean():.3f})", flush=True)

    pd.DataFrame(rows).to_csv(RAW_CSV, index=False)
    print(f"\nSaved {len(rows)} rows: {RAW_CSV}")
    print(f"Total wall time: {(time.time() - t0)/60:.1f} min")


if __name__ == "__main__":
    main()
