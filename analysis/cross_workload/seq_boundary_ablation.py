"""Ablation: does temporal-contiguity-respecting sequence construction change RQ1/RQ2?

Background in sequence_contiguity.py. training.py builds sequences with a dense sliding
window over frame order and never checks that consecutive rows are adjacent in time, so a
share of sequences splice telemetry from unrelated points in the campaign. The share grows
with seq_length: at the 30s anchor it is 12.6% of test sequences at seq_length 5 and 64.2%
at seq_length 20, the top of the paper's HPO search range.

That matters for RQ2 specifically. If longer contexts are penalised by an artifact rather
than by the data, HPO is pushed toward short sequences and the point-vs-sequence comparison
is biased against sequence models. This script measures the size of that effect.

Three arms decompose it:

  A  dense_train / dense_eval        the paper's behaviour
  B  dense_train / contiguous_eval   same trained model, corrupt test sequences removed
  C  contig_train / contiguous_eval  the full fix

A to B isolates test-time corruption. B to C isolates train-time corruption. Arms A and B
share one trained model per cell, so only two trainings are needed per cell.

Fold structure and the binary label mapping match the paper's main tables, so cell values
are directly comparable to the published RQ1 numbers. Folds are the clustering unit for
downstream statistics.

Output:
  seq_boundary_ablation_raw.csv
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
sys.path.insert(0, str(Path(__file__).resolve().parent))

from training import (  # noqa: E402
    LSTMAutoencoder, TransformerAutoencoder, CNN1DAutoencoder,
    LSTMVariationalAutoencoder, DeepSVDD,
    train_autoencoder, score_autoencoder, get_training_mse_stats,
    train_vae, score_vae, get_training_mse_stats_vae,
    train_deep_svdd, score_deep_svdd, get_training_dist_stats_svdd,
    create_sequences, find_optimal_threshold, set_seeds,
)
from sequence_contiguity import (  # noqa: E402
    create_sequences_contiguous, infer_stride,
)

OUT = Path(__file__).resolve().parent
RAW = OUT / "seq_boundary_ablation_raw.csv"

PARQUETS = {
    "ecommerce": ROOT / "data" / "features_v3" / "features_{ws}s.parquet",
    "iot": ROOT / "data" / "v3" / "iot" / "features" / "features_{ws}s.parquet",
}
META = {"window_start", "window_end", "window_mid", "n_samples", "label",
        "fault_id", "fault_category", "run_number", "repetition", "run_source"}
META |= {f"fold_{i}_split" for i in range(1, 11)}

SEQ_MODELS = ["LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE", "DeepSVDD"]
DEFAULTS = dict(hidden_dim=64, n_layers=2, lr=1e-3, dropout=0.1, epochs=30)


def build_model(name, n_features, seq_len):
    hd, nl, dr = DEFAULTS["hidden_dim"], DEFAULTS["n_layers"], DEFAULTS["dropout"]
    if name == "LSTM_AE":
        return LSTMAutoencoder(n_features, hidden_dim=hd, n_layers=nl, dropout=dr)
    if name == "Trans_AE":
        return TransformerAutoencoder(n_features, d_model=hd, nhead=4, n_layers=nl,
                                      dropout=dr, seq_len=seq_len)
    if name == "CNN_AE":
        return CNN1DAutoencoder(n_features, filters=hd, kernel_size=5, n_layers=nl,
                                dropout=dr)
    if name == "LSTM_VAE":
        return LSTMVariationalAutoencoder(n_features, hidden_dim=hd, n_layers=nl,
                                          latent_dim=32, dropout=dr)
    if name == "DeepSVDD":
        return DeepSVDD(n_features, hidden_dim=hd, n_layers=nl, latent_dim=32, dropout=dr)
    raise ValueError(name)


def fit_and_stats(name, X_train_seq, seq_len, n_features, seed):
    """Train one model on the given sequences, return (model, stat_a, stat_b, kind)."""
    set_seeds(seed)
    m = build_model(name, n_features, seq_len)
    if name == "LSTM_VAE":
        model, _ = train_vae(m, X_train_seq, lr=DEFAULTS["lr"], epochs=DEFAULTS["epochs"])
        return model, *get_training_mse_stats_vae(model, X_train_seq), "vae"
    if name == "DeepSVDD":
        model, _ = train_deep_svdd(m, X_train_seq, lr=DEFAULTS["lr"],
                                   pretrain_epochs=15, finetune_epochs=15)
        return model, *get_training_dist_stats_svdd(model, X_train_seq), "svdd"
    model, _ = train_autoencoder(m, X_train_seq, lr=DEFAULTS["lr"],
                                 epochs=DEFAULTS["epochs"])
    return model, *get_training_mse_stats(model, X_train_seq), "ae"


def score(kind, model, X_seq, a, b):
    if kind == "vae":
        return score_vae(model, X_seq, a, b)
    if kind == "svdd":
        return score_deep_svdd(model, X_seq, a, b)
    return score_autoencoder(model, X_seq, a, b)


def evaluate(scores, y_true):
    scores = np.asarray(scores, dtype=np.float64)
    n_unique = int(np.unique(np.round(scores, 12)).size)
    thr = find_optimal_threshold(scores, y_true)
    y_pred = (scores >= thr).astype(int)
    try:
        auc = float(roc_auc_score(y_true, scores))
    except Exception:
        auc = float("nan")
    p = float(np.mean(y_true))
    return dict(f1=float(f1_score(y_true, y_pred, zero_division=0)), auc=auc,
                n_unique_scores=n_unique, constant_scores=bool(n_unique <= 1),
                fire_rate=float(y_pred.mean()), n_eval_seq=int(len(y_true)),
                eval_prevalence=p)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workloads", nargs="*", default=list(PARQUETS))
    ap.add_argument("--window", type=int, default=30)
    ap.add_argument("--seq-lengths", type=int, nargs="*", default=[5, 8, 20])
    ap.add_argument("--models", nargs="*", default=SEQ_MODELS)
    ap.add_argument("--folds", type=int, nargs="*", default=list(range(1, 11)))
    ap.add_argument("--seeds", type=int, nargs="*", default=[42, 43, 44])
    args = ap.parse_args()

    rows, t0 = [], time.time()
    total = (len(args.workloads) * len(args.folds) * len(args.seq_lengths)
             * len(args.seeds) * len(args.models))
    done = 0

    for wl in args.workloads:
        path = Path(str(PARQUETS[wl]).format(ws=args.window))
        df = pd.read_parquet(path)
        feat_cols = sorted(c for c in df.columns if c not in META)
        n_features = len(feat_cols)
        stride = infer_stride(df["window_start"])
        print(f"\n### {wl}: {len(df)} rows, {n_features} features, stride {stride:.0f}s",
              flush=True)

        for fold in args.folds:
            col = f"fold_{fold}_split"
            tr = df[(df[col] == "train") & (df["label"] == "NORMAL")]
            te = df[(df[col] == "test") & (df["label"] != "EXCLUDED")]

            Xtr = tr[feat_cols].fillna(0.0).to_numpy(np.float32)
            Xte = te[feat_cols].fillna(0.0).to_numpy(np.float32)
            yte = (te["label"] == "ANOMALOUS").to_numpy()
            ttr = tr["window_start"].to_numpy(np.float64)
            tte = te["window_start"].to_numpy(np.float64)

            mu, sd = Xtr.mean(0), np.where(Xtr.std(0) < 1e-8, 1.0, Xtr.std(0))
            Xtr = ((Xtr - mu) / sd).astype(np.float32)
            Xte = ((Xte - mu) / sd).astype(np.float32)

            for L in args.seq_lengths:
                tr_dense = create_sequences(Xtr, L)
                te_dense = create_sequences(Xte, L)
                y_dense = yte[L - 1:][:len(te_dense)]
                tr_cont, _ = create_sequences_contiguous(Xtr, L, ttr, stride)
                te_cont, te_cont_idx = create_sequences_contiguous(Xte, L, tte, stride)
                y_cont = yte[te_cont_idx]

                sizes = dict(n_train_dense=len(tr_dense), n_train_contig=len(tr_cont),
                             n_test_dense=len(te_dense), n_test_contig=len(te_cont))
                if min(sizes.values()) < 32 or y_cont.sum() == 0 or y_dense.sum() == 0:
                    print(f"  [{wl} f{fold} L={L}] skipped, sizes {sizes}", flush=True)
                    done += len(args.seeds) * len(args.models)
                    continue

                for seed in args.seeds:
                    for name in args.models:
                        try:
                            # arms A and B share the dense-trained model
                            m_d, a_d, b_d, kind = fit_and_stats(
                                name, tr_dense, L, n_features, seed)
                            resA = evaluate(score(kind, m_d, te_dense, a_d, b_d), y_dense)
                            resB = evaluate(score(kind, m_d, te_cont, a_d, b_d), y_cont)
                            # arm C retrains on contiguous sequences
                            m_c, a_c, b_c, kind = fit_and_stats(
                                name, tr_cont, L, n_features, seed)
                            resC = evaluate(score(kind, m_c, te_cont, a_c, b_c), y_cont)
                            err = ""
                        except Exception as exc:  # noqa: BLE001
                            print(f"  FAILED {name}: {type(exc).__name__}: {exc}",
                                  flush=True)
                            err = f"{type(exc).__name__}: {exc}"
                            nan = dict(f1=np.nan, auc=np.nan, n_unique_scores=0,
                                       constant_scores=True, fire_rate=np.nan,
                                       n_eval_seq=0, eval_prevalence=np.nan)
                            resA = resB = resC = nan
                        for arm, res in (("A_dense_dense", resA),
                                         ("B_dense_contig", resB),
                                         ("C_contig_contig", resC)):
                            rows.append(dict(workload=wl, window=args.window, fold=fold,
                                             seq_length=L, seed=seed, model=name,
                                             arm=arm, error=err, **sizes, **res))
                        done += 1
                    pd.DataFrame(rows).to_csv(RAW, index=False)
                el = time.time() - t0
                print(f"  [{wl} f{fold} L={L}] done {done}/{total} "
                      f"elapsed {el/60:.1f}m eta {el/max(done,1)*(total-done)/60:.1f}m "
                      f"| test seq dense {sizes['n_test_dense']} "
                      f"contig {sizes['n_test_contig']}", flush=True)

    pd.DataFrame(rows).to_csv(RAW, index=False)
    print(f"\nSaved {len(rows)} rows: {RAW}")
    print(f"Wall time: {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    main()
