"""Classical / lightweight time-series anomaly-detection baselines.

Simple time-series detectors often hold up well against deep semi-supervised
methods under tight FPR budgets. This script adds seven such detectors and runs them
on the same per-fold feature parquets with the same cal/test split layout the
ML panel uses, then reports F1 / precision / recall / AUC-ROC / FPR per fold.

Models (all use the labeled cal split for F1-optimal thresholding, matching
the headline benchmark protocol):
  EWMA   per-feature deviation from a fixed reference: the last exponentially
         smoothed value (alpha 0.3) of the training-normal sequence, divided by the
         time-average of the training EWMA standard deviation; window score is the
         max over features. The reference does not move over the scored windows.
         ewma_mechanism.py analyses what this construction does.
  CUSUM  per-feature cumulative-sum drift detector; window score is
         max over features of CUSUM positive arm
  MP     univariate Matrix Profile (stumpy) on the leading PCA component;
         window score is the matrix-profile value (higher = more anomalous)
  LODA   PyOD Lightweight On-line Detector of Anomalies, multivariate
  HST    PyOD HBOS, the histogram-based outlier score (labelled HST in the outputs)
  COPOD  PyOD copula-based detector
  POT    peaks over threshold on the per-window max |z|, with a GPD fitted to the
         upper tail of training-normal scores (see score_pot)

Output: classical_baselines_per_fold.csv and classical_baselines_summary.csv.
"""
from pathlib import Path
import warnings
import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score
from sklearn.preprocessing import StandardScaler
from scipy.stats import genpareto
from pyod.models.loda import LODA
from pyod.models.hbos import HBOS
from pyod.models.copod import COPOD
import stumpy

warnings.filterwarnings("ignore")

ROOT = Path(__file__).resolve().parents[2]
WORKLOADS = {
    "ecommerce": ROOT / "data/features_v3/features_30s.parquet",
    "iot": ROOT / "data/v3/iot/features/features_30s.parquet",
}
OUT_DIR = Path(__file__).parent

WINDOW_SEC = 30
N_FOLDS = 10


def find_optimal_threshold(scores, y):
    if y.sum() == 0 or y.sum() == len(y):
        return float(np.nanmax(scores))
    best_t, best_f1 = float(np.nanmax(scores)), -1.0
    cand = np.unique(scores)
    if len(cand) > 200:
        cand = np.quantile(scores, np.linspace(0, 1, 201))
    for t in cand:
        preds = (scores >= t).astype(int)
        f = f1_score(y, preds, zero_division=0)
        if f > best_f1:
            best_f1, best_t = f, float(t)
    return best_t


EXCLUDE = {"label", "fault_id", "fault_category", "repetition", "window_start",
           "window_end", "window_mid", "n_samples", "run_number", "run_source"}


def feat_cols(df):
    return [c for c in df.columns
            if c not in EXCLUDE
            and not c.startswith("fold_")
            and df[c].dtype.kind in "fi"]


# ---------- per-detector score functions ----------

def score_ewma(X_train, X_eval, alpha=0.3):
    """Max over features of |x - mu_last| / mean EWMA std, both fitted on X_train only.
    mu_last is the final smoothed training value, so the reference is fixed for X_eval."""
    mu = pd.DataFrame(X_train).ewm(alpha=alpha, adjust=False).mean().to_numpy()
    sd = pd.DataFrame(X_train).ewm(alpha=alpha, adjust=False).std().to_numpy()
    sd_global = np.nanmean(sd, axis=0)
    sd_global = np.where(sd_global > 1e-9, sd_global, 1.0)
    mu_last = mu[-1]
    z = np.abs(X_eval - mu_last) / sd_global
    return np.nanmax(z, axis=1)


def score_cusum(X_train, X_eval, k=0.5):
    """Per-feature CUSUM positive arm; max over features."""
    mu = X_train.mean(axis=0)
    sd = X_train.std(axis=0)
    sd = np.where(sd > 1e-9, sd, 1.0)
    z = (X_eval - mu) / sd
    cs = np.zeros_like(z)
    cs[0] = np.maximum(0, z[0] - k)
    for i in range(1, len(z)):
        cs[i] = np.maximum(0, cs[i - 1] + z[i] - k)
    return np.nanmax(cs, axis=1)


def score_mp(X_train, X_eval, m=8):
    """Matrix-profile score on leading PCA component, computed on (train||eval)."""
    pca = PCA(n_components=1)
    pca.fit(X_train)
    s_full = pca.transform(np.vstack([X_train, X_eval])).ravel()
    if len(s_full) < 2 * m + 2:
        return np.zeros(len(X_eval))
    mp = stumpy.stump(s_full, m=m)
    profile = mp[:, 0].astype(float)
    if len(profile) < len(s_full):
        profile = np.concatenate([profile, np.full(len(s_full) - len(profile), np.nan)])
    eval_scores = profile[len(X_train):len(X_train) + len(X_eval)]
    return np.nan_to_num(eval_scores, nan=np.nanmean(profile))


def score_loda(X_train, X_eval):
    m = LODA(n_bins=20, n_random_cuts=100)
    m.fit(X_train)
    return m.decision_function(X_eval)


def score_hst(X_train, X_eval):
    """HBOS histogram-based outlier score, labelled HST in the outputs."""
    m = HBOS(n_bins=10)
    m.fit(X_train)
    return m.decision_function(X_eval)


def score_copod(X_train, X_eval):
    m = COPOD()
    m.fit(X_train)
    return m.decision_function(X_eval)


def score_pot(X_train, X_eval, q=0.95):
    """Peaks over threshold on the per-window max |z|. Below the training q-quantile u the score
    is the window's quantile in the training distribution. Above u it is q plus (1 - q) times the
    GPD CDF of the excess, with the GPD fitted to the training excesses. The score therefore rises
    monotonically with max |z|."""
    mu = X_train.mean(axis=0)
    sd = np.where(X_train.std(axis=0) > 0, X_train.std(axis=0), 1.0)
    s_train = np.nanmax(np.abs((X_train - mu) / sd), axis=1)
    s_eval = np.nanmax(np.abs((X_eval - mu) / sd), axis=1)
    u = float(np.quantile(s_train, q))
    below = np.minimum(np.searchsorted(np.sort(s_train), s_eval, side="right") / len(s_train), q)
    excess = s_train[s_train > u] - u
    if len(excess) < 5:
        return np.where(s_eval > u, q + (1 - q) * (1 - np.exp(-(s_eval - u))), below)
    c, _, scale = genpareto.fit(excess, floc=0)
    return np.where(s_eval > u, q + (1 - q) * genpareto.cdf(s_eval - u, c, loc=0, scale=scale), below)


SCORERS = {
    "EWMA":  score_ewma,
    "CUSUM": score_cusum,
    "MP":    score_mp,
    "LODA":  score_loda,
    "HST":   score_hst,
    "COPOD": score_copod,
    "POT":   score_pot,
}


# ---------- driver ----------

def evaluate_workload(name, parquet):
    print(f"\n=== {name} ===")
    df = pd.read_parquet(parquet)
    cols = feat_cols(df)
    rows = []
    for fold in range(1, N_FOLDS + 1):
        col = f"fold_{fold}_split"
        if col not in df.columns:
            continue
        train = df[df[col] == "train"]
        train_norm = train[train["label"] == "NORMAL"]
        cal = df[df[col] == "cal"]
        test = df[df[col] == "test"]
        if len(train_norm) == 0 or len(cal) == 0 or len(test) == 0:
            continue

        X_train = train_norm[cols].fillna(0).to_numpy()
        X_cal = cal[cols].fillna(0).to_numpy()
        X_test = test[cols].fillna(0).to_numpy()
        # Standardise on training-normal
        scaler = StandardScaler().fit(X_train)
        X_train = scaler.transform(X_train)
        X_cal = scaler.transform(X_cal)
        X_test = scaler.transform(X_test)

        y_cal = (cal["label"] == "ANOMALOUS").astype(int).to_numpy()
        y_test = (test["label"] == "ANOMALOUS").astype(int).to_numpy()

        for name_d, scorer in SCORERS.items():
            try:
                s_cal = scorer(X_train, X_cal)
                s_test = scorer(X_train, X_test)
            except Exception as e:
                print(f"  fold {fold} {name_d}: {e}")
                continue
            t = find_optimal_threshold(s_cal, y_cal)
            pred = (s_test >= t).astype(int)
            tn = int(((pred == 0) & (y_test == 0)).sum())
            fp = int(((pred == 1) & (y_test == 0)).sum())
            fn = int(((pred == 0) & (y_test == 1)).sum())
            tp = int(((pred == 1) & (y_test == 1)).sum())
            try:
                auc = roc_auc_score(y_test, s_test) if y_test.sum() > 0 and y_test.sum() < len(y_test) else float("nan")
            except Exception:
                auc = float("nan")
            rows.append({
                "workload": name, "fold": fold, "model": name_d,
                "tp": tp, "fp": fp, "fn": fn, "tn": tn,
                "tpr": tp / max(tp + fn, 1),
                "fpr": fp / max(fp + tn, 1),
                "precision": precision_score(y_test, pred, zero_division=0),
                "recall": recall_score(y_test, pred, zero_division=0),
                "f1": f1_score(y_test, pred, zero_division=0),
                "auc_roc": auc,
                "threshold": t,
            })
        print(f"  fold {fold} done")
    return pd.DataFrame(rows)


def main():
    all_rows = []
    for name, p in WORKLOADS.items():
        if not p.exists():
            print(f"skip {name}: parquet missing")
            continue
        df = evaluate_workload(name, p)
        all_rows.append(df)
    out = pd.concat(all_rows, ignore_index=True)
    out.to_csv(OUT_DIR / "classical_baselines_per_fold.csv", index=False)

    summary = out.groupby(["workload", "model"]).agg(
        f1_mean=("f1", "mean"), f1_std=("f1", "std"),
        prec_mean=("precision", "mean"),
        rec_mean=("recall", "mean"),
        tpr_mean=("tpr", "mean"), fpr_mean=("fpr", "mean"),
        auc_mean=("auc_roc", "mean"),
    ).round(3).reset_index()
    summary.to_csv(OUT_DIR / "classical_baselines_summary.csv", index=False)
    print("\n=== SUMMARY ===")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
