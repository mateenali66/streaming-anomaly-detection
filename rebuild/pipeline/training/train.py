#!/usr/bin/env python3
"""
ML training for the rebuilt streaming anomaly-detection benchmark.

Adapted from sagemaker/training.py (R1). The model classes and the train_* functions are the R1
code, with the hard-coded random_state=42 replaced by MODEL_SEED so seeds come from the
configuration. Review 4 changed the scoring and the threshold search, and the paper states it
(decision 26). tests/test_training_smoke.py compares the unchanged functions with the R1 file and
pins the source of the changed ones:

  * Isolation Forest and OC-SVM: the min-max map of the negated decision function is fitted once
    per fold on the train NORMAL windows (fit_score_scaler) and applied to every split. R1 fitted
    it on each scored set. Scores are not clipped, so they can leave [0, 1].
  * DAGMM: a NaN or infinite sample energy takes the largest finite train NORMAL energy. R1 took
    the largest finite energy of the scored set.
  * Deep SVDD: the degenerate-encoder check runs once on the train NORMAL encodings. R1 ran it per
    scoring batch of 256 windows and set that batch's scores to 1.0.
  * F1-optimal threshold: every distinct observed cal score is a candidate (MIN-8). R1 searched the
    grid 0.01 to 0.99.

What changed around them:

  * Input is windows_<w>s.parquet and folds.parquet from the preprocessing (contract 11.9).
  * Labels follow protocol 3.3. Semi-supervised models fit on role train and label NORMAL only.
    The random forest fits on train NORMAL and ANOMALOUS. RECOVERY, SHAM, SHAM_POST and TRAILING
    never enter a fit. Evaluation negatives are NORMAL, RECOVERY, SHAM and SHAM_POST (4.5.1).
  * Feature selection, imputation and scaling are fitted per fold on train NORMAL only
    (streamprep.fold_fit). The protected columns of config/pipeline.json fold_fit (TaskManager
    count and missingness, derived features, named event counters) bypass both filters (CAM-4).
  * Hyperparameter search maximises pooled AUC-ROC on hpo_val (6.1.1). Early stopping uses the
    normal-only loss on hpo_val NORMAL windows (6.1.3).
  * Sequences are contiguous inside one seq_group and this is the only mode (4.1.7). seq_length is
    searched on evaluation.seq_length of config/pipeline.json, 5 to 11 (decision 27).
  * Every model scores the same within-run negatives, the windows tagged within_run by the
    preprocessing, and every score file carries the tag (MIN-9).
  * An Optuna study is named and keyed by window size, held-out fault, derived switch and windows
    file hash, and a stored study with another identity is refused (MIN-7).
  * validate_data checks every role of a fold, not only the test block (MIN-6).
  * Seeds come from config: model seed = 1000 * campaign_index + 42 + fold, and the Optuna
    sampler gets the same seed (6.2.2).
  * Per-window scores are written for test, cal and trailing windows, so metrics can be computed
    later without retraining.

Usage:
    python3 training/train.py --model IF --windows derived/ec-a/windows_30s.parquet \
        --folds derived/ec-a/folds.parquet --output results/ec-a
"""

from __future__ import annotations

import argparse
import json
import os
import pickle
import sys
import time
import warnings
from pathlib import Path

import numpy as np
import optuna
import pandas as pd
from scipy.special import expit as sigmoid
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    precision_recall_fscore_support,
    roc_auc_score,
)
from sklearn.svm import OneClassSVM

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from streamprep.config import config_sha256  # noqa: E402
from streamprep.config import load_config as load_pipeline_config  # noqa: E402
from streamprep.contract import campaign_index_from_id, sha256_file  # noqa: E402
from streamprep.fold_fit import FoldTransform, feature_columns  # noqa: E402
from streamprep.sequences import sequence_index  # noqa: E402
from streamprep.windows import within_run_tags  # noqa: E402

warnings.filterwarnings("ignore")
optuna.logging.set_verbosity(optuna.logging.WARNING)

try:
    import torch
    import torch.nn as nn
    from torch.utils.data import DataLoader, TensorDataset

    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False
    print("WARNING: PyTorch not available. Deep learning models will be skipped.")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CATEGORIES = ["broker", "consumer", "flink", "jvm"]
DEEP_MODELS = {"LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE", "DeepSVDD", "DAGMM"}
CLASSICAL_MODELS = {"IF", "OC_SVM"}
ALL_MODELS = [
    "IF", "OC_SVM",
    "LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE",
    "DeepSVDD", "DAGMM",
    "RF", "RuleBased",
]
FAULTS = ["F1", "F2", "F4", "F5", "F6", "F7", "F8"]
EVAL_LABELS = ("NORMAL", "ANOMALOUS", "RECOVERY", "SHAM", "SHAM_POST")
FIT_FORBIDDEN = ("RECOVERY", "SHAM", "SHAM_POST", "TRAILING", "EXCLUDED")

BATCH_SIZE = 256
MAX_EPOCHS = 100
DEFAULT_TRIALS_DEEP = 100
DEFAULT_TRIALS_CLASSICAL = 100
DEFAULT_TRIALS_RF = 50
MODEL_SEED = 42  # replaced per fit by seed_for_fit(); 42 reproduces the R1 value
SAMPLER_SEED = 42
FP_BUDGETS = (0.01, 0.05)

DEFAULT_CONFIG = Path(__file__).resolve().parent / "config.json"

_forced = os.environ.get("STREAMPREP_DEVICE")
if HAS_TORCH:
    if _forced:
        DEVICE = _forced
    elif torch.cuda.is_available():
        DEVICE = "cuda"
    elif hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        DEVICE = "mps"
    else:
        DEVICE = "cpu"
else:
    DEVICE = "cpu"


def _empty_gpu_cache():
    """Free GPU memory after model training/scoring."""
    if not HAS_TORCH:
        return
    if DEVICE == "cuda":
        torch.cuda.empty_cache()
    elif DEVICE == "mps":
        torch.mps.empty_cache()


def set_seeds(seed: int):
    """Set random seeds for reproducibility."""
    np.random.seed(seed)
    if HAS_TORCH:
        torch.manual_seed(seed)
        if torch.cuda.is_available():
            torch.cuda.manual_seed_all(seed)


# ---------------------------------------------------------------------------
# Model Architectures
# ---------------------------------------------------------------------------

class LSTMAutoencoder(nn.Module):
    def __init__(self, n_features: int, hidden_dim: int = 64,
                 n_layers: int = 2, dropout: float = 0.1):
        super().__init__()
        self.encoder = nn.LSTM(
            n_features, hidden_dim, n_layers,
            batch_first=True, dropout=dropout if n_layers > 1 else 0.0,
        )
        self.decoder = nn.LSTM(
            hidden_dim, hidden_dim, n_layers,
            batch_first=True, dropout=dropout if n_layers > 1 else 0.0,
        )
        self.output_layer = nn.Linear(hidden_dim, n_features)

    def forward(self, x):
        _, (h, c) = self.encoder(x)
        decoder_input = h[-1].unsqueeze(1).repeat(1, x.size(1), 1)
        decoder_out, _ = self.decoder(decoder_input, (h, c))
        return self.output_layer(decoder_out)


class TransformerAutoencoder(nn.Module):
    def __init__(self, n_features: int, d_model: int = 64, nhead: int = 4,
                 n_layers: int = 2, dropout: float = 0.1, seq_len: int = 10):
        super().__init__()
        self.input_proj = nn.Linear(n_features, d_model)
        self.pos_encoding = nn.Parameter(torch.randn(1, seq_len, d_model) * 0.1)

        encoder_layer = nn.TransformerEncoderLayer(
            d_model=d_model, nhead=nhead, dim_feedforward=d_model * 4,
            dropout=dropout, batch_first=True,
        )
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=n_layers)

        decoder_layer = nn.TransformerDecoderLayer(
            d_model=d_model, nhead=nhead, dim_feedforward=d_model * 4,
            dropout=dropout, batch_first=True,
        )
        self.decoder = nn.TransformerDecoder(decoder_layer, num_layers=n_layers)
        self.output_proj = nn.Linear(d_model, n_features)

    def forward(self, x):
        x_proj = self.input_proj(x) + self.pos_encoding[:, :x.size(1), :]
        memory = self.encoder(x_proj)
        decoded = self.decoder(x_proj, memory)
        return self.output_proj(decoded)


class CNN1DAutoencoder(nn.Module):
    def __init__(self, n_features: int, filters: int = 64, kernel_size: int = 5,
                 n_layers: int = 2, dropout: float = 0.1):
        super().__init__()
        enc_layers = []
        in_ch = n_features
        for i in range(n_layers):
            out_ch = filters // (2 ** i) if i > 0 else filters
            out_ch = max(out_ch, 8)
            enc_layers.extend([
                nn.Conv1d(in_ch, out_ch, kernel_size, padding=kernel_size // 2),
                nn.ReLU(),
                nn.Dropout(dropout),
            ])
            in_ch = out_ch
        self.encoder = nn.Sequential(*enc_layers)

        dec_layers = []
        for i in range(n_layers - 1, -1, -1):
            out_ch = filters // (2 ** i) if i > 0 else filters
            out_ch = max(out_ch, 8)
            if i == 0:
                out_ch = n_features
            dec_layers.extend([
                nn.Conv1d(in_ch, out_ch, kernel_size, padding=kernel_size // 2),
                nn.ReLU() if i > 0 else nn.Identity(),
            ])
            in_ch = out_ch
        self.decoder = nn.Sequential(*dec_layers)

    def forward(self, x):
        x = x.transpose(1, 2)  # (batch, features, seq_len)
        encoded = self.encoder(x)
        decoded = self.decoder(encoded)
        return decoded.transpose(1, 2)


class LSTMVariationalAutoencoder(nn.Module):
    def __init__(self, n_features: int, hidden_dim: int = 64,
                 n_layers: int = 2, latent_dim: int = 32,
                 dropout: float = 0.1):
        super().__init__()
        self.hidden_dim = hidden_dim
        self.n_layers = n_layers
        self.latent_dim = latent_dim

        self.encoder = nn.LSTM(
            n_features, hidden_dim, n_layers,
            batch_first=True, dropout=dropout if n_layers > 1 else 0.0,
        )
        self.fc_mu = nn.Linear(hidden_dim, latent_dim)
        self.fc_logvar = nn.Linear(hidden_dim, latent_dim)
        self.fc_decode = nn.Linear(latent_dim, hidden_dim)
        self.decoder = nn.LSTM(
            hidden_dim, hidden_dim, n_layers,
            batch_first=True, dropout=dropout if n_layers > 1 else 0.0,
        )
        self.output_layer = nn.Linear(hidden_dim, n_features)

    def reparameterize(self, mu: torch.Tensor, logvar: torch.Tensor) -> torch.Tensor:
        std = torch.exp(0.5 * logvar)
        eps = torch.randn_like(std)
        return mu + eps * std

    def forward(self, x):
        _, (h, c) = self.encoder(x)
        h_last = h[-1]
        mu = self.fc_mu(h_last)
        logvar = self.fc_logvar(h_last)
        z = self.reparameterize(mu, logvar)
        z_proj = self.fc_decode(z)
        decoder_input = z_proj.unsqueeze(1).repeat(1, x.size(1), 1)
        h0 = z_proj.unsqueeze(0).repeat(self.n_layers, 1, 1)
        c0 = torch.zeros_like(h0)
        decoder_out, _ = self.decoder(decoder_input, (h0, c0))
        recon = self.output_layer(decoder_out)
        return recon, mu, logvar


def vae_loss(recon_x, x, mu, logvar, beta=0.1):
    mse = nn.functional.mse_loss(recon_x, x, reduction="mean")
    kl = -0.5 * torch.mean(1 + logvar - mu.pow(2) - logvar.exp())
    return mse + beta * kl


class DeepSVDD(nn.Module):
    """Deep Support Vector Data Description for sequence anomaly detection.

    LSTM encoder maps sequences into a latent space; anomaly score is the
    squared distance from the hypersphere center c. The output projection
    has bias=False per Ruff et al. 2018 to prevent the trivial c=0 solution.
    A decoder is included for two-stage training: (1) pretrain as a standard
    autoencoder on normal data, (2) fine-tune the encoder by minimizing
    ||phi(x) - c||^2.

    Reference: Ruff et al., "Deep One-Class Classification" (ICML 2018).
    """

    def __init__(self, n_features: int, hidden_dim: int = 64,
                 n_layers: int = 2, latent_dim: int = 32,
                 dropout: float = 0.1):
        super().__init__()
        self.hidden_dim = hidden_dim
        self.n_layers = n_layers
        self.latent_dim = latent_dim

        self.encoder_lstm = nn.LSTM(
            n_features, hidden_dim, n_layers,
            batch_first=True, dropout=dropout if n_layers > 1 else 0.0,
        )
        # bias=False on the latent projection per Ruff 2018 (avoids c=0 collapse)
        self.encoder_proj = nn.Linear(hidden_dim, latent_dim, bias=False)

        # Decoder for AE pretraining
        self.decoder_proj = nn.Linear(latent_dim, hidden_dim)
        self.decoder_lstm = nn.LSTM(
            hidden_dim, hidden_dim, n_layers,
            batch_first=True, dropout=dropout if n_layers > 1 else 0.0,
        )
        self.output_layer = nn.Linear(hidden_dim, n_features)

        # Hypersphere center; populated after pretrain
        self.register_buffer("center", torch.zeros(latent_dim))

    def encode(self, x):
        _, (h, _) = self.encoder_lstm(x)
        return self.encoder_proj(h[-1])

    def decode(self, z, seq_len):
        h = self.decoder_proj(z)
        decoder_input = h.unsqueeze(1).repeat(1, seq_len, 1)
        h0 = h.unsqueeze(0).repeat(self.n_layers, 1, 1)
        c0 = torch.zeros_like(h0)
        out, _ = self.decoder_lstm(decoder_input, (h0, c0))
        return self.output_layer(out)

    def forward(self, x):
        # Default forward produces reconstruction (used during AE pretrain)
        z = self.encode(x)
        return self.decode(z, seq_len=x.size(1))


class DAGMM(nn.Module):
    """Deep Autoencoding Gaussian Mixture Model.

    LSTM autoencoder produces a latent code z_c; reconstruction-error
    features (relative Euclidean distance and cosine similarity between
    the flattened input and reconstruction) are concatenated with z_c to
    form z. An estimation network outputs a soft GMM membership gamma
    over K components. GMM parameters (phi, mu, sigma) are computed from
    z and gamma. Anomaly score is the per-sample energy:
        E(z) = -log( sum_k phi_k * N(z; mu_k, sigma_k) ).

    Reference: Zong et al., "Deep Autoencoding Gaussian Mixture Model
    for Unsupervised Anomaly Detection" (ICLR 2018).
    """

    def __init__(self, n_features: int, hidden_dim: int = 64,
                 n_layers: int = 2, latent_dim: int = 8,
                 n_gmm: int = 4, dropout: float = 0.1):
        super().__init__()
        self.n_features = n_features
        self.hidden_dim = hidden_dim
        self.n_layers = n_layers
        self.latent_dim = latent_dim
        self.n_gmm = n_gmm
        self.estim_input_dim = latent_dim + 2  # +2 for [rel_euclidean, cosine]

        # Compression network: LSTM AE
        self.encoder_lstm = nn.LSTM(
            n_features, hidden_dim, n_layers,
            batch_first=True, dropout=dropout if n_layers > 1 else 0.0,
        )
        self.encoder_proj = nn.Linear(hidden_dim, latent_dim)
        self.decoder_proj = nn.Linear(latent_dim, hidden_dim)
        self.decoder_lstm = nn.LSTM(
            hidden_dim, hidden_dim, n_layers,
            batch_first=True, dropout=dropout if n_layers > 1 else 0.0,
        )
        self.output_layer = nn.Linear(hidden_dim, n_features)

        # Estimation network: maps z -> soft GMM membership
        self.estim_net = nn.Sequential(
            nn.Linear(self.estim_input_dim, max(hidden_dim // 2, 8)),
            nn.Tanh(),
            nn.Dropout(dropout),
            nn.Linear(max(hidden_dim // 2, 8), n_gmm),
            nn.Softmax(dim=1),
        )

        # GMM parameters (computed during training, saved for scoring)
        self.register_buffer("phi", torch.ones(n_gmm) / n_gmm)
        self.register_buffer("mu", torch.zeros(n_gmm, self.estim_input_dim))
        self.register_buffer(
            "sigma",
            torch.eye(self.estim_input_dim).unsqueeze(0).repeat(n_gmm, 1, 1),
        )

    @staticmethod
    def _relative_euclidean(x_flat, recon_flat):
        return torch.norm(x_flat - recon_flat, p=2, dim=1) / (
            torch.norm(x_flat, p=2, dim=1) + 1e-8
        )

    @staticmethod
    def _cosine_sim(x_flat, recon_flat):
        return (x_flat * recon_flat).sum(dim=1) / (
            torch.norm(x_flat, p=2, dim=1) * torch.norm(recon_flat, p=2, dim=1) + 1e-8
        )

    def forward(self, x):
        _, (h, _) = self.encoder_lstm(x)
        z_c = self.encoder_proj(h[-1])
        h_dec = self.decoder_proj(z_c)
        decoder_input = h_dec.unsqueeze(1).repeat(1, x.size(1), 1)
        h0 = h_dec.unsqueeze(0).repeat(self.n_layers, 1, 1)
        c0 = torch.zeros_like(h0)
        recon_seq, _ = self.decoder_lstm(decoder_input, (h0, c0))
        recon_seq = self.output_layer(recon_seq)

        x_flat = x.reshape(x.size(0), -1)
        recon_flat = recon_seq.reshape(recon_seq.size(0), -1)
        rec_eu = self._relative_euclidean(x_flat, recon_flat).unsqueeze(1)
        rec_cos = self._cosine_sim(x_flat, recon_flat).unsqueeze(1)

        z = torch.cat([z_c, rec_eu, rec_cos], dim=1)
        gamma = self.estim_net(z)
        return recon_seq, z, gamma

    def compute_gmm_params(self, z, gamma):
        """Compute GMM phi, mu, sigma from soft memberships (Zong 2018 Eq 5-7)."""
        N = z.size(0)
        sum_gamma = gamma.sum(dim=0)  # (n_gmm,)
        phi = sum_gamma / N
        mu = (gamma.unsqueeze(2) * z.unsqueeze(1)).sum(dim=0) / (
            sum_gamma.unsqueeze(1) + 1e-8
        )
        z_mu = z.unsqueeze(1) - mu.unsqueeze(0)  # (N, n_gmm, D)
        z_mu_outer = z_mu.unsqueeze(3) * z_mu.unsqueeze(2)  # (N, n_gmm, D, D)
        sigma = (gamma.unsqueeze(2).unsqueeze(3) * z_mu_outer).sum(dim=0) / (
            sum_gamma.unsqueeze(1).unsqueeze(2) + 1e-8
        )
        return phi, mu, sigma

    def sample_energy(self, z, phi, mu, sigma):
        """Per-sample GMM energy E(z) = -log sum_k phi_k N(z; mu_k, sigma_k)."""
        D = z.size(1)
        eps_eye = 1e-3 * torch.eye(D, device=z.device).unsqueeze(0)
        sigma_jit = sigma + eps_eye
        z_mu = z.unsqueeze(1) - mu.unsqueeze(0)  # (N, n_gmm, D)
        try:
            L = torch.linalg.cholesky(sigma_jit)
        except RuntimeError:
            L = torch.linalg.cholesky(
                sigma_jit + 1e-2 * torch.eye(D, device=z.device).unsqueeze(0)
            )
        log_p_per_k = []
        for k in range(self.n_gmm):
            sol = torch.linalg.solve_triangular(
                L[k], z_mu[:, k, :].unsqueeze(2), upper=False
            )
            mahal = (sol.squeeze(2) ** 2).sum(dim=1)
            log_det = 2.0 * torch.log(torch.diagonal(L[k]) + 1e-8).sum()
            log_p_k = -0.5 * mahal - 0.5 * log_det - 0.5 * D * float(np.log(2 * np.pi))
            log_p_per_k.append(log_p_k + torch.log(phi[k] + 1e-8))
        E = -torch.logsumexp(torch.stack(log_p_per_k, dim=1), dim=1)
        return E

    @staticmethod
    def cov_diag_loss(sigma):
        """Penalize small diagonal entries of sigma (Zong 2018 Eq 13)."""
        diag = torch.diagonal(sigma, dim1=1, dim2=2)
        return (1.0 / (diag + 1e-8)).sum()


# ---------------------------------------------------------------------------
# Sequence Creation (contiguous only, protocol 4.1.7)
# ---------------------------------------------------------------------------
# A sequence is seq_length consecutive windows of one seq_group: one attempt's PRE_FAULT,
# INJECTION and RECOVERY windows, or one baseline stretch. The R1 frame-order mode is gone.

SEQ_LENGTH_MIN = 5           # lower bound of the Optuna seq_length search, from evaluation.seq_length
MAX_SEQ_LENGTH = 11          # upper bound (decision 27); run_training sets both from config/pipeline.json
SPLIT_GROUPS = {}            # split name -> (seq_group, seq_index) aligned with the split's rows


def set_split_groups(**splits):
    """Record per-split seq_group and seq_index arrays for the current fold."""
    SPLIT_GROUPS.clear()
    for name, (groups, idx) in splits.items():
        SPLIT_GROUPS[name] = (np.asarray(groups, dtype=object), np.asarray(idx, dtype=np.int64))


def build_sequences(X: np.ndarray, seq_length: int, split: str):
    """Return (sequences, end_idx) where end_idx[k] indexes the last row of sequence k.

    The last window carries the sequence's label, as in R1.
    """
    if split not in SPLIT_GROUPS:
        raise RuntimeError(f"no seq_group information for split {split!r}")
    groups, idx = SPLIT_GROUPS[split]
    if len(groups) != len(X):
        raise RuntimeError(f"split {split!r}: {len(groups)} group rows for {len(X)} feature rows")
    ones = np.ones(len(X), dtype=bool)
    members, ends = sequence_index(groups, idx, ones, ones, seq_length)
    if len(ends) == 0:
        return np.zeros((0, seq_length, X.shape[1]), dtype=X.dtype), np.empty(0, dtype=int)
    return X[members], ends


# ---------------------------------------------------------------------------
# Training Functions
# ---------------------------------------------------------------------------

def train_isolation_forest(X_normal, n_estimators=200, contamination=0.05,
                           max_features=1.0, max_samples=1.0):
    n_samples = max(1, int(len(X_normal) * max_samples))
    model = IsolationForest(
        n_estimators=n_estimators, contamination=contamination,
        max_features=max_features,
        max_samples=min(n_samples, len(X_normal)),
        random_state=MODEL_SEED, n_jobs=-1,
    )
    model.fit(X_normal)
    return model


def fit_score_scaler(raw_train_scores):
    """Min-max map fitted once per fold on the train NORMAL raw scores (Review 4 CAM-5, decision 26).

    Returns (lo, span). The same map is applied to every split, so a threshold taken on cal means the same on test
    and trailing. Values are not clipped, so a window more extreme than every train NORMAL window scores above 1.
    R1 fitted a MinMaxScaler on each scored set instead.
    """
    raw = np.asarray(raw_train_scores, dtype=float)
    raw = raw[np.isfinite(raw)]
    lo, hi = float(raw.min()), float(raw.max())
    return lo, (hi - lo) if hi > lo else 1.0


def score_isolation_forest(model, X, scaler):
    raw = model.decision_function(X)
    lo, span = scaler
    return (-raw - lo) / span


def train_ocsvm(X_normal, nu=0.05, kernel="rbf", gamma="scale"):
    if len(X_normal) > 10000:
        idx = np.random.RandomState(MODEL_SEED).choice(len(X_normal), 10000, replace=False)
        X_train = X_normal[idx]
    else:
        X_train = X_normal
    model = OneClassSVM(kernel=kernel, nu=nu, gamma=gamma)
    model.fit(X_train)
    return model


def score_ocsvm(model, X, scaler):
    raw = model.decision_function(X)
    lo, span = scaler
    return (-raw - lo) / span


def train_random_forest(X_train, y_train, n_estimators=200, max_depth=None,
                        min_samples_leaf=1, max_features="sqrt"):
    model = RandomForestClassifier(
        n_estimators=n_estimators, max_depth=max_depth,
        min_samples_leaf=min_samples_leaf, max_features=max_features,
        random_state=MODEL_SEED, n_jobs=-1, class_weight="balanced",
    )
    model.fit(X_train, y_train)
    return model


def score_random_forest(model, X):
    return model.predict_proba(X)[:, 1]


def train_rule_based(X_normal):
    """Rule-based baseline: learns normal distribution per feature."""
    mean = np.mean(X_normal, axis=0)
    std = np.std(X_normal, axis=0) + 1e-8
    return mean, std


def score_rule_based(X, mean, std):
    """Score as max absolute z-score per window."""
    z_scores = np.abs((X - mean) / std)
    return np.max(z_scores, axis=1)


def train_autoencoder(model, X_train_seq, X_val_seq=None,
                      epochs=MAX_EPOCHS, batch_size=BATCH_SIZE,
                      lr=1e-3):
    """Train autoencoder (LSTM, Transformer, CNN). Returns model and threshold."""
    model = model.to(DEVICE)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    criterion = nn.MSELoss()

    train_ds = TensorDataset(torch.FloatTensor(X_train_seq))
    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)

    best_val_loss = float("inf")
    patience_counter = 0
    patience = 10

    for epoch in range(epochs):
        model.train()
        epoch_loss = 0.0
        for (batch,) in train_loader:
            batch = batch.to(DEVICE)
            optimizer.zero_grad()
            recon = model(batch)
            loss = criterion(recon, batch)
            if torch.isnan(loss):
                print(f"      NaN loss at epoch {epoch + 1}, stopping")
                break
            loss.backward()
            optimizer.step()
            epoch_loss += loss.item() * len(batch)
        else:
            epoch_loss /= len(X_train_seq)
            if X_val_seq is not None and len(X_val_seq) > 0:
                model.eval()
                with torch.no_grad():
                    val_loss = 0.0
                    n_val = 0
                    for vi in range(0, len(X_val_seq), batch_size):
                        vb = torch.FloatTensor(X_val_seq[vi:vi + batch_size]).to(DEVICE)
                        vr = model(vb)
                        val_loss += criterion(vr, vb).item() * len(vb)
                        n_val += len(vb)
                    val_loss /= max(n_val, 1)
                if val_loss < best_val_loss:
                    best_val_loss = val_loss
                    patience_counter = 0
                else:
                    patience_counter += 1
                    if patience_counter >= patience:
                        break
            continue
        break  # NaN detected

    model.eval()
    with torch.no_grad():
        all_mse = []
        for ti in range(0, len(X_train_seq), batch_size):
            tb = torch.FloatTensor(X_train_seq[ti:ti + batch_size]).to(DEVICE)
            tr = model(tb)
            mse = torch.mean((tb - tr) ** 2, dim=(1, 2)).cpu().numpy()
            all_mse.append(mse)
        train_mse = np.concatenate(all_mse)
    threshold = float(np.mean(train_mse) + 2 * np.std(train_mse))

    _empty_gpu_cache()
    return model, threshold


def train_vae(model, X_train_seq, X_val_seq=None,
              epochs=MAX_EPOCHS, batch_size=BATCH_SIZE,
              lr=1e-3, beta=0.1):
    """Train LSTM-VAE. Returns model and threshold."""
    model = model.to(DEVICE)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)

    train_ds = TensorDataset(torch.FloatTensor(X_train_seq))
    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)

    best_val_loss = float("inf")
    patience_counter = 0
    patience = 10

    for epoch in range(epochs):
        model.train()
        epoch_loss = 0.0
        for (batch,) in train_loader:
            batch = batch.to(DEVICE)
            optimizer.zero_grad()
            recon, mu, logvar = model(batch)
            loss = vae_loss(recon, batch, mu, logvar, beta=beta)
            if torch.isnan(loss):
                break
            loss.backward()
            optimizer.step()
            epoch_loss += loss.item() * len(batch)
        else:
            epoch_loss /= len(X_train_seq)
            if X_val_seq is not None and len(X_val_seq) > 0:
                model.eval()
                with torch.no_grad():
                    val_loss = 0.0
                    n_val = 0
                    for vi in range(0, len(X_val_seq), batch_size):
                        vb = torch.FloatTensor(X_val_seq[vi:vi + batch_size]).to(DEVICE)
                        vr, v_mu, v_logvar = model(vb)
                        vloss = vae_loss(vr, vb, v_mu, v_logvar, beta=beta)
                        val_loss += vloss.item() * len(vb)
                        n_val += len(vb)
                    val_loss /= max(n_val, 1)
                if val_loss < best_val_loss:
                    best_val_loss = val_loss
                    patience_counter = 0
                else:
                    patience_counter += 1
                    if patience_counter >= patience:
                        break
            continue
        break

    model.eval()
    with torch.no_grad():
        all_mse = []
        for ti in range(0, len(X_train_seq), batch_size):
            tb = torch.FloatTensor(X_train_seq[ti:ti + batch_size]).to(DEVICE)
            tr, _, _ = model(tb)
            mse = torch.mean((tb - tr) ** 2, dim=(1, 2)).cpu().numpy()
            all_mse.append(mse)
        train_mse = np.concatenate(all_mse)
    threshold = float(np.mean(train_mse) + 2 * np.std(train_mse))

    _empty_gpu_cache()
    return model, threshold


def score_autoencoder(model, X_seq, train_mse_mean, train_mse_std,
                      batch_size=BATCH_SIZE):
    """Anomaly scores in [0,1] from reconstruction error. Degenerate detection included."""
    model.eval()
    all_mse = []
    all_degen = []
    with torch.no_grad():
        for i in range(0, len(X_seq), batch_size):
            xb = torch.FloatTensor(X_seq[i:i + batch_size]).to(DEVICE)
            rb = model(xb)
            mse = torch.mean((xb - rb) ** 2, dim=(1, 2)).cpu().numpy()
            all_mse.append(mse)
            all_degen.append(torch.var(rb, dim=(1, 2)).cpu().numpy() < 1e-6)
    mse = np.concatenate(all_mse)
    degen = np.concatenate(all_degen)
    z_scores = (mse - train_mse_mean) / (train_mse_std + 1e-8)
    scores = sigmoid(z_scores)
    scores[degen] = 1.0
    _empty_gpu_cache()
    return scores


def score_vae(model, X_seq, train_mse_mean, train_mse_std,
              batch_size=BATCH_SIZE):
    """Anomaly scores in [0,1] from VAE reconstruction error."""
    model.eval()
    all_mse = []
    all_degen = []
    with torch.no_grad():
        for i in range(0, len(X_seq), batch_size):
            xb = torch.FloatTensor(X_seq[i:i + batch_size]).to(DEVICE)
            rb, _, _ = model(xb)
            mse = torch.mean((xb - rb) ** 2, dim=(1, 2)).cpu().numpy()
            all_mse.append(mse)
            all_degen.append(torch.var(rb, dim=(1, 2)).cpu().numpy() < 1e-6)
    mse = np.concatenate(all_mse)
    degen = np.concatenate(all_degen)
    z_scores = (mse - train_mse_mean) / (train_mse_std + 1e-8)
    scores = sigmoid(z_scores)
    scores[degen] = 1.0
    _empty_gpu_cache()
    return scores


def get_training_mse_stats(model, X_train_seq, batch_size=BATCH_SIZE):
    model.eval()
    all_mse = []
    with torch.no_grad():
        for i in range(0, len(X_train_seq), batch_size):
            xb = torch.FloatTensor(X_train_seq[i:i + batch_size]).to(DEVICE)
            rb = model(xb)
            mse = torch.mean((xb - rb) ** 2, dim=(1, 2)).cpu().numpy()
            all_mse.append(mse)
    mse = np.concatenate(all_mse)
    _empty_gpu_cache()
    return float(np.mean(mse)), float(np.std(mse))


def get_training_mse_stats_vae(model, X_train_seq, batch_size=BATCH_SIZE):
    model.eval()
    all_mse = []
    with torch.no_grad():
        for i in range(0, len(X_train_seq), batch_size):
            xb = torch.FloatTensor(X_train_seq[i:i + batch_size]).to(DEVICE)
            rb, _, _ = model(xb)
            mse = torch.mean((xb - rb) ** 2, dim=(1, 2)).cpu().numpy()
            all_mse.append(mse)
    mse = np.concatenate(all_mse)
    _empty_gpu_cache()
    return float(np.mean(mse)), float(np.std(mse))


# ---------------------------------------------------------------------------
# Deep SVDD (Ruff et al., ICML 2018)
# ---------------------------------------------------------------------------

def train_deep_svdd(model, X_train_seq, X_val_seq=None,
                    pretrain_epochs=30, finetune_epochs=30,
                    batch_size=BATCH_SIZE, lr=1e-3):
    """Two-stage Deep SVDD training: AE pretrain, then hypersphere fine-tune.

    Stage 1 (pretrain): treat the model as a standard autoencoder; minimize
        MSE between input sequence and reconstructed sequence.
    Stage 2 (fine-tune): freeze the decoder; encode all training samples to
        compute the hypersphere center c (mean of encodings, with eps-shift
        away from origin to avoid c=0); then minimize ||phi(x) - c||^2.

    Returns the trained model and the SVDD distance threshold (mean + 2*std
    of training distances) for downstream score calibration.
    """
    model = model.to(DEVICE)
    train_ds = TensorDataset(torch.FloatTensor(X_train_seq))
    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)

    # --- Stage 1: AE pretrain ---
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    criterion = nn.MSELoss()
    best_val = float("inf")
    patience_counter = 0
    patience = 10

    for epoch in range(pretrain_epochs):
        model.train()
        for (batch,) in train_loader:
            batch = batch.to(DEVICE)
            optimizer.zero_grad()
            recon = model(batch)
            loss = criterion(recon, batch)
            loss.backward()
            optimizer.step()

        if X_val_seq is not None and len(X_val_seq) > 0:
            model.eval()
            with torch.no_grad():
                vloss = 0.0
                n_v = 0
                for vi in range(0, len(X_val_seq), batch_size):
                    vb = torch.FloatTensor(X_val_seq[vi:vi + batch_size]).to(DEVICE)
                    vr = model(vb)
                    vloss += criterion(vr, vb).item() * len(vb)
                    n_v += len(vb)
                vloss /= max(n_v, 1)
            if vloss < best_val:
                best_val = vloss
                patience_counter = 0
            else:
                patience_counter += 1
                if patience_counter >= patience:
                    break

    # --- Compute hypersphere center from encoded training data ---
    model.eval()
    encs = []
    with torch.no_grad():
        for ti in range(0, len(X_train_seq), batch_size):
            tb = torch.FloatTensor(X_train_seq[ti:ti + batch_size]).to(DEVICE)
            encs.append(model.encode(tb).cpu())
    enc_all = torch.cat(encs, dim=0)
    center = enc_all.mean(dim=0)
    # Shift center coordinates near zero away from zero per Ruff 2018 (eps=0.1)
    eps = 0.1
    center[(torch.abs(center) < eps) & (center < 0)] = -eps
    center[(torch.abs(center) < eps) & (center >= 0)] = eps
    model.center.data = center.to(DEVICE)

    # --- Stage 2: hypersphere fine-tune ---
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    best_val = float("inf")
    patience_counter = 0
    for epoch in range(finetune_epochs):
        model.train()
        for (batch,) in train_loader:
            batch = batch.to(DEVICE)
            optimizer.zero_grad()
            z = model.encode(batch)
            dist = torch.sum((z - model.center) ** 2, dim=1)
            loss = dist.mean()
            loss.backward()
            optimizer.step()

        if X_val_seq is not None and len(X_val_seq) > 0:
            model.eval()
            with torch.no_grad():
                vloss = 0.0
                n_v = 0
                for vi in range(0, len(X_val_seq), batch_size):
                    vb = torch.FloatTensor(X_val_seq[vi:vi + batch_size]).to(DEVICE)
                    vz = model.encode(vb)
                    vd = torch.sum((vz - model.center) ** 2, dim=1)
                    vloss += vd.mean().item() * len(vb)
                    n_v += len(vb)
                vloss /= max(n_v, 1)
            if vloss < best_val:
                best_val = vloss
                patience_counter = 0
            else:
                patience_counter += 1
                if patience_counter >= patience:
                    break

    # Compute training distance distribution for threshold
    model.eval()
    with torch.no_grad():
        all_d = []
        for ti in range(0, len(X_train_seq), batch_size):
            tb = torch.FloatTensor(X_train_seq[ti:ti + batch_size]).to(DEVICE)
            tz = model.encode(tb)
            d = torch.sum((tz - model.center) ** 2, dim=1).cpu().numpy()
            all_d.append(d)
        train_dist = np.concatenate(all_d)
    threshold = float(np.mean(train_dist) + 2 * np.std(train_dist))

    _empty_gpu_cache()
    return model, threshold


def score_deep_svdd(model, X_seq, train_dist_mean, train_dist_std, degenerate,
                    batch_size=BATCH_SIZE):
    """Anomaly score in [0, 1] from Deep SVDD distance to hypersphere center.

    `degenerate` is decided once on the train NORMAL encodings (get_training_dist_stats_svdd, Review 4 CAM-5,
    decision 26). R1 decided it per scoring batch of 256, so a batch of similar test windows could set its own scores
    to 1.0.
    """
    model.eval()
    all_d = []
    with torch.no_grad():
        for i in range(0, len(X_seq), batch_size):
            xb = torch.FloatTensor(X_seq[i:i + batch_size]).to(DEVICE)
            z = model.encode(xb)
            d = torch.sum((z - model.center) ** 2, dim=1).cpu().numpy()
            all_d.append(d)
    dist = np.concatenate(all_d)
    z_scores = (dist - train_dist_mean) / (train_dist_std + 1e-8)
    scores = sigmoid(z_scores)
    if degenerate:
        scores[:] = 1.0
    _empty_gpu_cache()
    return scores


def get_training_dist_stats_svdd(model, X_train_seq, batch_size=BATCH_SIZE):
    """Mean and std of training Deep SVDD distances, and whether the encoder is degenerate on train NORMAL.

    Degenerate means the encodings of all train NORMAL sequences have a mean per-dimension variance below 1e-6, the
    R1 test applied once to the whole train set.
    """
    model.eval()
    all_d = []
    all_z = []
    with torch.no_grad():
        for i in range(0, len(X_train_seq), batch_size):
            xb = torch.FloatTensor(X_train_seq[i:i + batch_size]).to(DEVICE)
            z = model.encode(xb)
            d = torch.sum((z - model.center) ** 2, dim=1).cpu().numpy()
            all_d.append(d)
            all_z.append(z.detach().cpu())
    dist = np.concatenate(all_d)
    z_all = torch.cat(all_z, dim=0)
    z_var = torch.var(z_all, dim=0).mean().item() if z_all.size(0) > 1 else 1.0
    _empty_gpu_cache()
    return float(np.mean(dist)), float(np.std(dist)), bool(z_var < 1e-6)


# ---------------------------------------------------------------------------
# DAGMM (Zong et al., ICLR 2018)
# ---------------------------------------------------------------------------

def train_dagmm(model, X_train_seq, X_val_seq=None,
                epochs=MAX_EPOCHS, batch_size=BATCH_SIZE, lr=1e-3,
                lambda_energy=0.1, lambda_cov_diag=0.005):
    """Train DAGMM end-to-end with reconstruction + sample-energy + cov-diag loss.

    After training, accumulate (z, gamma) over the full training set to compute
    final GMM parameters (phi, mu, sigma) and store them as model buffers for
    use at scoring time.

    Returns the trained model and a placeholder threshold (mean + 2*std of
    training sample energies) for downstream score calibration.
    """
    model = model.to(DEVICE)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    train_ds = TensorDataset(torch.FloatTensor(X_train_seq))
    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)

    best_val = float("inf")
    patience_counter = 0
    patience = 10

    for epoch in range(epochs):
        model.train()
        for (batch,) in train_loader:
            batch = batch.to(DEVICE)
            optimizer.zero_grad()
            recon, z, gamma = model(batch)
            recon_loss = nn.functional.mse_loss(recon, batch, reduction="mean")
            phi_b, mu_b, sigma_b = model.compute_gmm_params(z, gamma)
            energy = model.sample_energy(z, phi_b, mu_b, sigma_b).mean()
            cov_loss = model.cov_diag_loss(sigma_b)
            loss = recon_loss + lambda_energy * energy + lambda_cov_diag * cov_loss
            if torch.isnan(loss) or torch.isinf(loss):
                # Numerically unstable batch; skip
                continue
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=5.0)
            optimizer.step()

        if X_val_seq is not None and len(X_val_seq) > 0:
            model.eval()
            with torch.no_grad():
                vloss = 0.0
                n_v = 0
                for vi in range(0, len(X_val_seq), batch_size):
                    vb = torch.FloatTensor(X_val_seq[vi:vi + batch_size]).to(DEVICE)
                    vr, vz, vgamma = model(vb)
                    vrecon = nn.functional.mse_loss(vr, vb, reduction="mean")
                    vphi, vmu, vsigma = model.compute_gmm_params(vz, vgamma)
                    venergy = model.sample_energy(vz, vphi, vmu, vsigma).mean()
                    v = vrecon + lambda_energy * venergy
                    if torch.isnan(v) or torch.isinf(v):
                        continue
                    vloss += v.item() * len(vb)
                    n_v += len(vb)
                vloss /= max(n_v, 1)
            if vloss < best_val:
                best_val = vloss
                patience_counter = 0
            else:
                patience_counter += 1
                if patience_counter >= patience:
                    break

    # --- Compute final GMM params over full training set ---
    model.eval()
    all_z, all_gamma = [], []
    with torch.no_grad():
        for ti in range(0, len(X_train_seq), batch_size):
            tb = torch.FloatTensor(X_train_seq[ti:ti + batch_size]).to(DEVICE)
            _, tz, tgamma = model(tb)
            all_z.append(tz.cpu())
            all_gamma.append(tgamma.cpu())
    z_full = torch.cat(all_z, dim=0).to(DEVICE)
    gamma_full = torch.cat(all_gamma, dim=0).to(DEVICE)
    phi, mu, sigma = model.compute_gmm_params(z_full, gamma_full)
    model.phi.data = phi.detach()
    model.mu.data = mu.detach()
    model.sigma.data = sigma.detach()

    # Compute training sample energies for threshold
    with torch.no_grad():
        E_full = model.sample_energy(z_full, model.phi, model.mu, model.sigma).cpu().numpy()
    threshold = float(np.mean(E_full) + 2 * np.std(E_full))

    _empty_gpu_cache()
    return model, threshold


def score_dagmm(model, X_seq, train_E_mean, train_E_std, train_E_max, batch_size=BATCH_SIZE):
    """Anomaly score in [0, 1] from DAGMM sample energy.

    A NaN or infinite energy takes `train_E_max`, the largest finite energy over the train NORMAL sequences (Review 4
    CAM-5, decision 26). R1 used the largest finite energy of the set being scored.
    """
    model.eval()
    all_E = []
    with torch.no_grad():
        for i in range(0, len(X_seq), batch_size):
            xb = torch.FloatTensor(X_seq[i:i + batch_size]).to(DEVICE)
            _, z, _ = model(xb)
            E = model.sample_energy(z, model.phi, model.mu, model.sigma).cpu().numpy()
            all_E.append(E)
    E = np.concatenate(all_E)
    E = np.where(np.isfinite(E), E, train_E_max)
    z_scores = (E - train_E_mean) / (train_E_std + 1e-8)
    scores = sigmoid(z_scores)
    _empty_gpu_cache()
    return scores


def get_training_energy_stats_dagmm(model, X_train_seq, batch_size=BATCH_SIZE):
    """Mean, std and maximum of the finite training DAGMM sample energies for score normalization."""
    model.eval()
    all_E = []
    with torch.no_grad():
        for i in range(0, len(X_train_seq), batch_size):
            xb = torch.FloatTensor(X_train_seq[i:i + batch_size]).to(DEVICE)
            _, z, _ = model(xb)
            E = model.sample_energy(z, model.phi, model.mu, model.sigma).cpu().numpy()
            all_E.append(E)
    E = np.concatenate(all_E)
    finite_E = E[np.isfinite(E)]
    if len(finite_E) == 0:
        return 0.0, 1.0, float("inf")
    return float(np.mean(finite_E)), float(np.std(finite_E) + 1e-8), float(np.max(finite_E))


# ---------------------------------------------------------------------------
# Threshold Selection
# ---------------------------------------------------------------------------

def find_optimal_threshold(scores, y_true):
    """F1-optimal threshold over the observed calibration scores (Review 4 MIN-8).

    Every distinct observed score is a candidate, so the search always spans the scores, whatever their range. A
    window is flagged when its score is at or above the threshold. The lowest threshold with the highest F1 is kept,
    as R1's ascending grid did. R1 searched the fixed grid 0.01 to 0.99, which sits below almost every RuleBased score
    (max |z|), so its RuleBased F1 was the always-fire F1. With no positive window F1 is undefined, and the threshold
    is +inf, which flags nothing.
    """
    scores = np.asarray(scores, dtype=float)
    y = np.asarray(y_true).astype(int)
    if len(scores) == 0 or len(y) == 0:
        return 0.5
    if y.sum() == 0:
        return float("inf")
    ok = ~np.isnan(scores)
    cand = np.unique(scores[ok])
    pos = np.sort(scores[ok & (y == 1)])
    neg = np.sort(scores[ok & (y == 0)])
    tp = len(pos) - np.searchsorted(pos, cand, side="left")
    fp = len(neg) - np.searchsorted(neg, cand, side="left")
    fn = int(y.sum()) - tp
    f1 = 2.0 * tp / np.maximum(2.0 * tp + fp + fn, 1.0)
    return float(cand[int(np.argmax(f1))])


# ---------------------------------------------------------------------------
# Optuna Objectives (pooled AUC-ROC on hpo_val, protocol 6.1.1)
# ---------------------------------------------------------------------------

def auc_or_zero(y_true, scores):
    """Pooled AUC-ROC, or 0.0 when it is undefined (one class) or scores are not finite."""
    y_true = np.asarray(y_true)
    scores = np.asarray(scores, dtype=float)
    if len(y_true) == 0 or y_true.min() == y_true.max() or not np.isfinite(scores).all():
        return 0.0
    return float(roc_auc_score(y_true, scores))


def optuna_objective_if(trial, X_normal, X_val, y_val):
    n_estimators = trial.suggest_int("n_estimators", 50, 500, step=50)
    contamination = trial.suggest_float("contamination", 0.01, 0.15)
    max_features = trial.suggest_float("max_features", 0.5, 1.0)
    max_samples = trial.suggest_float("max_samples", 0.5, 1.0)
    model = train_isolation_forest(X_normal, n_estimators, contamination,
                                   max_features, max_samples)
    scaler = fit_score_scaler(-model.decision_function(X_normal))
    return auc_or_zero(y_val, score_isolation_forest(model, X_val, scaler))


def optuna_objective_ocsvm(trial, X_normal, X_val, y_val):
    kernel = trial.suggest_categorical("kernel", ["rbf", "poly", "sigmoid"])
    nu = trial.suggest_float("nu", 0.01, 0.2)
    gamma = trial.suggest_categorical("gamma", ["scale", "auto"])
    model = train_ocsvm(X_normal, nu=nu, kernel=kernel, gamma=gamma)
    scaler = fit_score_scaler(-model.decision_function(X_normal))
    return auc_or_zero(y_val, score_ocsvm(model, X_val, scaler))


def optuna_objective_deep(trial, model_name, n_features, X_normal, X_val_normal, X_val, y_val):
    """Unified objective for the deep models. Early stopping sees hpo_val NORMAL windows only."""
    seq_length = trial.suggest_int("seq_length", SEQ_LENGTH_MIN, MAX_SEQ_LENGTH)
    lr = trial.suggest_float("lr", 1e-4, 1e-2, log=True)
    dropout = trial.suggest_float("dropout", 0.0, 0.5)
    if model_name == "DeepSVDD":
        epochs = None
    else:
        epochs = trial.suggest_int("epochs", 30, MAX_EPOCHS, step=10)

    X_normal_seq, _ = build_sequences(X_normal, seq_length, "normal")
    X_stop_seq, _ = build_sequences(X_val_normal, seq_length, "hpo_val_normal")
    X_val_seq, _val_idx = build_sequences(X_val, seq_length, "hpo_val")
    y_val_aligned = y_val[_val_idx]

    if len(X_normal_seq) < 10 or len(X_val_seq) < 5 or y_val_aligned.sum() == 0 \
            or y_val_aligned.sum() == len(y_val_aligned):
        return 0.0

    try:
        if model_name == "LSTM_AE":
            hidden_dim = trial.suggest_categorical("hidden_dim", [32, 64, 128])
            n_layers = trial.suggest_int("n_layers", 1, 2)
            model = LSTMAutoencoder(n_features, hidden_dim, n_layers, dropout)
            model, _ = train_autoencoder(model, X_normal_seq, X_stop_seq, epochs=epochs, lr=lr)
            mse_mean, mse_std = get_training_mse_stats(model, X_normal_seq)
            val_scores = score_autoencoder(model, X_val_seq, mse_mean, mse_std)
        elif model_name == "Trans_AE":
            d_model = trial.suggest_categorical("d_model", [32, 64, 128])
            nhead = trial.suggest_categorical("nhead", [2, 4, 8])
            if d_model % nhead != 0:
                return 0.0
            n_layers = trial.suggest_int("n_layers", 1, 3)
            model = TransformerAutoencoder(n_features, d_model, nhead, n_layers, dropout, seq_length)
            model, _ = train_autoencoder(model, X_normal_seq, X_stop_seq, epochs=epochs, lr=lr)
            mse_mean, mse_std = get_training_mse_stats(model, X_normal_seq)
            val_scores = score_autoencoder(model, X_val_seq, mse_mean, mse_std)
        elif model_name == "CNN_AE":
            filters = trial.suggest_categorical("filters", [16, 32, 64, 128])
            kernel_size = trial.suggest_categorical("kernel_size", [3, 5, 7])
            n_layers = trial.suggest_int("n_layers", 2, 4)
            model = CNN1DAutoencoder(n_features, filters, kernel_size, n_layers, dropout)
            model, _ = train_autoencoder(model, X_normal_seq, X_stop_seq, epochs=epochs, lr=lr)
            mse_mean, mse_std = get_training_mse_stats(model, X_normal_seq)
            val_scores = score_autoencoder(model, X_val_seq, mse_mean, mse_std)
        elif model_name == "LSTM_VAE":
            hidden_dim = trial.suggest_categorical("hidden_dim", [32, 64, 128])
            n_layers = trial.suggest_int("n_layers", 1, 2)
            latent_dim = trial.suggest_categorical("latent_dim", [16, 32, 64])
            beta = trial.suggest_float("beta", 0.01, 0.5, log=True)
            model = LSTMVariationalAutoencoder(n_features, hidden_dim, n_layers, latent_dim, dropout)
            model, _ = train_vae(model, X_normal_seq, X_stop_seq, epochs=epochs, lr=lr, beta=beta)
            mse_mean, mse_std = get_training_mse_stats_vae(model, X_normal_seq)
            val_scores = score_vae(model, X_val_seq, mse_mean, mse_std)
        elif model_name == "DeepSVDD":
            hidden_dim = trial.suggest_categorical("hidden_dim", [32, 64, 128])
            n_layers = trial.suggest_int("n_layers", 1, 2)
            latent_dim = trial.suggest_categorical("latent_dim", [8, 16, 32])
            pretrain_epochs = trial.suggest_int("pretrain_epochs", 20, 60, step=10)
            finetune_epochs = trial.suggest_int("finetune_epochs", 20, 60, step=10)
            model = DeepSVDD(n_features, hidden_dim=hidden_dim, n_layers=n_layers,
                             latent_dim=latent_dim, dropout=dropout)
            model, _ = train_deep_svdd(model, X_normal_seq, X_stop_seq,
                                       pretrain_epochs=pretrain_epochs,
                                       finetune_epochs=finetune_epochs, lr=lr)
            d_mean, d_std, degenerate = get_training_dist_stats_svdd(model, X_normal_seq)
            val_scores = score_deep_svdd(model, X_val_seq, d_mean, d_std, degenerate)
        elif model_name == "DAGMM":
            hidden_dim = trial.suggest_categorical("hidden_dim", [32, 64, 128])
            n_layers = trial.suggest_int("n_layers", 1, 2)
            latent_dim = trial.suggest_categorical("latent_dim", [4, 8, 16])
            n_gmm = trial.suggest_categorical("n_gmm", [2, 4, 8])
            lambda_energy = trial.suggest_float("lambda_energy", 0.05, 0.5, log=True)
            lambda_cov_diag = trial.suggest_float("lambda_cov_diag", 1e-3, 1e-2, log=True)
            model = DAGMM(n_features, hidden_dim=hidden_dim, n_layers=n_layers,
                          latent_dim=latent_dim, n_gmm=n_gmm, dropout=dropout)
            model, _ = train_dagmm(model, X_normal_seq, X_stop_seq, epochs=epochs, lr=lr,
                                   lambda_energy=lambda_energy, lambda_cov_diag=lambda_cov_diag)
            e_mean, e_std, e_max = get_training_energy_stats_dagmm(model, X_normal_seq)
            val_scores = score_dagmm(model, X_val_seq, e_mean, e_std, e_max)
        else:
            return 0.0
    except Exception as e:
        print(f"      Trial failed: {e}")
        return 0.0
    finally:
        if "model" in locals():
            del model
        _empty_gpu_cache()

    return auc_or_zero(y_val_aligned, val_scores)


def optuna_objective_rf(trial, X_train, y_train, X_val, y_val):
    n_estimators = trial.suggest_int("n_estimators", 50, 500, step=50)
    max_depth = trial.suggest_int("max_depth", 5, 30)
    min_samples_leaf = trial.suggest_int("min_samples_leaf", 1, 20)
    max_features = trial.suggest_categorical("max_features", ["sqrt", "log2"])
    model = train_random_forest(X_train, y_train, n_estimators=n_estimators,
                                max_depth=max_depth, min_samples_leaf=min_samples_leaf,
                                max_features=max_features)
    return auc_or_zero(y_val, score_random_forest(model, X_val))


def study_identity(model_name, fold_id, campaign_id, category_label, args, cfg):
    """Everything that makes two searches different analyses (Review 4 MIN-7). Stored on the study, checked on resume."""
    return {
        "campaign_id": campaign_id, "model": model_name, "fold": int(fold_id), "category": category_label,
        "window_s": int(args.window_s), "lofo_held_out": args.lofo_held_out or "none",
        "exclude_derived": bool(args.exclude_derived), "windows_sha256": args.windows_sha256,
        "folds_sha256": args.folds_sha256, "pipeline_config_sha256": cfg["pipeline_config_sha256"],
        "seq_length_range": [int(SEQ_LENGTH_MIN), int(MAX_SEQ_LENGTH)], "objective": "pooled_auc_roc_hpo_val",
    }


def study_name_for(identity, category_suffix=""):
    return (f"{identity['campaign_id']}_w{identity['window_s']}_{identity['model']}_fold{identity['fold']}"
            f"{category_suffix}_lofo-{identity['lofo_held_out']}_derived-{'off' if identity['exclude_derived'] else 'on'}"
            f"_win-{identity['windows_sha256'][:12]}")


def run_optuna_study(model_name, objective_fn, n_trials, n_workers,
                     storage_dir, fold_id, campaign_id, category_suffix="", identity=None):
    """Run or resume an Optuna study with SQLite persistence. The sampler seed is SAMPLER_SEED.

    The study name carries the window size, the leave-one-fault-out fault, the derived-feature switch and the windows
    file hash, and the study stores its full identity as user attributes. A stored study whose identity differs from
    this run is refused, never resumed (Review 4 MIN-7).
    """
    if identity is None:
        raise SystemExit("ERROR: run_optuna_study needs the study identity (Review 4 MIN-7)")
    study_name = study_name_for(identity, category_suffix)
    db_path = storage_dir / f"{campaign_id}_w{identity['window_s']}_{model_name}{category_suffix}.db"
    storage = f"sqlite:///{db_path}"
    existed = study_name in optuna.study.get_all_study_names(storage=storage)

    study = optuna.create_study(
        storage=storage, study_name=study_name,
        direction="maximize", load_if_exists=True,
        sampler=optuna.samplers.TPESampler(seed=SAMPLER_SEED),
    )
    if existed:
        stored = {k: study.user_attrs.get(k) for k in identity}
        diff = sorted(k for k in identity if stored[k] != identity[k])
        if diff:
            raise SystemExit(f"ERROR: refusing to resume Optuna study {study_name} in {db_path}: its recorded "
                             f"{diff} differ from this run. Use a new --output.")
    else:
        for k, v in identity.items():
            study.set_user_attr(k, v)
    completed = len([t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE])
    remaining = max(0, n_trials - completed)
    if remaining > 0:
        print(f"    Optuna: {completed} done, running {remaining} more ({n_workers} workers)")
        study.optimize(objective_fn, n_trials=remaining, n_jobs=n_workers)
    else:
        print(f"    Optuna: all {n_trials} trials already complete, resuming")
    if len(study.trials) == 0:
        return {"best_val_auc": 0.0, "n_trials": 0, "study_name": study_name}
    best = study.best_params.copy()
    best["best_val_auc"] = study.best_value
    best["n_trials"] = len(study.trials)
    best["study_name"] = study_name
    print(f"    Optuna best: AUC={study.best_value:.4f} params={study.best_params}")
    return best


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

def evaluate_predictions(y_true, y_pred, scores=None, model_name="", category="all"):
    precision, recall, f1, _ = precision_recall_fscore_support(
        y_true, y_pred, average="binary", zero_division=0)
    p = float(np.mean(y_true)) if len(y_true) else 0.0
    return {
        "model": model_name,
        "category": category,
        "precision": round(float(precision), 4),
        "recall": round(float(recall), 4),
        "f1_score": round(float(f1), 4),
        "auc_roc": round(auc_or_zero(y_true, scores if scores is not None else y_pred), 4),
        "pr_auc": round(float(average_precision_score(y_true, scores)) if y_true.sum() else 0.0, 4),
        "prevalence": round(p, 4),
        "always_fire_f1": round(2 * p / (1 + p), 4) if p > 0 else 0.0,
        "accuracy": round(float(accuracy_score(y_true, y_pred)), 4),
        "true_anomalies": int(y_true.sum()),
        "predicted_anomalies": int(y_pred.sum()),
        "total_samples": len(y_true),
    }


def evaluate_per_fault_type(df_eval, y_pred, model_name, category="all"):
    """Per-fault recall on that fault's ANOMALOUS windows of the test block."""
    results = []
    for rt in FAULTS:
        mask = ((df_eval["run_type"] == rt) & (df_eval["label"] == "ANOMALOUS")).to_numpy()
        if mask.sum() == 0:
            continue
        results.append({
            "model": model_name, "category": category, "run_type": rt,
            "recall": round(float(y_pred[mask].mean()), 4),
            "n_anomalous": int(mask.sum()),
        })
    return results


def evaluate_detection_latency(df_eval, y_pred, model_name, window_seconds=30, category="all"):
    """Per attempt: time from the verified onset (the first INJECTION window starts there) to the
    first alert, censored at the end of the labelled span."""
    results = []
    anom = df_eval["label"].to_numpy() == "ANOMALOUS"
    if anom.sum() == 0:
        return results
    sub = df_eval[anom].assign(y_pred=y_pred[anom])
    for key, ep in sub.groupby("attempt_key", sort=True):
        ep = ep.sort_values("start_at")
        hits = ep["y_pred"].to_numpy() > 0
        detected = bool(hits.any())
        first = int(np.argmax(hits)) if detected else -1
        results.append({
            "model": model_name, "category": category, "attempt_key": key,
            "run_type": ep["run_type"].iloc[0], "n_windows": len(ep),
            "detected": detected, "censored": not detected,
            "first_detection_idx": first,
            "latency_seconds": first * window_seconds if detected else len(ep) * window_seconds,
        })
    return results


def append_csv(filepath, rows):
    if not rows:
        return
    df = pd.DataFrame(rows)
    if filepath.exists():
        df.to_csv(filepath, mode="a", header=False, index=False)
    else:
        df.to_csv(filepath, index=False)


# ---------------------------------------------------------------------------
# Data Loading & Preparation
# ---------------------------------------------------------------------------

def _check_sidecar(path: Path):
    side = path.parent / (path.name + ".sha256")
    if not side.is_file():
        raise SystemExit(f"ERROR: {side} is missing; outputs must carry their hash (11.9)")
    if side.read_text().strip() != sha256_file(path):
        raise SystemExit(f"ERROR: {path} does not match {side}")


def load_inputs(windows_path, folds_path, pipeline_cfg=None):
    windows_path, folds_path = Path(windows_path), Path(folds_path)
    _check_sidecar(windows_path)
    _check_sidecar(folds_path)
    df = pd.read_parquet(windows_path)
    folds = pd.read_parquet(folds_path)
    required = {"window_id", "campaign_id", "window_s", "start_at", "label", "segment_type",
                "attempt_key", "block", "run_type", "seq_group", "seq_index", "meta__telemetry_ok", "within_run"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"ERROR: windows file lacks {sorted(missing)}")
    if df["campaign_id"].nunique() != 1 or df["window_s"].nunique() != 1:
        raise SystemExit("ERROR: a windows file must hold one campaign and one window size")
    bad = set(df["label"].unique()) - {"NORMAL", "ANOMALOUS", "RECOVERY", "SHAM", "SHAM_POST",
                                       "TRAILING", "EXCLUDED"}
    if bad:
        raise SystemExit(f"ERROR: unknown labels {sorted(bad)}")
    df = df.sort_values("start_at", kind="stable").reset_index(drop=True)
    # Review 4 MIN-9: every model uses the same within-run negatives, tagged by the preprocessing. Recompute the tags
    # from the configuration this run uses and refuse a file that disagrees.
    pcfg = pipeline_cfg or load_pipeline_config()
    expected = pd.Series(within_run_tags(df, pcfg), dtype=object)
    got = df["within_run"].astype(object).where(df["within_run"].notna(), None)
    if not (expected.fillna("-").to_numpy() == got.fillna("-").to_numpy()).all():
        raise SystemExit("ERROR: the windows file's within_run tags disagree with evaluation.within_run of "
                         "config/pipeline.json (Review 4 MIN-9)")
    print(f"Loaded: {len(df):,} windows, {df.shape[1]} cols, campaign {df['campaign_id'].iloc[0]}, "
          f"w={int(df['window_s'].iloc[0])} s")
    print(f"  Labels: {df['label'].value_counts().to_dict()}")
    return df, folds


def fold_roles(df, folds, fold_id):
    f = folds[folds["fold"] == fold_id].set_index("window_id")["role"]
    role = df["window_id"].map(f)
    if role.isna().any():
        raise SystemExit(f"ERROR: {int(role.isna().sum())} windows have no role in fold {fold_id}")
    return role


def prepare_fold_data(df, folds, fold_id, feature_cols, cfg, held_out_fault=None):
    """Masks follow protocol 3.3 on top of the fold roles of 11.9."""
    role = fold_roles(df, folds, fold_id)
    lab = df["label"]
    m = {
        "normal": (role == "train") & (lab == "NORMAL"),
        "train_rf": (role == "train") & lab.isin(["NORMAL", "ANOMALOUS"]),
        "hpo_val": (role == "hpo_val") & lab.isin(EVAL_LABELS),
        "hpo_val_normal": (role == "hpo_val") & (lab == "NORMAL"),
        "cal": (role == "cal") & lab.isin(EVAL_LABELS),
        "test": (role == "test") & lab.isin(EVAL_LABELS),
        "trailing": (role == "eval_only") & (lab == "TRAILING"),
    }
    if held_out_fault is not None:
        # Exploratory leave-one-fault-out. Whole attempts of the held-out fault leave every fit
        # set (3.4.1). The test set keeps the held-out fault's attempts and the shams, whole.
        held = df["run_type"] == held_out_fault
        for k in ("normal", "train_rf", "hpo_val", "hpo_val_normal", "cal"):
            m[k] = m[k] & ~held
        m["test"] = m["test"] & (held | (df["run_type"] == "F0"))
    for k in ("normal", "train_rf"):
        if lab[m[k]].isin(FIT_FORBIDDEN).any():
            raise SystemExit(f"ERROR: a forbidden label entered the {k} fit set")
    transform = FoldTransform.from_config(cfg)
    transform.fit(df.loc[m["normal"], feature_cols])
    y_all = (lab == "ANOMALOUS").astype(int).to_numpy()
    out = {"transform": transform, "masks": m, "role": role}
    for k, mask in m.items():
        out[f"X_{k}"] = transform.transform(df.loc[mask])
        out[f"y_{k}"] = y_all[mask.to_numpy()]
        out[f"df_{k}"] = df.loc[mask].reset_index(drop=True)
    set_split_groups(**{k: (df.loc[mask, "seq_group"].to_numpy(), df.loc[mask, "seq_index"].to_numpy())
                        for k, mask in m.items()})
    print(f"  Fold {fold_id}: {len(out['X_normal'])} train NORMAL, "
          f"{len(out['X_train_rf'])} RF train ({out['y_train_rf'].sum()} anom), "
          f"{len(out['X_hpo_val'])} hpo_val ({out['y_hpo_val'].sum()} anom), "
          f"{len(out['X_cal'])} cal ({out['y_cal'].sum()} anom), "
          f"{len(out['X_test'])} test ({out['y_test'].sum()} anom), {len(out['X_trailing'])} trailing; "
          f"{len(transform.selected)} features after per-fold selection, "
          f"{transform.report['nan_cells_imputed_in_train_normal']} NaN cells imputed in train NORMAL, "
          f"{transform.report['protected_kept']} protected columns kept")
    return out


def validate_data(df, folds, fold_id):
    """Fails fast on fold leakage or missing anomalies (protocol 4.2, contract 11.9, Review 4 MIN-6).

    Checks every role, not only the test block: one role per attempt and per block, exactly one block each for test,
    cal and hpo_val (the one-block pilot has a test block only, README A15), BASELINE only in train, TRAILING only in
    eval_only, and ANOMALOUS windows in test, and in cal and hpo_val unless it is the pilot.
    """
    role = fold_roles(df, folds, fold_id)
    errors = []
    unknown = set(role.unique()) - {"train", "hpo_val", "cal", "test", "eval_only"}
    if unknown:
        errors.append(f"  unknown roles {sorted(unknown)}")
    att = df["attempt_key"].notna()
    seg = df["segment_type"]
    per_attempt = role[att].groupby(df.loc[att, "attempt_key"]).nunique()
    if (per_attempt > 1).any():
        errors.append(f"  attempts with windows in more than one role: {sorted(per_attempt.index[per_attempt > 1])[:3]}")
    per_block = role[att].groupby(df.loc[att, "block"]).nunique()
    if (per_block > 1).any():
        errors.append(f"  blocks with windows in more than one role: {sorted(per_block.index[per_block > 1].tolist())[:3]}")
    n_blocks = df.loc[att, "block"].nunique()
    for r in ("test", "cal", "hpo_val"):
        got = sorted(df.loc[att & (role == r), "block"].unique().tolist())
        want = 1 if (r == "test" or n_blocks > 1) else 0
        if len(got) != want:
            errors.append(f"  role {r} holds {len(got)} blocks {got[:4]}, expected {want}")
    if (seg.eq("BASELINE") & role.ne("train")).any():
        errors.append("  BASELINE windows outside the train role")
    if (seg.eq("TRAILING") & role.ne("eval_only")).any():
        errors.append("  TRAILING windows outside the eval_only role")
    if (df.loc[role == "eval_only", "label"] != "TRAILING").any():
        errors.append("  eval_only holds windows that are not TRAILING")
    if n_blocks > 1 and ((role == "hpo_val") & (df["label"] == "ANOMALOUS")).sum() == 0:
        errors.append("  no ANOMALOUS windows in hpo_val")
    if n_blocks > 1 and ((role == "cal") & (df["label"] == "ANOMALOUS")).sum() == 0:
        errors.append("  no ANOMALOUS windows in cal")
    if ((role == "test") & (df["label"] == "ANOMALOUS")).sum() == 0:
        errors.append("  no ANOMALOUS windows in test")
    if errors:
        print(f"DATA VALIDATION ERRORS (fold {fold_id}):")
        for e in errors:
            print(e)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Main Training Logic
# ---------------------------------------------------------------------------

def seed_for_fit(campaign_index, fold_id, cfg):
    """Protocol 6.2.2: 1000 times the campaign index, plus 42, plus the fold."""
    return int(cfg["seed_campaign_multiplier"]) * int(campaign_index) + int(cfg["seed_base"]) + int(fold_id)


def train_deep_model_final(model_name, n_features, X_normal, X_stop, best_params):
    """Train the final deep model with fixed hyperparameters. Early stopping on hpo_val NORMAL."""
    hp = best_params
    seq_length = hp.get("seq_length", 10)
    lr = hp.get("lr", 1e-3)
    dropout = hp.get("dropout", 0.1)
    epochs = hp.get("epochs", MAX_EPOCHS)
    X_normal_seq, _ = build_sequences(X_normal, seq_length, "normal")
    X_stop_seq = build_sequences(X_stop, seq_length, "hpo_val_normal")[0] if len(X_stop) > 0 else None
    if model_name == "LSTM_AE":
        model = LSTMAutoencoder(n_features, hp.get("hidden_dim", 64), hp.get("n_layers", 2), dropout)
        kind = "ae"
    elif model_name == "Trans_AE":
        d_model, nhead = hp.get("d_model", 64), hp.get("nhead", 4)
        if d_model % nhead != 0:
            nhead = 2
        model = TransformerAutoencoder(n_features, d_model, nhead, hp.get("n_layers", 2), dropout, seq_length)
        kind = "ae"
    elif model_name == "CNN_AE":
        model = CNN1DAutoencoder(n_features, hp.get("filters", 64), hp.get("kernel_size", 5),
                                 hp.get("n_layers", 2), dropout)
        kind = "ae"
    elif model_name == "LSTM_VAE":
        model = LSTMVariationalAutoencoder(n_features, hp.get("hidden_dim", 64), hp.get("n_layers", 2),
                                           hp.get("latent_dim", 32), dropout)
        kind = "vae"
    elif model_name == "DeepSVDD":
        model = DeepSVDD(n_features, hidden_dim=hp.get("hidden_dim", 64), n_layers=hp.get("n_layers", 2),
                         latent_dim=hp.get("latent_dim", 32), dropout=dropout)
        kind = "dsvdd"
    elif model_name == "DAGMM":
        model = DAGMM(n_features, hidden_dim=hp.get("hidden_dim", 64), n_layers=hp.get("n_layers", 2),
                      latent_dim=hp.get("latent_dim", 8), n_gmm=hp.get("n_gmm", 4), dropout=dropout)
        kind = "dagmm"
    else:
        raise ValueError(f"Unknown deep model: {model_name}")
    if kind == "vae":
        model, _ = train_vae(model, X_normal_seq, X_stop_seq, epochs=epochs, lr=lr, beta=hp.get("beta", 0.1))
        ref = get_training_mse_stats_vae(model, X_normal_seq)
    elif kind == "ae":
        model, _ = train_autoencoder(model, X_normal_seq, X_stop_seq, epochs=epochs, lr=lr)
        ref = get_training_mse_stats(model, X_normal_seq)
    elif kind == "dsvdd":
        model, _ = train_deep_svdd(model, X_normal_seq, X_stop_seq,
                                   pretrain_epochs=hp.get("pretrain_epochs", 30),
                                   finetune_epochs=hp.get("finetune_epochs", 30), lr=lr)
        ref = get_training_dist_stats_svdd(model, X_normal_seq)  # (mean, std, degenerate on train)
    else:
        model, _ = train_dagmm(model, X_normal_seq, X_stop_seq, epochs=epochs, lr=lr,
                               lambda_energy=hp.get("lambda_energy", 0.1),
                               lambda_cov_diag=hp.get("lambda_cov_diag", 0.005))
        ref = get_training_energy_stats_dagmm(model, X_normal_seq)  # (mean, std, train maximum)
    return model, tuple(ref), seq_length, kind


def _score_deep(model, kind, X, split, seq_length, ref):
    """Scores of one split. `ref` holds only train NORMAL quantities, never anything of the scored set."""
    X_seq, ends = build_sequences(X, seq_length, split)
    if len(X_seq) == 0:
        return np.array([]), ends
    fn = {"ae": score_autoencoder, "vae": score_vae, "dsvdd": score_deep_svdd, "dagmm": score_dagmm}[kind]
    return fn(model, X_seq, *ref), ends


def run_model_on_fold(model_name, fold_id, df, folds, feature_cols, args, cfg, campaign_id, campaign_index):
    global MODEL_SEED, SAMPLER_SEED
    category_suffix, category_label = "", "all"
    if args.category:
        category_suffix, category_label = f"_{args.category}", args.category
    elif args.exclude_category:
        category_suffix, category_label = f"_no_{args.exclude_category}", f"no_{args.exclude_category}"

    seed = seed_for_fit(campaign_index, fold_id, cfg)
    MODEL_SEED = SAMPLER_SEED = seed
    set_seeds(seed)
    if HAS_TORCH:
        torch.backends.cudnn.deterministic = True
        torch.backends.cudnn.benchmark = False

    held_out_fault = getattr(args, "lofo_held_out", None)
    d = prepare_fold_data(df, folds, fold_id, feature_cols, cfg, held_out_fault)
    n_features = d["X_normal"].shape[1]
    output_dir = Path(args.output)
    storage_dir = output_dir / "optuna_dbs"
    storage_dir.mkdir(parents=True, exist_ok=True)
    t0 = time.time()

    frozen = None
    if args.frozen_params:
        frozen_all = json.loads(Path(args.frozen_params).read_text())
        if model_name not in frozen_all:
            raise SystemExit(f"ERROR: {args.frozen_params} has no configuration for {model_name}")
        frozen = dict(frozen_all[model_name])
        if model_name in DEEP_MODELS and "seq_length" in frozen and \
                not SEQ_LENGTH_MIN <= int(frozen["seq_length"]) <= MAX_SEQ_LENGTH:
            raise SystemExit(f"ERROR: frozen seq_length {frozen['seq_length']} lies outside "
                             f"{SEQ_LENGTH_MIN}..{MAX_SEQ_LENGTH} (evaluation.seq_length, decision 27)")
    identity = study_identity(model_name, fold_id, campaign_id, category_label, args, cfg)

    if frozen is not None:
        best_params = frozen
        print(f"    === {model_name} (frozen configuration, no search) ===")
    elif model_name in CLASSICAL_MODELS:
        n_trials = args.n_trials or cfg["trials"]["classical"]
        if model_name == "IF":
            obj = lambda trial: optuna_objective_if(trial, d["X_normal"], d["X_hpo_val"], d["y_hpo_val"])
        else:
            obj = lambda trial: optuna_objective_ocsvm(trial, d["X_normal"], d["X_hpo_val"], d["y_hpo_val"])
        best_params = run_optuna_study(model_name, obj, n_trials, 1, storage_dir, fold_id, campaign_id, category_suffix,
                                       identity)
    elif model_name in DEEP_MODELS:
        if not HAS_TORCH:
            print(f"    Skipping {model_name}: PyTorch not available")
            return None
        n_trials = args.n_trials or cfg["trials"]["deep"]
        obj = lambda trial: optuna_objective_deep(trial, model_name, n_features, d["X_normal"],
                                                  d["X_hpo_val_normal"], d["X_hpo_val"], d["y_hpo_val"])
        best_params = run_optuna_study(model_name, obj, n_trials, args.n_workers, storage_dir, fold_id,
                                       campaign_id, category_suffix, identity)
    elif model_name == "RF":
        n_trials = args.n_trials or cfg["trials"]["RF"]
        obj = lambda trial: optuna_objective_rf(trial, d["X_train_rf"], d["y_train_rf"], d["X_hpo_val"], d["y_hpo_val"])
        best_params = run_optuna_study(model_name, obj, n_trials, 1, storage_dir, fold_id, campaign_id, category_suffix,
                                       identity)
    elif model_name == "RuleBased":
        best_params = {}
    else:
        print(f"    Unknown model: {model_name}")
        return None
    MODEL_SEED = seed
    set_seeds(seed)

    seq_length = None
    ends = {}
    if model_name == "IF":
        model = train_isolation_forest(d["X_normal"], n_estimators=best_params.get("n_estimators", 200),
                                       contamination=best_params.get("contamination", 0.05),
                                       max_features=best_params.get("max_features", 1.0),
                                       max_samples=best_params.get("max_samples", 1.0))
        scaler = fit_score_scaler(-model.decision_function(d["X_normal"]))  # once per fold, train NORMAL (CAM-5)
        score = lambda X: score_isolation_forest(model, X, scaler)
    elif model_name == "OC_SVM":
        model = train_ocsvm(d["X_normal"], nu=best_params.get("nu", 0.05),
                            kernel=best_params.get("kernel", "rbf"), gamma=best_params.get("gamma", "scale"))
        scaler = fit_score_scaler(-model.decision_function(d["X_normal"]))  # once per fold, train NORMAL (CAM-5)
        score = lambda X: score_ocsvm(model, X, scaler)
    elif model_name in DEEP_MODELS:
        model, ref, seq_length, kind = train_deep_model_final(model_name, n_features, d["X_normal"],
                                                              d["X_hpo_val_normal"], best_params)
    elif model_name == "RF":
        model = train_random_forest(d["X_train_rf"], d["y_train_rf"],
                                    n_estimators=best_params.get("n_estimators", 200),
                                    max_depth=best_params.get("max_depth", None),
                                    min_samples_leaf=best_params.get("min_samples_leaf", 1),
                                    max_features=best_params.get("max_features", "sqrt"))
        score = lambda X: score_random_forest(model, X)
    else:
        rb_mean, rb_std = train_rule_based(d["X_normal"])
        model = {"mean": rb_mean, "std": rb_std}
        score = lambda X: score_rule_based(X, rb_mean, rb_std)

    scored = {}
    for split in ("test", "cal", "trailing"):
        X = d[f"X_{split}"]
        if model_name in DEEP_MODELS:
            s, e = _score_deep(model, kind, X, split, seq_length, ref)
        else:
            s, e = (score(X), np.arange(len(X))) if len(X) else (np.array([]), np.arange(0))
        scored[split] = (s, e)
    test_scores, test_idx = scored["test"]
    if len(test_scores) < 1:
        print(f"    No test scores (seq_length={seq_length}), skipping")
        return None
    cal_scores, cal_idx = scored["cal"]
    y_test = d["y_test"][test_idx]
    y_cal = d["y_cal"][cal_idx] if len(cal_scores) else np.array([])
    df_test = d["df_test"].iloc[test_idx].reset_index(drop=True)
    df_cal = d["df_cal"].iloc[cal_idx].reset_index(drop=True) if len(cal_scores) else d["df_cal"].iloc[:0]

    # Review 4 MIN-9: every model must score every within-run negative of the test block (evaluation.within_run).
    wr_all = d["df_test"]["within_run"].notna().to_numpy()
    wr_expected, wr_scored = int(wr_all.sum()), int(wr_all[test_idx].sum())
    window_s = int(df["window_s"].iloc[0])
    if wr_scored != wr_expected and window_s == int(cfg["evaluation"]["anchor_window_s"]):
        raise SystemExit(f"ERROR: {model_name} scores {wr_scored} of the {wr_expected} within-run negatives of the "
                         f"test block at the {window_s} s anchor (seq_length={seq_length}). Every model must score "
                         "the same negatives (decision 27).")
    opt_threshold = find_optimal_threshold(cal_scores, y_cal) if len(cal_scores) and len(y_cal) else 0.5
    preds = (test_scores >= opt_threshold).astype(int)
    cal_normal = cal_scores[(df_cal["label"] == "NORMAL").to_numpy()] if len(cal_scores) else np.array([])
    budgets = {f"fpb_{b_}": float(np.quantile(cal_normal, 1.0 - b_)) for b_ in cfg["fp_budgets"]} \
        if len(cal_normal) else {}
    train_time = round(time.time() - t0, 2)

    result = evaluate_predictions(y_test, preds, test_scores, model_name, category_label)
    result.update({"fold": fold_id, "seed": seed, "threshold": round(opt_threshold, 4), "window_s": window_s,
                   "within_run_negatives": wr_scored, "within_run_expected": wr_expected,
                   "within_run_complete": wr_scored == wr_expected,
                   "study_name": best_params.get("study_name", "frozen"),
                   "train_time_s": train_time, "n_features": n_features,
                   "best_hyperparams": json.dumps({k: v for k, v in best_params.items()
                                                   if k not in ("best_val_auc", "n_trials", "study_name")}),
                   "hpo_objective": "pooled_auc_roc_hpo_val" if frozen is None else "frozen"})
    if seq_length:
        result["seq_length"] = seq_length
    if held_out_fault:
        result["held_out_fault"] = held_out_fault
    fault_results = evaluate_per_fault_type(df_test, preds, model_name, category_label)
    latency_results = evaluate_detection_latency(df_test, preds, model_name,
                                                 int(df["window_s"].iloc[0]), category_label)
    for r in fault_results + latency_results:
        r["fold"] = fold_id
    print(f"    {model_name} fold {fold_id}: P={result['precision']:.3f} R={result['recall']:.3f} "
          f"F1={result['f1_score']:.3f} AUC={result['auc_roc']:.3f} time={train_time}s")

    rows = []
    for split, role_name in (("test", "test"), ("cal", "cal"), ("trailing", "eval_only")):
        s, e = scored[split]
        if len(s) == 0:
            continue
        wid = d[f"df_{split}"]["window_id"].to_numpy()[e]
        wr = d[f"df_{split}"]["within_run"].astype(object).to_numpy()[e]
        rows.append(pd.DataFrame({"window_id": wid, "fold": np.int32(fold_id), "role": role_name,
                                  "model": model_name, "category": category_label, "score": s.astype(float),
                                  "within_run": pd.Series(wr, dtype=object).where(pd.notna(wr), None)}))
    score_dir = output_dir / "scores"
    score_dir.mkdir(parents=True, exist_ok=True)
    pd.concat(rows, ignore_index=True).to_parquet(
        score_dir / f"{model_name}{category_suffix}_fold{fold_id}.parquet", index=False)

    model_dir = output_dir / "models"
    model_dir.mkdir(parents=True, exist_ok=True)
    prefix = f"{model_name}{category_suffix}_fold{fold_id}"
    if model_name in DEEP_MODELS and HAS_TORCH:
        torch.save(model.state_dict(), model_dir / f"{prefix}.pt")
        (model_dir / f"{prefix}_score_ref.json").write_text(json.dumps({"kind": kind, "ref": list(ref)}))
    elif model_name in CLASSICAL_MODELS:
        with open(model_dir / f"{prefix}.pkl", "wb") as fh:
            pickle.dump({"model": model, "score_scaler": list(scaler)}, fh)
    else:
        with open(model_dir / f"{prefix}.pkl", "wb") as fh:
            pickle.dump(model, fh)
    (model_dir / f"fold_transform{category_suffix}_fold{fold_id}.json").write_text(
        json.dumps(d["transform"].to_dict(), indent=1))

    append_csv(output_dir / "model_results.csv", [result])
    append_csv(output_dir / "per_fault_type_results.csv", fault_results)
    append_csv(output_dir / "detection_latency_results.csv", latency_results)
    append_csv(output_dir / "optuna_results.csv", [{"model": model_name, "fold": fold_id, "seed": seed,
                                                    "category": category_label, **best_params}])
    from sklearn.metrics import confusion_matrix as sk_confusion_matrix
    tn, fp, fn, tp = sk_confusion_matrix(y_test, preds, labels=[0, 1]).ravel()
    append_csv(output_dir / "confusion_matrices.csv", [{"model": model_name, "fold": fold_id,
                                                        "category": category_label, "threshold_type": "f1_optimal_cal",
                                                        "tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp)}])
    trail_scores = scored["trailing"][0]
    for name, thr in budgets.items():
        p_ = (test_scores >= thr).astype(int)
        r_ = evaluate_predictions(y_test, p_, test_scores, model_name, category_label)
        r_.update({"fold": fold_id, "threshold": round(thr, 6), "threshold_type": name,
                   "trailing_fpr": round(float((trail_scores >= thr).mean()), 4) if len(trail_scores) else None})
        append_csv(output_dir / "model_results_unsupervised.csv", [r_])
    if model_name == "RF" and hasattr(model, "feature_importances_"):
        imp = model.feature_importances_
        sel = d["transform"].selected
        top = np.argsort(imp)[-50:][::-1]
        append_csv(output_dir / "rf_feature_importance.csv",
                   [{"fold": fold_id, "rank": r + 1, "feature": sel[i], "importance": float(imp[i])}
                    for r, i in enumerate(top)])
    return {"result": result, "fault_results": fault_results, "latency_results": latency_results}


def load_training_config(path, pipeline_config=None):
    """training/config.json plus the fold_fit and evaluation sections of config/pipeline.json.

    Feature selection (Review 4 CAM-4) and the evaluation sets (MIN-9) are defined once, in config/pipeline.json, for
    the preprocessing and for training. A training config may still name variance_threshold or correlation_threshold,
    but only with the value config/pipeline.json gives.
    """
    cfg = dict(json.loads(Path(path or DEFAULT_CONFIG).read_text()))
    pcfg = pipeline_config if isinstance(pipeline_config, dict) else load_pipeline_config(pipeline_config)
    ff = pcfg["fold_fit"]
    for k in ("variance_threshold", "correlation_threshold"):
        if k in cfg and float(cfg[k]) != float(ff[k]):
            raise SystemExit(f"ERROR: the training config sets {k}={cfg[k]}, but fold_fit.{k} in config/pipeline.json "
                             f"is {ff[k]}. The value lives in config/pipeline.json.")
    cfg["fold_fit"] = ff
    cfg["evaluation"] = pcfg["evaluation"]
    cfg["pipeline_config_sha256"] = config_sha256(pcfg)
    cfg["_pipeline"] = pcfg
    return cfg


def run_training(args):
    global MAX_SEQ_LENGTH, MAX_EPOCHS, SEQ_LENGTH_MIN
    cfg = load_training_config(args.config, args.pipeline_config)
    MAX_EPOCHS = int(cfg["max_epochs"])
    if MAX_EPOCHS < 30:
        raise SystemExit("ERROR: max_epochs must be at least 30, the lower end of the R1 epoch search")
    sl = cfg["evaluation"]["seq_length"]
    SEQ_LENGTH_MIN, cfg_max = int(sl["min"]), int(sl["max"])
    if args.max_seq_length is None:
        MAX_SEQ_LENGTH = cfg_max
    elif not SEQ_LENGTH_MIN <= args.max_seq_length <= cfg_max:
        raise SystemExit(f"ERROR: --max-seq-length {args.max_seq_length} lies outside the registered range "
                         f"{SEQ_LENGTH_MIN}..{cfg_max} (evaluation.seq_length, decision 27). It may only lower the top.")
    else:
        MAX_SEQ_LENGTH = int(args.max_seq_length)
    print("=" * 70)
    print("Streaming Pipeline Anomaly Detection Training (rebuild)")
    print(f"  Model:    {args.model}")
    print(f"  Device:   {DEVICE}")
    print(f"  Windows:  {args.windows}")
    print(f"  Folds:    {args.folds}")
    print(f"  Output:   {args.output}")
    print(f"  Sequence: contiguous only, seq_length {SEQ_LENGTH_MIN}..{MAX_SEQ_LENGTH}")
    print("=" * 70)
    df, folds = load_inputs(args.windows, args.folds, cfg["_pipeline"])
    args.window_s = int(df["window_s"].iloc[0])
    args.windows_sha256 = sha256_file(Path(args.windows))
    args.folds_sha256 = sha256_file(Path(args.folds))
    campaign_id = df["campaign_id"].iloc[0]
    _wl, campaign_index = campaign_index_from_id(campaign_id)
    if campaign_index not in (0, int(cfg["search_campaign_index"])):
        if not args.freeze_marker or not Path(args.freeze_marker).is_file():
            raise SystemExit(f"ERROR: {campaign_id} is a replicate campaign. It is not read before the "
                             "freeze tag exists (6.1, 7.1). Pass --freeze-marker.")
        if not args.frozen_params:
            raise SystemExit(f"ERROR: {campaign_id} is a replicate campaign. It uses the frozen "
                             "configuration with no search (6.1.3). Pass --frozen-params.")
    if df.loc[df["attempt_key"].notna(), "block"].nunique() == 1 and args.model != "RuleBased" and not args.frozen_params:
        raise SystemExit(f"ERROR: {campaign_id} is a one-block pilot with no hpo_val block to search on (README A15). "
                         "Pass --frozen-params.")
    feature_cols = feature_columns(df, args.category, args.exclude_category, args.exclude_derived)
    print(f"  Features before per-fold selection: {len(feature_cols)}")
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    n_folds = int(folds["fold"].max())
    fold_list = [args.fold] if args.fold else list(range(1, n_folds + 1))
    all_results = []
    for fold_id in fold_list:
        print(f"\n{'=' * 70}\nFOLD {fold_id} / {n_folds}\n{'=' * 70}")
        validate_data(df, folds, fold_id)
        r = run_model_on_fold(args.model, fold_id, df, folds, feature_cols, args, cfg, campaign_id, campaign_index)
        if r:
            all_results.append(r)
    if all_results:
        aucs = [r["result"]["auc_roc"] for r in all_results]
        summary = {"model": args.model, "campaign_id": campaign_id, "n_folds": len(all_results),
                   "device": DEVICE, "mean_auc": round(float(np.mean(aucs)), 4),
                   "std_auc": round(float(np.std(aucs)), 4), "per_fold": [r["result"] for r in all_results]}
        suffix = f"_lofo_{args.lofo_held_out}" if args.lofo_held_out else ""
        with open(output_dir / f"summary_{args.model}{suffix}.json", "w") as fh:
            json.dump(summary, fh, indent=2, default=str)
        print(f"\nSUMMARY {args.model}: AUC {np.mean(aucs):.4f} +/- {np.std(aucs):.4f}")
    print(f"\nResults saved to: {output_dir}")
    return all_results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser():
    p = argparse.ArgumentParser(description="ML training for the rebuilt streaming benchmark")
    p.add_argument("--model", required=True, choices=ALL_MODELS)
    p.add_argument("--windows", required=True, help="windows_<w>s.parquet from the preprocessing")
    p.add_argument("--folds", required=True, help="folds.parquet from the preprocessing")
    p.add_argument("--config", default=None, help="training config JSON (default training/config.json)")
    p.add_argument("--pipeline-config", default=None,
                   help="JSON overrides for config/pipeline.json, as for streamprep build --config")
    p.add_argument("--output", default="results")
    p.add_argument("--fold", type=int, choices=list(range(1, 11)))
    p.add_argument("--n-workers", type=int, default=1)
    p.add_argument("--n-trials", type=int, default=None)
    p.add_argument("--category", choices=CATEGORIES)
    p.add_argument("--exclude-category", choices=CATEGORIES)
    p.add_argument("--exclude-derived", action="store_true")
    p.add_argument("--lofo-held-out", choices=FAULTS)
    p.add_argument("--max-seq-length", type=int, default=None,
                   help="lower the top of the seq_length search; default and ceiling evaluation.seq_length.max")
    p.add_argument("--frozen-params", default=None, help="JSON of model -> frozen hyperparameters (6.1.2)")
    p.add_argument("--freeze-marker", default=None, help="file that exists once the freeze tag exists")
    return p


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.category and args.exclude_category:
        parser.error("Cannot use both --category and --exclude-category")
    return run_training(args)


if __name__ == "__main__":
    main()
