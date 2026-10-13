#!/usr/bin/env python3
"""
ML training for streaming pipeline anomaly detection.

Semi-supervised models (trained on NORMAL only):
    IF, OC_SVM, LSTM_AE, Trans_AE, CNN_AE, LSTM_VAE, DeepSVDD, DAGMM

Baselines:
    RF (supervised), RuleBased (max z-score threshold)

Features:
    - Optuna HPO with SQLite persistence (resume-safe)
    - Per-fold median NaN imputation from NORMAL training data
    - SEQ_LENGTH tuned by Optuna [5, 20] for deep models
    - Per-model CLI for multi-instance parallelism
    - Category filtering for the RQ3 category ablation

Usage:
    python training.py --model IF --data data/features/features_30s.parquet --output results/
    python training.py --model LSTM_AE --n-workers 4 --data ... --output ...
    python training.py --model IF --category broker --data ... --output ...
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
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    f1_score,
    precision_recall_fscore_support,
    roc_auc_score,
)
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.svm import OneClassSVM

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

METADATA_COLS = {
    "window_start", "window_end", "window_mid", "n_samples",
    "label", "fault_id", "fault_category", "run_number",
    "repetition", "run_source",
} | {f"fold_{i}_split" for i in range(1, 11)}

CATEGORIES = ["broker", "consumer", "flink", "jvm"]
DEEP_MODELS = {"LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE", "DeepSVDD", "DAGMM"}
CLASSICAL_MODELS = {"IF", "OC_SVM"}
ALL_MODELS = [
    "IF", "OC_SVM",
    "LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE",
    "DeepSVDD", "DAGMM",
    "RF", "RuleBased",
]

BATCH_SIZE = 256
MAX_EPOCHS = 100
DEFAULT_TRIALS_DEEP = 50
DEFAULT_TRIALS_CLASSICAL = 30

if HAS_TORCH:
    if torch.cuda.is_available():
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


def get_feature_cols(df: pd.DataFrame, category: str | None = None,
                     exclude_category: str | None = None,
                     exclude_derived: bool = False) -> list[str]:
    """Get numeric feature columns, optionally filtered by category prefix."""
    cols = [
        c for c in df.columns
        if c not in METADATA_COLS
        and df[c].dtype in ("float64", "float32", "int64", "int32")
    ]
    if category:
        cols = [c for c in cols if c.startswith(f"{category}_")]
    if exclude_category:
        cols = [c for c in cols if not c.startswith(f"{exclude_category}_")]
    if exclude_derived:
        cols = [c for c in cols if not c.startswith("derived__")]
    return cols


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
# Sequence Creation
# ---------------------------------------------------------------------------

def create_sequences(X: np.ndarray, seq_length: int) -> np.ndarray:
    """Create sliding window sequences for temporal models."""
    if len(X) < seq_length:
        return np.array([]).reshape(0, seq_length, X.shape[1])
    sequences = []
    for i in range(len(X) - seq_length + 1):
        sequences.append(X[i:i + seq_length])
    return np.array(sequences)


# --- Temporal contiguity (opt-in) ------------------------------------------
# create_sequences slides over frame order and never checks that consecutive rows
# are adjacent in time. Cooldown windows are dropped before sequencing and a fold's
# test split spans runs collected hours apart, so some sequences splice unrelated
# telemetry. Enabling CONTIGUOUS_SEQUENCES restricts sequences to temporally
# adjacent windows. Defaults preserve the original behaviour exactly.

MAX_SEQ_LENGTH = 20          # upper bound of the Optuna seq_length search
CONTIGUOUS_SEQUENCES = False  # when True, sequences never span a temporal break
SEQ_STRIDE = None             # window spacing in seconds; inferred per run
SPLIT_TIMES = {}              # split name -> window_start array for the current fold


def set_split_times(**splits):
    """Record per-split window_start arrays for the current fold."""
    SPLIT_TIMES.clear()
    for name, values in splits.items():
        SPLIT_TIMES[name] = None if values is None else \
            np.asarray(values, dtype=np.float64)


def _contiguity_breaks(t, stride):
    return np.diff(np.asarray(t, dtype=np.float64)) > stride * 2.5


def build_sequences(X: np.ndarray, seq_length: int, split: str):
    """Return (sequences, end_idx) where end_idx[k] indexes the last row of sequence k.

    With CONTIGUOUS_SEQUENCES off this is exactly create_sequences plus the implicit
    dense alignment y[seq_length - 1:], so results are unchanged.
    """
    if len(X) < seq_length:
        empty = np.array([]).reshape(0, seq_length, X.shape[1])
        return empty, np.empty(0, dtype=int)

    if not CONTIGUOUS_SEQUENCES:
        seqs = create_sequences(X, seq_length)
        return seqs, np.arange(seq_length - 1, seq_length - 1 + len(seqs))

    t = SPLIT_TIMES.get(split)
    if t is None or len(t) != len(X):
        raise RuntimeError(
            f"contiguous sequences requested but window_start for split {split!r} "
            f"is missing or misaligned ({None if t is None else len(t)} vs {len(X)})")

    stride = SEQ_STRIDE
    if stride is None:
        d = np.diff(t)
        d = d[d > 0]
        stride = float(np.median(d)) if d.size else 1.0

    prefix = np.concatenate([[0], np.cumsum(_contiguity_breaks(t, stride).astype(np.int64))])
    starts = [i for i in range(len(X) - seq_length + 1)
              if prefix[i + seq_length - 1] - prefix[i] == 0]
    if not starts:
        empty = np.array([]).reshape(0, seq_length, X.shape[1])
        return empty, np.empty(0, dtype=int)
    starts = np.asarray(starts, dtype=int)
    seqs = np.stack([X[i:i + seq_length] for i in starts])
    return seqs, starts + seq_length - 1


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
        random_state=42, n_jobs=-1,
    )
    model.fit(X_normal)
    return model


def score_isolation_forest(model, X):
    raw = model.decision_function(X)
    return MinMaxScaler().fit_transform(-raw.reshape(-1, 1)).ravel()


def train_ocsvm(X_normal, nu=0.05, kernel="rbf", gamma="scale"):
    if len(X_normal) > 10000:
        idx = np.random.RandomState(42).choice(len(X_normal), 10000, replace=False)
        X_train = X_normal[idx]
    else:
        X_train = X_normal
    model = OneClassSVM(kernel=kernel, nu=nu, gamma=gamma)
    model.fit(X_train)
    return model


def score_ocsvm(model, X):
    raw = model.decision_function(X)
    return MinMaxScaler().fit_transform(-raw.reshape(-1, 1)).ravel()


def train_random_forest(X_train, y_train, n_estimators=200, max_depth=None,
                        min_samples_leaf=1, max_features="sqrt"):
    model = RandomForestClassifier(
        n_estimators=n_estimators, max_depth=max_depth,
        min_samples_leaf=min_samples_leaf, max_features=max_features,
        random_state=42, n_jobs=-1, class_weight="balanced",
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


def score_deep_svdd(model, X_seq, train_dist_mean, train_dist_std,
                    batch_size=BATCH_SIZE):
    """Anomaly score in [0, 1] from Deep SVDD distance to hypersphere center."""
    model.eval()
    all_d = []
    all_degenerate = []
    with torch.no_grad():
        for i in range(0, len(X_seq), batch_size):
            xb = torch.FloatTensor(X_seq[i:i + batch_size]).to(DEVICE)
            z = model.encode(xb)
            d = torch.sum((z - model.center) ** 2, dim=1).cpu().numpy()
            all_d.append(d)
            # Detect degenerate constant encodings (all samples map to same z)
            z_var = torch.var(z, dim=0).mean().item() if z.size(0) > 1 else 1.0
            all_degenerate.append(np.full(len(d), z_var < 1e-6))
    dist = np.concatenate(all_d)
    degenerate_mask = np.concatenate(all_degenerate)
    z_scores = (dist - train_dist_mean) / (train_dist_std + 1e-8)
    scores = sigmoid(z_scores)
    scores[degenerate_mask] = 1.0
    _empty_gpu_cache()
    return scores


def get_training_dist_stats_svdd(model, X_train_seq, batch_size=BATCH_SIZE):
    """Mean and std of training Deep SVDD distances for score normalization."""
    model.eval()
    all_d = []
    with torch.no_grad():
        for i in range(0, len(X_train_seq), batch_size):
            xb = torch.FloatTensor(X_train_seq[i:i + batch_size]).to(DEVICE)
            z = model.encode(xb)
            d = torch.sum((z - model.center) ** 2, dim=1).cpu().numpy()
            all_d.append(d)
    dist = np.concatenate(all_d)
    _empty_gpu_cache()
    return float(np.mean(dist)), float(np.std(dist))


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


def score_dagmm(model, X_seq, train_E_mean, train_E_std, batch_size=BATCH_SIZE):
    """Anomaly score in [0, 1] from DAGMM sample energy."""
    model.eval()
    all_E = []
    with torch.no_grad():
        for i in range(0, len(X_seq), batch_size):
            xb = torch.FloatTensor(X_seq[i:i + batch_size]).to(DEVICE)
            _, z, _ = model(xb)
            E = model.sample_energy(z, model.phi, model.mu, model.sigma).cpu().numpy()
            all_E.append(E)
    E = np.concatenate(all_E)
    # Replace any NaN/Inf in energy with the largest finite energy seen
    finite_E = E[np.isfinite(E)]
    if len(finite_E) > 0:
        max_E = float(np.max(finite_E))
        E = np.where(np.isfinite(E), E, max_E)
    z_scores = (E - train_E_mean) / (train_E_std + 1e-8)
    scores = sigmoid(z_scores)
    _empty_gpu_cache()
    return scores


def get_training_energy_stats_dagmm(model, X_train_seq, batch_size=BATCH_SIZE):
    """Mean and std of training DAGMM sample energies for score normalization."""
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
        return 0.0, 1.0
    return float(np.mean(finite_E)), float(np.std(finite_E) + 1e-8)


# ---------------------------------------------------------------------------
# Threshold Selection
# ---------------------------------------------------------------------------

def find_optimal_threshold(scores, y_true):
    """Find F1-optimal threshold via grid search on validation data."""
    if len(scores) == 0 or len(y_true) == 0:
        return 0.5
    best_f1 = 0.0
    best_t = 0.5
    for t in np.linspace(0.01, 0.99, 99):
        preds = (scores >= t).astype(int)
        f1 = f1_score(y_true, preds, zero_division=0)
        if f1 > best_f1:
            best_f1 = f1
            best_t = t
    return best_t


# ---------------------------------------------------------------------------
# Optuna Objectives
# ---------------------------------------------------------------------------

def optuna_objective_if(trial, X_normal, X_val, y_val):
    n_estimators = trial.suggest_int("n_estimators", 50, 500, step=50)
    contamination = trial.suggest_float("contamination", 0.01, 0.15)
    max_features = trial.suggest_float("max_features", 0.5, 1.0)
    max_samples = trial.suggest_float("max_samples", 0.5, 1.0)
    model = train_isolation_forest(X_normal, n_estimators, contamination,
                                   max_features, max_samples)
    val_scores = score_isolation_forest(model, X_val)
    threshold = find_optimal_threshold(val_scores, y_val)
    preds = (val_scores >= threshold).astype(int)
    return f1_score(y_val, preds, zero_division=0)


def optuna_objective_ocsvm(trial, X_normal, X_val, y_val):
    kernel = trial.suggest_categorical("kernel", ["rbf", "poly", "sigmoid"])
    nu = trial.suggest_float("nu", 0.01, 0.2)
    gamma = trial.suggest_categorical("gamma", ["scale", "auto"])
    model = train_ocsvm(X_normal, nu=nu, kernel=kernel, gamma=gamma)
    val_scores = score_ocsvm(model, X_val)
    threshold = find_optimal_threshold(val_scores, y_val)
    preds = (val_scores >= threshold).astype(int)
    return f1_score(y_val, preds, zero_division=0)


def optuna_objective_deep(trial, model_name, n_features, X_normal, X_val, y_val):
    """Unified Optuna objective for all deep models. Tunes seq_length."""
    seq_length = trial.suggest_int("seq_length", 5, MAX_SEQ_LENGTH)
    lr = trial.suggest_float("lr", 1e-4, 1e-2, log=True)
    dropout = trial.suggest_float("dropout", 0.0, 0.5)
    # DeepSVDD has its own pretrain/finetune epoch knobs; other deep models share `epochs`
    if model_name == "DeepSVDD":
        epochs = None
    else:
        epochs = trial.suggest_int("epochs", 30, MAX_EPOCHS, step=10)

    X_normal_seq, _ = build_sequences(X_normal, seq_length, "normal")
    X_val_seq, _val_idx = build_sequences(X_val, seq_length, "hpo_val")
    y_val_aligned = y_val[_val_idx]

    if len(X_normal_seq) < 10 or len(X_val_seq) < 5 or y_val_aligned.sum() == 0:
        return 0.0

    try:
        if model_name == "LSTM_AE":
            hidden_dim = trial.suggest_categorical("hidden_dim", [32, 64, 128])
            n_layers = trial.suggest_int("n_layers", 1, 2)
            model = LSTMAutoencoder(n_features, hidden_dim, n_layers, dropout)
            model, _ = train_autoencoder(model, X_normal_seq, X_val_seq,
                                         epochs=epochs, lr=lr)
            mse_mean, mse_std = get_training_mse_stats(model, X_normal_seq)
            val_scores = score_autoencoder(model, X_val_seq, mse_mean, mse_std)

        elif model_name == "Trans_AE":
            d_model = trial.suggest_categorical("d_model", [32, 64, 128])
            nhead = trial.suggest_categorical("nhead", [2, 4, 8])
            if d_model % nhead != 0:
                return 0.0
            n_layers = trial.suggest_int("n_layers", 1, 3)
            model = TransformerAutoencoder(n_features, d_model, nhead,
                                           n_layers, dropout, seq_length)
            model, _ = train_autoencoder(model, X_normal_seq, X_val_seq,
                                         epochs=epochs, lr=lr)
            mse_mean, mse_std = get_training_mse_stats(model, X_normal_seq)
            val_scores = score_autoencoder(model, X_val_seq, mse_mean, mse_std)

        elif model_name == "CNN_AE":
            filters = trial.suggest_categorical("filters", [16, 32, 64, 128])
            kernel_size = trial.suggest_categorical("kernel_size", [3, 5, 7])
            n_layers = trial.suggest_int("n_layers", 2, 4)
            model = CNN1DAutoencoder(n_features, filters, kernel_size,
                                     n_layers, dropout)
            model, _ = train_autoencoder(model, X_normal_seq, X_val_seq,
                                         epochs=epochs, lr=lr)
            mse_mean, mse_std = get_training_mse_stats(model, X_normal_seq)
            val_scores = score_autoencoder(model, X_val_seq, mse_mean, mse_std)

        elif model_name == "LSTM_VAE":
            hidden_dim = trial.suggest_categorical("hidden_dim", [32, 64, 128])
            n_layers = trial.suggest_int("n_layers", 1, 2)
            latent_dim = trial.suggest_categorical("latent_dim", [16, 32, 64])
            beta = trial.suggest_float("beta", 0.01, 0.5, log=True)
            model = LSTMVariationalAutoencoder(n_features, hidden_dim,
                                                n_layers, latent_dim, dropout)
            model, _ = train_vae(model, X_normal_seq, X_val_seq,
                                  epochs=epochs, lr=lr, beta=beta)
            mse_mean, mse_std = get_training_mse_stats_vae(model, X_normal_seq)
            val_scores = score_vae(model, X_val_seq, mse_mean, mse_std)

        elif model_name == "DeepSVDD":
            hidden_dim = trial.suggest_categorical("hidden_dim", [32, 64, 128])
            n_layers = trial.suggest_int("n_layers", 1, 2)
            latent_dim = trial.suggest_categorical("latent_dim", [8, 16, 32])
            pretrain_epochs = trial.suggest_int("pretrain_epochs", 20, 60, step=10)
            finetune_epochs = trial.suggest_int("finetune_epochs", 20, 60, step=10)
            model = DeepSVDD(n_features, hidden_dim=hidden_dim,
                             n_layers=n_layers, latent_dim=latent_dim,
                             dropout=dropout)
            model, _ = train_deep_svdd(model, X_normal_seq, X_val_seq,
                                        pretrain_epochs=pretrain_epochs,
                                        finetune_epochs=finetune_epochs, lr=lr)
            d_mean, d_std = get_training_dist_stats_svdd(model, X_normal_seq)
            val_scores = score_deep_svdd(model, X_val_seq, d_mean, d_std)

        elif model_name == "DAGMM":
            hidden_dim = trial.suggest_categorical("hidden_dim", [32, 64, 128])
            n_layers = trial.suggest_int("n_layers", 1, 2)
            latent_dim = trial.suggest_categorical("latent_dim", [4, 8, 16])
            n_gmm = trial.suggest_categorical("n_gmm", [2, 4, 8])
            lambda_energy = trial.suggest_float("lambda_energy", 0.05, 0.5, log=True)
            lambda_cov_diag = trial.suggest_float("lambda_cov_diag", 1e-3, 1e-2, log=True)
            model = DAGMM(n_features, hidden_dim=hidden_dim, n_layers=n_layers,
                          latent_dim=latent_dim, n_gmm=n_gmm, dropout=dropout)
            model, _ = train_dagmm(model, X_normal_seq, X_val_seq,
                                    epochs=epochs, lr=lr,
                                    lambda_energy=lambda_energy,
                                    lambda_cov_diag=lambda_cov_diag)
            e_mean, e_std = get_training_energy_stats_dagmm(model, X_normal_seq)
            val_scores = score_dagmm(model, X_val_seq, e_mean, e_std)

        else:
            return 0.0
    except Exception as e:
        print(f"      Trial failed: {e}")
        return 0.0
    finally:
        if "model" in locals():
            del model
        _empty_gpu_cache()

    threshold = find_optimal_threshold(val_scores, y_val_aligned)
    preds = (val_scores >= threshold).astype(int)
    return f1_score(y_val_aligned, preds, zero_division=0)


def optuna_objective_rf(trial, X_train, y_train, X_val, y_val):
    n_estimators = trial.suggest_int("n_estimators", 50, 500, step=50)
    max_depth = trial.suggest_int("max_depth", 5, 30)
    min_samples_leaf = trial.suggest_int("min_samples_leaf", 1, 20)
    max_features = trial.suggest_categorical("max_features", ["sqrt", "log2"])
    model = train_random_forest(X_train, y_train, n_estimators=n_estimators,
                                max_depth=max_depth, min_samples_leaf=min_samples_leaf,
                                max_features=max_features)
    val_scores = score_random_forest(model, X_val)
    threshold = find_optimal_threshold(val_scores, y_val)
    preds = (val_scores >= threshold).astype(int)
    return f1_score(y_val, preds, zero_division=0)


def run_optuna_study(model_name, objective_fn, n_trials, n_workers,
                     storage_dir, fold_id, category_suffix=""):
    """Run or resume Optuna study with SQLite persistence."""
    study_name = f"{model_name}_fold{fold_id}{category_suffix}"
    db_path = storage_dir / f"{model_name}{category_suffix}.db"
    storage = f"sqlite:///{db_path}"

    study = optuna.create_study(
        storage=storage, study_name=study_name,
        direction="maximize", load_if_exists=True,
        sampler=optuna.samplers.TPESampler(seed=42 + fold_id),
    )

    completed = len([t for t in study.trials
                     if t.state == optuna.trial.TrialState.COMPLETE])
    remaining = max(0, n_trials - completed)

    if remaining > 0:
        print(f"    Optuna: {completed} done, running {remaining} more "
              f"({n_workers} workers)")
        study.optimize(objective_fn, n_trials=remaining, n_jobs=n_workers)
    else:
        print(f"    Optuna: all {n_trials} trials already complete, resuming")

    if len(study.trials) == 0:
        print(f"    WARNING: No completed trials for {study_name}")
        return {"best_val_f1": 0.0, "n_trials": 0, "study_name": study_name}

    best = study.best_params.copy()
    best["best_val_f1"] = study.best_value
    best["n_trials"] = len(study.trials)
    best["study_name"] = study_name
    print(f"    Optuna best: F1={study.best_value:.4f} params={study.best_params}")
    return best


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

def evaluate_predictions(y_true, y_pred, scores=None,
                         model_name="", category="all"):
    precision, recall, f1, _ = precision_recall_fscore_support(
        y_true, y_pred, average="binary", zero_division=0)
    try:
        auc = roc_auc_score(y_true, scores if scores is not None else y_pred)
    except ValueError:
        auc = 0.0
    try:
        pr_auc = average_precision_score(
            y_true, scores if scores is not None else y_pred)
    except ValueError:
        pr_auc = 0.0
    return {
        "model": model_name,
        "category": category,
        "precision": round(float(precision), 4),
        "recall": round(float(recall), 4),
        "f1_score": round(float(f1), 4),
        "auc_roc": round(float(auc), 4),
        "pr_auc": round(float(pr_auc), 4),
        "accuracy": round(float(accuracy_score(y_true, y_pred)), 4),
        "true_anomalies": int(y_true.sum()),
        "predicted_anomalies": int(y_pred.sum()),
        "total_samples": len(y_true),
    }


def evaluate_per_fault_type(df_test, y_pred, model_name, category="all"):
    """Per-fault recall: what fraction of each fault's ANOMALOUS windows detected?"""
    results = []
    for fault_id in sorted(df_test["fault_id"].unique()):
        if not fault_id or pd.isna(fault_id) or fault_id == "":
            continue
        mask = (df_test["fault_id"] == fault_id)
        y_true_ft = (df_test.loc[mask, "label"] == "ANOMALOUS").astype(int).values
        y_pred_ft = y_pred[mask.values]
        if y_true_ft.sum() == 0:
            continue
        precision, recall, f1, _ = precision_recall_fscore_support(
            y_true_ft, y_pred_ft, average="binary", zero_division=0)
        results.append({
            "model": model_name, "category": category,
            "fault_id": fault_id,
            "precision": round(float(precision), 4),
            "recall": round(float(recall), 4),
            "f1_score": round(float(f1), 4),
            "n_anomalous": int(y_true_ft.sum()),
            "n_total": int(mask.sum()),
        })
    return results


def evaluate_detection_latency(df_test, y_pred, model_name,
                               window_seconds=30, category="all"):
    """Per-episode detection latency: time from fault start to first detection."""
    results = []
    anom_mask = df_test["label"] == "ANOMALOUS"
    if anom_mask.sum() == 0:
        return results

    anom_df = df_test[anom_mask].copy()
    anom_df["y_pred"] = y_pred[anom_mask.values]

    for (fault_id, rep), episode in anom_df.groupby(["fault_id", "repetition"]):
        episode = episode.sort_values("window_mid")
        n_windows = len(episode)
        n_detected = int(episode["y_pred"].sum())
        detected = n_detected > 0

        if detected:
            first_idx = int(np.argmax(episode["y_pred"].values > 0))
            latency_seconds = first_idx * window_seconds
            censored = False
        else:
            first_idx = -1
            latency_seconds = n_windows * window_seconds  # censored: max possible
            censored = True

        results.append({
            "model": model_name, "category": category,
            "fault_id": fault_id, "repetition": int(rep),
            "n_windows": n_windows, "n_detected": n_detected,
            "detected": detected,
            "censored": censored,
            "first_detection_idx": first_idx,
            "latency_seconds": latency_seconds,
        })
    return results


def append_csv(filepath, rows):
    """Append results to CSV, creating with header if new."""
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

def load_data(data_path):
    """Load feature parquet and validate structure."""
    df = pd.read_parquet(data_path)
    print(f"Loaded: {len(df):,} rows, {df.shape[1]} cols")

    # Validate expected columns
    required = {"label", "fault_id", "repetition", "window_mid"}
    # Check for at least fold_1_split (V2: 5 folds, V3: 10 folds)
    required.add("fold_1_split")
    missing = required - set(df.columns)
    if missing:
        print(f"ERROR: Missing columns: {missing}")
        sys.exit(1)

    # Validate labels
    labels = set(df["label"].unique())
    expected_labels = {"NORMAL", "ANOMALOUS", "RECOVERY", "EXCLUDED"}
    if not labels.issubset(expected_labels):
        print(f"WARNING: Unexpected labels: {labels - expected_labels}")

    # Validate faults
    fault_ids = [f for f in df["fault_id"].unique() if f and not pd.isna(f)]
    print(f"  Labels: {df['label'].value_counts().to_dict()}")
    print(f"  Faults: {sorted(fault_ids)}")
    print(f"  Repetitions: {sorted(df['repetition'].unique())}")

    return df


def prepare_fold_data(df, fold_id, feature_cols, held_out_fault=None):
    """Split, impute, and scale data for one fold.

    V3: 3-way split, with hpo_val for HPO and early stopping and cal for thresholds.
    Falls back to V2 format (single 'val' column) for backward compatibility.

    LOFO mode (held_out_fault set): drop anomalous rows of held_out_fault from
    train/hpo_val/cal; restrict test to NORMAL/RECOVERY plus only the held-out
    fault's anomalous rows. Applied at fit time, so HPO and final training also
    never see the held-out fault.

    Returns (X_normal, X_train_all, y_train_all, X_hpo_val, y_hpo_val,
             X_cal, y_cal, X_test, y_test, df_test, df_cal,
             imputer, scaler, imputation_stats).
    """
    fold_col = f"fold_{fold_id}_split"

    train_mask = df[fold_col] == "train"
    test_mask = df[fold_col] == "test"

    # V3: separate hpo_val and cal splits
    has_3way = (df[fold_col] == "hpo_val").any()
    if has_3way:
        hpo_val_mask = df[fold_col] == "hpo_val"
        cal_mask = df[fold_col] == "cal"
    else:
        # V2 backward compat: val serves as both hpo_val and cal
        hpo_val_mask = df[fold_col] == "val"
        cal_mask = df[fold_col] == "val"

    if held_out_fault is not None:
        is_anom = df["label"] == "ANOMALOUS"
        is_held = df["fault_id"] == held_out_fault
        drop_anom_held = is_anom & is_held
        train_mask = train_mask & ~drop_anom_held
        hpo_val_mask = hpo_val_mask & ~drop_anom_held
        cal_mask = cal_mask & ~drop_anom_held
        test_mask = test_mask & (~is_anom | is_held)
        n_test_held = int((test_mask & is_anom).sum())
        print(f"  LOFO: held_out={held_out_fault} "
              f"(test anomalous={n_test_held}, "
              f"cal anomalous after drop={int((cal_mask & is_anom).sum())}, "
              f"hpo_val anomalous after drop={int((hpo_val_mask & is_anom).sum())})")

    # Binary labels: ANOMALOUS=1, NORMAL/RECOVERY=0
    y_all = (df["label"] == "ANOMALOUS").astype(int).values

    # Extract feature matrices
    X_all = df[feature_cols].values.astype(np.float32)

    X_train_raw = X_all[train_mask]
    y_train = y_all[train_mask]
    X_hpo_val_raw = X_all[hpo_val_mask]
    y_hpo_val = y_all[hpo_val_mask]
    X_cal_raw = X_all[cal_mask]
    y_cal = y_all[cal_mask]
    X_test_raw = X_all[test_mask]
    y_test = y_all[test_mask]
    df_test = df[test_mask].reset_index(drop=True)
    df_cal = df[cal_mask].reset_index(drop=True)

    # NORMAL training data (for semi-supervised models + imputation)
    normal_train_mask = train_mask & (df["label"] == "NORMAL")
    X_normal_raw = X_all[normal_train_mask]

    # NaN imputation: per-fold median from NORMAL training data
    imputer = SimpleImputer(strategy="median")
    imputer.fit(X_normal_raw)

    nan_count_before = np.isnan(X_normal_raw).sum()
    X_normal = imputer.transform(X_normal_raw)
    X_train_all = imputer.transform(X_train_raw)
    X_hpo_val = imputer.transform(X_hpo_val_raw)
    X_cal = imputer.transform(X_cal_raw)
    X_test = imputer.transform(X_test_raw)

    imputation_stats = {
        "nan_before_normal": int(nan_count_before),
        "nan_before_hpo_val": int(np.isnan(X_hpo_val_raw).sum()),
        "nan_before_cal": int(np.isnan(X_cal_raw).sum()),
        "nan_before_test": int(np.isnan(X_test_raw).sum()),
        "features_with_nan": int((np.isnan(X_normal_raw).any(axis=0)).sum()),
    }

    # Replace any remaining inf/nan after imputation
    for arr in [X_normal, X_train_all, X_hpo_val, X_cal, X_test]:
        np.nan_to_num(arr, copy=False, nan=0.0, posinf=0.0, neginf=0.0)

    # Scale: fit on NORMAL training data only
    scaler = StandardScaler()
    scaler.fit(X_normal)
    X_normal = scaler.transform(X_normal)
    X_train_all = scaler.transform(X_train_all)
    X_hpo_val = scaler.transform(X_hpo_val)
    X_cal = scaler.transform(X_cal)
    X_test = scaler.transform(X_test)

    y_train_all = y_train
    split_type = "3-way (hpo_val/cal/test)" if has_3way else "2-way (val=cal)"

    print(f"  Fold {fold_id} [{split_type}]: {len(X_normal)} normal train, "
          f"{len(X_train_all)} total train ({y_train.sum()} anom), "
          f"{len(X_hpo_val)} hpo_val ({y_hpo_val.sum()} anom), "
          f"{len(X_cal)} cal ({y_cal.sum()} anom), "
          f"{len(X_test)} test ({y_test.sum()} anom)")
    print(f"    NaN imputed: {imputation_stats['nan_before_normal']} normal, "
          f"{imputation_stats['features_with_nan']} features had NaN")

    # Record per-split window_start so build_sequences can enforce temporal contiguity.
    # Order must match the row order of the X_* arrays above.
    if "window_start" in df.columns:
        set_split_times(
            normal=df.loc[normal_train_mask, "window_start"].values,
            hpo_val=df.loc[hpo_val_mask, "window_start"].values,
            cal=df.loc[cal_mask, "window_start"].values,
            test=df.loc[test_mask, "window_start"].values,
        )

    return (X_normal, X_train_all, y_train_all, X_hpo_val, y_hpo_val,
            X_cal, y_cal, X_test, y_test, df_test, df_cal,
            imputer, scaler, imputation_stats)


# ---------------------------------------------------------------------------
# Data Integrity Guards
# ---------------------------------------------------------------------------

def validate_data(df, feature_cols, fold_id):
    """Verify data integrity before training. Fails fast on issues."""
    fold_col = f"fold_{fold_id}_split"
    errors = []

    # No EXCLUDED in train/test/val
    for split in ["train", "val", "test"]:
        mask = df[fold_col] == split
        excluded_in_split = (df.loc[mask, "label"] == "EXCLUDED").sum()
        if excluded_in_split > 0:
            errors.append(f"  {excluded_in_split} EXCLUDED windows in {split} split")

    # All 8 faults present in test set
    test_mask = df[fold_col] == "test"
    test_faults = set(df.loc[test_mask & (df["label"] == "ANOMALOUS"), "fault_id"].unique())
    expected_faults = {f"F{i}" for i in range(1, 9)}
    missing_faults = expected_faults - test_faults
    if missing_faults:
        # Not all faults in every fold's test set; this is expected with 2-rep test sets
        pass

    # Anomalous windows exist in val/hpo_val and test
    has_3way = (df[fold_col] == "hpo_val").any()
    val_col_value = "hpo_val" if has_3way else "val"
    val_mask = df[fold_col] == val_col_value
    if (df.loc[val_mask, "label"] == "ANOMALOUS").sum() == 0:
        errors.append(f"  No ANOMALOUS windows in {val_col_value} split")
    if (df.loc[test_mask, "label"] == "ANOMALOUS").sum() == 0:
        errors.append("  No ANOMALOUS windows in test split")

    # Feature count
    if len(feature_cols) == 0:
        errors.append("  No feature columns found")

    if errors:
        print(f"DATA VALIDATION ERRORS (fold {fold_id}):")
        for e in errors:
            print(e)
        sys.exit(1)

    print(f"  Data validation passed: {len(feature_cols)} features, "
          f"{len(test_faults)} faults in test")


# ---------------------------------------------------------------------------
# Main Training Logic
# ---------------------------------------------------------------------------

def train_deep_model_final(model_name, n_features, X_normal, X_val,
                           best_params):
    """Train final deep model with best Optuna hyperparameters.

    Returns (model, stat_a, stat_b, seq_length, model_kind) where:
        - For AEs (LSTM_AE/Trans_AE/CNN_AE): model_kind='ae', stats=(mse_mean,mse_std)
        - For LSTM_VAE: model_kind='vae', stats=(mse_mean, mse_std)
        - For DeepSVDD: model_kind='dsvdd', stats=(dist_mean, dist_std)
        - For DAGMM: model_kind='dagmm', stats=(energy_mean, energy_std)
    """
    hp = best_params
    seq_length = hp.get("seq_length", 10)
    lr = hp.get("lr", 1e-3)
    dropout = hp.get("dropout", 0.1)
    epochs = hp.get("epochs", MAX_EPOCHS)

    X_normal_seq, _ = build_sequences(X_normal, seq_length, "normal")
    X_val_seq = build_sequences(X_val, seq_length, "hpo_val")[0] \
        if len(X_val) > 0 else None

    if model_name == "LSTM_AE":
        model = LSTMAutoencoder(n_features, hp.get("hidden_dim", 64),
                                hp.get("n_layers", 2), dropout)
        model_kind = "ae"
    elif model_name == "Trans_AE":
        d_model = hp.get("d_model", 64)
        nhead = hp.get("nhead", 4)
        if d_model % nhead != 0:
            nhead = 2
        model = TransformerAutoencoder(n_features, d_model, nhead,
                                       hp.get("n_layers", 2), dropout, seq_length)
        model_kind = "ae"
    elif model_name == "CNN_AE":
        model = CNN1DAutoencoder(n_features, hp.get("filters", 64),
                                 hp.get("kernel_size", 5),
                                 hp.get("n_layers", 2), dropout)
        model_kind = "ae"
    elif model_name == "LSTM_VAE":
        model = LSTMVariationalAutoencoder(
            n_features, hp.get("hidden_dim", 64), hp.get("n_layers", 2),
            hp.get("latent_dim", 32), dropout)
        model_kind = "vae"
    elif model_name == "DeepSVDD":
        model = DeepSVDD(
            n_features, hidden_dim=hp.get("hidden_dim", 64),
            n_layers=hp.get("n_layers", 2),
            latent_dim=hp.get("latent_dim", 32), dropout=dropout,
        )
        model_kind = "dsvdd"
    elif model_name == "DAGMM":
        model = DAGMM(
            n_features, hidden_dim=hp.get("hidden_dim", 64),
            n_layers=hp.get("n_layers", 2),
            latent_dim=hp.get("latent_dim", 8),
            n_gmm=hp.get("n_gmm", 4), dropout=dropout,
        )
        model_kind = "dagmm"
    else:
        raise ValueError(f"Unknown deep model: {model_name}")

    if model_kind == "vae":
        model, _ = train_vae(model, X_normal_seq, X_val_seq,
                             epochs=epochs, lr=lr,
                             beta=hp.get("beta", 0.1))
        stat_a, stat_b = get_training_mse_stats_vae(model, X_normal_seq)
    elif model_kind == "ae":
        model, _ = train_autoencoder(model, X_normal_seq, X_val_seq,
                                     epochs=epochs, lr=lr)
        stat_a, stat_b = get_training_mse_stats(model, X_normal_seq)
    elif model_kind == "dsvdd":
        model, _ = train_deep_svdd(
            model, X_normal_seq, X_val_seq,
            pretrain_epochs=hp.get("pretrain_epochs", 30),
            finetune_epochs=hp.get("finetune_epochs", 30), lr=lr,
        )
        stat_a, stat_b = get_training_dist_stats_svdd(model, X_normal_seq)
    elif model_kind == "dagmm":
        model, _ = train_dagmm(
            model, X_normal_seq, X_val_seq,
            epochs=epochs, lr=lr,
            lambda_energy=hp.get("lambda_energy", 0.1),
            lambda_cov_diag=hp.get("lambda_cov_diag", 0.005),
        )
        stat_a, stat_b = get_training_energy_stats_dagmm(model, X_normal_seq)
    else:
        raise ValueError(f"Unhandled model_kind: {model_kind}")

    return model, stat_a, stat_b, seq_length, model_kind


def run_model_on_fold(model_name, fold_id, df, feature_cols, args):
    """Train and evaluate one model on one fold. Returns result dicts."""
    category_suffix = ""
    category_label = "all"
    if args.category:
        category_suffix = f"_{args.category}"
        category_label = args.category
    elif args.exclude_category:
        category_suffix = f"_no_{args.exclude_category}"
        category_label = f"no_{args.exclude_category}"

    n_features = len(feature_cols)
    set_seeds(42 + fold_id)

    # Prepare data (V3: 3-way split with separate hpo_val and cal)
    held_out_fault = getattr(args, "lofo_held_out", None)
    (X_normal, X_train_all, y_train_all, X_hpo_val, y_hpo_val,
     X_cal, y_cal, X_test, y_test, df_test, df_cal,
     imputer, scaler, imputation_stats) = prepare_fold_data(
        df, fold_id, feature_cols, held_out_fault=held_out_fault)

    # Backward compat aliases: HPO uses hpo_val, threshold uses cal
    X_val = X_hpo_val  # for Optuna HPO + early stopping
    y_val = y_hpo_val

    output_dir = Path(args.output)
    storage_dir = output_dir / "optuna_dbs"
    storage_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.time()

    # --- Optuna HPO ---
    print(f"    === {model_name} (Optuna HPO) ===")

    if model_name in CLASSICAL_MODELS:
        n_trials = args.n_trials or DEFAULT_TRIALS_CLASSICAL
        if model_name == "IF":
            obj = lambda trial: optuna_objective_if(trial, X_normal, X_val, y_val)
        else:
            obj = lambda trial: optuna_objective_ocsvm(trial, X_normal, X_val, y_val)
        best_params = run_optuna_study(model_name, obj, n_trials, 1,
                                       storage_dir, fold_id, category_suffix)

    elif model_name in DEEP_MODELS:
        if not HAS_TORCH:
            print(f"    Skipping {model_name}: PyTorch not available")
            return None
        n_trials = args.n_trials or DEFAULT_TRIALS_DEEP
        obj = lambda trial: optuna_objective_deep(
            trial, model_name, n_features, X_normal, X_val, y_val)
        best_params = run_optuna_study(model_name, obj, n_trials,
                                       args.n_workers, storage_dir,
                                       fold_id, category_suffix)

    elif model_name == "RF":
        n_trials = args.n_trials or DEFAULT_TRIALS_CLASSICAL
        obj = lambda trial: optuna_objective_rf(
            trial, X_train_all, y_train_all, X_val, y_val)
        best_params = run_optuna_study(model_name, obj, n_trials, 1,
                                       storage_dir, fold_id, category_suffix)

    elif model_name == "RuleBased":
        best_params = {}

    else:
        print(f"    Unknown model: {model_name}")
        return None

    # --- Train final model ---
    print(f"    Training final model with best params...")

    is_sequence = False
    seq_length = None

    if model_name == "IF":
        model = train_isolation_forest(
            X_normal,
            n_estimators=best_params.get("n_estimators", 200),
            contamination=best_params.get("contamination", 0.05),
            max_features=best_params.get("max_features", 1.0),
            max_samples=best_params.get("max_samples", 1.0),
        )
        test_scores = score_isolation_forest(model, X_test)
        cal_scores = score_isolation_forest(model, X_cal)

    elif model_name == "OC_SVM":
        model = train_ocsvm(
            X_normal,
            nu=best_params.get("nu", 0.05),
            kernel=best_params.get("kernel", "rbf"),
            gamma=best_params.get("gamma", "scale"),
        )
        test_scores = score_ocsvm(model, X_test)
        cal_scores = score_ocsvm(model, X_cal)

    elif model_name in DEEP_MODELS:
        model, stat_a, stat_b, seq_length, model_kind = \
            train_deep_model_final(model_name, n_features, X_normal, X_val,
                                   best_params)
        X_test_seq, seq_test_idx = build_sequences(X_test, seq_length, "test")
        X_cal_seq, seq_cal_idx = build_sequences(X_cal, seq_length, "cal")

        if len(X_test_seq) < 1:
            print(f"    No test sequences (seq_length={seq_length}), skipping")
            return None

        if model_kind == "vae":
            test_scores = score_vae(model, X_test_seq, stat_a, stat_b)
            cal_scores = score_vae(model, X_cal_seq, stat_a, stat_b) \
                if len(X_cal_seq) > 0 else np.array([])
        elif model_kind == "ae":
            test_scores = score_autoencoder(model, X_test_seq, stat_a, stat_b)
            cal_scores = score_autoencoder(model, X_cal_seq, stat_a, stat_b) \
                if len(X_cal_seq) > 0 else np.array([])
        elif model_kind == "dsvdd":
            test_scores = score_deep_svdd(model, X_test_seq, stat_a, stat_b)
            cal_scores = score_deep_svdd(model, X_cal_seq, stat_a, stat_b) \
                if len(X_cal_seq) > 0 else np.array([])
        elif model_kind == "dagmm":
            test_scores = score_dagmm(model, X_test_seq, stat_a, stat_b)
            cal_scores = score_dagmm(model, X_cal_seq, stat_a, stat_b) \
                if len(X_cal_seq) > 0 else np.array([])
        else:
            raise ValueError(f"Unhandled model_kind: {model_kind}")
        is_sequence = True

    elif model_name == "RF":
        model = train_random_forest(
            X_train_all, y_train_all,
            n_estimators=best_params.get("n_estimators", 200),
            max_depth=best_params.get("max_depth", None),
            min_samples_leaf=best_params.get("min_samples_leaf", 1),
            max_features=best_params.get("max_features", "sqrt"),
        )
        test_scores = score_random_forest(model, X_test)
        cal_scores = score_random_forest(model, X_cal)

    elif model_name == "RuleBased":
        rb_mean, rb_std = train_rule_based(X_normal)
        test_scores = score_rule_based(X_test, rb_mean, rb_std)
        cal_scores = score_rule_based(X_cal, rb_mean, rb_std)
        model = {"mean": rb_mean, "std": rb_std}

    # --- Align labels for sequence models ---
    if is_sequence and seq_length:
        y_test_eval = y_test[seq_test_idx][:len(test_scores)]
        y_cal_eval = y_cal[seq_cal_idx][:len(cal_scores)] \
            if len(cal_scores) > 0 else np.array([])
        df_test_eval = df_test.iloc[seq_test_idx[:len(test_scores)]] \
            .reset_index(drop=True)
    else:
        y_test_eval = y_test
        y_cal_eval = y_cal
        df_test_eval = df_test

    # --- Find threshold on CALIBRATION set (independent of HPO) ---
    opt_threshold = find_optimal_threshold(cal_scores, y_cal_eval) \
        if len(cal_scores) > 0 and len(y_cal_eval) > 0 else 0.5
    preds = (test_scores >= opt_threshold).astype(int)

    # --- Unsupervised thresholds (no labeled anomalies needed) ---
    normal_cal_scores = cal_scores[y_cal_eval == 0] if len(y_cal_eval) > 0 else cal_scores
    unsup_thresholds = {}
    if len(normal_cal_scores) > 0:
        unsup_thresholds["p95"] = float(np.percentile(normal_cal_scores, 95))
        unsup_thresholds["p99"] = float(np.percentile(normal_cal_scores, 99))
        q1 = np.percentile(normal_cal_scores, 25)
        q3 = np.percentile(normal_cal_scores, 75)
        unsup_thresholds["iqr"] = float(q3 + 1.5 * (q3 - q1))

    t1 = time.time()
    train_time = round(t1 - t0, 2)

    # --- Evaluate ---
    result = evaluate_predictions(y_test_eval, preds, test_scores,
                                  model_name, category_label)
    result["fold"] = fold_id
    result["threshold"] = round(opt_threshold, 4)
    result["train_time_s"] = train_time
    result["n_features"] = n_features
    hp_clean = {k: v for k, v in best_params.items()
                if k not in ("best_val_f1", "n_trials", "study_name")}
    result["best_hyperparams"] = json.dumps(hp_clean)
    if seq_length:
        result["seq_length"] = seq_length

    fault_results = evaluate_per_fault_type(
        df_test_eval, preds, model_name, category_label)
    for fr in fault_results:
        fr["fold"] = fold_id

    window_seconds = infer_window_seconds(args.data)
    latency_results = evaluate_detection_latency(
        df_test_eval, preds, model_name,
        window_seconds=window_seconds, category=category_label)
    for lr_row in latency_results:
        lr_row["fold"] = fold_id

    # Optuna result row
    optuna_row = {
        "model": model_name, "fold": fold_id,
        "category": category_label,
        "held_out_fault": held_out_fault or "",
        **best_params,
    }
    if held_out_fault:
        result["held_out_fault"] = held_out_fault
        for r in fault_results:
            r["held_out_fault"] = held_out_fault
        for r in latency_results:
            r["held_out_fault"] = held_out_fault

    print(f"    {model_name} fold {fold_id}: "
          f"P={result['precision']:.3f} R={result['recall']:.3f} "
          f"F1={result['f1_score']:.3f} AUC={result['auc_roc']:.3f} "
          f"time={train_time}s")

    # --- Save artifacts ---
    raw_dir = output_dir / "raw_scores"
    raw_dir.mkdir(parents=True, exist_ok=True)
    np.save(raw_dir / f"{model_name}{category_suffix}_fold{fold_id}_scores.npy",
            test_scores)
    np.save(raw_dir / f"{model_name}{category_suffix}_fold{fold_id}_labels.npy",
            y_test_eval)

    model_dir = output_dir / "models"
    model_dir.mkdir(parents=True, exist_ok=True)
    save_prefix = f"{model_name}{category_suffix}_fold{fold_id}"

    if model_name in DEEP_MODELS and HAS_TORCH:
        torch.save(model.state_dict(), model_dir / f"{save_prefix}.pt")
    elif model_name == "RuleBased":
        with open(model_dir / f"{save_prefix}.pkl", "wb") as f:
            pickle.dump(model, f)
    else:
        with open(model_dir / f"{save_prefix}.pkl", "wb") as f:
            pickle.dump(model, f)

    # Save imputer + scaler (once per fold, not per model)
    imp_path = model_dir / f"imputer{category_suffix}_fold{fold_id}.pkl"
    scl_path = model_dir / f"scaler{category_suffix}_fold{fold_id}.pkl"
    if not imp_path.exists():
        with open(imp_path, "wb") as f:
            pickle.dump(imputer, f)
        with open(scl_path, "wb") as f:
            pickle.dump(scaler, f)

    # Append to CSVs immediately (crash-safe)
    append_csv(output_dir / "model_results.csv", [result])
    append_csv(output_dir / "per_fault_type_results.csv", fault_results)
    append_csv(output_dir / "detection_latency_results.csv", latency_results)
    append_csv(output_dir / "optuna_results.csv", [optuna_row])

    # --- Confusion matrix ---
    from sklearn.metrics import confusion_matrix as sk_confusion_matrix
    tn, fp, fn, tp = sk_confusion_matrix(y_test_eval, preds, labels=[0, 1]).ravel()
    cm_row = {"model": model_name, "fold": fold_id, "category": category_label,
              "threshold_type": "f1_optimal",
              "tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp)}
    append_csv(output_dir / "confusion_matrices.csv", [cm_row])

    # --- Unsupervised threshold evaluation ---
    for thr_name, thr_val in unsup_thresholds.items():
        unsup_preds = (test_scores >= thr_val).astype(int)
        unsup_result = evaluate_predictions(y_test_eval, unsup_preds, test_scores,
                                            model_name, category_label)
        unsup_result["fold"] = fold_id
        unsup_result["threshold"] = round(thr_val, 4)
        unsup_result["threshold_type"] = thr_name
        unsup_result["n_features"] = n_features
        append_csv(output_dir / "model_results_unsupervised.csv", [unsup_result])

        # Confusion matrix for unsupervised thresholds too
        tn_u, fp_u, fn_u, tp_u = sk_confusion_matrix(
            y_test_eval, unsup_preds, labels=[0, 1]).ravel()
        cm_unsup = {"model": model_name, "fold": fold_id, "category": category_label,
                    "threshold_type": thr_name,
                    "tn": int(tn_u), "fp": int(fp_u), "fn": int(fn_u), "tp": int(tp_u)}
        append_csv(output_dir / "confusion_matrices.csv", [cm_unsup])

    # --- RF feature importance ---
    if model_name == "RF" and hasattr(model, "feature_importances_"):
        importances = model.feature_importances_
        top50_idx = np.argsort(importances)[-50:][::-1]
        fi_rows = [{"fold": fold_id, "rank": r + 1,
                    "feature": feature_cols[i],
                    "importance": float(importances[i])}
                   for r, i in enumerate(top50_idx)]
        append_csv(output_dir / "rf_feature_importance.csv", fi_rows)

    return {
        "result": result,
        "fault_results": fault_results,
        "latency_results": latency_results,
        "optuna": optuna_row,
    }


def infer_window_seconds(data_path):
    """Infer window size from filename (e.g. features_30s.parquet -> 30)."""
    stem = Path(data_path).stem
    for ws in [15, 30, 60, 120]:
        if f"_{ws}s" in stem:
            return ws
    return 30


def run_training(args):
    """Main entry point: run specified model(s) across all folds."""
    print("=" * 70)
    print("Streaming Pipeline Anomaly Detection Training")
    print(f"  Model:    {args.model}")
    print(f"  Device:   {DEVICE}")
    print(f"  Data:     {args.data}")
    print(f"  Output:   {args.output}")
    if args.category:
        print(f"  Category: {args.category}")
    if args.exclude_category:
        print(f"  Exclude:  {args.exclude_category}")
    if getattr(args, "lofo_held_out", None):
        print(f"  LOFO:     held_out={args.lofo_held_out}")
    print(f"  Workers:  {args.n_workers}")
    print("=" * 70)

    # Load data
    df = load_data(args.data)
    feature_cols = get_feature_cols(df, args.category, args.exclude_category,
                                    getattr(args, 'exclude_derived', False))
    print(f"  Features: {len(feature_cols)} "
          f"({'filtered: ' + (args.category or 'no_' + args.exclude_category) if args.category or args.exclude_category else 'all categories'})")

    # Create output directory
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Determine folds
    if args.fold:
        folds = [args.fold]
    else:
        folds = list(range(1, 11))

    all_results = []

    for fold_id in folds:
        print(f"\n{'=' * 70}")
        print(f"FOLD {fold_id} / {len(folds)}")
        print(f"{'=' * 70}")

        validate_data(df, feature_cols, fold_id)

        result = run_model_on_fold(args.model, fold_id, df, feature_cols, args)
        if result:
            all_results.append(result)

    # Summary
    if all_results:
        print(f"\n{'=' * 70}")
        print(f"SUMMARY: {args.model}")
        print(f"{'=' * 70}")
        f1s = [r["result"]["f1_score"] for r in all_results]
        aucs = [r["result"]["auc_roc"] for r in all_results]
        print(f"  F1:  {np.mean(f1s):.4f} +/- {np.std(f1s):.4f}")
        print(f"  AUC: {np.mean(aucs):.4f} +/- {np.std(aucs):.4f}")

        # Save summary JSON
        summary = {
            "model": args.model,
            "category": args.category or (f"no_{args.exclude_category}" if args.exclude_category else "all"),
            "held_out_fault": getattr(args, "lofo_held_out", None),
            "n_folds": len(folds),
            "n_features": len(feature_cols),
            "device": DEVICE,
            "mean_f1": round(float(np.mean(f1s)), 4),
            "std_f1": round(float(np.std(f1s)), 4),
            "mean_auc": round(float(np.mean(aucs)), 4),
            "std_auc": round(float(np.std(aucs)), 4),
            "per_fold": [r["result"] for r in all_results],
        }
        lofo_suffix = f"_lofo_{args.lofo_held_out}" if getattr(args, "lofo_held_out", None) else ""
        summary_path = output_dir / f"summary_{args.model}{lofo_suffix}.json"
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2, default=str)

    print(f"\nResults saved to: {output_dir}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="ML training for streaming anomaly detection")
    parser.add_argument("--model", required=True, choices=ALL_MODELS,
                        help="Model to train")
    parser.add_argument("--data", required=True,
                        help="Path to feature parquet file")
    parser.add_argument("--output", default="results",
                        help="Output directory for results")
    parser.add_argument("--fold", type=int, choices=list(range(1, 11)),
                        help="Run only this fold (default: all 10)")
    parser.add_argument("--n-workers", type=int, default=1,
                        help="Parallel Optuna workers (default: 1)")
    parser.add_argument("--n-trials", type=int, default=None,
                        help="Optuna trials per fold (default: 50 deep, 30 classical)")
    parser.add_argument("--category", choices=CATEGORIES,
                        help="Filter features to single category (RQ3 ablation)")
    parser.add_argument("--exclude-category", choices=CATEGORIES,
                        help="Exclude one category (RQ3 leave-one-out)")
    parser.add_argument("--exclude-derived", action="store_true",
                        help="Exclude derived__ features (ablation)")
    parser.add_argument("--lofo-held-out",
                        choices=[f"F{i}" for i in range(1, 9)],
                        help="Leave-one-fault-out: drop anomalous windows of "
                             "this fault from train/hpo_val/cal and restrict "
                             "test to NORMAL/RECOVERY plus that fault.")
    parser.add_argument("--max-seq-length", type=int, default=20,
                        help="Upper bound of the Optuna seq_length search "
                             "(default 20, the published range).")
    parser.add_argument("--contiguous-sequences", action="store_true",
                        help="Build sequences only over temporally adjacent windows, "
                             "so none spans a cooldown gap or a cross-run jump. "
                             "Off by default, which reproduces the published runs.")
    parser.add_argument("--seq-stride", type=float, default=None,
                        help="Window spacing in seconds for the contiguity check. "
                             "Inferred from the data when omitted.")
    args = parser.parse_args()

    if args.category and args.exclude_category:
        parser.error("Cannot use both --category and --exclude-category")
    if args.max_seq_length < 5:
        parser.error("--max-seq-length must be at least 5")

    global MAX_SEQ_LENGTH, CONTIGUOUS_SEQUENCES, SEQ_STRIDE
    MAX_SEQ_LENGTH = args.max_seq_length
    CONTIGUOUS_SEQUENCES = args.contiguous_sequences
    SEQ_STRIDE = args.seq_stride
    print(f"Sequence config: max_seq_length={MAX_SEQ_LENGTH}, "
          f"contiguous={CONTIGUOUS_SEQUENCES}, stride={SEQ_STRIDE}")

    run_training(args)


if __name__ == "__main__":
    main()
