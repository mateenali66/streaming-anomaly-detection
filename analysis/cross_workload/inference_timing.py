"""Per-window scoring latency on CPU for every detector in the panel.

Measures every detector the same way. It is the source of the scoring-time column of the
computational-cost table.

Protocol. One 30-second window at a time (batch size 1, no batching), 20 warm-up calls,
then 256 timed calls with time.perf_counter; we report the median and 95th percentile in
milliseconds per window, including the score computation each model uses at test time.

- Point models (IF, OC-SVM, RF) are the saved fold-1 e-commerce models, scored on real
  fold-1 test windows after the saved fold-1 imputer and scaler.
- Max-z uses the saved fold-1 training mean and standard deviation; EWMA uses the
  published construction fitted on fold-1 training-normal windows.
- Deep models take the hyperparameters selected for fold 1 in the capped,
  contiguity-corrected rerun (capped_hpo/), at sequence length 11 and 475 features.
  Weights are left at initialisation because scoring cost does not depend on weight
  values; input is a real fold-1 test sequence. Deep SVDD scores the squared distance
  to a centre; DAGMM scores the sample energy under mixture parameters estimated once,
  outside the timed loop, from 64 training sequences.

Run:  python analysis/cross_workload/inference_timing.py
Output: inference_timing.csv, inference_timing.txt (this directory)
"""
from pathlib import Path
import csv
import pickle
import platform
import sys
import time
import warnings

import numpy as np
import pandas as pd
import torch

warnings.filterwarnings("ignore")
ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).parent
sys.path.insert(0, str(ROOT / "sagemaker"))
import training as T  # noqa: E402

T.DEVICE = "cpu"
torch.manual_seed(0)
N_WARM, N_TIMED, L = 20, 256, 11
MODELS = ROOT / "data/results_v3/cpu-1/models"
EXCLUDE = {"label", "fault_id", "fault_category", "repetition", "window_start",
           "window_end", "window_mid", "n_samples", "run_number", "run_source"}

# best_params are written in Optuna suggest order after seq_length, lr, dropout
HP_ORDER = {
    "LSTM_AE": ["epochs", "hidden_dim", "n_layers"],
    "Trans_AE": ["epochs", "d_model", "nhead", "n_layers"],
    "CNN_AE": ["epochs", "filters", "kernel_size", "n_layers"],
    "LSTM_VAE": ["epochs", "hidden_dim", "n_layers", "latent_dim", "beta"],
    "DeepSVDD": ["hidden_dim", "n_layers", "latent_dim", "pretrain_epochs", "finetune_epochs"],
    "DAGMM": ["epochs", "hidden_dim", "n_layers", "latent_dim", "n_gmm",
              "lambda_energy", "lambda_cov_diag"],
}


def fold1_hp(model):
    rows = list(csv.reader(open(OUT / "capped_hpo" / "ecommerce" / "optuna_results.csv")))
    for r in rows[1:]:
        if r[0] == model and r[1] == "1":
            vals = r[4:]                      # seq_length onward
            assert int(float(vals[0])) == L
            keys = ["seq_length", "lr", "dropout"] + HP_ORDER[model]
            hp = {k: float(v) for k, v in zip(keys, vals)}
            return {k: (int(v) if v.is_integer() else v) for k, v in hp.items()}
    raise KeyError(model)


def timed(fn, inputs):
    for i in range(N_WARM):
        fn(inputs[i % len(inputs)])
    ts = []
    for i in range(N_TIMED):
        x = inputs[i % len(inputs)]
        t0 = time.perf_counter()
        fn(x)
        ts.append((time.perf_counter() - t0) * 1e3)
    return float(np.median(ts)), float(np.percentile(ts, 95))


def main():
    df = pd.read_parquet(ROOT / "data/features_v3/features_30s.parquet")
    cols = [c for c in df.columns if c not in EXCLUDE and not c.startswith("fold_")
            and df[c].dtype.kind in "fi"]
    assert len(cols) == 475
    train = df[(df.fold_1_split == "train") & (df.label == "NORMAL")][cols].to_numpy()
    test = df[df.fold_1_split == "test"][cols].to_numpy()

    imputer = pickle.load(open(MODELS / "imputer_fold1.pkl", "rb"))
    scaler = pickle.load(open(MODELS / "scaler_fold1.pkl", "rb"))
    Xtr = scaler.transform(imputer.transform(train)).astype(np.float32)
    Xte = scaler.transform(imputer.transform(test)).astype(np.float32)
    rows_1 = [Xte[i:i + 1] for i in range(len(Xte))]
    seqs = [torch.from_numpy(Xte[i:i + L][None]) for i in range(len(Xte) - L)]

    res = []
    # point models
    for m, fn in (("IF", lambda o: (lambda x: o.decision_function(x))),
                  ("OC_SVM", lambda o: (lambda x: o.decision_function(x))),
                  ("RF", lambda o: (lambda x: o.predict_proba(x)))):
        obj = pickle.load(open(MODELS / f"{m}_fold1.pkl", "rb"))
        res.append((m, *timed(fn(obj), rows_1)))
    mz = pickle.load(open(MODELS / "RuleBased_fold1.pkl", "rb"))
    mu, sd = np.asarray(mz["mean"]), np.where(np.asarray(mz["std"]) > 0, mz["std"], 1.0)
    res.append(("RuleBased", *timed(lambda x: np.max(np.abs((x - mu) / sd), axis=1), rows_1)))
    ew_mu = pd.DataFrame(Xtr).ewm(alpha=0.3, adjust=False).mean().to_numpy()[-1]
    ew_sd = pd.DataFrame(Xtr).ewm(alpha=0.3, adjust=False).std().to_numpy().mean(0)
    ew_sd = np.where(ew_sd > 1e-9, ew_sd, 1.0)
    res.append(("EWMA", *timed(lambda x: np.max(np.abs(x - ew_mu) / ew_sd, axis=1), rows_1)))

    # deep models
    nf = 475
    with torch.no_grad():
        for m in ("LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE", "DeepSVDD", "DAGMM"):
            hp = fold1_hp(m)
            if m == "LSTM_AE":
                net = T.LSTMAutoencoder(nf, hp["hidden_dim"], hp["n_layers"], hp["dropout"])
            elif m == "Trans_AE":
                nhead = hp["nhead"] if hp["d_model"] % hp["nhead"] == 0 else 2
                net = T.TransformerAutoencoder(nf, hp["d_model"], nhead, hp["n_layers"],
                                               hp["dropout"], L)
            elif m == "CNN_AE":
                net = T.CNN1DAutoencoder(nf, hp["filters"], hp["kernel_size"], hp["n_layers"],
                                         hp["dropout"])
            elif m == "LSTM_VAE":
                net = T.LSTMVariationalAutoencoder(nf, hp["hidden_dim"], hp["n_layers"],
                                                   hp["latent_dim"], hp["dropout"])
            elif m == "DeepSVDD":
                net = T.DeepSVDD(nf, hidden_dim=hp["hidden_dim"], n_layers=hp["n_layers"],
                                 latent_dim=hp["latent_dim"], dropout=hp["dropout"])
            else:
                net = T.DAGMM(nf, hidden_dim=hp["hidden_dim"], n_layers=hp["n_layers"],
                              latent_dim=hp["latent_dim"], n_gmm=hp["n_gmm"],
                              dropout=hp["dropout"])
            net.eval()
            if m in ("LSTM_AE", "Trans_AE", "CNN_AE"):
                def score(x, net=net):
                    return torch.mean((x - net(x)) ** 2, dim=(1, 2))
            elif m == "LSTM_VAE":
                def score(x, net=net):
                    return torch.mean((x - net(x)[0]) ** 2, dim=(1, 2))
            elif m == "DeepSVDD":
                c = torch.zeros(hp["latent_dim"])

                def score(x, net=net, c=c):
                    return ((net.encode(x) - c) ** 2).sum(dim=1)
            else:
                ref = torch.from_numpy(np.stack([Xtr[i:i + L] for i in range(0, 64 * L, L)]))
                _, z, gamma = net(ref)
                phi, mu_g, sig = net.compute_gmm_params(z, gamma)

                def score(x, net=net, phi=phi, mu_g=mu_g, sig=sig):
                    _, z, _ = net(x)
                    return net.sample_energy(z, phi, mu_g, sig)
            res.append((m, *timed(score, seqs)))

    out = pd.DataFrame(res, columns=["model", "median_ms", "p95_ms"])
    out.round(4).to_csv(OUT / "inference_timing.csv", index=False)
    env = (f"CPU {platform.machine()} ({platform.processor() or 'n/a'}), Python "
           f"{platform.python_version()}, torch {torch.__version__}, "
           f"{torch.get_num_threads()} torch threads")
    txt = ("PER-WINDOW SCORING LATENCY, CPU, BATCH SIZE 1\n\n" + env + "\n"
           f"{N_WARM} warm-up calls, {N_TIMED} timed calls per model; "
           f"sequence length {L}, 475 features\n\n" + out.round(4).to_string(index=False) + "\n")
    (OUT / "inference_timing.txt").write_text(txt)
    print(txt)


if __name__ == "__main__":
    main()
