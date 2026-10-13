"""Detection latency at F1-optimal thresholds, e-commerce, 30-second windows.

Reads the per-episode latency files written by the training pipeline for the run behind the
RQ1 tables (data/results_v3/merged) and for the Deep SVDD and DAGMM run on the same folds
(data/v3/results/ecom-gapfill). An episode is one fault repetition. Its latency is the time
from fault onset to the first window flagged anomalous. Episodes with no flagged window are
undetected. "Detected only" statistics use detected episodes. "Censored" statistics set each
undetected episode to the length of its labelled fault span, which is the pipeline's own
censored value. Percentiles use numpy's default (linear interpolation).

Run:  python analysis/cross_workload/detection_latency_table.py
Output: detection_latency_table.csv (this directory)
"""
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).parent
SOURCES = [ROOT / "data/results_v3/merged/detection_latency_results.csv",
           ROOT / "data/v3/results/ecom-gapfill/detection_latency_results.csv"]
ORDER = ["IF", "OC_SVM", "LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE", "DeepSVDD", "DAGMM", "RF",
         "RuleBased"]


def load():
    df = pd.concat([pd.read_csv(p) for p in SOURCES], ignore_index=True)
    return df[df["category"] == "all"]


def main():
    df = load()
    rows = []
    for m in ORDER:
        x = df[df["model"] == m]
        det = x["detected"].astype(bool)
        lat = x["latency_seconds"].astype(float)
        cens = lat    # undetected episodes already carry their labelled span length
        rows.append(dict(model=m, episodes=len(x), detected=int(det.sum()),
                         detection_rate=float(det.mean()),
                         median_detected_s=float(np.median(lat[det])),
                         p95_detected_s=float(np.percentile(lat[det], 95)),
                         median_censored_s=float(np.median(cens)),
                         p95_censored_s=float(np.percentile(cens, 95))))
    t = pd.DataFrame(rows)
    t.round(6).to_csv(OUT / "detection_latency_table.csv", index=False)
    print(t.round(3).to_string(index=False))


if __name__ == "__main__":
    main()
