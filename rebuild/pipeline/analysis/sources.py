"""Where the stored per-window scores live (make scores, 7.1 step 6), as a list of sources for metrics.py.

The training runs keep train.py's layout. The phase B run holds the frozen fits of all three campaigns per workload
(campaign 1 refitted with the frozen configuration, 6.1.4), the category fits and the cross-workload fits. baselines.py
writes the references that need no training.

    <phase B run>/<cid>/w30-frozen/<model>/fold<k>/scores/<model>_fold<k>.parquet
    <phase B run>/<cid>/w30-frozen/IF_<variant>/fold<k>/scores/IF_<variant>_fold<k>.parquet
    <phase B run>/cross/<source>/<model>/scores/<model>_<target>.parquet       (test role, all ten folds)
    <baselines>/<cid>/<detector>/<detector>_fold<k>.parquet
    <search run>/<cid>/w<W>/<model>/fold<k>/scores/<model>_fold<k>.parquet     (campaign 1, window sizes, 6.1.5)
"""
from __future__ import annotations

from pathlib import Path

CAMPAIGNS = ["ec-a", "ec-b", "ec-c", "iot-a", "iot-b", "iot-c"]
DEEP = ["LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE", "DeepSVDD", "DAGMM"]
POINT = ["IF", "OC_SVM"]
PANEL = POINT + DEEP
REFERENCES = ["RF", "RuleBased", "EWMA"]
C2B = ["EWMA_train_mean"]
DIAGNOSTICS = ["TimeOnly", "PhasePosition"]
CATEGORY_VARIANTS = ["broker", "consumer", "flink", "jvm", "no_broker", "no_consumer", "no_flink", "no_jvm"]
BASELINE_DETECTORS = ["RuleBased", "EWMA", "EWMA_train_mean", "TimeOnly", "PhasePosition"]
N_FOLDS = 10


def family(detector: str) -> str:
    if detector in DEEP:
        return "deep"
    if detector in POINT:
        return "point"
    if detector in DIAGNOSTICS:
        return "diagnostic"
    if detector.startswith("IF_"):
        return "category"
    return "reference"


def _need(p: Path) -> Path:
    if not p.is_file():
        raise SystemExit(f"ERROR: missing score file {p}")
    return p


def frozen_sources(phase_b: Path, baselines: Path, campaigns=CAMPAIGNS) -> list[dict]:
    """Every 30 s detector on every campaign and fold: the panel and the random forest from the frozen fits, the
    category variants, and the references and diagnostics of baselines.py."""
    out = []
    for cid in campaigns:
        for k in range(1, N_FOLDS + 1):
            for m in PANEL + ["RF"]:
                p = phase_b / cid / "w30-frozen" / m / f"fold{k}" / "scores" / f"{m}_fold{k}.parquet"
                out.append({"campaign_id": cid, "detector": m, "family": family(m), "fold": k,
                            "run": phase_b.name, "path": str(_need(p))})
            for v in CATEGORY_VARIANTS:
                d = f"IF_{v}"
                p = phase_b / cid / "w30-frozen" / d / f"fold{k}" / "scores" / f"{d}_fold{k}.parquet"
                out.append({"campaign_id": cid, "detector": d, "family": "category", "fold": k,
                            "run": phase_b.name, "path": str(_need(p))})
            for d in BASELINE_DETECTORS:
                p = baselines / cid / d / f"{d}_fold{k}.parquet"
                out.append({"campaign_id": cid, "detector": d, "family": family(d), "fold": k,
                            "run": baselines.name, "path": str(_need(p))})
    return out


def cross_sources(phase_b: Path, campaigns=CAMPAIGNS) -> list[dict]:
    out = []
    for src in campaigns:
        for m in PANEL + ["RF"]:
            for tgt in campaigns:
                if tgt == src:
                    continue
                p = phase_b / "cross" / src / m / "scores" / f"{m}_{tgt}.parquet"
                out.append({"source": src, "target": tgt, "detector": m, "path": str(_need(p))})
    return out


def window_sources(search_run: Path, campaigns=("ec-a", "iot-a")) -> list[dict]:
    """Campaign 1 window sizes of 6.1.5. A job with no summary has no score file (D3), so it is skipped and listed."""
    out = []
    for cid in campaigns:
        for wdir in sorted((search_run / cid).glob("w*")):
            w = int(wdir.name[1:])
            for m in PANEL:
                for k in range(1, N_FOLDS + 1):
                    p = wdir / m / f"fold{k}" / "scores" / f"{m}_fold{k}.parquet"
                    out.append({"campaign_id": cid, "window_s": w, "detector": m, "family": family(m), "fold": k,
                                "run": search_run.name, "path": str(p), "exists": p.is_file()})
    return out
