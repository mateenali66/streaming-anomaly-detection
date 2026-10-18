"""Descriptive results behind the RQ tables (protocol 4.5, 4.6 and 8.4): detector performance with the three
diagnostic rows, operating points and alert burden, Kaplan-Meier latency, and the RQ3 category models.

Added on 18 October 2026, after registration (D4). Each value is reported per campaign (the mean over its ten folds,
with a t-based 95% interval) and pooled over the confirmatory campaigns 2 and 3 as the stratified estimate of 5.3.1.
Campaign 1 is reported beside the pooled value. The single-category and leave-one-category-out models are exploratory
(8.1, RQ3).

    python3 -m analysis.describe --out DIR     (reads DIR/fold_metrics.parquet and DIR/run_metrics.parquet)

writes DIR/describe/performance.parquet, operating.parquet, latency.parquet, categories.parquet and describe.json.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PIPE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPE))
from analysis import confirm as C  # noqa: E402
from analysis import metrics as M  # noqa: E402
from analysis import stats as S  # noqa: E402
from analysis.sources import CATEGORY_VARIANTS, PANEL  # noqa: E402

DETECTORS = PANEL + ["RF", "RuleBased", "EWMA", "TimeOnly", "PhasePosition"]
PERF = ["pooled_auc", "within_run_auc", "within_run_pc", "sham_placebo", "r1_placebo", "pr_auc", "prevalence",
        "f1_f1opt", "precision_f1opt", "recall_f1opt", "always_fire_f1_f1opt"]
BUDGETS = [f"fpb_{b}" for b in M.FP_BUDGETS]
OPER = ["recall", "precision", "f1", "always_fire_f1", "fpr", "fa_per_h", "trailing_fa_per_h", "trailing_duty",
        "episode_rate", "sham_episode"]


def _ci(values: np.ndarray) -> tuple[float, float, float]:
    v = np.asarray(values, float)
    w = S.within_campaign(v - v.mean())
    return float(v.mean()), float(v.mean() + w["ci95_lo"]), float(v.mean() + w["ci95_hi"])


def summarise(fm: pd.DataFrame, detectors: list, metrics: list) -> pd.DataFrame:
    rows = []
    for w, cs in C.WORKLOADS.items():
        for d in detectors:
            for m in metrics:
                per = {}
                for c in cs:
                    s = fm[(fm["detector"] == d) & (fm["campaign_id"] == c)].set_index("fold")[m].sort_index()
                    if len(s) != 10 or s.isna().any():
                        per = None
                        break
                    per[c] = s.to_numpy()
                if per is None:
                    continue
                row = {"workload": w, "detector": d, "metric": m}
                for i, c in enumerate(cs, start=1):
                    row[f"c{i}_mean"], row[f"c{i}_lo"], row[f"c{i}_hi"] = _ci(per[c])
                p = S.pooled({c: per[c] for c in C.CONFIRMATORY[w]})
                row.update({"pooled_mean": p["mean"], "pooled_lo": p["ci95_lo"], "pooled_hi": p["ci95_hi"],
                            "pooled_se": p["se"]})
                rows.append(row)
    return pd.DataFrame(rows)


def always_fire_rows(fm: pd.DataFrame) -> pd.DataFrame:
    """Always-fire (4.4.3): every window flagged. Pooled and within-run AUC are 0.5, PR-AUC is the prevalence and
    F1 is 2p/(1+p), on the population of the point models (IF's test windows)."""
    base = fm[fm["detector"] == "IF"].copy()
    base["detector"] = "AlwaysFire"
    base["pooled_auc"] = base["within_run_auc"] = base["sham_placebo"] = base["r1_placebo"] = 0.5
    base["within_run_pc"] = 0.0
    base["pr_auc"] = base["prevalence"]
    base["f1_f1opt"] = base["always_fire_f1_f1opt"]
    base["precision_f1opt"] = base["prevalence"]
    base["recall_f1opt"] = 1.0
    return base


def latency(runs: pd.DataFrame, detectors: list) -> pd.DataFrame:
    out = []
    fr = runs[runs["detector"].isin(detectors) & (runs["run_type"] != "F0")].copy()
    fr["workload"] = fr["campaign_id"].map(C.workload_of)
    fr["set"] = np.where(fr["campaign_id"].str.endswith("-a"), "c1", "pooled")
    for b in BUDGETS:
        for by, label in ((["workload", "set", "detector"], "all faults"),
                          (["workload", "set", "detector", "run_type"], "per fault")):
            k = M.latency_km(fr, by, b)
            k["scope"] = label
            out.append(k)
    return pd.concat(out, ignore_index=True)


def categories(fm: pd.DataFrame) -> pd.DataFrame:
    """Each IF category variant against IF on all features, paired over folds (exploratory)."""
    rows = []
    for w in C.WORKLOADS:
        for v in CATEGORY_VARIANTS:
            for m in ("pooled_auc", "within_run_auc"):
                r = C.contrast(fm, f"IF_{v}", "IF", m, w, two_sided=True)
                p = r["pooled"]
                rows.append({"workload": w, "variant": v, "metric": m, "diff_pooled": p["mean"],
                             "diff_lo": p["ci95_lo"], "diff_hi": p["ci95_hi"], "p": p["p_t"],
                             **{f"diff_{c}": r["per_campaign"][c]["mean"] for c in C.WORKLOADS[w]}})
    return pd.DataFrame(rows)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--out", required=True)
    a = p.parse_args(argv)
    out = Path(a.out)
    fm = pd.read_parquet(out / "fold_metrics.parquet")
    runs = pd.read_parquet(out / "run_metrics.parquet")
    dest = out / "describe"
    dest.mkdir(exist_ok=True)
    fm_af = pd.concat([fm, always_fire_rows(fm)], ignore_index=True)
    perf = summarise(fm_af, DETECTORS + ["AlwaysFire"] + [f"IF_{v}" for v in CATEGORY_VARIANTS], PERF)
    oper_cols = [f"{m}_{b}" for b in BUDGETS for m in OPER]
    oper = summarise(fm, DETECTORS, oper_cols)
    lat = latency(runs, DETECTORS)
    cat = categories(fm)
    perf.to_parquet(dest / "performance.parquet", index=False)
    oper.to_parquet(dest / "operating.parquet", index=False)
    lat.to_parquet(dest / "latency.parquet", index=False)
    cat.to_parquet(dest / "categories.parquet", index=False)
    summary = {"n_performance_rows": len(perf), "n_operating_rows": len(oper), "n_latency_rows": len(lat),
               "n_category_rows": len(cat)}
    (dest / "describe.json").write_text(json.dumps(summary, indent=1) + "\n")
    print(f"describe: {summary}")


if __name__ == "__main__":
    main()
