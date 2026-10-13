"""Headline results with the F1 and F2 injection windows removed, and with F3 removed too.

injection_check.py shows that the F1 (broker pod kill) and F2 (broker network partition)
injections left no trace in the brokers' own metrics, so their windows labelled ANOMALOUS
contain no fault. This recomputes the RQ1 metrics for every panel model and the EWMA
baseline with those windows removed from each test fold. F3 (disk I/O latency) has no direct
check in the exported metrics, and its windows are ranked like those of F1 and F2, so a second
subset removes F3 as well. Scores and the calibrated
thresholds are the primary run's (see calibrated_threshold_tables.py). Removing positives
lowers prevalence, so F1 is reported next to the always-fire F1 of the same windows.

It also ranks each of F1 to F3 against the folds' NORMAL windows alone, to show how the
detectors score windows in which no fault occurred.

Run:  python analysis/cross_workload/broker_fault_sensitivity.py
Output: broker_fault_sensitivity_per_fold.csv, broker_fault_sensitivity_summary.csv,
        broker_fault_vs_normal_auc.csv
"""
from pathlib import Path
import sys

import numpy as np
import pandas as pd
from sklearn.metrics import f1_score, roc_auc_score

OUT = Path(__file__).parent
sys.path.insert(0, str(OUT))
from calibrated_threshold_tables import always_fire_f1, load_workload, stored_thresholds  # noqa: E402
from ewma_mechanism import WORKLOADS  # noqa: E402

SUBSETS = {"all": [], "without_F1_F2": ["F1", "F2"], "without_F1_F3": ["F1", "F2", "F3"]}


def main():
    thr = stored_thresholds()
    rows, vs_normal = [], []
    for wl in WORKLOADS:
        data, _ = load_workload(wl, thr)
        for (model, fold), d in data.items():
            normal = d["phase"] == "NORMAL"
            for fault in ("F1", "F2", "F3"):
                pos = (d["phase"] == "ANOMALOUS") & (d["fault"] == fault)
                if pos.any() and normal.any():
                    y = np.r_[np.ones(pos.sum()), np.zeros(normal.sum())]
                    vs_normal.append(dict(workload=wl, model=model, fold=fold, fault=fault,
                                          auc=roc_auc_score(y, np.r_[d["s"][pos], d["s"][normal]])))
            for subset, faults in SUBSETS.items():
                m = (d["phase"] != "EXCLUDED") & ~((d["phase"] == "ANOMALOUS") & np.isin(d["fault"], faults))
                y = (d["phase"][m] == "ANOMALOUS").astype(int)
                s = d["s"][m]
                pred = (s >= thr[(wl, model, fold)][0]).astype(int)
                rows.append(dict(workload=wl, model=model, fold=fold, subset=subset,
                                 auc=roc_auc_score(y, s), f1=f1_score(y, pred, zero_division=0),
                                 always_fire_f1=always_fire_f1(y), prevalence=float(y.mean())))
    vn = pd.DataFrame(vs_normal).groupby(["workload", "model", "fault"]).auc.mean().reset_index()
    vn.round(6).to_csv(OUT / "broker_fault_vs_normal_auc.csv", index=False)
    a = pd.DataFrame(rows)
    a.round(6).to_csv(OUT / "broker_fault_sensitivity_per_fold.csv", index=False)
    s = (a.pivot_table(index=["workload", "model"], columns="subset",
                       values=["auc", "f1", "always_fire_f1", "prevalence"]))
    s.columns = [f"{m}_{sub}" for m, sub in s.columns]
    s = s.reset_index()
    s.round(6).to_csv(OUT / "broker_fault_sensitivity_summary.csv", index=False)
    print(s.round(3).to_string(index=False))


if __name__ == "__main__":
    main()
