"""Per-fault difficulty-rank transfer between workloads, on AUC-ROC instead of F1.

subexp34_rank_correlation.py correlates per-fault F1 between the two
workloads. For a detector that fires on (nearly) every window, per-fault F1 is fixed by
each fault's share of anomalous windows, 2p/(1+p), and the fault durations are the same
on both workloads, so the F1 rank correlation is high by construction: the always-firing
max-z diagnostic reaches rho = 0.96 that way. AUC-ROC does not depend on prevalence, so
this script repeats the correlation on per-fault AUC-ROC.

Input: ewma_mechanism_per_fault_auc.csv (per workload x model x fault, mean over folds;
positives are one fault's ANOMALOUS windows, negatives every non-anomalous test window).
Run:  python analysis/cross_workload/fault_rank_transfer_auc.py   (after ewma_mechanism.py)
Output: fault_rank_transfer_auc.csv, fault_rank_transfer_auc.txt
"""
from pathlib import Path

import pandas as pd
from scipy.stats import spearmanr

OUT = Path(__file__).parent


def main():
    pf = pd.read_csv(OUT / "ewma_mechanism_per_fault_auc.csv")
    w = pf.pivot_table(index=["model", "fault_id"], columns="workload", values="mean").reset_index()
    f1 = pd.read_csv(OUT / "subexp3_per_fault_spearman.csv").set_index("model").spearman_rho
    rows = []
    for m, g in w.groupby("model"):
        r, p = spearmanr(g.ecommerce, g.iot)
        rows.append(dict(model=m, n_faults=len(g), rho_auc=r, p_auc=p,
                         rho_f1_published=f1.get(m, float("nan"))))
    out = pd.DataFrame(rows).sort_values("rho_auc", ascending=False)
    out.round(4).to_csv(OUT / "fault_rank_transfer_auc.csv", index=False)
    txt = ("PER-FAULT DIFFICULTY-RANK TRANSFER, E-COMMERCE vs IoT, 30s, N = 8 FAULTS\n"
           "rho_auc: Spearman on per-fault AUC-ROC (this script); "
           "rho_f1_published: Spearman on per-fault F1 (subexp3)\n\n"
           + out.round(3).to_string(index=False) + "\n")
    (OUT / "fault_rank_transfer_auc.txt").write_text(txt)
    print(txt)


if __name__ == "__main__":
    main()
