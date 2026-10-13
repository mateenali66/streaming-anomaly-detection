# Artifact map

Each table and figure, the script that produces it, and the data it writes or reads. Scripts are in `analysis/cross_workload/` unless a path is given.

| Table or figure | Label | Script | Data |
|---|---|---|---|
| Fault taxonomy | `tab:faults` | campaign manifests in the dataset archives | |
| Label accounting | `tab:labels` | feature parquets | |
| Configuration, HPO | `tab:config`, `tab:hpo`, `tab:hpo_objective` | `sagemaker/training.py` | |
| Folds | `tab:folds` | `sagemaker/preprocessing.py` (`assign_folds`) | |
| RQ1, both workloads | `tab:rq1`, `tab:iot-rq1`, Fig. 3 | `merge_panels.py`\*, `rq1_with_ewma.py`, `regen_fig3_fig4.py` | `panel_overall.csv`, `rq1_with_ewma_summary.csv` |
| RQ2 | `tab:rq2` | `rq2_mixedeffects.py` | `rq2_mixedeffects.csv` |
| RQ2 matched | `tab:rq2-matched`, Fig. 4 | `rq2_matched_comparison.py`\*, `regen_fig3_fig4.py` | `rq2_matched_*.csv`, reads `capped_hpo/` |
| RQ3 ablation and fusion | `tab:fusion`, Fig. 5 | `rq3_fusion_corrected.py`\*, `figures/generate_figures.py`\* | `rq3_fusion_corrected_*.csv` |
| Detection latency | `tab:latency`, Fig. 7 | `detection_latency_table.py`, `regen_fig7.py` | `detection_latency_table.csv` |
| Latency at a false-positive budget | `tab:fpr_latency` | `latency_fixed_fpr_v2.py` | `latency_fixed_fpr_v2_*.csv` |
| Recovery windows, threshold-only leave-one-fault-out, three-state | `tab:cooldown`, `tab:threshold_lofo`, `tab:three_state` | `calibrated_threshold_tables.py`, `three_state_auc.py` | `calibrated_regimes_*.csv`, `fault_holdout_threshold_*.csv`, `three_state_auc_*.csv` |
| Operational replay | `tab:replay` | `operational_replay.py` | `operational_replay_*.csv` |
| Prevalence | `tab:prevalence` | `prevalence_sweep.py`, `prevalence_sweep_bootstrap.py`, `curated_rules*.py`\* | `prevalence_sweep*.csv` |
| Cost | `tab:cost` | `merge_panels.py`\* (training time), `inference_timing.py` (scoring time) | `panel_overall.csv`, `inference_timing.csv` |
| Confusion matrix, RF importance | `tab:confusion`, `tab:rf-importance` | `analysis/generate_tables.py` | reads `data/results_v3/merged/` |
| Label-free thresholds | `tab:unsupervised` | `sagemaker/training.py` | reads `model_results_unsupervised.csv` under `data/` |
| Cross-workload training | `tab:cross-train` | `subexp2_cross_train_folds.py`\*, `subexp2_transfer_stats.py` | `subexp2_*` |
| Classical baselines | `tab:classical` | `classical_baselines.py` | `classical_baselines_*.csv` |
| Full leave-one-fault-out | `tab:full_lofo` | `sagemaker/training.py --lofo-held-out`\* | `full_lofo_summary.csv`, summarised from those runs |
| EWMA mechanism | `tab:ewma` | `ewma_mechanism.py` | `ewma_mechanism_*` |
| Broker faults that did not take effect | `tab:broker_sensitivity` | `injection_check.py`, `broker_fault_sensitivity.py` | `injection_check_*`, `broker_fault_sensitivity_*.csv` |
| Per-fault heatmap, window size | Fig. 6, Fig. 8 | `regen_figures.py` | reads `panel_overall.csv`, `panel_per_fault.csv` |
| Architecture, protocol | Fig. 1, Fig. 2 | `figures/fig1_architecture.py` (Python diagrams), protocol drawn by hand | |
| Fault-rank transfer, bucket and window transfer | text | `fault_rank_transfer_auc.py`, `subexp1_bucket_consistency.py`, `subexp34_rank_correlation.py` | `fault_rank_transfer_auc.csv`, `subexp1_*`, `subexp3_*`, `subexp4_*` |
| Symptom alignment, feature selection, sequence contiguity | text | `symptom_alignment.py`, `feature_selection_leakage.py`\*, `sequence_contiguity.py`\*, `seq_boundary_ablation.py`\* | `symptom_*`, `feature_selection_*`, `sequence_contiguity_audit.txt` |
| Numeric claims | all | `verify_claims.py` | printed report |

Notes:

- Scripts marked \* need training outputs or campaign data that are not in this deposit. The others, and `verify_claims.py`, run from the deposit with the two dataset archives unpacked.
- The F1 (broker crash) and F2 (network partition) injections did not take effect, so their ANOMALOUS windows contain no fault (`injection_check.py`).
- Rows labelled `RuleBased` in the CSVs are the statistical max-z diagnostic, and `HST` is HBOS.
- `subexp2_cross_train.py` and `subexp2_cross_train_v2.py` are the earlier source-scaled transfer runs that `tab:cross-train` corrects.
