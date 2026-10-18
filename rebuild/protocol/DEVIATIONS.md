# Deviations from the registered protocol

Every change made after the registration of 14 October 2026 is recorded here, with the date, what changed, why, which campaigns and attempts it touches, and the author's approval. The paper reports each one. A change that is not recorded here did not happen.

## D1. The JVM buffer pool gauges pass the monotone guard (17 October 2026)

All six campaigns had finished before this change, so it touches no campaign data and no attempt. One line of `pipeline/config/pipeline.json` changed. `monotone_guard.allow_metric_patterns` went from an empty list to `["^jvm_buffer_pool_(used|capacity)_bytes$"]`. The code, the plans, the thresholds, the image lock and every other config value are as registered.

| | Registered | After D1 |
|---|---|---|
| SHA-256 of `pipeline/config/pipeline.json` | `a0b70b7cd2f1bc49cd7a83b5d3ef13b40370c63d5e15777098391e92e9bd6510` | `709aedb95909e252366a7fa60de74995415e56024d4488deec0cf8e5f26958d8` |
| `config_sha256` in build and training reports | `9b81d27e84ef7f528dbe60d8d2ea741e0f3da1c88bb6a69bfc12838485260b20` | `5e379394852faff090fa9a141772dc91f718cb03d69b90402d9eb177008efc5e` |

The registered config stopped the ec-a build at the monotone guard (protocol section 4.1, item 8). The guard stopped 2 of the 14,937 ec-a series. Both are broker 2's JVM direct buffer pool, `jvm_buffer_pool_used_bytes` and `jvm_buffer_pool_capacity_bytes` with pool `direct`, at Spearman 0.992. Over the 24 h leading baseline the gauge crept up by under 0.1 MiB near 0.6 MiB. After the baseline it moved with the faults between 0 and 9 MiB and fell 31 times. The guard stopped no series in iot-a, ec-b, iot-b, ec-c or iot-c. Until this was resolved, ec-a could not be built, and no campaign could be trained.

Three ways out were tested on the six verified campaign copies before the change, using the registered functions with the config changed in memory only.

1. An `exclude_series` rule for the direct pool, in the form of decision 42, would have removed the same 6 series (both gauges on each of the three brokers) from every campaign.
2. Switching the guard's policy from error to drop would have removed only the 2 ec-a series, so the ec-a features would differ from the other five.
3. Allowing the two gauges past the guard keeps them as features in every campaign.

The author chose option 3 on 17 October 2026.

Every other buffer pool series already passed the guard under the registered config and was a feature. So D1 changes no feature in iot-a, ec-b, iot-b, ec-c or iot-c. In ec-a it adds the 2 stopped series to the features, named by the registered naming rules. With D1 the guard stops no series in any of the six campaigns, and the pipeline tests pass (145 of 145).

The guard exists to keep out gauges that act as clocks, so that risk now stays in for these 2 series. A measurement made after the choice shows the gauge carries campaign history in all 18 direct pool series (6 per campaign), not only the 2 the guard stopped. On every broker of every campaign, `jvm_buffer_pool_used_bytes` with pool `direct` has a median of 0.54 to 0.93 MiB over the leading baseline and 6.83 to 8.97 MiB over the trailing baseline. Every trailing step lies above the leading baseline's maximum, and so do 97 to 99% of all steps after the leading baseline. On this gauge alone, the drift check (section 1.5, item 4) can tell the leading baseline from the trailing one in every campaign. It can also tell the first six hours of the leading baseline from the last six in every campaign, because the gauge creeps up inside the baseline as well. Its last six hours lie entirely above its first six on 17 of the 18 series, and the remaining one (iot-c broker 1) gives a single-series AUC of 0.717. The author was shown this before any build and kept option 3 on 17 October 2026. The paper reports the drift check as it comes out. The registered analysis limits what drift can do to the results. The within-run AUC compares each run's ANOMALOUS windows with the same attempt's own pre-fault windows (section 4.5, item 2), which all come after the baseline. A pooled result is flagged when its campaign's time-only AUC excludes 0.5 or its drift check fails (section 4.6, item 4, and section 1.5, item 4). A ranking claim needs both the pooled and the placebo-corrected within-run AUC (section 4.6, item 2). The paper reports D1 in its deviations appendix and in the response to the reviewers.

## D2. Training code added after registration, and the choices it makes (17 October 2026)

The registered protocol names the training steps (sections 6.1 to 6.3 and 7.1) but not every program that runs them. Four pieces were added on 17 October 2026, after all six campaigns had finished and before any feature, label or score file of campaigns 2 and 3 was read. None changes `pipeline/training/train.py`, `streamprep`, the configs, the plans or the locks. Each is in a signed local commit.

1. `pipeline/training/run_matrix.py` schedules the registered `train.py`, one process per model and fold, on one multi-GPU machine. It passes only options `train.py` already has and adds no model, metric or rule. Its job counts equal section 6.3: 180 search jobs (16,000 trials plus 1,000 random forest trials), 560 window jobs (16,800 trials), 540 frozen fits (360 on campaigns 2 and 3 and the 180-fit refit of campaign 1), 480 category fits and 54 cross-workload fits.
2. `pipeline/training/freeze.py` applies the consensus rule of section 6.1, item 2. The protocol says ties go to the smaller model but not how a tie is broken in every case. The rules used are these. A numeric median that falls exactly halfway between two grid points takes the lower point. A tied categorical choice takes the smallest value when the choices are numbers, and the first choice listed in the search space when they are names such as `rbf` or `sigmoid`, which have no size order. Every tie is listed in `frozen/FREEZE.json`. The script refuses a study with fewer trials than the registered count. The author was told of these rules before the search finished.
3. `pipeline/training/cross_train.py` runs the cross-workload fits of section 4.3, item 2, with the model, training and scoring functions of `train.py` imported unchanged. The protocol fixes the R1 cross-train design, the shared features, the frozen configuration, the target scaler and 54 fits. The details below were chosen before any cross-workload fit ran, and the author accepted the recommended design on 17 October 2026.
   1. The shared features are the feature columns present in the 30 s windows of all six campaigns, one list for every fit.
   2. Each fit uses the source split of section 4.3, item 1. Blocks 1 to 8 and the BASELINE train, block 9 is `hpo_val` for early stopping and block 10 is `cal`. Selection, imputation and scaling are fitted on the source's training NORMAL windows, as in every fit.
   3. The configuration is the source workload's frozen one. The seed is 1000 times the campaign index plus 42, with fold 0 standing for this split.
   4. Each fit scores the test block of every fold of the other five campaigns. For target fold k, imputation and scaling are refitted on that fold's training NORMAL windows over the source's selected columns. That is the target scaler of section 4.3, item 2. The selection stays the source's, so the model sees the columns it was trained on.
   5. The within-workload control is the same fit scored on the other two campaigns of its own workload. It has the same features, configuration and target scaler, so the two arms differ only in the workload the model was trained on. R1 scored its control on the source's own training windows.
   6. For C6, per target campaign and test block, the within-workload value is the mean pooled AUC of the two same-workload sources and the cross-workload value is the mean of the three other-workload sources, both on the same windows. Their difference gives ten paired values per campaign, pooled over campaigns 2 and 3 as targets (section 5.3), with campaign 1 reported beside them. The random forest is reported as a reference and is not part of C6.
4. Section 7.3, item 1 asks for training in a container pinned by digest. The image in `pipeline/training/image` installs CPython 3.14.8 and 54 packages from a hash-locked `requirements.lock`, and records torch 2.12.1+cu130, CUDA 13.0 and cuDNN 9.20.0 (image ID `sha256:0a30da99b6f0945b407e810d57745c24a26ebfba2c3eb79cb08bb28c1c61c000`). Code is mounted read-only from a `git archive` of one commit, so a run is pinned by the image ID and the commit. A first run in a venv with the same package versions started at 18:28 and was stopped at 21:11 with 322 of 740 jobs done, when the author chose to run all training in the image. Its results are kept only as a cross-check and are never analysed. On ec-a fold 1 the image reproduced that venv run exactly for Isolation Forest (all 100 trials and every window score) and the random forest (every parameter set and window score, one of 50 validation AUCs off by 3.6e-05), and on the first three CUDA trials of DAGMM and LSTM-AE. The full matrix runs in the image from 21:11.

The pipeline tests pass with these additions (148 of 148, the 145 registered tests and 3 for the cross-workload fits). The paper reports D2 in its deviations appendix.

## D3. The sequence models give no score at 300 s on IoT (18 October 2026)

This is a finding, not a change. No code, config, plan or lock changed. It touches only the window-size sensitivity on campaign 1 (section 6.1, item 5), which feeds exploratory statements only.

The registered window analysis fits every model at 300 s on iot-a. The six sequence models, LSTM-AE, LSTM-VAE, Transformer-AE, CNN-AE, Deep SVDD and DAGMM, cannot score any window there. A sequence is a run of consecutive windows from one attempt's PRE_FAULT, INJECTION and RECOVERY, or from one baseline stretch (section 4.1, item 7). Its length is searched on 5 to 11 windows (decision 27). At 300 s each of the 80 iot-a attempts holds 3 or 4 windows (54 hold 3 and 26 hold 4), so no sequence of 5 windows fits in any attempt of `hpo_val`, `cal` or the test block. The 24 baseline stretches hold 12 windows each, so the models could train but had nothing to score.

In run c1-20261017T2111Z all 60 of these jobs (six models, ten folds) ran their 30 trials. Every trial returned 0.0, the value `train.py` returns when no validation sequence exists. The final fit then printed "No test scores" and wrote no summary. The driver's retry gave the same result from the same finished study, so it marked the 60 jobs as failed. They are the only jobs that did not finish in either training run.

Decision 35 foresaw part of this. It notes that the pre-fault holds 2 windows at 300 s, so the sequence models cannot score the full within-run set there. It did not foresee that they cannot score any window at all. The window analysis reports the six sequence models at 300 s on IoT as not estimable, with these counts, and compares only Isolation Forest and OC-SVM at that size.

Those two models scored all ten folds at 300 s, but each test block holds only 2 or 3 ANOMALOUS windows among 26 to 28. Their fold AUCs range from 0.93 to 1.00 for Isolation Forest and from 0.06 to 0.89 for OC-SVM, so the paper reports this point as low-powered. The paper reports D3 in its deviations appendix and in the response to the reviewers.

## D4. Analysis code added after registration, and the choices it makes (18 October 2026)

The registered protocol defines the metrics (section 4.5), the statistics (section 5) and the hypotheses (section 5.8), and names `make metrics` and `make stats` (section 7.1), but the registered code holds no program for them. The code in `pipeline/analysis` was written on 18 October 2026, after both training runs had finished. It reads stored scores only and trains nothing. It was committed and tagged before any contrast of C1 to C6 was computed on campaigns 2 and 3. Three things about campaigns 2 and 3 had been seen before that commit. They are the ranges of the fold AUCs over all phase B fits when the run was checked (campaign log, 18 October 05:15), the max-z AUC of one ec-b fold in a smoke test, and the first two rows of one cross-workload AUC file (Isolation Forest trained on ec-b and scored on ec-a). The author accepted the choices below on 18 October 2026.

1. The references that need no training (sections 4.4 and 6.3) come from `analysis/baselines.py`, which uses the fold data of `train.py` unchanged.
   1. Max-z is the RuleBased model of `train.py`. On ec-b fold 1 it gives exactly the scores of `train.py --model RuleBased`.
   2. EWMA is `score_ewma` of `classical_baselines.py` at the registered commit, with its code unchanged. It gets the same per-fold features as max-z and every panel model, with selection, imputation and scaling fitted on the fold's training NORMAL windows. R1 gave it every column, with gaps set to 0 and its own scaler. The author chose the panel's features so that C2a and C2b differ only in the score function.
   3. The C2b comparator moves only the centre, to the training NORMAL mean (`ewma_mechanism.py`, variant `centre_train_mean`).
   4. Time-only is the window's start time. Phase position is the seconds since the start of the attempt's PRE_FAULT segment, so it has no value on TRAILING windows.
2. Metrics (section 4.5) come from `analysis/metrics.py`. Labels, roles and within-run tags come from the preprocessing outputs, and every score file is checked against them.
   1. Thresholds follow `train.py`. A fixed budget b is the (1 minus b) quantile of the cal block's NORMAL scores as numpy computes it, the F1-optimal threshold is `find_optimal_threshold`, and a window is in alert at or above the threshold.
   2. PR-AUC is average precision. The R1 placebo is the mean over the block's seven fault runs.
   3. An alert onset is an alerted window whose predecessor in the same contiguous stretch is not in alert or has no score. False alerts per hour are the onsets on negative windows divided by the negative windows' time.
   4. Latency uses the convention of `train.py`. It is the index of the first alerted window of the labelled span times 30 s, censored at the span's length, at both fixed budgets. Kaplan-Meier summaries pool a campaign's 70 fault runs and also run per fault, restricted at the longest labelled span in the group.
3. Statistics (section 5) come from `analysis/stats.py`.
   1. The exact Wilcoxon p enumerates all 1,024 sign vectors, with zero differences dropped and tied magnitudes at mid-ranks.
   2. The BCa interval uses 10,000 resamples of the test blocks with seed 20261018.
   3. With ten folds in every campaign, REML has the closed form of the one-way estimator truncated at zero. A test checks it against statsmodels.
   4. Sign flips are exact over all 2^20 vectors for two campaigns and 1,000,000 random vectors with seed 20261018 for three.
   5. Superior means a lower 95% bound above 0 and a Holm-adjusted p below 0.05. A two-sided contrast (C2a, C3 and C4) can also end inferior. Replication (section 5.5) is judged for the outcome the pooled estimate reaches.
4. The Holm family of each workload (section 5.7.1) has seven members, C1, C2a, C2b, C3, C4, C5 and C6. The text counts six hypotheses, and the author chose to include C2b so that every confirmatory superiority claim is adjusted. Each member enters with the p of its superiority test.
   1. For C1, a deep model's p is the larger of its two metric p-values, or 1 unless both estimates are above 0. C1 enters with the smallest of these after Holm over the six models.
   2. For C2a, the p is the larger of its two metric p-values, or 1 if their signs differ.
   3. C2b, C3 and C4 enter with their pooled p. C5 enters with its permutation p, in both families.
   4. For C6, each panel model's p is its pooled p, or 1 unless its estimate is above 0. C6 enters with the smallest of these after Holm over the eight models.
   5. Non-superiority and equivalence are intersection-union tests at 0.05 with no adjustment (section 5.7, items 2 and 3).
5. The hypotheses come from `analysis/confirm.py`.
   1. C3 compares pooled AUC on the test windows that all eight panel models score in the fold, because a sequence model does not score the first windows of each stretch. Within-run AUC needs no matching, because every model scores the same within-run negatives and every ANOMALOUS window.
   2. C4 learns the fusion weights as `rq3_fusion_corrected.py` does, with a logistic regression on the four category scores of the other nine folds' test windows, balanced classes and seed 42.
   3. C5 and C7 use eleven detectors, the eight panel models, the random forest, max-z and EWMA. The C5 p is one-sided, the share of the 5,040 orderings whose median rho is at least the observed one.
   4. C6 follows D2, item 3.6. Pooled AUC is computed from the score files, and every value is checked against the fit's `auc.csv`.
6. The analysis environment is `analysis/requirements.lock`, with hashes. Every package it shares with the training lock has the training lock's version, and it adds matplotlib and statsmodels. `analysis/baselines.py` imports `train.py` and runs in the training environment. The other analysis steps need no PyTorch. `analysis/metrics.py` holds copies of two functions of `train.py`, and a test checks they are identical.

The paper reports D4 in its deviations appendix.
