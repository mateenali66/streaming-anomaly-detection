# Review 4 fixes, part B: preprocessing and training

13 October 2026. This covers CAM-4, CAM-5, MIN-6, MIN-7, MIN-8, MIN-9 and MIN-11 of `diagnostics/REVIEW4_DATA_VALIDITY.md`, under decisions 25, 26 and 27 of `diagnostics/REBUILD_DECISIONS.md`. Nothing ran on AWS and no network was used.

Files changed, all under `rebuild/pipeline/` unless stated.

- `config/pipeline.json` gains `labels.naming`, `fold_fit` and `evaluation`.
- `streamprep/features.py`, `streamprep/fold_fit.py`, `streamprep/windows.py`.
- `training/train.py` and `training/config.json`.
- New tests `tests/test_fold_selection.py`, `tests/test_feature_naming.py`, `tests/test_training_validation.py`, `tests/test_training_scoring.py`. Updated `tests/test_training_smoke.py`, `tests/test_outputs.py` and `tests/test_features.py`.
- `README.md` (the R1-code paragraph and row A4) and `diagnostics/REBUILD_PROTOCOL.md` sections 4.1, 4.4, 4.5, 5.4, 5.8 and 11.9.

Every new test was run on the unchanged code first. All 20 failed, then passed after the fix.

## CAM-4. The variance filter and silent TaskManager imputation (decision 25)

What changed. `FoldTransform` now reads `fold_fit` from `config/pipeline.json`. Columns that match `fold_fit.protected.patterns` are never removed by the variance filter or by the correlation filter, and they take no part in the correlation filter, which runs on the other columns exactly as in R1. The list holds 13 patterns.

- The TaskManager count, `f__flink__taskmanagers_reporting__`.
- The new TaskManager missingness features.
- The eight derived features, `f__derived__`.
- Under-replicated partitions, partitions under minimum ISR, offline partitions, ISR shrinks and ISR expands, and failed checkpoints, all named in the brief.
- Four more zero-in-normal indicators from `orchestrator/config/feature_allowlist.yaml`. Preferred-replica imbalance is 0 in steady state and rises after a broker restart. Dead-letter topic traffic (messages, bytes in, bytes out, produce requests) is zero because `EnrichmentJob.java` writes there only on a deserialisation failure. The same holds for records into its `dead-letter-sink` operator. Failed offset commits and consumer rebalances complete the list.

A column whose train NORMAL values are all equal after imputation keeps unit scale and is centred on that exact value, so train NORMAL maps to exactly 0. The test is `max == min`, not `std > 0`, because rounding in the mean of a constant float column gives a standard deviation near 1e-17 and would divide by it. A protected column that is NaN in every train NORMAL window cannot be imputed. It is still removed and the fold report lists it under `protected_removed_all_nan`.

Missingness. `features.py` adds `f__<source>__tm_aggregates_missing_frac__cluster__<stat>` for `consumer`, `flink` and `jvm`. At each 10 s step it is the share of that source's TaskManager aggregate columns with no finite value. A TaskManager aggregate is NaN at a step exactly then, and `fold_fit` fills a window statistic with the train median only when every step of the window is NaN. So every imputed TaskManager value sits in a window whose missingness feature is above 0. That is 18 new columns, all protected.

Tests. `tests/test_fold_selection.py` has four. Before the fix all six TaskManager-count statistics were dropped, there was no missingness feature, and `FoldTransform` had no protection.

Result on the synthetic e-commerce campaign, fold 1, 1,360 train NORMAL windows. These come from the R1 rules and the new rules on the same windows (`b_pipe/cam4_probe.py` in the scratch folder).

| | R1 rules | Rebuild |
|---|---|---|
| TaskManager-count statistics kept | 0 of 6 | 6 of 6 |
| Derived-feature statistics kept | 10 of 48 | 48 of 48 |
| Under-replicated partition columns kept | 0 of 18 | 18 of 18 |
| ISR shrink and expand columns kept | 0 of 36 | 36 of 36 |
| Columns after selection | 229 | 399 |
| F5 ANOMALOUS windows with a median-imputed `tm_*` column | 50 of 100 | 50 of 100 |
| Of those, windows whose selected missingness feature is above 0 | 0 | 50 |

168 of the 180 protected columns have train NORMAL variance under 0.01, and 71 columns are constant on train NORMAL. On the reviewer's simulated pilot dump the protected list matches 324 columns. Two patterns match nothing there, the dead-letter sink operator and failed commits, because the simulated cluster does not export those series. The fold report lists unmatched patterns.

One choice to confirm. Decision 25 names the variance filter only. I also exempted protected columns from the correlation filter, because protocol 4.1.5 says the derived features stay, and a correlated, higher-variance raw column would otherwise remove a derived statistic. Setting `fold_fit.protected.exempt_from` to `["variance"]` gives the narrower reading with no code change.

MIN-5. With these counters kept, the F6 injector's pre-onset traffic would have become an F6-only precursor in NORMAL windows (request rates on the brokers, and dead-letter traffic if any record went out early). Decision 28 removes that traffic in the orchestrator, so nothing in this part depends on it.

## CAM-5. Scores scaled on the scored set (decision 26)

What changed in `training/train.py`.

- `fit_score_scaler` fits a min-max map once per fold on the negated `decision_function` of the train NORMAL windows. `score_isolation_forest` and `score_ocsvm` apply it to every split, including the Optuna validation scores. Isolation Forest and OC-SVM model pickles now hold `{"model", "score_scaler"}`.
- `get_training_energy_stats_dagmm` also returns the largest finite train energy, and `score_dagmm` gives that value to every NaN or infinite energy.
- `get_training_dist_stats_svdd` also returns whether the encoder is degenerate on the train NORMAL encodings. `score_deep_svdd` sets every score to 1.0 when it is and never decides per batch.
- `tests/test_training_smoke.py::test_model_code_is_the_r1_code_or_the_pinned_rebuild_code` replaces `test_model_code_is_the_r1_code`. Unchanged functions must still equal R1 except for the seed. The eight changed or new functions are pinned by the SHA-256 of their source, so any later edit fails the test.

Tests. `tests/test_training_scoring.py` trains each model on fold 1 and recomputes every saved test, cal and trailing score of Isolation Forest and OC-SVM from the train NORMAL map. Before the fix they did not match, since each split was mapped onto [0, 1] by itself. Two stub-model tests cover DAGMM and Deep SVDD.

Result, from probe 2 of the review rerun on both versions (`b_pipe/cam5_probe.py`). It shows the realised false-positive rate at the 1% and 5% budgets on the same 400 NORMAL test rows, with 0 or 10 anomalies added to the test block.

| Test block | R1 | Rebuild |
|---|---|---|
| NORMAL only | 0.007 / 0.043 | 0.013 / 0.055 |
| Plus 10 anomalies at shift 6 | 0.000 / 0.000 | 0.013 / 0.055 |
| Plus 10 anomalies at shift 12 | 0.000 / 0.000 | 0.013 / 0.055 |

### Exact scoring differences from R1, for the paper

1. Isolation Forest and OC-SVM. R1 scored a set X as `MinMaxScaler().fit_transform(-d(X))`, so each scored set (test, cal, trailing) was mapped to [0, 1] by its own lowest and highest window. The rebuild scores `(-d(x) - a) / (b - a)`, where `a` and `b` are the minimum and maximum of `-d` over the fold's train NORMAL windows (`b - a` becomes 1 if it is 0). The map is the same for every split and the scores are not clipped, so they can lie outside [0, 1]. Pooled and within-run AUC inside one scored set are unchanged. Thresholds, budgets, false-alert rates, latency and C4 fusion are affected.
2. DAGMM. R1 replaced a NaN or infinite sample energy with the largest finite energy of the set being scored. The rebuild uses the largest finite energy over the train NORMAL sequences. If no train energy is finite, the replacement is +inf and the score is 1. The z-score still uses the train mean and standard deviation, as in R1.
3. Deep SVDD. R1 computed the mean per-dimension variance of the latent codes within each scoring batch of 256 sequences, and set the whole batch's scores to 1.0 when it was below 1e-6. The rebuild computes the same statistic once over all train NORMAL sequences. If it is below 1e-6 every score of that fit is 1.0, and otherwise no score is overridden.
4. F1-optimal threshold (MIN-8, below). R1 searched the grid 0.01 to 0.99. The rebuild uses every distinct observed cal score.

The model classes, the `train_*` functions, the autoencoder and VAE scoring (including their per-window degenerate check, which depends on the window alone) and the random forest and rule-based scoring are the R1 code, with only the seed changed.

## MIN-6. `validate_data` checked only the test block

What changed. `validate_data` now fails on any of these.

- An unknown role, an attempt or a block with windows in more than one role.
- A role among test, cal and hpo_val that does not hold exactly one block. The one-block pilot must hold one test block and no cal or hpo_val block (README A15).
- A BASELINE window outside train, a TRAILING window outside eval_only, or eval_only holding something other than TRAILING.
- No ANOMALOUS window in test, hpo_val or (outside the pilot) cal.

Test. `tests/test_training_validation.py::test_validate_data_checks_every_role` builds ten corrupt fold tables from the synthetic campaign. The old code missed six. These were a cal ANOMALOUS window moved to train, an hpo_val attempt split with cal, BASELINE in test, BASELINE in cal, TRAILING in train, and two cal blocks. Now all ten fail and the clean table passes. The reviewer's earlier ten-block simulated dump (`iot1`, five window sizes) passes every fold and size, and `integration/check_e2e.py --train-validate` on it reports 0 failed checks.

## MIN-7. Optuna studies reused across analyses

What changed. The study name is `<campaign>_w<w>_<model>_fold<k>[_category]_lofo-<fault|none>_derived-<on|off>_win-<first 12 hex of the windows file hash>`, and the database is `<campaign>_w<w>_<model>[_category].db`. A new study stores its identity as user attributes. These are campaign, model, fold, category, window size, held-out fault, derived switch, the full windows and folds hashes, the `config/pipeline.json` hash, the sequence-length range and the objective. A stored study whose identity differs is refused with an error and never resumed.

Test. `tests/test_training_validation.py::test_optuna_studies_are_never_reused_across_analyses` runs Isolation Forest at 30 s, at 60 s, with `--exclude-derived` and with `--lofo-held-out F4` into one `--output`. The old code gave all four the name `ec-a_IF_fold1` and reused the first search. Now there are four studies with two trials each. The test then reruns 30 s, which resumes with no new trial, and edits the stored folds hash, which makes the rerun refuse.

## MIN-8. The RuleBased F1 grid stopped at 0.99

What changed. `find_optimal_threshold` takes every distinct observed cal score as a candidate, computes F1 for all of them at once from sorted scores, and keeps the lowest threshold with the highest F1, as R1's ascending grid did. With no positive window it returns +inf, which flags nothing.

Test. `tests/test_training_scoring.py::test_f1_threshold_is_searched_over_observed_scores`, on max |z| scores above 1, plus the RuleBased result of a real fold-1 fit. In the rerun of probe 2 the R1 threshold was 0.970 and flagged 99.8% of windows. The rebuild threshold is 4.053 and flags 4.8%.

## MIN-9. Sequence length and the within-run sets (decision 27)

What changed.

- `config/pipeline.json` `evaluation` holds `seq_length` 5 to 11, `within_run` with 5 late and 5 early windows, `anchor_window_s` 30 and `c5_time_reference` set to `position`.
- `windows.py` adds the key column `within_run` to `windows_<w>s.parquet` and `labels_<w>s.parquet`. It is `late` for the last 5 PRE_FAULT windows of every valid attempt (fault runs and shams), counted back from the planned onset, `early` for the 5 before them, and null elsewhere.
- `train.py` searches `seq_length` on 5 to 11 and refuses `--max-seq-length` above 11 or a frozen `seq_length` outside the range. It recomputes the tags from the configuration and refuses a windows file that disagrees. It copies `within_run` into every score file and stops at the 30 s anchor if a model does not score every tagged test window. `model_results.csv` records `within_run_negatives`, `within_run_expected` and `within_run_complete`.
- Protocol 4.1.7, 4.4.1, 4.5.2, 4.5.3, 5.4 and 5.8 (C5) now give these definitions. The C5 time reference is each fault's mean block `position`, over a workload's confirmatory campaigns, with Spearman rho between the two workloads' per-fault values.

Tests. In `tests/test_training_scoring.py`, `test_seq_length_is_searched_on_5_to_11`, `test_windows_tag_the_last_ten_pre_fault_windows`, `test_every_model_scores_the_same_within_run_negatives[IF]`, `[CNN_AE]` and `test_train_refuses_a_windows_file_whose_tags_disagree_with_the_config`. Before the fix there was no tag, and the search reached a length of 13.

Result. In the synthetic campaign every valid attempt has 20 pre-fault windows at 30 s. Isolation Forest and CNN-AE score the same 80 tagged test windows of fold 1, 10 for each of the block's seven fault runs and its sham.

## MIN-11. Feature names depended on labels varying inside a campaign

What changed. `labels.naming` in `config/pipeline.json` holds 18 rules matched on the Prometheus metric name, where the first match wins. A label in the rule that the series carries goes into the name in sorted key order, whether or not it varies. A feature metric with no rule fails the build. A label outside the rule that is not dropped stays part of the series identity, so two series that differ only in such a label fail the build instead of merging.

The rules follow the real exporter configs. These are `infra/k8s/base/kafka/kafka-metrics-configmap.yaml` (labels `topic`, `request`, `error`, `delayedoperation`, `client_id`, `partition`), the JMX exporter's JVM collectors (`area`, `pool`, `gc`, `state`), and the Flink reporter scopes (`job_name`, `task_name`, `operator_name`, plus `topic` and `partition` on KafkaSourceReader partition metrics). They also follow the series of `orchestrator/orchestrator/fakes/series.py`. On the reviewer's simulated pilot every feature metric found a rule and nothing collided. The Flink `task_id` and `operator_id` hashes are not in the names and stay as identity, so a changed hash fails loudly.

Tests. `tests/test_feature_naming.py` has four. The old code named the young-generation GC series `jvm_gc_collection_seconds_count_per_s` in a campaign with one collector, and `...per_s.gc-G1_Young_Generation` in one with two. It also added a varying unlisted label to the name and accepted a metric with no rule.

Visible effect. In the synthetic campaign the GC columns now carry `.gc-G1_Young_Generation`, and Flink task columns carry `job_name` and `task_name` but no longer `task_id`.

## Interface changes for the other agents

1. `windows_<w>s.parquet` and `labels_<w>s.parquet` gain the key column `within_run` (string, nullable) after `seq_index`. `check_e2e.py` could recompute it from the timeline.
2. `build.py` calls `build_windows(tl, campaign_id, workload, w)` without its configuration, so `windows.py` falls back to the default `config/pipeline.json`. A `--config` override of `evaluation.within_run` is therefore ignored today. `train.py` would then refuse the windows file, which is loud, not silent. Fix in `build.py`: pass `cfg` as the fifth argument.
3. 18 new feature columns, `f__{consumer,flink,jvm}__tm_aggregates_missing_frac__cluster__<stat>`.
4. Feature names now follow `labels.naming`. A newly allowlisted metric needs a rule there, or the build fails with a message naming the metric. `CONTRACT_CHANGES.md` C18 and README A4 described the old rule. I updated A4, not C18.
5. `training/config.json` no longer holds `variance_threshold` and `correlation_threshold`. `train.py` reads `fold_fit` and `evaluation` from `config/pipeline.json` and has a `--pipeline-config` option. A training config may still name the two thresholds, but only with the values `config/pipeline.json` gives. `--max-seq-length` now defaults to the configured 11.
6. Score files gain `within_run`. `model_results.csv` gains `window_s`, `within_run_negatives`, `within_run_expected`, `within_run_complete` and `study_name`. Optuna databases carry `_w<w>_` in their names.
7. Simulated cluster. In `fakes/series.py` the task counters `numRecordsIn` and `numRecordsOut` are the current rate times the seconds since the attempt started, so they fall whenever the load falls. On the reviewer's simulated pilot they fell in 354 of 480 pre-fault steps, and the preprocessing drops each such step as a reset. That leaves those six TaskManager columns NaN in most pre-fault steps and sets the Flink missingness feature to 0.24 there. Real Flink counters do not fall, so the fake should integrate the rate.

## Not fixed, or open for the author

1. Within-run sets above 30 s. Pre-fault holds 10 windows at 60 s, 5 at 120 s and 2 at 300 s. All are tagged, but a sequence model of length L cannot score the first L minus 1 windows of an attempt, and 120 s and 300 s have no `early` windows. `train.py` stops only at the 30 s anchor and records `within_run_complete` false elsewhere. At 15 s the rule works as at 30 s. Window-size results are exploratory and campaign 1 only (6.1.5). Defining within-run metrics there for sequence models is the author's call, and I did not invent a rule.
2. C5 time reference. Decision 27 says "block position". I wrote it into 5.8 as each fault's mean `position` and the Spearman rho of those values between workloads. Please confirm that reading.
3. Broker gaps. When a broker is down (F1) its `b<n>` columns are median-imputed with no missingness feature. Decision 25 covers TaskManagers only. Addressed in the follow-up below, as decision 25 extended to brokers, author to confirm.
4. Text outside sections 4 and 5 still describes the old design. Protocol 6.4 (option C and the sequence length of 11 for RQ2), 8.1 (RQ2 "5 to 20 range") and 8.3 ("full sequence lengths") need the author's or the protocol owner's edit, given decision 27. Addressed in the follow-up below. Decision 1 in section 10 still lists option C.
5. End-to-end scenarios. See the next section.

## Test counts and commands

Pipeline suite, from `rebuild/pipeline`. 106 tests passed before this work. With my changes alone, 126 passed in 82 s (20 new test cases, three tests updated). The final run, after the other agent's concurrent changes landed, gave 133 passed in 120 s. The extra seven tests are theirs.

```
<venv>/bin/python -m pytest tests -q
133 passed in 119.91s
```

End-to-end, from the paper folder.

```
ORCH_PY=python3.12 PIPE_PY=<venv>/bin/python \
  bash rebuild/integration/run_e2e.sh --work <scratch>/b_pipe/e2e7
```

1. Work directory `b_pipe/e2e6`, run while the other agent's tree was consistent. All six scenarios (ec0, iot1, full, fail, resume, remediation) passed with 0 failed checks. iot1 and full include "training/train.py validate_data passes for every fold and window size".
2. Work directory `b_pipe/e2e7`, the final run on the current tree. The other agent has since added eight scenarios and a ground-truth check (their MIN-1 fix). 12 of 14 scenarios pass. ec0 and iot1 fail one new check only, "no NORMAL, RECOVERY, SHAM or TRAILING window overlaps a ground-truth disturbance". The windows it names are RECOVERY windows of F2, F4, F7 and F8 where the simulated effect outlasts the labelled end. That is labelling and gate timing, outside these files. My only change to the windows table is the added `within_run` column, and the `validate_data` check passes in both scenarios.

Earlier runs `b_pipe/e2e1` to `e2e5` failed before reaching the preprocessing, because the orchestrator and loader were mid-edit by the other agent (a `KeyError` in `fakes/prom.py`, a `TypeError` in the F6 injector, then a new manifest field the loader did not know yet).

## Follow-up after the coordinator's review (13 October 2026)

### Decision 25 extended to brokers, author to confirm

F1 removes the target broker's series while it is down, and `fold_fit` then fills its `b<n>` columns with train medians. That is the silent fill decision 25 closes for TaskManagers. The change below applies the same rule to brokers. Decision 25 names TaskManagers only, so the author should confirm it.

What changed.

- `features.py` adds `f__broker__broker_series_missing_frac__b<n>__<stat>` and `f__jvm__broker_series_missing_frac__b<n>__<stat>` for b0, b1 and b2. At each 10 s step each is the share of that broker's columns of that source (broker metrics, or the JMX exporter's JVM collectors) with no finite value. It shares one helper with the TaskManager feature.
- `features.py` adds `f__broker__brokers_reporting__cluster__<stat>`, the number of broker identities with any sample at each step, the broker twin of the TaskManager count.
- These 42 columns are present in every campaign and protected in `config/pipeline.json`.

Two refinements apply to the TaskManager and the broker shares alike, so both use one construction.

1. A column counts as expected only from its first finite value in the campaign on. Before that it is neither present nor missing.
2. Columns matching `missingness.exclude_column_regex` stay features but are left out of every share. The list holds one pattern, the dead-letter topic's per-topic broker meters. The other agent's simulated cluster models Kafka's lazy meters (REVIEW4_LIVE_CONTRACT L3). A broker's dead-letter meters open at the first append to a partition it leads and close when it loses leadership. The dead-letter topic is written only during F6. So after an F1 or F2 on a broker, its dead-letter meters stay closed until the next F6. Counting them put b0's share above 0 in 40% of the simulated pilot's NORMAL windows, which is fault history leaking into NORMAL time. With the exclusion it is above 0 in 1 of 200 NORMAL windows, the first baseline window, where counter rates have no previous sample yet. Filling the raw dead-letter columns with the train median gives 0, which is the right value when the meter is closed.

Tests, all in `tests/test_fold_selection.py`, each run on the old code first.

- `test_down_broker_carries_missingness_and_it_survives_both_filters`. On every valid synthetic F1 attempt, the fault windows that overlap the target's gap carry a share above 0 for that broker on both sources, 0 for the other two brokers, and a positive value after the fold transform. The broker count falls to 2. All 42 columns are among fold 1's selected columns. It failed before because the columns did not exist.
- `test_a_series_that_first_appears_mid_campaign_is_not_missing_before_it_exists`. It failed on the first version of the share, which gave 0.5 before the second series existed.
- `test_lazily_created_meters_do_not_enter_the_missingness_share`. It failed because the exclusion did not exist.

Results.

- On the synthetic campaign, fold 1, the R1 rules would drop all 42 new columns. The rebuild keeps all 42. Selected columns rose from 399 to 441, and 222 columns are protected.
- On the simulated cluster (work directory `b_pipe/e2e10`, scenarios ec0 and fail), the F1 target is b0. Its share is 1.0 on both sources and the count is 2 in the two windows where it is down. Their transformed values are 7.13, with both filters fitted on the pilot's train NORMAL windows, and all the new columns are selected. In the other eight windows of the fixed 300 s F1 span the broker reports again, so the share is 0 and the count is 3. The other brokers stay at 0 throughout.

Protocol text. The coordinator limited protocol edits to 6.4, 8.1 and 8.3, so 4.1.3, 4.1.4 and 11.9 do not yet describe the broker features. Once the author confirms, the proposed text is as follows.

1. Append to 4.1.3: "The same holds per broker. For each broker identity and for the broker and JVM sources, the share of that broker's columns with no value at the step is a feature, and the number of brokers reporting is a feature."
2. In 4.1.4, add "the broker count, the broker missingness features" to the list of protected columns.
3. Append to the 11.9 paragraph on TaskManager series: "`f__broker__broker_series_missing_frac__b<n>__<stat>` and `f__jvm__broker_series_missing_frac__b<n>__<stat>` give the same share per broker identity, and `f__broker__brokers_reporting__cluster__<stat>` counts the broker identities with any sample at each 10 s step. In every missingness share a column counts from its first value in the campaign, and the columns of `missingness.exclude_column_regex` are left out."

### Protocol 6.4, 8.1 and 8.3

- 6.4 now says that option C fits the cap but decision 27 makes it unworkable as written. Its 300 s pre-fault holds 10 windows at 30 s. A sequence model of length L cannot score the first L minus 1 of them, so no sequence model scores all 10 within-run negatives and a length of 11 scores none. Option C would drop the six sequence models from the within-run analysis that C1, C3 and C5 use. The choice after the pilot is between A and B.
- 8.1, row RQ2, now gives the searched range of 5 to 11 windows and the reason for the cap, and says that option C would bring back R1's problem and is no longer workable.
- 8.3, row "Sequence models no better than point models", now says the retest uses lengths of 5 to 11, each scoring the same within-run negatives as the point models, and that option C is no longer workable.

Decision 1 in section 10 still offers option C. It sits outside the sections I was asked to edit.

### Counts after the follow-up

```
cd rebuild/pipeline
<venv>/bin/python -m pytest tests -q
142 passed in 101.20s
```

That is 133 from the first pass, 3 new tests of mine, and 6 the other agent added in the meantime.

End-to-end on the current tree (work directory `b_pipe/e2e11`): all 14 scenarios pass, with 0 failed checks. They are ec0, iot1, full, fail, resume, remediation, sham1, sham2, ctrlread, jobsend, underrun, f6term, prefault and basegap. In iot1 and full, "training/train.py validate_data passes for every fold and window size".
