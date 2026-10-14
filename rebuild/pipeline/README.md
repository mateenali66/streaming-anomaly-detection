# Preprocessing pipeline for the rebuilt benchmark

This folder turns one campaign, as the orchestrator and exporter leave it in `campaign/<campaign_id>/`, into labelled windows, features and fold roles. It implements sections 3, 4 and 11 of `diagnostics/REBUILD_PROTOCOL.md`. Nothing here talks to AWS. The input is a local copy of the campaign prefix.

The package is `streamprep`. The training entry point adapted from `sagemaker/training.py` is in `training/`. A synthetic campaign generator writes contract-conformant inputs, and the test suite runs on them.

## Requirements

Python 3.12 or later with numpy, pandas and pyarrow for the preprocessing, and pytest for the tests. Training also needs scikit-learn, scipy, optuna and PyTorch, the same as R1. The tests were run with Python 3.14.8, numpy 2.4.6, pandas 2.3.3, pyarrow 24.0.0, scikit-learn 1.9.0, optuna 5.0.0 and torch 2.12.1.

## Running it

All commands run from this folder.

```bash
# Check a campaign against section 11 without writing anything
python3 -m streamprep validate --campaign-dir /data/campaign/iot-b

# Build one campaign end to end (labels, features, folds)
python3 -m streamprep build --campaign-dir /data/campaign/iot-a --out /data

# A replicate campaign (index 2 to 5) needs the freeze marker for features and folds
python3 -m streamprep build --campaign-dir /data/campaign/iot-b --out /data --freeze-marker /repo/frozen/FREEZE.json

# One stage at a time, as in protocol 7.1
make labels   CAMPAIGN=/data/campaign/iot-b OUT=/data
make features CAMPAIGN=/data/campaign/iot-b OUT=/data FREEZE=/repo/frozen/FREEZE.json
make folds    CAMPAIGN=/data/campaign/iot-b OUT=/data FREEZE=/repo/frozen/FREEZE.json

# Compare outputs with hashes committed earlier (3.4.5). A mismatch stops the build.
make labels CAMPAIGN=/data/campaign/iot-b OUT=/data CHECK=/repo/derived-hashes/iot-b

# Write a synthetic campaign and run the tests
python3 -m streamprep synth --out /tmp/synth/campaign --truth /tmp/synth/truth
make test
```

A build that breaks a rule exits with status 2 and prints every problem it found in that phase. It never writes a partial output set for the failing stage.

## Outputs

Files go to `<out>/derived/<campaign_id>/`, each with a `.sha256` file beside it that holds the hash and a newline.

| File | Content |
|---|---|
| `timeline.parquet` | One row per segment, 11.9 columns. The build fails unless the rows cover the timeline exactly. |
| `labels_<w>s.parquet` | The key and meta columns of 11.9 for every window. These depend only on the plan and manifest, so `make labels` can hash them before any feature exists. |
| `windows_<w>s.parquet` | The same key columns, then the `meta__` columns, then every `f__` feature column in sorted order. |
| `folds.parquet` | `window_id`, `fold`, `role` for every window of every window size, ten rows per window. |
| `build_report.json` | Input hashes, output hashes, counts per segment and label, feature counts, telemetry summaries and warnings. It holds no timestamps, so it is deterministic too. |

Window sizes follow 11.9. Every campaign gets 30 s. The search campaign (index 1) also gets 15, 60 and 120 s, plus 300 s on IoT.

## How the build works

1. `loader.py` checks `COMPLETE`, every hash in `checksums/SHA256SUMS`, every object name against the 11.7 layout, the plan (canonical bytes, seed rule, draws regenerated from the seed), the manifest, every attempt record, every verification record, the baseline statistics and the export file sets. JSON readers reject unknown fields, missing fields, NaN and duplicate keys.
2. `timeline.py` builds one segment table from the campaign segments and every attempt's segments, in integer milliseconds. It fails if any instant is unassigned or assigned twice, and checks the per-second coverage a second way. EXCLUDED is the default label. Only the 3.3 table gives any other.
3. `windows.py` lays a grid on each PRE_FAULT, INJECTION, RECOVERY, BASELINE and TRAILING segment, anchored as 3.2 says, and drops the partial window at the free end. Windows of failed attempts exist with label EXCLUDED.
4. `metrics_io.py` reads the exported feature files only. It checks the 11.8 layout, sort order, spans and `series_id`, removes the duplicate samples at export boundaries, and fails if two files disagree on a sample.
5. `features.py` turns counters into per-second rates (a negative step is a reset and is dropped), maps series to columns, aggregates TaskManagers by role into `tm_min`, `tm_max` and `tm_mean`, adds the TaskManager count, computes the eight R1 derived features per 10 s step, and takes six statistics per column over each window's steps. `lag_velocity` and `lag_acceleration` difference two steps only when both lie in the same segment.
6. `telemetry.py` recomputes the 3.4.3 rule, scrape health (decision 21, CONTRACT_CHANGES C22), from each valid attempt's exported verification file (`up` and `kube_pod_info`) with the gate's rule and the registered parameters of `run/thresholds.json` (`streamprep/health.py`). A disagreement with the verification record's `scrapes` rows, or an untargeted target with more than 3 missed scrapes, stops the build. Series presence decides nothing; it stays in the record and the SERIES_GAP flag.
7. `labels.py` applies the NORMAL-time health rule (decision 24, C26): a BASELINE or TRAILING window whose verification-channel evidence (the window plus 10 s on each side) shows a job not RUNNING or with a new job ID, fewer than three registered TaskManagers, a replaced or restarted pod, a TCP pair under 95% or a TaskManager with more than 2% packet loss, or no sample of a kind, is EXCLUDED. The counts per window size, segment and condition are in `build_report.json` under `normal_time_health`.
8. `timeline.py` adds SIGNATURE_FALSE_PASS to every window of a fault whose signature false-passed on two or more sham attempts (2.5, decision 22, C23). The manifest never carries that flag.
9. `folds.py` assigns roles by block with the R1 rotation. `fold_fit.py` fits selection, imputation and scaling on one fold's train NORMAL windows. `sequences.py` builds sequences of consecutive windows inside one `seq_group`.

## Configuration

Defaults are in `config/pipeline.json`. Pass `--config file.json` with only the keys to change.

| Key | Default | Meaning |
|---|---|---|
| `search_campaign_index` | 1 | Campaign that gets the extra window sizes and needs no freeze marker (6.1, 9.2) |
| `window_sizes` | 30, plus 15/60/120 (and 300 on IoT) for the search campaign | 11.9 |
| `export_duplicates` | `error` | What to do when two export files hold different values for one sample. `prefer_later` keeps the later file |
| `require_all_cells_valid` | true | A COMPLETE campaign must end every plan cell with exactly one valid attempt |
| `f6_apply_max_s` | 5 | APPLY limit for valid F6 attempts (1.3) |
| `labels.drop_before_naming` | `job_id`, `task_attempt_id`, `task_attempt_num`, `node` | Labels never used in feature names. See A1 below |
| `labels.tm_identity` | `subtask_index`, `tm_id`, `host` | Labels dropped before TaskManager aggregation, with the pod (11.9) |
| `counters.extra_patterns` | Flink cumulative values typed gauge, `_total`, `_bucket` | Series treated as counters besides `metric_type == counter` and histogram or summary `_sum`, `_count`, `_bucket` |
| `counters.exclude_feature_patterns` | `_currentInputWatermark$`, `_job_uptime$` | Clock-like levels kept out of the direct features. The watermark still feeds `watermark_lag` |
| `monotone_guard` | `error` when a gauge is non-decreasing on 99% of BASELINE steps with Spearman 0.99 against time | Stops a clock-like level from becoming a feature (Review 2, item 6) |
| `telemetry.attempt_min_coverage_frac` | 0.95 | Unused since C22; the scrape rule takes its numbers from `run/thresholds.json` |
| `telemetry.window_min_steps_num`, `_den` | 2, 3 | `meta__telemetry_ok` needs samples in two thirds of a window's steps (11.9) |
| `telemetry.feature_recompute` | `error` | Stop when a valid attempt fails the scrape-health rule (C22) on the exported verification files |
| `telemetry.baseline_outage` | `error` | Stop when a BASELINE window still labelled NORMAL holds no Flink data at all, because it would be NORMAL training data (windows the health rule EXCLUDED do not count, C26) |
| `derived.inputs` | R1 metric names | Metric, role and cross-series reduction behind each derived feature |
| `feature_chunk_columns` | 128 | Column sources processed together, which bounds memory |

The training configuration is `training/config.json`. Model seed is `seed_campaign_multiplier * campaign_index + seed_base + fold` (6.2.2), and the Optuna sampler gets the same seed. Trial counts follow 6.1 and 6.3.

## Training

```bash
python3 training/train.py --model IF --windows /data/derived/iot-a/windows_30s.parquet \
    --folds /data/derived/iot-a/folds.parquet --output /data/results/iot-a
```

The model classes and the `train_*` functions are the R1 code, with `random_state=42` becoming `MODEL_SEED`. Review 4 changed the scoring (decision 26) and the F1 threshold search (MIN-8): Isolation Forest and OC-SVM use one min-max map per fold fitted on train NORMAL scores, DAGMM's non-finite energies take the train maximum, Deep SVDD's degenerate check runs on train, and the F1 threshold is searched over the observed cal scores. `test_model_code_is_the_r1_code_or_the_pinned_rebuild_code` compares the unchanged functions with `sagemaker/training.py` and pins the SHA-256 of the changed ones. `rebuild/review4_fixes/B_pipeline.md` lists the exact differences from R1. Feature selection settings (`fold_fit`) and the evaluation sets (`evaluation`: sequence length 5 to 11, the within-run negatives) come from `config/pipeline.json`, and every score file carries the `within_run` tag. Optuna studies are keyed by window size, held-out fault, derived switch and windows-file hash, and a stored study with another identity is refused. The search maximises pooled AUC-ROC on `hpo_val`. Early stopping uses `hpo_val` NORMAL windows only. Sequences are contiguous and there is no other mode. Semi-supervised models fit on train NORMAL, the random forest on train NORMAL and ANOMALOUS. Per-window scores for test, cal and trailing windows go to `scores/<model>_fold<k>.parquet`. A replicate campaign needs `--freeze-marker` and `--frozen-params`. `STREAMPREP_DEVICE=cpu` forces the CPU.

## Review findings, code and tests

The first column quotes `diagnostics/REVIEW2_PIPELINE.md` or the task list for this rebuild.

| Finding or rule | Code | Tests |
|---|---|---|
| Time outside completed runs became NORMAL training data (item 1) | `timeline.build_timeline`, `timeline.check_coverage`, `contract.label_for` | `test_timeline.py` (all), `test_normal_only_from_baseline_and_valid_prefault` |
| Every second in exactly one segment, EXCLUDED by default, build fails if unassigned | `timeline.check_coverage`, `timeline.check_per_second` | `test_every_second_covered_exactly_once`, `test_excluded_is_the_default_label`, `test_unassigned_second_fails_the_build`, `test_second_assigned_twice_fails_the_build` |
| NORMAL only from the leading baseline and valid pre-fault | `contract.label_for` | `test_normal_only_from_baseline_and_valid_prefault`, `test_known_counts_at_30s` |
| Trailing baseline went to train | `folds.assign_folds`, `contract.label_for` | `test_trailing_is_its_own_label`, `test_baseline_train_and_trailing_eval_only_in_every_fold` |
| Sham injection windows became ANOMALOUS | `contract.label_for`, `run_type` column | `test_sham_windows_get_their_own_labels` |
| Invalid, retried, interrupted and TELEMETRY_FAILED attempts excluded whole | `contract.label_for` (outcome only), `loader._check_attempts` | `test_failed_attempts_excluded_whole`, `test_retried_attempt_excluded_and_retry_labelled` |
| Label edges from the wrong timestamps on a global grid (item 3) | `windows.grid_for_segment`, `loader._check_attempt_segments` | `test_grids_anchor_at_their_segment_and_never_straddle`, `test_exact_window_counts_per_segment`, `test_anomalous_windows_equal_span_over_30` |
| RECOVERY could hold an overrunning fault | `loader._check_valid_attempt_timing`, `windows.grid_for_segment` | `test_overrun_extends_injection_and_never_leaks_into_recovery`, `test_overrun_flag_must_match_the_span` |
| Stuck chaos resource carries a live fault into later windows (item 4) | `loader._check_resources` | `test_stuck_resource_into_next_attempt_is_an_error`, `test_sham_with_resource_is_an_error`, `test_resource_names_follow_11_2` |
| Resume loses or duplicates runs, keys not unique (item 5) | `loader._check_attempts` (keys, run_ids, numbering, plan order, reserve gaps) | `test_duplicate_run_id_is_an_error`, `test_attempt_key_must_name_this_campaign`, `test_repeats_consume_reserve_gaps_in_order`, `test_repeat_uses_the_reserve_settle_gap`, `test_two_valid_attempts_for_one_cell_is_an_error`, `test_attempt_keys_and_window_ids_unique_across_campaigns` |
| Counter levels acted as clocks (item 6) | `features.counter_mask`, `features.rate_values`, `features.monotone_levels` | `test_counter_rates_match_truth_across_resets`, `test_counter_names_become_per_s`, `test_clock_like_gauge_stops_the_build` |
| TaskManagers averaged with no min, max or count, gaps imputed silently (item 6) | `features.map_columns`, `features.compute_features`, meta columns | `test_role_aggregation_matches_raw_min_max_mean`, `test_taskmanagers_reporting_counts_pods`, `test_telemetry_meta_matches_sample_presence`, `test_telemetry_flags_set_during_gaps` |
| TELEMETRY_FAILED not implemented (item 6) | `loader._check_verifications`, `telemetry.recompute_attempt_telemetry` | `test_valid_outcome_with_failing_telemetry_record_is_an_error`, `test_telemetry_failed_outcome_needs_failing_telemetry`, `test_valid_attempt_with_untargeted_gap_in_features_fails` |
| Same-name series overwrote each other silently (item 6) | `features.map_columns`, collision check in `features.compute_features` | `test_two_series_on_one_name_stop_the_build`, `test_jobmanager_series_keep_one_column_across_the_redeploy` |
| `lag_velocity` differenced across run boundaries | `features.derived_step_columns` | `test_derived_features_never_difference_across_segments` |
| Exports concatenated without deduplication, failures exited 0 (item 9) | `metrics_io.read_feature_files`, `loader._check_complete_and_checksums`, `loader._check_exports` | `test_checksum_mismatch_is_an_error`, `test_unlisted_local_file_is_an_error`, `test_object_outside_the_11_7_layout_is_an_error`, `test_injector_log_only_for_f6`, `test_abandoned_campaign_is_refused`, `test_outputs_do_not_depend_on_input_parquet_encoding` |
| Feature selection fitted on all NORMAL windows (item 11) | `fold_fit.FoldTransform`, `training/train.py prepare_fold_data` | `test_selection_uses_only_train_normal`, `test_selection_is_refitted_per_fold` |
| Fold roles leak, test-block windows in train, cal or hpo_val (items 1 and 11) | `folds.assign_folds`, `training/train.py validate_data` | `test_no_fold_leakage`, `test_role_rotation_matches_r1_table5`, `test_every_window_has_one_role_per_fold` |
| Sequences were not contiguous by default (item 11) | `sequences.sequence_index`, `training/train.py build_sequences` | `test_training_sequences_are_contiguous_normal_windows`, `test_eval_sequences_cross_only_the_apply_gap`, `test_sequences_respect_the_member_mask` |
| Verification and probe series must not be features (4.1.6) | `metrics_io` reads feature files only | `test_verification_and_probe_series_never_reach_features` |
| Search optimised F1 (decision 18) | `training/train.py` objectives | `test_trains_isolation_forest_and_an_autoencoder_in_under_a_minute` |
| Replicates read before the freeze (6.1) | `build._freeze_ok`, `training/train.py run_training` | `test_replicate_campaign_needs_the_freeze_tag`, `test_replicate_campaign_requires_freeze_and_frozen_params` |
| Outputs deterministic and hashed (3.4.5, 7.4) | `outputs.write_parquet`, canonical series order in `metrics_io` | `test_outputs_are_deterministic`, `test_sidecars_hold_the_file_hash`, `test_committed_hashes_are_checked`, `test_windows_schema_and_no_nan` |

Items 2, 7, 8 and 10 of Review 2 belong to the gate and the orchestrator. The preprocessing only checks what they record, for example that a valid attempt's recovery gate passed and that remediation matches the outcome.

## Contract ambiguities and how this code resolves them

Each of these needs a decision before registration. Where the code departs from the most literal reading it says why. The
integration of 13 October 2026 settled several of them in protocol section 11; the Id column names the item of
`rebuild/CONTRACT_CHANGES.md` where that happened.

| Id | Point in section 11 | Resolution here |
|---|---|---|
| A1 (accepted, C18) | 11.8 removes a fixed label list, and 11.9 appends every retained label that distinguishes series. Flink's `job_id`, `task_attempt_id` and `task_attempt_num` and the `node` label that the infra relabelling adds are not removed. Read literally, every job restart or resubmission creates new columns, names differ between campaigns, and node IPs enter names. This contradicts 4.1.3 (a replaced pod creates no new columns). | Not literal. These four labels are dropped before naming (`labels.drop_before_naming`). Setting the list to empty gives the literal behaviour. Recommend adding them to the 11.8 removal list with a schema bump. |
| A2 | "Pod, subtask and TaskManager ID labels" for TaskManager aggregation. | The pod column, `subtask_index`, `tm_id` and `host`. The last two are already removed by the exporter. |
| A3 (accepted, C18) | "Two series mapping to one name fail the build." Read literally, a JobManager replaced by a full redeploy (part of the remediation path) fails every such campaign. | The build fails when two series share a name with samples at the same step inside a windowed segment, or when two different label sets sanitise to one name. Outside windowed segments the step is blanked and counted in the report. |
| A4 | Which labels "distinguish series". | Superseded by Review 4 MIN-11. The labels come from the fixed per-metric list `labels.naming` in `config/pipeline.json`, first matching rule wins, never from which labels vary inside one campaign. A feature metric with no rule, or two series that differ only in a label outside the list, fail the build. |
| A5 | Counters are named by `metric_type == counter`, but Flink reports cumulative counts (`numRecordsIn`, checkpoint counts, GC counts, Kafka client totals) as gauges. | Extra counter patterns in config, and a guard that stops the build on any gauge that only grows over the BASELINE. The watermark and job uptime are kept out of the direct features. |
| A6 (C20) | `SHA256SUMS` lists "every object except COMPLETE", which would include itself. | It excludes itself as well. |
| A7 (C16) | Keys in the manifest and verification records may or may not carry `campaign/<campaign_id>/`. Section 11 now says they are relative. | Both forms are accepted. |
| A8 | `gate.thresholds_sha256` has no stated source. | Read as the SHA-256 of `run/thresholds.json`. |
| A9 (C17) | `series_id` hashes "canonical JSON of {pod, labels}". `labels` could be the object or its canonical string. Section 11 now says the object. `series_id` is a file key only: `tests/test_series_identity.py` rewrites every volatile label, so every such series gets a new id, and the features do not change. | Either is accepted if every series uses the same form. The generator uses the object. |
| A10 | `seed_rule` could be the template or the substituted string. | Either is accepted. The generator writes the substituted one. |
| A11 | The pilot plan's draw count. | Draws for one block only. |
| A12 (C4) | Snapshot record times and condition times are marked non-null, yet an `APPLY_PLUS_10S` snapshot cannot hold a recover time. C4 also makes the per-fault values the orchestrator could not observe nullable (controller ids, F5 kill apply times, F6 counts and times, F8 sizes); a valid attempt must have them for its run type. | Null is accepted for those two fields only. |
| A13 | `tm_working_set_bytes` nesting. | Any object. |
| A14 | "Feature source" in `meta__samples__<source>` and `meta__telemetry_ok`. | The four 11.8 feature sources. One missing broker does not clear `meta__telemetry_ok`. |
| A15 | The rotation needs three blocks and the pilot has one. | The pilot gets fold 1 only, with block 1 as test and the baseline as train. With no hpo_val block there is nothing to search on, so `train.py` asks for `--frozen-params` on a pilot (from the pilot of 14 October 2026). |
| A16 | Index bases of `segment_index`, `seq_index` and the `<n>` in `baseline-<n>`. | All 0-based, like `campaign_index`. |
| A17 | Which windows exist. | Windows on PRE_FAULT, INJECTION, RECOVERY, BASELINE and TRAILING only (the 3.2 grids). Failed attempts' windows are kept as EXCLUDED with their block's role. |
| A18 | Which samples belong to a window. | Steps whose instant lies in [start, end). A step's value describes the 10 s before it, so a window's first step can reflect up to 10 s before its start. |
| A19 | Derived features are computed "inside one seq_group". The task asks for no differencing across any segment boundary. | Per segment, which satisfies both. |
| A20 (C21) | An ABORTED attempt's running segment extends to `ended_at`, so its list can end before GATE, against "GATE always". | ABORTED attempts may hold any prefix of the 11.5 order. |
| A21 | INJECTION start when something was applied but onset was never verified. | APPLY's end. INJECTION ends at `labelled_end_at` or `aborted_at`. |
| A22 (C2, C22) | `failed_checks` as "check names from the verification record". Section 11 now says the distinct failing check names; a telemetry failure is the TELEMETRY check `telemetry_scrape_misses`, and `result.telemetry` needs at least one `scrapes` row. | Must equal the set of failing check names, except for ABORTED attempts. |
| A23 (C23) | False passes "in two or more of a campaign's ten shams". A sham that passes a signature fails the gate and is repeated. | Counted over every F0 attempt that has a verification record, failed and aborted ones included. The windows of that fault's runs get SIGNATURE_FALSE_PASS from `timeline.py`; the manifest may not carry it, and a valid sham with a passing signature is refused. |
| A24 | Chunk boundaries. | Chunks restart at each segment's start, named by the floor of their first instant. Rows may lie within the chunk span rounded out to 10 s. |
| A25 | The freeze tag has no form in section 11. | A marker file passed with `--freeze-marker`. |
| A26 | Series of role `loadgen` or `other` that carry a pod have no scope in 11.9. | The build fails if one appears in the feature file. |
| A27 | Duplicate samples at export boundaries. | Kept once. Different values stop the build unless `export_duplicates` is `prefer_later`. |
| A28 | Infinite feature values. | Rejected like NaN. |
| A29 | Instants with more than millisecond precision. | Rejected. |
| A30 (C26) | NORMAL time must be healthy (decision 24). | BASELINE and TRAILING windows that fail the health rule are EXCLUDED (`labels.py`). A BASELINE window still NORMAL with no Flink data stops the build. Partial gaps are counted in the report. |

Rules added beyond section 11, each an error by default, are that every plan cell ends with one valid attempt, cells run in plan order, repeats use the reserve settle gaps in order, health events match the `HEALTH_EVENT` flag, the node inventory never changes, the start clock skew is under 1 s, a valid attempt ends with exactly the expected jobs RUNNING (terminal jobs in Flink's history do not count, C19), and every resource is confirmed NotFound before the next attempt. `recovery_gate.started_at` may be null only for an ABORTED attempt (C3).

## End to end with the orchestrator

`rebuild/integration/run_e2e.sh` dumps orchestrator dry runs against the simulated cluster (whose exporters now produce the real
label sets, Flink's gauge-typed counters, job restarts, scrape gaps during F5 and F7 and a JobManager redeploy) and runs
`validate`, `build`, a second build with `--check-against` and its own assertions on six scenarios. See that script.

## What needs pilot data

1. The real label set of each exported series, to confirm A1 and the resulting column names and count.
2. Which Flink and Kafka client series are cumulative, and what the monotone guard reports on a real baseline.
3. Whether adjacent exports agree on the boundary step. An export queried right after `ended_at` may see an incomplete last step.
4. Whether scrape failures and pod restarts show as missing rows in the range query, not as values carried over by Prometheus lookback.
5. Scrape health during F4, F5, F7 and F8. The TaskManagers are those faults' own targets and are exempt in INJECTION and RECOVERY (C22). The pilot shows how many scrapes they miss.
6. That a JobManager replaced by a redeploy leaves no overlap with the old series.
7. That the derived-feature metric names in `config/pipeline.json` match the real exporter output. They were taken from the R1 IoT export.
8. Run time and memory on real volume. The R1 IoT baseline export had about 1,100 series, which suggests about 27 million samples per campaign. A synthetic IoT campaign 1 at full length (3.5 million samples, 5,180 series, five window sizes) built in 3.5 s with 1.5 GB peak memory on an Apple Silicon laptop.
9. The share of BASELINE windows that fail `meta__telemetry_ok`.

## Tests

`make test` runs the suite on three synthetic campaigns. One is e-commerce campaign 1 with every scenario in `synthetic.full_scenario`. One is IoT campaign 2, a replicate built with a freeze marker. One is the IoT pilot. Expectations are recomputed from the raw exported rows or from the generator's truth file, not from the pipeline's own functions.
