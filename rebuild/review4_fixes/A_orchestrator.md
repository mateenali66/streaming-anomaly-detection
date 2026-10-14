# Review 4 fixes, part A: orchestrator, integration and the loader side

13 October 2026. This covers PIL-1, CAM-1, CAM-2, CAM-3, MIN-1, MIN-2, MIN-3, MIN-4, MIN-5 and MIN-10 of
`diagnostics/REVIEW4_DATA_VALIDITY.md`, L2 to L8 of `diagnostics/REVIEW4_LIVE_CONTRACT.md`, and the orchestrator side of B6,
C1, C6, M10 and M12 of `diagnostics/REVIEW4_OPERATIONS.md`. It also covers the requests of the infra report
(`C_infra.md`, "Changes needed from the other agents") and of the pipeline report (`B_pipeline.md`). Decisions 21 to 24, 28
and 29 of `diagnostics/REBUILD_DECISIONS.md` govern them. Nothing ran on AWS or on a cluster. The network was used for
documentation only.

Every interface change is written up as C22 to C40 in `rebuild/CONTRACT_CHANGES.md`, with its reason, and is marked
"(C<n>)" in the protocol where it applies.

## Files changed

Orchestrator, under `rebuild/orchestrator/`.

- `orchestrator/gate.py`, `recovery.py`, `campaign.py`, `injectors.py`, `f6_injector.py`, `thresholds.py`, `queries.py`,
  `schema.py`, `config.py`, `ids.py`, `flinkrest.py`, `exporter.py`, `runtime.py`, `cli.py`.
- New `orchestrator/health.py` (AWS Health events).
- `config/feature_allowlist.yaml` gains the verification-only `scrape` source (`up`, `scrape_samples_scraped`).
- The simulated cluster: `orchestrator/fakes/world.py`, `series.py`, `prom.py`, `kube.py`, `services.py`, `store.py`,
  `harness.py`.
- New `tests/test_review4.py` (45 tests). Updated `tests/test_gate_rules.py`, `test_attempt_flow.py`, `test_components.py`
  and `test_schema.py`.
- `README.md` (layout, sections 5 and 8).

Integration, under `rebuild/integration/`: `dump_dry_run.py`, `check_e2e.py`, `run_e2e.sh` (8 new scenarios) and
`plan_tool.sh`.

Pipeline, under `rebuild/pipeline/`, the modules this brief assigned: `streamprep/loader.py`, `telemetry.py`,
`timeline.py`, `contract.py`, `schema.py`, `synthetic.py`, `build.py`, and the new `streamprep/health.py` and
`streamprep/labels.py`. Tests in `tests/test_validation.py` and `tests/test_labels.py`. One test in B's
`tests/test_features.py` was adapted (see CAM-3). `README.md` steps 6 to 8, the configuration table, rows A22, A23 and A30,
and pilot question 5.

Documents: `rebuild/CONTRACT_CHANGES.md` C22 to C40, and `diagnostics/REBUILD_PROTOCOL.md`. Sections 2, 3 and 11 as the
brief asked (2.1.5, 2.3.4, 2.3.10, 2.4, 2.5, 2.6.5, 3.3, 3.4.3, 3.4.4, 3.4.6, 3.4.7, the 11 introduction, 11.4 to 11.8 and
11.11). The fixes also needed short additions in section 1, which I made and list here so the owner of section 1 can check
them: 1.3 (AFTER_RESUBMIT), 1.5.3 (settle before TRAILING), 1.6 items 7 and 8 and the remediation paragraph (dead-letter
source, per-pair probes, redeploy order, C40), and 1.7 (Health events).

## How the tests were checked against the original code

The new orchestrator tests were run against the pristine copy of the original code, in a scratch directory, with the
corrected simulated cluster. The original code halts at the startup gate on every run there, because the broker
dead-letter series it requires does not exist before the first write (L3). That alone fails every campaign test. So the
run was repeated with the simulation answering the original code's broker dead-letter query with 0, as the kind cluster
did. Then 42 of the 44 tests that existed at that time failed. Most failed on the property they test: the outcome
(VALID where GATE_FAILED is right), a check value, or the bucket name in the stored record. The rest failed because the
original code has no such record field or function (`scrapes`, `redact_log`, `strip_registry_host`, `full_path`,
`flink_job_settings`), or because it crashed on a changed interface of the simulation. The scratch copy needed three small
shims to run at all. The harness ran without the Health source, the simulation did not need the new remediation constant, and
it kept the old dead-letter meter and one-shot recovery for the tests that do not test them. The two that passed are the B6 parser test (the CLI already parsed the options, infra fixed the Job)
and the watchdog test (the selectors already ignored the watchdog, as infra said). The Health startup test, added last, was not part
of that run. The new pipeline tests were not run against the original pipeline.

## PIL-1. Telemetry from scrape health (decision 21, C22)

What changed. TELEMETRY_FAILED now comes from scrape health. A scrape target is a broker, JobManager or TaskManager pod
while kube-state-metrics shows it. A scrape is missed when Prometheus `up` for that pod is absent or 0 at a 10 s step. More
than 3 missed scrapes of one untargeted target in one PRE_FAULT, INJECTION or RECOVERY segment fail the attempt with the
TELEMETRY check `telemetry_scrape_misses`. The fault's own targets are exempt in INJECTION and RECOVERY: the target broker
for F1 and F2, every TaskManager pod for F4, F5, F7 and F8 (F5's replacements included). The verification record holds one
`scrapes` row per target and segment. Task-level series presence stays in the record as values and flags SERIES_GAP below
0.95. It never fails an attempt. The exporter writes `up` and `scrape_samples_scraped` into the verification file.

The preprocessing recomputes the rows from each valid attempt's exported `up` and `kube_pod_info` with the registered
parameters of `run/thresholds.json` (`streamprep/health.py`, `telemetry.py`). It stops the build when a recomputed row
differs from the record or an untargeted target breaks the rule. The loader checks `result.telemetry` against the
`scrapes` rows and SERIES_GAP against the presence rows, and refuses a thresholds file without the gate's rule parameters.

Tests. `test_untargeted_series_gap_is_a_flag_not_a_failure` reproduces probe 8 (one missed broker JVM series step inside
F8 used to give TELEMETRY_FAILED at 0.944. It is now VALID with SERIES_GAP). `test_untargeted_scrape_misses_decide_telemetry`
(3 misses VALID, 5 misses TELEMETRY_FAILED), `test_targeted_scrape_misses_are_exempt_in_injection_and_recovery`,
`test_f5_job_restore_running_into_recovery_is_not_a_telemetry_failure`, and two updated tests in `test_gate_rules.py`.
Pipeline: `test_valid_attempt_with_untargeted_scrape_misses_in_the_export_fails`,
`test_valid_outcome_with_failing_scrape_record_is_an_error`, `test_series_gap_flag_must_match_the_presence_rows`,
`test_thresholds_without_the_gate_rule_parameters_are_refused`. The e2e `fail` scenario now makes the first F2
TELEMETRY_FAILED through missed scrapes.

Result. All pass. One reading needs the author: I read "the fault's own targets" in decision 21 as every fault's targets,
F2, F4 and F8 included, not only the F1, F5 and F7 of the old rule.

## CAM-1. A sham false pass fails the gate (decision 22, C23)

What changed. A sham now has one SIGNATURE check `f0_signature_false_pass` per fault, and they are part of `result.gate`.
A sham whose signature passes is GATE_FAILED and repeated. The manifest never carries SIGNATURE_FALSE_PASS (it is out of
`MANIFEST_FLAGS`). The preprocessing counts false passes over every F0 attempt that has a verification record, failed and
aborted ones included. `timeline.py` adds SIGNATURE_FALSE_PASS to every window of a fault with two or more. The loader
refuses a valid sham whose signature passed.

Tests. `test_sham_false_pass_is_gate_failed_and_repeated` (probe 3: 50% burst loss in the sham span made its F7 signature
pass and the sham stayed VALID). Pipeline `test_signature_false_pass_is_never_a_manifest_flag`,
`test_valid_sham_with_a_passing_signature_is_an_error`, `test_fault_runs_flagged_after_two_sham_false_passes`. E2e
scenarios `sham1` (one false pass, flags nothing) and `sham2` (two false passes in a row halt the pilot, it resumes, and
every F7 window carries SIGNATURE_FALSE_PASS).

Result. All pass.

## CAM-2. Every loader rule on a valid attempt is a gate check (decision 23, C25)

What changed, item by item.

1. Controller after injection. The gate check `controller_read_after_injection` fails an F1 or F2 whose read after the
   labelled end is missing. The read is tried three times.
2. Flink jobs at the end. The snapshot is retried until it answers. After 15 minutes without an answer the campaign halts
   with FLINK_REST_UNREACHABLE_15_MIN. The ENVELOPE check `flink_jobs_at_end_running` holds the loader's rule.
3. Early end. TIMING check `end_underrun` fails an end more than 60 s before onset plus duration.
4. F6 log. `AttemptCtx.ensure_injector_log` writes `injector.log` on every path that created the Job: before the poison Job
   is deleted, on a halt, on SIGTERM and on abort (then with the `orchestrator_observed` line). The halt path stores it as a
   deferred object.

Beyond the four, C25 lists every loader rule on a valid attempt with the gate check that now holds it.

Tests. `test_failed_controller_read_after_injection_is_a_repeat`, `test_flink_job_list_at_end_is_retried_until_it_answers`,
`test_end_more_than_60_s_early_fails_timing`, `test_f6_attempt_stopped_mid_injection_keeps_its_injector_log`, and
`test_loader_accepts_every_campaign_the_gate_completes`. It injects all four defects into one simulated pilot, resumes
it after the F6 stop, and the preprocessing's loader, which runs every valid-attempt rule, accepts the completed campaign. E2e scenarios `ctrlread`, `jobsend`, `underrun` and `f6term`.

Result. All pass.

## CAM-3. NORMAL time must be healthy (decision 24, C26)

What changed. PRE_FAULT has five ENVELOPE checks: `pre_fault_jobs_running_same_id`, `pre_fault_taskmanagers_registered`
(three), `pre_fault_no_pod_change` (no UID or restart count change), `pre_fault_tcp_ok` (every pair at 95% or more) and
`pre_fault_packet_loss` (every TaskManager at 2% or less). The thresholds are registered under `gate.pre_fault`. For
BASELINE and TRAILING, `streamprep/labels.py` applies the same conditions per window from the verification channel (the
window plus 10 s on each side) and sets EXCLUDED on every window that fails. The build report gives the counts per window
size, segment and condition under `normal_time_health`. The BASELINE outage rule counts only windows still NORMAL.

Tests. `test_job_restart_inside_pre_fault_fails_the_envelope` (probe 5), `test_taskmanager_replaced_inside_pre_fault_fails_the_envelope`.
Pipeline `test_baseline_window_with_a_job_restart_is_excluded`, `test_baseline_window_with_a_replaced_pod_is_excluded`,
`test_trailing_window_with_a_failed_connect_is_excluded`, `test_full_campaign_baseline_and_trailing_are_healthy`. E2e
`prefault` and `basegap`. B's `test_verification_and_probe_series_never_reach_features` was adapted with the smallest
change. It now compares only the `f__` and `meta__` columns, with `feature_recompute` at warn. The test removes the
verification files from the export, and the scrape recompute and the health labels now read them.

Result. All pass.

## MIN-1. Onset and labelled end recomputed (C27)

What changed. The gate recomputes onset and labelled end from the stored chaos records and the F6 injector log
(`onset_from_records`, `end_from_records`, check `onset_end_sources`), with a 2 ms tolerance. So the offline gate catches a
moved time. `dump_dry_run.py` writes the simulated cluster's ground truth (every effect interval) into `summary.json`.
`check_e2e.py` asserts that no NORMAL, SHAM or RECOVERY window overlaps an effect, that every ANOMALOUS window lies inside
one, that every exclusion by the health rule lies near a real disturbance, and that window grids are anchored where 3.2
says.

One tolerance needs a word. Chaos Mesh event timestamps have 1 s precision, so a labelled end can sit up to 1 s before the
true end of the effect. `check_e2e.py` allows 1.0 s for that (`EVENT_PRECISION_S`) and keeps RECOVERY in the overlap check,
as protocol 1.3 item 5 asks. B's report raised this overlap.

Tests. `test_offline_gate_detects_a_moved_labelled_end` (probe 9). Every e2e scenario runs the ground-truth checks.

Result. All pass.

## MIN-2. AFTER_RESUBMIT (C28)

What changed. A remediation that resubmitted any job flags the next attempt AFTER_RESUBMIT and adds the same 600 s extra
settle as AFTER_REDEPLOY. The loader expects the extra settle for both flags, and 3.4.6 lists the new flag.

Tests. `test_resubmission_flags_and_delays_the_next_attempt`, pipeline `test_after_resubmit_adds_the_extra_settle`.

Result. Both pass.

## MIN-3. A settle before TRAILING (C29)

What changed. Before TRAILING the orchestrator runs the health gate, a fixed settle of `settle_max_s`, and the 600 s extra
of a pending AFTER_REDEPLOY or AFTER_RESUBMIT.

Test. `test_trailing_starts_after_the_health_gate_and_a_fixed_settle`. It passes.

## MIN-4 and M12. AWS Health events (C30)

What changed. New `orchestrator/health.py`. Infra's EventBridge rule writes the region's `aws.health` events to the
CloudWatch Logs group `/streaming-anomaly-detection/health`. The orchestrator reads it with boto3 `filter_log_events`, with
5 s and 10 s timeouts, every 10 minutes from inside its sleeps and only when the sleep has 60 s or more left. It reads once
more before the last manifest write, retried for 15 minutes. A poll error is logged and retried, never fatal. Each event is
kept with its time, service, type code, status, start and end, and a 16-hex hash of its ARN. The account fields and the
affected entities are never stored. Every attempt that overlaps an event is flagged HEALTH_EVENT, also after it finished.
`health_events_polled_until` records how far the record reaches.

One rule was added after the first version. The preprocessing refuses a manifest whose `health_events_polled_until` is null.
So one read must succeed at a start, before WARMUP, and at a resume while the field is still null. It is retried every 30 s
for 15 minutes, then the start halts with HEALTH_RECORD_UNAVAILABLE. Without this, a wrong log group name or a missing
permission would only show after the campaign, as an unbuildable record.

Tests. `test_health_events_are_recorded_and_flag_overlapping_attempts` (an event during the pilot, three failed polls,
the flag on exactly the overlapping attempts, no account ID in the record),
`test_unreadable_health_record_halts_the_start_before_warmup`. Pipeline
`test_health_event_flags_follow_the_event_interval`, `test_health_record_must_have_been_read`.

Result. All pass. The event shape follows the AWS Health EventBridge schema in the AWS Health user guide. It has not met a
real event.

## MIN-5. The F6 injector connects at its start time (decision 28, C31)

What changed. `f6_injector.run` creates the producer at START_AT, not at process start. The real `make_producer` checks the
brokers with `list_topics` at that moment. The self-test checks the creation time.

Test. `test_f6_injector_opens_no_connection_before_its_start_time`. It passes.

## MIN-10 and L7. Probes per pair, and a failed exec is a missing sample (C32)

What changed. The recovery gate and the pre-fault checks read the worst TCP pair and the worst TaskManager, not a pooled
fraction. Rows whose error starts with EXEC are missing samples, never failed connects. An F1 check that needs a failed
connect ignores them too.

Tests. `test_recovery_gate_checks_every_probe_pair` (one broker pair at 50% inside nine healthy pairs pooled to 0.95 and
passed), `test_failed_exec_is_a_missing_sample_not_a_failed_connect`. Both pass.

## L2. The recovery proof of a one-shot pod-kill (C33)

What changed. For one-shot PodChaos, `all_recovered_observed` passes on a confirmed NotFound within 60 s of the delete call,
and the check's `series` names that rule. The orchestrator no longer waits for AllRecovered on pod-kill. The simulated
cluster keeps one-shot records Injected until the object is deleted, as Chaos Mesh 2.8.4 does.

Test. `test_pod_kill_records_stay_injected_and_notfound_is_the_recovery_proof`. It passes.

## L3. The dead-letter rate (C34)

What changed. The sentinel goes out before the startup gate. The gate's dead-letter rate is the change of the verification
consumer's `dead_letter_count` over 60 s, which needs no broker meter. A missing value still fails. The broker query is
gone. The simulated brokers now create the topic meter lazily, close it on a restart, and restart it from zero.

Test. `test_dead_letter_rate_needs_no_broker_meter`. It passes.

## L4. The redeploy order (C35)

What changed. The redeploy waits for the new JobManager and its REST API, resubmits every job, then waits for three
TaskManagers with 12 slots. The simulated session cluster now starts TaskManagers from the slots that jobs request, with
the floor of `slotmanager.number-of-slots.min: "12"` that infra set.

Tests. `test_redeploy_resubmits_before_waiting_for_taskmanagers`, and
`test_redeploy_works_with_and_without_the_minimum_slots` with the floor at 0 and at 12. All pass.

## L5. F8 sizing from recorded samples (C36)

What changed. The F8 injector records every sizing sample (timestamps and values of the working set and the limit) in
`facts["f8"]`. The gate recomputes the size from those samples. The size from the export is recorded as
`export_stressor_bytes` and is not a gate.

Test. `test_f8_size_comes_from_the_recorded_samples_when_cadvisor_arrives_late`. It passes.

## L6. The controller is read for every run type (C37)

What changed. The controller is read right before every apply and after every labelled end, for every run type. It is
required for F0, F1 and F2 and best effort for the others. The read after the end is tried three times.

Test. `test_controller_is_read_before_and_after_every_run_type` for F4 to F8. It passes.

## L8. A stressor OOM event fails the F8 envelope (C38)

What changed. ENVELOPE check `env_no_oom_event`: `container_oom_events_total` of `flink-main-container` may not rise on any
TaskManager between onset and the end of RECOVERY.

Test. `test_stressor_killed_by_the_kernel_fails_the_f8_envelope`. It passes.

## C1, orchestrator side (decision 29, C24)

What changed.

1. The `flink_jobs` probe rows and every stored checkpoint path are scrubbed to the relative form
   `flink-checkpoints/...` before they are written. The F5 envelope checks the relative prefix.
2. The remediation rebuilds the full URI from the stored form (`Remediation.full_path`). Live samples already carry the
   full URI.
3. The exporter strips the registry host from `image`, `image_id` and `image_spec` before `series_id` is hashed. Two
   series that become identical are refused by the exporter's existing duplicate check. This is a second line behind
   infra's relabel.
4. The manifest stores the orchestrator's image digest only (`sha256:...`), and the verification record stores the F6
   injector's digest only.
5. The uploaded log is redacted. Every ARN becomes `<arn>`, every 12-digit number not inside a longer word becomes
   `<account-id>`, and the bucket name becomes `<results-bucket>`. Digits inside a hex digest are left alone.

Tests. `test_no_bucket_name_or_account_id_in_the_stored_record`,
`test_registry_host_stripped_before_hashing_and_collisions_refused`, `test_uploaded_log_is_redacted`,
`test_image_references_are_recorded_without_the_registry_host`, `test_restore_path_from_the_stored_relative_form`.
All pass.

## B6, orchestrator side (C39)

What changed. Nothing in the parser. The CLI already accepted `run [--resume] [--fault F] [--block N]`. The docs of the
options and the exit codes are now in `cli.py` and 11.11.

Test. `test_run_accepts_resume_fault_and_block_as_the_job_passes_them`. It passes.

## Requests from the infra report

1. L1, the remediation path (C40). The remediation resubmits with `/opt/flink/bin/run-job.sh <main-class>
   /opt/flink/usrlib/<jar> [<checkpoint URI>]` in `flink-main-container` of the JobManager pod. The simulated cluster
   accepts only that path and refuses a plain `flink run` with exit 126. Test `test_remediation_resubmits_with_run_job_sh`.
2. L1, the job settings check (C40). At every start (after the startup gate, and after the resume's recovery gate) and
   after every resubmission or redeploy, every expected job's `/jobs/<id>/config` and `/jobs/<id>/checkpoints/config` must
   show parallelism 4, `exactly_once`, interval 60000, timeout 120000, minimum pause 30000, externalization enabled and
   delete on cancellation false. A mismatch or an unreadable answer halts with FLINK_JOB_SETTINGS_WRONG. The values are
   registered in `run/thresholds.json` (`flink_job_settings`). Tests `test_job_settings_wrong_at_startup_halt_before_warmup`
   and `test_job_settings_are_the_ones_run_job_sh_passes`, which reads `run-job.sh` with infra's own
   `check_flink_jobs.expected_settings`.
3. L1 and L4, the fake. Done, see L4.
4. C1. Done, see above.
5. C6. Every pod and Job count of the orchestrator uses the `rebuild/campaign` or `rebuild/attempt` selectors or the pod
   roles. Test `test_watchdog_pods_and_jobs_count_for_nothing` adds a watchdog pod and Job with `app=campaign-watchdog` and
   the attempt stays VALID with `no_campaign_chaos_or_jobs` at 0.
6. B6. Nothing needed.
7. M10. `plan_tool.sh` makes `--out`, `--plan` and `--config` absolute against the caller's directory before the `cd`, in
   both the `--out DIR` and `--out=DIR` forms. Test `test_plan_tool_resolves_paths_against_the_callers_directory` runs it
   from a temporary directory and verifies the plan there.
8. The restart strategy. No gate rule assumes a restart budget. The simulated job restart delay is now 30 s, as in
   `run-job.sh`.

## Requests from the pipeline report

1. `build.py` passes `cfg` to `build_windows`. Done.
2. Cumulative counters. The simulated `numRecordsIn` and `numRecordsOut` now integrate the load since the task attempt
   started. A reopened dead-letter topic meter restarts from zero, while the all-topics meters keep counting.
   `test_fake_counters_never_decrease_outside_a_restart` checks more than 100,000 consecutive counter pairs of a full
   simulated pilot and finds no decrease outside a restart.
3. The RECOVERY overlap. See MIN-1.

## Infra changes needed

None block the pilot. Two things should be checked there.

1. The orchestrator role needs `logs:FilterLogEvents` on `/streaming-anomaly-detection/health`, and the EventBridge rule must
   deliver to that group. Infra has both (`terraform/cluster/iam.tf`, `terraform/shared/main.tf`). The group name follows
   `var.project`, so changing that variable breaks the read. The start then halts with HEALTH_RECORD_UNAVAILABLE.
2. `verification-probes` needs nothing new. The checkpoint path is scrubbed on the orchestrator side.

## Not fixed, or not proven

1. Nothing here has met a real cluster. The REST shapes of `/jobs/<id>/config` and `/jobs/<id>/checkpoints/config` follow
   infra's `check_flink_jobs.py` and the Flink 1.19 REST API. The Health event shape follows the AWS documentation. The
   README section 8 list (items 1, 6 and 13) names what the pilot must confirm.
2. The redaction of the uploaded log covers ARNs, 12-digit numbers and the bucket name. A message that names a role or a
   profile without an ARN would pass. None of the code paths I know writes one.
3. Protocol 1.7 item 3 lists two Flink changes from R1. Decision 31 adds two more (checkpoints kept on cancel, failure-rate
   restarts). That item is not in my sections and is left to its owner.
4. The decision 21 reading above, which the author should confirm.

## Final test runs

Commands, with `SP` the session scratch directory that holds the pipeline venv:

```bash
cd rebuild/orchestrator && python3.12 -m pytest tests -q
cd rebuild/pipeline && $SP/venv/bin/python -m pytest tests -q
ORCH_PY=python3.12 PIPE_PY=$SP/venv/bin/python bash rebuild/integration/run_e2e.sh --work $SP/A_work/e2e_final
```

The last code change came before all three runs. After them only prose changed (this report, the READMEs,
`CONTRACT_CHANGES.md` and the protocol).

| Suite | Result | Time |
|---|---|---|
| Orchestrator | 265 passed, 0 failed (219 before Review 4, 45 in `test_review4.py`, 1 new in `test_gate_rules.py`) | 11 min 20 s |
| Pipeline | 142 passed, 0 failed (B's and my tests together) | 1 min 52 s |
| End to end | 14 of 14 scenarios passed, "ALL SCENARIOS PASSED", 452 `ok` assertions and none failing | 7 min |

The scenario outcomes are as designed. `ec0` 8 VALID. `iot1` 80 VALID over five window sizes. `full` 80 VALID at full
length. `fail` 8 VALID, 5 GATE_FAILED and 1 TELEMETRY_FAILED. `resume` 8 VALID and 1 ABORTED. `remediation` 7 VALID,
1 VALID_REMEDIATED and 1 GATE_FAILED. `sham1` and `ctrlread` and `underrun` and `prefault` (IoT) 8 VALID and 1 GATE_FAILED
each. `sham2` 8 VALID and 2 GATE_FAILED. `jobsend` and `basegap` 8 VALID. `f6term` 8 VALID and 1 ABORTED. Every one ends with
0 failed checks in the offline gate, the validator, both builds and `check_e2e.py`.
