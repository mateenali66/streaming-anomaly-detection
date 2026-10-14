# Campaign orchestrator (rebuild)

The orchestrator for the re-run of the Kafka and Flink fault-injection benchmark. It implements
`diagnostics/REBUILD_PROTOCOL.md` sections 1 to 3 and the interface contract of section 11, and it runs inside the image
that `rebuild/infra/images/orchestrator/Dockerfile` builds from this folder.

Nothing here has run against AWS or a cluster. Everything was tested offline against a simulated cluster (section 6), and
end to end with the preprocessing (`rebuild/integration/run_e2e.sh`). The interface with rebuild/infra and the preprocessing is
the one of protocol section 11 as clarified at integration and after Review 4; `rebuild/CONTRACT_CHANGES.md` lists every change
(C1 to C40). `rebuild/review4_fixes/A_orchestrator.md` maps each Review 4 finding to its change and its test.

## 1. Layout

```
orchestrator/            the package (single entrypoint: python -m orchestrator <command>)
  cli.py                 commands: plan, verify-plan, thresholds, run, export, probes, consumer, f6-inject,
                         gate-offline, verify-complete, abandon
  plan.py                seeds (1.4.1), the plan and its draw order (11.3), byte-for-byte verification
  campaign.py            the state machine: preflight, warm-up, baseline, blocks, attempts, trailing, resume, halts
  injectors.py           F0 sham and the F1, F2, F4, F5, F6, F7, F8 injectors
  gate.py                the injection gate, the only place a pass or fail is decided (online and offline)
  recovery.py            the recovery gate, the health gate and the scripted remediation (1.6)
  health.py              AWS Health events from infra's CloudWatch Logs group, polled from the clock's sleep hook (C30)
  exporter.py            Prometheus to the 11.8 long layout (runs as Job export-<attempt_key> and export-<kind>-<t>)
  probe_service.py       the verification-channel probes (2.3 items 4 to 9), Deployment verification-probes
  consumer_service.py    the verification consumer (2.3 item 8), infra's Deployment verification-consumer
  f6_injector.py         THE F6 injector (C6): infra's f6-injector image runs this file; it also holds the log parser
  baseline.py            baseline statistics (11.12)
  complete.py            verify_complete: checksums/SHA256SUMS and COMPLETE (11.10)
  manifest.py            atomic manifest writes, uploads, the 15-minute S3 halt rule, per-attempt facts
  schema.py              strict validators for every section 11 record
  chaos.py, kube.py, prom.py, store.py, flinkrest.py, httpjson.py, queries.py, thresholds.py, ids.py, config.py
  fakes/                 the simulated cluster: world, Kubernetes API, Chaos Mesh, Prometheus, S3, Flink, probes, clock;
                         fakes/series.py produces the exporters' real label sets and metric names (section 6)
config/campaign_config.yaml   the ONE configuration file (section 3)
config/feature_allowlist.yaml run/feature_allowlist.yaml (11.8)
tests/                   219 pytest tests (section 6)
```

## 2. How to run it

Requirements for the tests: Python 3.12 with numpy, pyarrow, PyYAML and pytest. No AWS library is needed offline.

```bash
cd rebuild/orchestrator
python3.12 -m pytest tests -q                       # the whole suite, about 6 minutes
python3.12 -m orchestrator run --dry-run            # pilot plan (ecommerce, index 0) against the fakes, shortened phases
python3.12 -m orchestrator run --dry-run --workload iot --index 1 --full-length   # a whole 10-block campaign, real lengths
python3.12 -m orchestrator run --dry-run --fault F4  # pilot mode: one named fault (also --block N)
```

The dry run prints the attempts, their outcomes, the halts and whether COMPLETE was written.

### For the pilot and the campaigns (with rebuild/infra)

1. **Plan and thresholds.** Generate them with the image's exact NumPy (2.3.5) and PyYAML, because the plan records
   `numpy_version` and every start compares bytes. The one supported way is the pinned tool environment (C14):
   `rebuild/integration/plan_tool.sh plan --workload ecommerce --index 0 --out plans/pilot-ec` writes `plan.json` and
   `plan.json.sha256`, and `rebuild/integration/plan_tool.sh thresholds --out plans/thresholds.json` the registered thresholds.
   `orchestrator plan` refuses any other NumPy. Under NumPy 2.4.3 the draws are identical; only `numpy_version` differs.
2. **Deploy** with infra's script: `deploy_workload.sh --phase all --plan-file plans/<id>/plan.json`, then `--phase run
   --plan-file plans/<id>/plan.json --thresholds-file plans/thresholds.json`. The script reads `PLAN_SHA256`, `CAMPAIGN_INDEX`
   and the load generator's seed from the plan (C12) and mounts the plan, the thresholds, `versions.json` and `images.lock.yaml`
   at `/config` (C5, C8). The orchestrator refuses to start if any of them differs from what it regenerates or registers.
3. **Probe service.** Part of infra's kustomization (`rebuild/infra/k8s/base/probes/`, phase `load`, C9), so `--phase all`
   deploys it before the orchestrator Job. The orchestrator refuses to start if the service does not answer.
4. **Pilot subsets.** For the pilot only (campaign index 0), set the Job args to `["run", "--fault", "F4"]` or `["run", "--block", "1"]`.
5. **Resume.** After a halt, a SIGTERM or a crash, the orchestrator refuses to continue until the author confirms the cause was
   transient and nothing changed (2.6.6): recreate the Job with args `["run", "--resume"]`. A campaign (index 1 to 5) is not
   resumed after a pause of two hours or more, or after a change of image, code, thresholds or allowlist; then run
   `abandon --confirm` (it finishes running attempts as ABORTED and writes COMPLETE with status ABANDONED).
6. **Offline gate** (`make gate`): sync the campaign prefix to a directory and run `orchestrator gate-offline --store <dir>`.
   It re-evaluates every attempt from the stored files of the 11.7 layout alone and exits 1 if any check differs from the
   online record. `run_e2e.sh` runs it on every dumped campaign.

Exit codes of `run`: 0 finished (or a pilot subset finished), 2 refused to start, 3 halted (2.6), 4 resume refused, 143 SIGTERM, 1 defect.

## 3. Configuration

One file, `config/campaign_config.yaml`. Every value is the protocol default; each line names the rule and the alternative.

| Key | Default | Alternatives and effect |
|---|---|---|
| `pre_fault_s` | 600 | 300 (cost option C). Enters the plan, so it changes PLAN_SHA256 |
| `settle_max_s` | 300 | 120 (cost option C). Enters the plan |
| `trailing_baseline` | true | false (options B and C). In thresholds.json |
| `f5_job_recovery_limit_s` | 180 | Every expected job RUNNING within this time of the last F5 kill. Enters the plan and thresholds |
| `f8_target_limit_permille` | 850 | F8 stressor = 0.85 x limit minus working set. Integer per mille, because plan params hold no floats |
| `f8_min_stressor_mib` | 256 | A smaller stressor fails the F8 mechanism, nothing is applied |
| `halt_consecutive_failed` | 2 | Consecutive failed attempts across faults that halt the campaign |
| `halt_total_failed_gt` | 8 | Halt when the total of failed attempts exceeds this |
| `f1_labelled_span` | FIXED_300 | NOT_READY is accepted by the config but refused by `run`, because 11.5 has no `end_source` code for it |
| `f1_f2_urp_presence_check` | true | false drops the presence-only URP check of 2.1 |
| `fixed.*` | protocol values | Only the dry run and tests may shorten them; a real run refuses any change |
| `prometheus_jobs`, `metric_names` | infra names | Scrape job labels and JMX metric names (rebuild/infra/NAMES.md section 6) |

The workload is not in the file. It is the environment variable `WORKLOAD` (ECOMMERCE or IOT) with no default anywhere.

Values that enter the plan change its bytes; the others are in `run/thresholds.json`, whose SHA-256 is in the manifest and in
every verification record. A restart compares both.

## 4. Interface with rebuild/infra

### Environment and paths read (NAMES.md section 4, `k8s/base/orchestrator/orchestrator-job.yaml`, protocol 11.11, C5)

Required, no defaults, one spelling: `WORKLOAD` (ECOMMERCE or IOT), `CAMPAIGN_ID`, `CAMPAIGN_INDEX`, `PLAN_SHA256`,
`RESULTS_BUCKET`, `RESULTS_PREFIX` (`campaign/<id>`). For a run, the four inputs that ConfigMap `campaign-config` mounts at
`/config`: `PLAN_PATH`, `THRESHOLDS_PATH`, `VERSIONS_FILE`, `IMAGES_LOCK_FILE`. With defaults that equal the 11.11 names:
`PROMETHEUS_URL`, `KAFKA_BOOTSTRAP`, `FLINK_REST_URL`, `LOADGEN_STATUS_URL`, `PROBES_URL`, `NAMESPACE` (must be streaming),
`MANIFEST_DIR` (/manifest), `KUBECTL_BIN`. Also read: `F6_INJECTOR_IMAGE` (digest-pinned for a run), `FLINK_EXPECTED_JOBS`
(must equal the 11.11 names), `SNS_TOPIC_ARN`, `LOADGEN_BASE_RATE` (recorded only), `HOSTNAME` (the pod's own name, to read
its image digest), optional `ORCHESTRATOR_IMAGE` and `ORCHESTRATOR_GIT_COMMIT`. The retired names `SCHEDULE_PATH`,
`SCHEDULE_SHA256`, `CAMPAIGN_SEED` and `LOADGEN_METRICS_URL` are refused. `tests/test_config.py` reads infra's Job manifest and
checks it sets exactly this interface.

Writes only to `/manifest` and `/tmp`. `/manifest/manifest.json` is the record (uploaded after every write, mirrored by the
sidecar); `/manifest/state/` holds the resume state (`plan.json` copy, `pending.json`, `run.lock`, `campaign.log`,
`attempts/<key>/facts.json`), which never leaves the volume (C1).

### Names used

Kubernetes: namespace `streaming`; brokers by `strimzi.io/cluster=streaming-kafka,strimzi.io/pool-name=brokers`, container `kafka`,
node ID = pod ordinal, ports 9090, 9091, 9092; JobManager `app=streaming-flink,component=jobmanager`, TaskManagers
`app=streaming-flink,component=taskmanager`, container `flink-main-container`, RPC 6123; FlinkDeployment `streaming-flink`
(patched `spec.restartNonce`); jars in `/opt/flink/usrlib` with the 11.11 classes; load generator `app=load-generator`,
`/status` on 9102 (C7); verification consumer `app=verification-consumer`, group `rebuild-verification`, `/status` on 9103 (C7);
probe service `verification-probes:8000` (C9); Job `campaign-orchestrator` (excluded from the leftover check); ServiceAccount
`metric-exporter` for the exporter Jobs this code creates (C11); node label and taint `workload=monitoring`; node labels
`topology.kubernetes.io/zone`, `spec.providerID`; broker scripts `/opt/kafka/bin/kafka-metadata-quorum.sh` and
`kafka-leader-election.sh`; the RBAC verbs of `k8s/base/rbac/orchestrator-rbac.yaml`. Prometheus jobs `kafka-brokers`
(including the KafkaController metrics, C10), `flink-metrics`, `kubelet`, `kube-state-metrics`; the probe job
`rebuild-verification` comes from infra's PodMonitor. Flink state under `s3://<bucket>/scratch/<id>/flink-checkpoints/` (C13).

### The differences this section used to list

The sixteen differences between infra and contract 11.11 that an earlier version of this README listed are resolved: the
WORKLOAD values, CAMPAIGN_INDEX, the plan path and hash and the results prefix (C5); the manifest file name and the S3 prefixes
(C1, NAMES.md section 2 now lists the 11.7 layout); the load generator's status endpoint and the consumer's group and port (C7);
the F6 header and acknowledgement times (C6); the controller metrics job (C10); the baseline export Job (C11); the image lock and
version records (C8); the code identity (unchanged: `sha256-tree:<hash>` unless a commit is provided); the labels on the
orchestrator Job (unchanged: excluded by name); the checkpoint path (C13); the S3 layout (C1); IOChaos (unchanged: F3 is dropped).

## 5. Contract readings where section 11 is ambiguous

The most literal reading was taken each time; none of these is a new variant of the contract.

1. Time between attempts (export, gate evaluation, health gate) and any pause has no attempt segment, so it is an IDLE campaign segment, recorded when the next segment starts.
2. `ended_at` is the recovery gate's pass; the export of that span, the gate and the verification record come after it, in IDLE.
3. The health gate uses the recovery-gate conditions once (no hold) plus the instance set and the clock. Before warm-up there are no baseline statistics, so the lag condition is not evaluated. Checkpoints count since the last labelled end, or over the last 180 s when that is more recent.
4. SKIPPED_UNHEALTHY is not an attempt and the manifest has no field for it: it is logged, and the time is IDLE.
5. Export hash fields are strings that are not nullable: they are the empty string while the export is PENDING.
6. A check's `op` and `threshold` compare `during_value`, except checks named `*_pre`, which compare `pre_value`. Ratio checks are stated as fractions so thresholds are constants.
7. Chaos Mesh conditions carry no transition time: `last_transition_at` is when this orchestrator first saw that status.
8. `series_id` hashes `{"pod": <pod>, "labels": <remaining labels as an object>}`; a series without a pod gets `pod` = "".
   It hashes the volatile labels too (`job_id`, `task_attempt_id`, `node`), so it is a file key only, never a feature identity
   (C17; the preprocessing test `tests/test_series_identity.py` proves its features never depend on it).
9. SHA256SUMS lists every object except COMPLETE and except itself.
10. An ABORTED attempt ends with the segment that was running at the crash (11.5 resume rule), not with GATE.
11. F0: `onset_verified_at` is the planned onset with source PLANNED (2.4 table), although nothing was applied.
12. Failed attempts are GATE_FAILED and TELEMETRY_FAILED. ABORTED neither counts nor resets the streak. A resumed halt resets the streak, never the total.
13. A recovery gate that fails after remediation halts the campaign with the attempt still RUNNING; the resume finishes it as ABORTED.
14. F8 "working set over the last 60 s of pre-fault" is the median, like every pre value.
15. F5: kill 1 at the planned onset, kills 2 to 5 at the verified onset plus 60, 120, 180, 240 s.
16. F5 "RUNNING within 180 s of the last kill": the earliest sample after which every later sample to the end of RECOVERY has all expected jobs RUNNING.
17. "No extra jobs": no non-terminal job with another name and no second job with an expected name; finished jobs in Flink's history are ignored.
18. "No resource or Job carrying the campaign label": chaos resources, Jobs (except the orchestrator's) and pods with `rebuild/attempt`; Deployments such as the consumer are not campaign attempts.
19. Telemetry (decision 21, C22): scrape health per broker, JobManager and TaskManager pod, from `up` and `kube_pod_info`; more than 3 missed scrapes of an untargeted pod in a segment fail; the fault's own targets are exempt in INJECTION and RECOVERY. Series presence per source (brokers per node ID, TaskManagers counted per 10 s step up to 3) is recorded and flags SERIES_GAP.
20. `pods_after` is taken at the end of RECOVERY, so a remediation in the GATE cannot fail the envelope.
21. Baseline standard deviations are population values (ddof 0).
22. WARMUP, BASELINE and TRAILING are recorded hour by hour as contiguous segments, so a crash loses at most the hour in progress, which becomes IDLE; the baseline can be several stretches (11.9 `baseline-<n>`).
23. For a sham, one SIGNATURE check `f0_signature_false_pass` per fault; a false pass fails the gate and the sham is repeated (decision 22, C23). Counting false passes and flagging a fault's runs after two is the preprocessing's step; the manifest never carries SIGNATURE_FALSE_PASS.
24. 11.6 has no field for the Ready TaskManagers at each F5 kill, so the rank rule is enforced online; offline the gate checks that each kill resource names the recorded pod.
25. `applied` is set before the first create call, so a crash between the call and the manifest write cannot hide a live fault.
26. The manifest is created when WARMUP starts (`timeline_start_at` is not nullable); a crash before that restarts the preflight.

## 6. Tests

`python3.12 -m pytest tests -q`: 265 tests, all passing (13 October 2026 after the Review 4 fixes, 11 min 20 s on a laptop while the end-to-end run shared it).

| File | Tests | What it proves |
|---|---|---|
| test_plan.py | 16 | the 11.3 draw order rewritten from the text, determinism, seeds, hash and byte checks, plan stored first, wrong hash refused, filters keep positions; the image's NumPy pin agrees across the orchestrator, the image and the plan tool, and `plan` refuses any other NumPy (C14) |
| test_gate_faults.py | 43 | every fault passes on a realistic effect and fails when absent, partial, late or on the wrong target; F1 never kills the controller; F2 clients, F5 S3 restore, F8 sizing, F7 TaskManager loss flag |
| test_gate_rules.py | 12 | shams fail every signature on both workloads, missing values fail, one target never stands in for another, telemetry rule and its exemptions, empty queries block the start, feature values never change a check, offline gate equals online |
| test_attempt_flow.py | 13 | recovery and cooldown after failures, confirmed and timed deletion, a stuck resource and a leftover Job block the next attempt, overrun, validity only in the final write, remediation, redeploy and AFTER_REDEPLOY, job names |
| test_resume.py | 38 | SIGKILL and SIGTERM in 15 phases (startup, warm-up, baseline, idle, settle, pre-fault, apply, injection, recovery, cooldown, gate, post-processing, second attempt settle and injection, trailing), kill right after verification, probe or export uploads, a second crash during the resume, refusals |
| test_halts.py | 13 | the streak across faults, the total, a ten-block campaign halting at its ninth failure, node replaced, clock skew, S3 outage, export failure, alerts |
| test_schema.py | 21 | manifest, verification records, baseline statistics, parquet schemas, S3 layout and object names against section 11, and rejection of invalid manifests; every object is an 11.7 object and the resume state never leaves the volume (C1) |
| test_complete.py | 11 | COMPLETE contents, written once and last, refused for each failed condition, ABANDONED marker |
| test_exporter.py | 13 | series ids, removed labels, NaN, repeated pairs, empty sources, probe and consumer jobs kept out of features, write-role limits, non-zero exit |
| test_components.py | 10 | the F6 injector, probe output parsing, the consumer, chaos builders and record parsing; the load generator's `/status`, the F6 header constant, attribution of malformed records by the attempt header (C6, C7) |
| test_config.py | 29 | the one environment of 11.11 (C5): infra's Job manifest sets exactly it, WORKLOAD without default, bad and retired environments refused, mounted inputs required, thresholds file, mounted plan, load generator seed, Job environments |

Rows added at integration: test_gate_faults covers a checkpoint in another campaign's scratch prefix (C13) and F6 records that
never reach raw-events (C6); test_exporter checks that the simulated exporters produce the real shapes (below).

The fake cluster (`orchestrator/fakes/`) simulates three brokers with KRaft fencing and URP, a Flink session cluster with
checkpoints, restarts and redeploys, Chaos Mesh records and events, the load generator, the consumer, the F6 injector (it runs
the real `f6_injector.run`), kube-state-metrics, cAdvisor, Prometheus range and instant queries, S3 with write roles, and a
virtual clock. A simulated SIGKILL makes every later call fail, so a `finally` block cannot quietly do work a real kill would
prevent. Since integration, `fakes/series.py` gives every series the label set and metric name the real exporter and scrape job
produce: Prometheus target labels (`job`, `instance`, `pod`, `namespace`, `node`, `kafka_broker`, `flink_component`), Kafka
counters per topic and for all topics, the R1 request rule's `request="Produce, version=9"`, the JMX agent's JVM collectors,
Flink's filtered label values and gauge-typed counters, task metrics with `job_id`, `task_attempt_id` and `task_attempt_num`
that change on a job restart and a resubmission, scrape gaps during F5 (killed pods, task metrics gone while jobs restart) and
F7 (one TaskManager scrape in three), and a new JobManager pod after a redeploy. About 1,300 feature series per pilot, against
about 25 before. The suite therefore takes longer than the 6 minutes it took with the old fake.

The review test scripts in `diagnostics/review1_scripts` and `review2_scripts` drive the old `orch.final.py` functions, which this
code does not have. Their scenarios are re-expressed against this code: Review 1 T1 (`test_failure_after_partial_injection...`),
T2 (`test_missing_pre_fault_probes_fail_the_attempt`, `test_make_check_missing_values_fail`), T3 (`test_always_failing_fault...`);
Review 2 adversarial A (`test_f6_injector_sends_exactly...`), B (`test_resume.py`), C (`test_pilot_fault_filter_keeps_plan_positions`),
D (`test_halt_rule_counts`), E (F1 no longer uses `resets()`, `test_realistic_effect_passes[F1]`), F (`test_attempt_flow.py`).

## 7. Review findings: where each is fixed, and the test

| Finding | Code | Test |
|---|---|---|
| R1 A1 stateful jobs cannot restore after a TaskManager loss | gate.py `envelope_checks` F5 (`env_jobs_running_after_last_kill`, `env_checkpoints_on_s3`); recovery.py `flink_conditions` (jobs by name, two checkpoints since the labelled end); `Remediation.resubmit_missing` (`run-job.sh` with the latest S3 checkpoint, C40) | test_f5_fails_when_jobs_cannot_restore_from_s3, test_remediation_resubmits_missing_jobs_from_s3_and_is_flagged |
| R1 A2 wrong jobs on IoT accepted | config.py `environment_from` (no default WORKLOAD, FLINK_EXPECTED_JOBS checked); recovery.py and gate.py check jobs by name | test_jobs_are_checked_by_name_not_by_count, test_workload_has_no_default_anywhere |
| R1 A3 a failed attempt skips post-fault and cooldown | campaign.py `run_attempt`: once `applied`, INJECTION, RECOVERY and COOLDOWN always run | test_failure_after_partial_injection_still_runs_recovery_and_cooldown, test_fault_on_two_of_three_pods... |
| R1 A4 a missing pre value passes ratio rules | gate.py `make_check` (MISSING fails), ratios as fractions | test_make_check_missing_values_fail, test_missing_pre_fault_probes_fail_the_attempt |
| R1 A5 F8 passes with nothing injected | gate.py `sig_f8` (increase at least half the pod's own stressor), `f8_size_from_rows` per pod | test_degraded_effect_fails[*-F8], test_f8_per_pod_sizing_uses_each_pods_own_working_set |
| R1 A6 every sham passes F6 | gate.py `sig_f6` against the planned count; injectors.py `Sham` pseudo-targets | test_sham_f6_signature_uses_the_planned_count..., test_full_pilot_shams_fail_every_signature... |
| R1 B7 F3 cannot run on ARM | dropped: plan.py `RUN_TYPE_ORDER`, ids.py `CHAOS_KINDS` | test_plan_follows_the_contract_draw_order |
| R1 B8, R2 7 the F1 signature can never pass | gate.py `sig_f1`: UID change, not Ready within 60 s, TCP to 9091 from both other brokers, URP presence | test_realistic_effect_passes[F1], test_degraded_effect_fails[*-F1] |
| R1 B9 F2 ISR shrinks never count; controller isolated | gate.py `sig_f2` (probes, fenced count, URP); follower rule in injectors.py and gate.py `follower_rule` | test_f1_never_targets_the_active_controller, F2 cases |
| R1 B10 F4 busy-time rule fails IoT | gate.py `sig_f4`: container CPU at least 2x pre and 1.6 cores on every TaskManager | F4 cases, test_one_target_never_stands_in_for_another |
| R1 B11 F8 sizing query has no data | `kube_pod_container_resource_limits` in gate.py `f8_size_from_rows` | test_f8_* |
| R1 B12, R2 10 IoT stats give a zero floor | baseline.py from the gate's rate() query; recovery.py `rate_mismatch` compares to the load generator, no floor | test_complete_pilot_manifest_and_records_validate |
| R1 C13 F6 late onset, runtime pip | injectors.py `PoisonPill` (prebuilt image, created 90 s before onset, onset and end from the injector log) | F6 cases, test_f6_injector_sends_exactly... |
| R1 C14 F7 loss hidden by retries; TaskManager loss not flagged | probe_service.py 10 x 0.5 s connects; gate.py F7 flag F7_TM_LOSS | test_f7_taskmanager_loss_is_flagged_not_failed |
| R1 C15 halt after 2; single-block pilot; seed saved late | campaign.py `check_halt_rules`, `--block`/`--fault`, `store_plan` first | test_halt_rule_counts, test_pilot_fault_filter..., test_plan_is_the_first_object_written... |
| R1 D gate item 9 would halt after F5 | recovery.py: registered TaskManagers and slots instead of a CPU and memory envelope | test_realistic_effect_passes[F5] |
| R2 1 time outside completed runs became NORMAL | campaign.py `fill_idle`, segments, schema.py `timeline_gaps`, complete.py timeline check | test_segments_cover_each_attempt_and_the_timeline, test_resume.py `check_final` |
| R2 2 the gate selects on detector features | gate.py reads features only for sample counts | test_gate_never_reads_feature_values |
| R2 3 label edges from the wrong timestamps | injectors.py onset from Apply events or the first ack, end from Recover events or the last ack; RECOVERY starts at the labelled end | test_segments_cover..., test_overrun_between_60_and_120_s... |
| R2 4 a stuck chaos resource carries a live fault | injectors.py `delete_confirmed` (NotFound polled and timed); gate.py `deleted_not_found_within`, `all_recovered_observed`; recovery.py leftover conditions | test_stuck_chaos_resource_blocks_the_next_attempt, test_every_deletion_is_confirmed..., test_leftover_injector_job_blocks_the_health_gate |
| R2 5 resume loses or duplicates runs | campaign.py `resume_existing`, `abort_attempt`, attempt numbering, `export_missing_chunks`; per-attempt facts on the volume | test_resume.py (all) |
| R2 6 missing data hidden; telemetry rule absent | gate.py `telemetry_rows`; exporter.py raw counters with `metric_type`; no imputation | test_untargeted_feature_source_gap_is_telemetry_failed, test_targeted_source_gap... |
| R2 8 halt streak per fault | campaign.py `check_halt_rules` across faults | test_halt_rule_counts |
| R2 9 failed queries return [], uploads exit 0 | prom.py raises; exporter.py exits 1, empty source is an error; runtime.py retries Jobs, hashes downloads; manifest.py 15-minute halt | test_exporter_job_exits_non_zero..., test_empty_source_fails_the_export, test_export_failing..., test_s3_failing... |
| R2 10 recovery gate weaker than 1.6 | recovery.py `evaluate` (all nine conditions), 120 s hold, 900 s, remediation, 900 s | test_attempt_flow.py |
| R2 medium: F6 under-delivers | f6_injector.py paces exactly the planned count | test_f6_injector_sends_exactly... |
| R2 medium: false-pass paths (missing pod UIDs, max over targets) | gate.py per-target checks, MISSING on absent facts | test_one_target_never_stands_in_for_another |
| R2 medium: fixed run length phase-locks onsets | plan.py settle gaps; load phase in every loadgen probe sample | test_plan_follows_the_contract_draw_order |
| Validity set only in the final status | campaign.py `finalize`; schema.py rejects RUNNING with an outcome | test_validity_is_set_only_in_the_final_status |
| The plan is stored and its hash checked | campaign.py `store_plan`, plan.py `verify_plan` | test_plan.py |
| Sham shows a campaign resource | injectors.py `Sham.assert_nothing` | test_sham_that_sees_a_campaign_resource_halts |

Review 2 item 11 (fold roles) is the preprocessing builder's.

## 8. What only the pilot can show

1. Chaos Mesh 2.8.4 status on containerd 2 and cgroup v2: `containerRecords` events with `type`, `operation`, `timestamp`; `selectorKey` `.Target` for the F2 target side; StressChaos accepting `MiB` sizes and charging `flink-main-container`. A one-shot pod-kill never reaches AllRecovered while it exists (Review 4 L2, v2.8.4 source), so its recovery proof is the NotFound within 60 s of the delete (C33).
2. The exec probes: `bash`, `/dev/tcp`, `timeout` and `$EPOCHREALTIME` in the Strimzi, Flink and load generator images; exec latency inside the 10 s tick; the probe service's RBAC.
3. `kafka-metadata-quorum.sh` and `kafka-leader-election.sh` paths and output; `run-job.sh <class> <jar> <s3 path>` from the JobManager with Pod Identity (C40).
4. Flink 1.19.3 REST field names (`latest.completed.external_path`, `latest_ack_timestamp`, `history[].status`, `failure_timestamp`).
5. The metric names and scrape job labels in the config resolve; the PodMonitor is selected (job `rebuild-verification`); JobManager pod names start with `streaming-flink-` (role mapping of cAdvisor and kube-state rows).
6. F8 sizing: the gate recomputes the size from the samples the injector recorded (C36); the size from the export is recorded only. The pilot shows how often the two differ.
7. The load generator: acked = produced minus failures, the 5% rate match during IoT bursts, and `loadgen_seed` equal to the plan.
8. The consumer keeps up with IoT raw-events (about 11,000 per second in Python).
9. One exporter Job per hour of IoT baseline within 1,800 s and 4 Gi.
10. The sidecar uploads nothing after COMPLETE: it mirrors `manifest.json` alone, whose last write precedes its last upload by
    the orchestrator, so `aws s3 sync` finds nothing newer (C1). S3's LastModified against the node clock is the assumption.
11. Clock agreement between the Chaos Mesh controller and the orchestrator (an Apply event before the planned onset fails the attempt).
12. Every item of protocol 1.8 step 8.
13. The AWS Health log group receives EventBridge events as their JSON message (C30), and the Flink REST answers of `/jobs/<id>/config` and `/jobs/<id>/checkpoints/config` match the fields the job settings check reads (C40).
