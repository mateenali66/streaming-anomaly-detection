# Contract changes agreed at integration

Integration of the orchestrator, the infrastructure and the preprocessing on 13 October 2026, against
`diagnostics/REBUILD_PROTOCOL.md` section 11. Each item below is a clarification of section 11 that the three parts now
share. Each one is also written into section 11 at the place it applies, marked "(C<n>)". C22 to C40 came after Review 4
(section "Review 4 changes").

Nothing has run, nothing is registered and no campaign data exists, so these are pre-registration clarifications. The
records keep `schema_version` "1". Section 11 says that a change bumps `schema_version` and is logged in `DEVIATIONS.md`.
That file is outside this integration, so whether these items also go there is left to the author (see the last section).

## C1. Only `manifest.json` leaves the manifest volume (11.4, 11.7)

**Change.** The volume `campaign-manifest` (`/manifest`) holds `manifest.json`, which is the record, and `state/`, which holds the
orchestrator's internal resume state: the plan copy, `pending.json`, `run.lock`, `campaign.log` and
`attempts/<attempt_key>/facts.json`. Only `manifest.json` is uploaded, to `manifest/manifest.json`. Infra's `s3-sync` sidecar
mirrors that one file (`--exclude "*" --include "manifest.json"`), so `state/`, `manifest.json.tmp` and `.s3sync.status` never
reach the bucket. Nothing under `state/` is listed in `checksums/SHA256SUMS` or counted in `COMPLETE`. The orchestrator no longer
mirrors the volume before `COMPLETE`.

**Reason.** The sidecar synced the whole volume, and the orchestrator mirrored it before `verify_complete`, so `facts.json` files
and a second plan copy landed under `manifest/` outside the 11.7 layout. They were listed in `SHA256SUMS` and counted in
`total_bytes`, and the preprocessing refused the campaign. Every fact in `facts.json` also reaches S3 inside
`attempts/<attempt_key>/verification.json`, which the offline gate reads (`orchestrator gate-offline`). So nothing needed for
offline re-gating lives only on the volume. The log reaches S3 as `logs/orchestrator-<start_epoch>.log.gz`, as 11.7 says.

## C2. `failed_checks` names checks, and the telemetry rule is a check (11.5, 11.6)

**Change.** `failed_checks` lists the distinct `name` of every failing entry of the record's `checks`, in record order. Targets
and values stay in the record. The 3.4.3 rule appears in `checks` as one TELEMETRY-layer check per untargeted `(source,
segment)` row of `telemetry`. The check is named `telemetry_coverage`. Its target is the source key `<source>:<scope>`, its
`pre_value` is the expected sample count and its `during_value` is the coverage fraction, with `>=` 0.95. A segment shorter than
one 10 s step gets no telemetry row. `result.telemetry` is true when every TELEMETRY check passes and at least one telemetry row
exists. Online and offline records are compared check by check on `(name, target, layer, series)`.

**Reason.** The orchestrator wrote names with targets (`f4_cpu_cores[<pod>]`) and invented names for telemetry failures that no
check carried. The preprocessing compared `failed_checks` with the failing check names, so every failed attempt stopped the build.
The record now names a telemetry failure as a check, as 2.6.1 asks ("with the failing checks named").

## C3. `recovery_gate.started_at` may be null for an ABORTED attempt (11.5)

**Change.** `recovery_gate.started_at` is nullable. It is null only when the attempt ended, as ABORTED, before its GATE segment
began. Every other finished attempt records it.

**Reason.** 11.5 says the recovery gate after a resume belongs to an IDLE segment, not to the attempt. So an attempt that crashed
before its GATE segment has no recovery gate start to record. The orchestrator wrote null and the preprocessing refused it.

## C4. Values the orchestrator could not observe are null (11.6)

**Change.** These verification-record fields are nullable:

- `pseudo_targets.follower_pod` and `pseudo_targets.f6_planned_count`.
- `controller.node_id_before_apply` and `node_id_after_injection`.
- `f5.kills[].apply_at` and `f5.new_tm_uid_count`.
- `f6.pod_ready_at`, `sent_count`, `acked_count`, `first_produce_at`, `last_produce_at` and `consumer_count`.
- Every number in `f8.per_pod[]`.
- `chaos_snapshots[].records[].first_apply_at` and `last_recover_at`. An `APPLY_PLUS_10S` snapshot has no recover time yet. The
  preprocessing's A12 also accepts a null condition `last_transition_at`, which the orchestrator never writes.

A check
that needs a null value fails with reason MISSING. A VALID or VALID_REMEDIATED attempt carries none of these as null for its run
type, and the preprocessing checks that.

**Reason.** A failed attempt often has no such value. Examples are an injector that never reported, a kill that never got an Apply
event, or a controller that could not be read. The orchestrator wrote null, as the gate's MISSING rule requires (2.4), and the
preprocessing refused the record. The fields stay non-null wherever the gate passed.

## C5. One orchestrator environment (11.11)

**Change.** The orchestrator reads exactly these names, with no default and no other spelling:

- `WORKLOAD`, which is `ECOMMERCE` or `IOT`, the upper-case form of the 11.2 token. Every record still carries `ecommerce` or `iot`.
- `CAMPAIGN_ID`.
- `CAMPAIGN_INDEX`, which must agree with the id.
- `PLAN_SHA256`.
- `RESULTS_BUCKET`.
- `RESULTS_PREFIX`, which is `campaign/<campaign_id>` with no `s3://` and no slash at either end.
- For a run, the four inputs that ConfigMap `campaign-config` mounts at `/config`. They are `PLAN_PATH` (`/config/plan.json`),
  `THRESHOLDS_PATH`, `VERSIONS_FILE` and `IMAGES_LOCK_FILE`.

The retired names `SCHEDULE_PATH`, `SCHEDULE_SHA256`, `CAMPAIGN_SEED` and `LOADGEN_METRICS_URL` are refused when set. Infra's Job
manifest sets this list, and a test reads the manifest and compares.

**Reason.** Infra set `SCHEDULE_PATH`, `SCHEDULE_SHA256`, `CAMPAIGN_SEED`, `RESULTS_BUCKET` and `campaign/<id>`, and gave no
`CAMPAIGN_INDEX`. The contract named `CAMPAIGN_INDEX`, `PLAN_SHA256` and `RESULTS_PREFIX`, and the orchestrator accepted both
sets. Upper-case `WORKLOAD` stays because the R1 load generator and `submit-jobs.sh` read the same ConfigMap value and compare it
with `IOT` and `ECOMMERCE`.

## C6. One F6 injector, attribution by header (11.6, 11.7, 11.8, 11.11)

**Change.** The F6 injector has one implementation, `rebuild/orchestrator/orchestrator/f6_injector.py`. Infra's `f6-injector`
image copies that file through the named build context `orchestrator_src`, runs it as its ENTRYPOINT and self-tests it at build
time. Infra's separate `images/f6-injector/injector.py` is deleted.

- **Environment.** `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC`, `INJECT_RATE`, `INJECT_DURATION`, `INJECT_SEED`, `START_AT_UNIX` (required)
  and `ATTEMPT_KEY` (required), plus `LATE_TOLERANCE_SEC` and `MIN_DELIVERED_FRACTION`.
- **Header.** Every record carries `rebuild-attempt=<ATTEMPT_KEY>`.
- **Reporting.** `acked_count` counts deliveries the brokers acknowledged, with `acks=all` and an idempotent producer.
  `first_produce_at` and `last_produce_at` are the first and last acknowledgement times. Send times are logged for diagnosis only.
- **Log.** `attempts/<attempt_key>/injector.log` is the pod log, which holds JSON lines and the final
  `INJECTED sent= delivered= failed= planned= first_epoch= last_epoch= payload_sha256=` line, plus the orchestrator's
  `orchestrator_observed` line with the exit code. The offline gate reads it.
- **Consumer and gate.** The consumer probe's `detail` holds the verification consumer's `/status`, including
  `malformed_by_attempt`, the cumulative malformed count per attempt key. The F6 signature counts only this attempt's key: the
  change from the last consumer sample at or before the planned onset to the last one at or before the end of RECOVERY. A
  consumer restart inside that span makes the value missing.

**Reason.** Two injectors existed. Infra's, which the image ran, set no header and reported send times. The orchestrator's gate
therefore attributed malformed records by time window, against 2.3.8 and 11.11, and its F6 onset and end were not
acknowledgements as 2.4 requires.

## C7. Status endpoints of the load generator and the verification consumer (11.8, 11.11)

**Change.** The load generator serves `/status` (JSON) and `/metrics` on port 9102. `acked_count` comes from the delivery
callbacks. A thin proxy around the producer counts acknowledgements, instead of taking produced minus failures. The other fields are
`produced_count`, `delivery_failures_count`, `produce_errors_count`, `commanded_rate_per_s`, `cycle_phase_frac`, `epoch_at`,
`started_at`, `seed`, `workload`, `profile` and `at`. The verification consumer uses consumer group `rebuild-verification` and
serves `/status` and `/metrics` on port 9103. The `loadgen` probe's `value` is `acked_count`.

**Reason.** 11.11 promised `/status` on port 8000. Infra had Prometheus text on 9102 with `loadgen_produced_total`, which counts
records handed to the producer, so the orchestrator approximated acknowledgements. Infra also set consumer group
`verification-consumer`. One port per service avoids a second HTTP server in each pod.

## C8. `run/versions.json` and `run/images.lock.yaml` come from mounted inputs (11.4, 11.7)

**Change.** `versions.json` is made by `rebuild/infra/scripts/campaign_inputs.py` from the captured
`terraform/cluster/addon-versions.auto.tfvars` and the chart pins of `variables.tf`. The tfvars file is written by
`capture_versions.py` and holds the EKS add-on API versions and the node AMI release from SSM, as applied. `images.lock.yaml` is
`rebuild/infra/images/images.lock.yaml`. Both are mounted from `campaign-config` and written once to `run/`. The orchestrator's
fallbacks, which read versions from node labels and images from running pods, are removed. The manifest's `node_ami_release` is
the SSM release.

**Reason.** Neither file was mounted, and the orchestrator role has no SSM access. So 11.4's "from SSM, as applied" could not
hold, and `images_lock_sha256` hashed an ad hoc listing instead of the lock that pins the images.

## C9. The verification probe service is part of the infrastructure (11.11)

**Change.** New 11.11 row. Deployment and Service `verification-probes` run the orchestrator image with command `probes`, under
ServiceAccount and Role `verification-probes` (pods get and list, pods/exec create). They serve `/status`, `/samples` and
`/metrics` on port 8000. PodMonitor `verification-probes` gives them the Prometheus job label `rebuild-verification`. The manifest
moved from `rebuild/orchestrator/k8s/` to `rebuild/infra/k8s/base/probes/` (phase `load`).

**Reason.** The orchestrator refuses to start without the probe service, but the infra kustomization did not include it, so
`deploy_workload.sh --phase all` never deployed it.

## C10. The controller metrics are scraped by `kafka-brokers` (11.8)

**Change.** `kafka_controller_kafkacontroller_activecontrollercount` and `fencedbrokercount` come from the broker JMX exporter's
scrape job `kafka-brokers`, like every broker metric. The separation that 2.3.3 asks for is made at export. The two metrics are in
the verification allowlist only (`kafka_controller`), never in a feature source, and the exporter refuses a series that matches
two sources.

**Reason.** 2.3.3 asks for "a separate scrape job". A second job on the same JMX endpoint would make every broker collect all
MBeans twice every 10 s, which changes the measured system. The allowlist already keeps the series out of every feature file.
2.3.3 is outside section 11, so its wording is left to the author (last section).

## C11. Exporter Jobs are the orchestrator's (11.7, 11.11)

**Change.** The orchestrator creates every exporter Job: `export-<attempt_key>`, `export-baseline-<chunk_start>` and
`export-trailing-<chunk_start>`. They run the orchestrator image with command `export` under ServiceAccount `metric-exporter`, with
`RESULTS_BUCKET`, `RESULTS_PREFIX=campaign/<id>`, `AWS_REGION` and `FEATURE_ALLOWLIST_SHA256`. Infra creates only the
ServiceAccount, in phase `rbac`. Infra's suspended Job `export-baseline` and its 50 Gi scratch volume are removed, and so are the
CLI aliases `export-baseline` and `verify-consumer`.

**Reason.** One 24 h export Job could not produce the hour-by-hour chunks of 11.7, would have needed a hand unsuspend, and would
have written files the orchestrator does not record.

## C12. The plan is the only source of the seeds (11.3, 11.11)

**Change.** `deploy_workload.sh --plan-file plan.json` checks the plan and reads `PLAN_SHA256`, `CAMPAIGN_INDEX` and
`load_generator_seed` from it. It checks the canonical bytes, the campaign, the index and `plan.json.sha256` next to it. The load
generator's `LOADGEN_SEED` is the plan's `load_generator_seed`. The options `--campaign-seed`, `--loadgen-seed` and
`--schedule-file` are refused, and no component reads a `CAMPAIGN_SEED`. `render.py` now writes a placeholder that is a whole YAML
scalar as a quoted string. kustomize drops the quotes, so a seed or an index would otherwise reach the API server as an integer
env value, which it rejects.

**Reason.** Infra defaulted `LOADGEN_SEED` to the campaign seed, while 11.3 step 6 draws a separate load generator seed. The
orchestrator refused that at preflight, so every campaign deployed as documented would have failed to start.

## C13. Flink state paths (11.7, 11.11)

**Change.** Flink state goes to `state.checkpoints.dir` `s3://<bucket>/scratch/<id>/flink-checkpoints` and
`state.savepoints.dir` `.../flink-savepoints`, as 11.7 names them. The gate's F5 envelope check now requires every expected job's
latest checkpoint under `s3://<any bucket>/scratch/<campaign_id>/flink-checkpoints/`. The bucket name stays out of the record.

**Reason.** Infra used `scratch/<id>/checkpoints` and `savepoints`. The gate only checked that a path began with `s3://`, so a
checkpoint in another campaign's prefix would have passed.

## C14. Plans and thresholds are made with the image's exact versions (11.3)

**Change.** `rebuild/integration/plan_tool.sh` (`plan`, `thresholds`, `verify-plan`) is the one supported way. It builds a
CPython 3.12 environment with exactly the image's NumPy 2.3.5 and PyYAML 6.0.3, from `rebuild/integration/plan-tool.lock`
installed with `--require-hashes`. The lock holds hashes for macOS and Linux wheels. The tool checks that the pins are still the
image's and runs `python -m orchestrator`. `orchestrator plan` and `verify-plan` refuse any NumPy but
`orchestrator/plan.py IMAGE_NUMPY_VERSION`. A test checks that this constant, the image's `requirements.in` and lock, and the
tool's lock agree.

**Reason.** The plan records `numpy_version` and the image compares bytes at every start. The local Python had NumPy 2.4.3, so
every plan made locally was refused. Running the image needs a Docker daemon, which this machine does not run. The draws
themselves are identical under 2.3.5 and 2.4.3 for all twelve seeds. Only the recorded version differs.

## C15. Parsing feature names (11.9)

**Change.** The `<metric>` part of `f__<source>__<metric>__<scope>__<stat>` may itself contain `__`. Examples are the topic
`__consumer_offsets` and the request label `"Produce, version=9"` after sanitising. Read `<source>` from the left and `<scope>` and
`<stat>` from the right.

**Reason.** The realistic exporter series (C18) produce such names. Splitting on `__` would misread them.

## C16. Keys in records are relative (11.1)

**Change.** Every object key stored in a record is relative to `campaign/<campaign_id>/`. This covers the manifest, the
verification records and the baseline statistics key. The orchestrator writes only that form. The preprocessing still accepts a
prefixed key (its A7), which is harmless.

**Reason.** 11.4 to 11.6 did not say which form is stored.

## C17. `series_id` is a file key, not a feature identity (11.8, 11.9)

**Change.** `series_id` hashes `{"pod": <pod>, "labels": <the remaining labels as an object>}`. It identifies a series inside
the exported files only, where it serves the hash check, the removal of duplicate samples at export boundaries and a fixed
series order. It hashes
the volatile labels `job_id`, `task_attempt_id`, `task_attempt_num` and `node`, so a job restart gives a series a new
`series_id`. Feature columns are named and matched from the source, the metric, the scope and the retained labels, never from
`series_id`. The pipeline test `tests/test_series_identity.py` rewrites every volatile label of every feature series, so every
such series gets a new `series_id`, and checks that the same columns come out with the same values.

**Reason.** This was raised in the orchestrator README section 5 item 8 and the pipeline README A9.

## C18. Naming rules A1 and A3 (11.9)

**Change.** These are accepted as written in the pipeline README. A1: `job_id`, `task_attempt_id`, `task_attempt_num` and
`node` are dropped before feature columns are named. A3: two series mapping to one name fail the build only when they have
samples at the same step inside one windowed segment, or when two different label sets sanitise to one name.

**Reason.** Read literally, every job restart, resubmission or JobManager redeploy would create new columns or fail the build,
against 4.1.3. The simulated cluster now produces the restarts, resubmissions and the redeploy (orchestrator
`fakes/series.py`), and the end-to-end run passes with these rules.

## C19. Terminal jobs in Flink's history are not extra jobs (11.5)

**Change.** `flink_jobs_at_end` lists every job that the Flink REST API reports when the attempt ends. That includes jobs in a
terminal state (FAILED, CANCELED, FINISHED) that Flink keeps in its job history. "No extra jobs" (1.6.4) counts the jobs that are
not terminal, and those must be exactly the workload's three names, once each, all RUNNING.

**Reason.** The gate already read 1.6.4 this way (orchestrator README section 5 item 17), but the preprocessing compared every
listed job. After a remediation resubmitted a failed job, every later valid attempt still listed the failed run, and the build
stopped. The end-to-end run with a remediation (scenario `remediation`) found this.

## C20. `SHA256SUMS` does not list itself (11.10)

**Change.** `checksums/SHA256SUMS` lists every object under the campaign prefix except `COMPLETE` and except itself.
`COMPLETE.object_count` and `total_bytes` count the listed objects.

**Reason.** A file cannot hold its own hash. Both builders read 11.10 this way (orchestrator README section 5 item 9, pipeline
README A6). The end-to-end run checks that the two agree object for object.

## C21. An ABORTED attempt's segments are a prefix (11.5)

**Change.** The segments of an ABORTED attempt are a prefix of the 11.5 order, the last one being the segment that was running
at the crash, cut at `ended_at`. GATE is present only when the crash came during or after the recovery gate.

**Reason.** The table says GATE is always present, while the resume rule of 11.5 extends the running segment to the restart time.
Both builders read it this way (orchestrator item 10, pipeline A20). The end-to-end scenario `resume` exercises it.

## Review 4 changes (13 October 2026)

Review 4 (`diagnostics/REVIEW4_DATA_VALIDITY.md`, `REVIEW4_LIVE_CONTRACT.md`, `REVIEW4_OPERATIONS.md`) and author decisions 21 to
30 (`diagnostics/REBUILD_DECISIONS.md`) changed the interface again before any data existed. C22 to C40 record each change.
Nothing is registered and no campaign record exists, so they are pre-registration changes like C1 to C21, and
`schema_version` stays "1". The tests and end-to-end scenarios that show each one are listed in
`rebuild/review4_fixes/A_orchestrator.md`.

### C22. Telemetry is scrape health (decision 21; 3.4.3, 3.4.4, 11.6, 11.8)

**Change.** An attempt is TELEMETRY_FAILED when an untargeted scrape target misses more than 3 scrapes in one PRE_FAULT,
INJECTION or RECOVERY segment. A scrape target is a broker, JobManager or TaskManager pod while kube-state-metrics shows it
(`kube_pod_info`). A scrape is missed when Prometheus `up` for that pod is absent or 0 at a 10 s step.

- **Export.** A fifth verification source, `scrape`, holds `up` and `scrape_samples_scraped` of the scrape jobs
  `kafka-brokers` and `flink-metrics` (`run/feature_allowlist.yaml`). Its rows carry `role` and `pod` like every other row.
- **Exemptions.** The fault's own targets are exempt in INJECTION and RECOVERY: the target broker for F1 and F2, every
  TaskManager pod for F4, F5, F7 and F8 (the replacements F5 creates included). F6 and F0 have none. This reads decision
  21's "the fault's own targets" as every fault's targets, which also covers the F4 and F8 stress that slows the
  TaskManagers' scrapes (Review 4 PIL-1). The author may confirm the reading.
- **Record.** `scrapes` is a new array of the verification record, one row per target and segment: `target`
  (`<role>:<pod>`), `segment`, `expected_count` (steps the pod exists), `missed_count`, `targeted`. The TELEMETRY check
  `telemetry_scrape_misses` replaces `telemetry_coverage` (C2): one per untargeted row, `pre_value` the expected count,
  `during_value` the missed count, `<=` 3. `result.telemetry` is true when every such check passes and at least one row
  exists.
- **Series presence.** The `telemetry` rows stay as recorded values. An untargeted row under 95% coverage flags the attempt
  SERIES_GAP, a new flag. It never fails an attempt.
- **Preprocessing.** The loader checks `result.telemetry` against `scrapes` and SERIES_GAP against `telemetry`. The build
  recomputes `scrapes` from each valid attempt's exported verification file with the same rule
  (`streamprep/health.py`) and fails on a difference. Both builders take the numbers from `run/thresholds.json`
  (`gate.telemetry`), so they cannot apply different ones.

**Reason.** Series presence failed attempts whose fault had a stronger effect: Flink removes task metrics while a job
restarts, and a stressed TaskManager answers scrapes late. That repeated runs until a milder one passed, which selects on
the outcome (Review 4 PIL-1).

### C23. A sham false pass fails the gate (decision 22; 2.4, 2.5, 11.5)

**Change.** For F0 the gate writes one SIGNATURE check `f0_signature_false_pass` per fault, which fails when that fault's
signature passed on the sham's pseudo-targets. `result.signature` and `result.gate` include it, so a false-passing sham is
GATE_FAILED and repeated. The orchestrator no longer writes SIGNATURE_FALSE_PASS. The preprocessing counts false passes over
every F0 attempt that has a verification record (failed and aborted shams included) and puts SIGNATURE_FALSE_PASS on every
window of a fault whose signature false-passed on two or more shams (`streamprep/timeline.py`). The manifest may not carry the
flag, and a valid sham with a passing signature is refused.

**Reason.** A false-passing sham was VALID online and made the campaign unbuildable, and nobody wrote the fault-run flag the
loader expected (Review 4 CAM-1).

### C24. No bucket name or account ID in the record (decision 29; 11.5, 11.8, 11.11)

**Change.**

- The stored `flink_jobs` probe detail holds `latest_completed_path` relative to `s3://<results-bucket>/scratch/<campaign_id>/`,
  for example `flink-checkpoints/<job id>/chk-7`. A path anywhere else keeps its key with the bucket removed (`s3:///<key>`),
  or its own scheme (`file://...`). The orchestrator rewrites the detail before it writes `probes.parquet` and the probe chunks.
  The F5 envelope check `env_checkpoints_on_s3` (C13) now requires the prefix `flink-checkpoints/`, which only a checkpoint
  under this campaign's scratch prefix gets. Remediation reads the live probe samples, which keep the full URI.
- Remediation actions name the same relative form (`RESUBMIT <job> from=flink-checkpoints/...`).
- The exporter strips the registry host from every image reference label (`image`, `image_id`, `image_spec`) before
  `series_id` is hashed. An ECR host holds the account ID. Two series that differed only in the host would collide, and the
  exporter's duplicate check refuses that.
- The uploaded orchestrator log replaces the bucket name and every 12-digit number before upload.

**Reason.** The checkpoint URI and the ECR host put the account ID into probe files and metric labels, which are deposited
(Review 4 C1). Infra renames the bucket so it no longer holds the account ID. The record no longer depends on that.

### C25. Every preprocessing rule on a valid attempt is a gate check (decision 23; 2.4, 11.5, 11.6, 11.7)

**Change.** New gate checks: `controller_at_onset_recorded` (F0, F1, F2), `controller_read_after_injection` (F1, F2),
`flink_jobs_at_end_running` (every run type), `end_underrun` (F2, F4, F7, F8, below -60 s fails), `overrun_matches_span`,
`fixed_span` (F1, F5), `last_ack_after_first` (F6) and `onset_end_sources` (the 2.4 source pair). `f6_record_matches_log`
also compares `sent_count`. The controller read after the labelled end is tried three times. `flink_jobs_at_end` is retried
every 10 s until the Flink REST API answers. Fifteen minutes without an answer halt with FLINK_REST_UNREACHABLE_15_MIN. Every F6
attempt that created its injector Job stores `attempts/<attempt_key>/injector.log`: before the Job is deleted on every path
(halt, abort, failure), and at the latest when the attempt finishes, with the `orchestrator_observed` line alone when the pod
log is gone.

**Reason.** A null controller read, an empty job list, an early end and a missing F6 log each passed the gate and stopped the
build of the whole campaign afterwards (Review 4 CAM-2).

### C26. NORMAL time must be healthy (decision 24; 2.4, 3.3, 3.4, 11.6, 11.9)

**Change.** Every attempt, the sham included, gets five ENVELOPE checks over its PRE_FAULT segment:
`pre_fault_jobs_running_same_id` (every `flink_jobs` sample shows exactly the expected jobs RUNNING, with unchanged job IDs),
`pre_fault_taskmanagers_registered` (every `flink_tms` sample shows 3), `pre_fault_no_pod_change` (kube-state shows 3 broker,
1 JobManager and 3 TaskManager UIDs and no restart-counter increase), `pre_fault_tcp_ok` (the worst source and destination
pair at least 95%) and `pre_fault_packet_loss` (the worst TaskManager at most 2%). A segment with no sample of a kind fails as
MISSING. The preprocessing applies the same conditions to every BASELINE and TRAILING window, over the window plus 10 s on
each side, and labels a failing window EXCLUDED (`streamprep/labels.py`). `build_report.json` gives, per window size and
segment, the windows checked, the windows excluded and the failing conditions. The numbers are in `run/thresholds.json`
(`gate.pre_fault`). The BASELINE outage check of the build counts only windows still NORMAL.

**Reason.** NORMAL labels came from the schedule alone, so a job failure in PRE_FAULT or the baseline became NORMAL training
data (Review 4 CAM-3).

### C27. Onset and labelled end are recomputed from the stored records (11.6)

**Change.** Two TIMING checks, `onset_from_records` and `end_from_records`, recompute the 2.4 onset and end from the stored
chaos snapshot YAML, the injector log or the plan, and compare them with the attempt record within 0.002 s. The offline gate
therefore finds a manifest whose times were moved. The dry run writes the simulated cluster's own disturbance intervals into
`summary.json` (`ground_truth`), and `check_e2e.py` checks that no NORMAL, RECOVERY, SHAM, SHAM_POST or TRAILING window
overlaps one and that every ANOMALOUS window lies inside one of its attempt.

**Reason.** Neither the offline gate nor the end-to-end check could catch a wrong onset or labelled end (Review 4 MIN-1).

### C28. AFTER_RESUBMIT (1.3, 3.4.6, 11.5)

**Change.** A remediation that resubmitted a job without a full redeploy flags the next attempt AFTER_RESUBMIT, a new flag,
and gives it the same 600 s extra settle as AFTER_REDEPLOY. The 11.5 SETTLE rule reads "plus 600 s if AFTER_REDEPLOY or
AFTER_RESUBMIT". The flag joins the 3.4.6 sensitivity list.

**Reason.** After a resubmission the next PRE_FAULT could start minutes after a job restart with no flag (Review 4 MIN-2).

### C29. A settle before TRAILING (1.5)

**Change.** Before the first TRAILING chunk the orchestrator runs the health gate, then waits a fixed settle of `settle_max`
seconds (300 s), plus 600 s when the last remediation redeployed or resubmitted. That time is IDLE.

**Reason.** TRAILING started right after the last recovery gate, so a redeploy's warm-up could enter it (Review 4 MIN-3).

### C30. AWS Health events (1.7, 11.4)

**Change.** Infra's EventBridge rule writes the region's `aws.health` events to the CloudWatch Logs group
`/streaming-anomaly-detection/health`. The orchestrator reads it with `logs:FilterLogEvents` every 10 minutes, inside its
sleeps, and once more before it writes the last manifest. Each `health_events` entry is one AWS Health event: `at` (the
latest publication's EventBridge `time`), `service`, `event_type_code`, `status`, `event_key` (the first 16 hex of SHA-256 of
the event ARN), `start_at` and `end_at` (null while open). The account fields and affected entities are never stored. An
attempt is flagged HEALTH_EVENT when `[start_at, end_at)` meets `[started_at, ended_at)`, also after it finished. The new
manifest field `health_events_polled_until` is the end of the last successful poll. A poll error is logged and retried, never
fatal. The poll before COMPLETE is retried for 15 minutes. One read must succeed at a start, before WARMUP, and at a resume
while the field is still null. It is retried every 30 s for 15 minutes, then the start halts with HEALTH_RECORD_UNAVAILABLE.
The preprocessing refuses a manifest whose field is null, so this keeps that rule a check during the campaign (decision 23).

**Reason.** Nothing recorded Health events, and the Health API needs a support plan the account does not have (Review 4
MIN-4, M12).

### C31. The F6 injector connects at its start time (decision 28; 11.11)

**Change.** The injector creates its producer and fetches the topic metadata at `START_AT_UNIX`, never before. The pod is
still created 90 s ahead and Ready long before onset.

**Reason.** Its connection and metadata requests reached broker features in F6 pre-fault windows only (Review 4 MIN-5).

### C32. Probes per pair, and a failed exec is a missing sample (1.6.8)

**Change.** The recovery gate's `probe_tcp_ok_frac` is the worst source and destination pair's ok fraction and
`probe_packet_loss_frac` the worst TaskManager's loss, where both were pooled. A probe row whose error starts with `EXEC` (a
failed `kubectl exec`) is a missing sample in the gate, the recovery gate and the preprocessing. NO_POD_IP and CONNECT_FAILED
stay failures.

**Reason.** One bad pair passed inside a pooled fraction (Review 4 MIN-10), and an API or kubelet hiccup counted as a failed
connect (Review 4 L7).

### C33. The recovery proof of a one-shot pod-kill (2.4)

**Change.** For a PodChaos resource, `all_recovered_observed` passes when the resource was NotFound within 60 s of the delete
call. Its `series` names the rule. The injectors no longer wait for AllRecovered on pod-kill.

**Reason.** Chaos Mesh keeps a one-shot experiment in Run until it is deleted, so AllRecovered never shows while the object
exists, and every F1 and F5 attempt failed (Review 4 L2). The finalizer is released only after every record is Not Injected.

### C34. The dead-letter rate comes from the verification consumer (1.6.7, 2.3.10)

**Change.** The recovery gate's `dead_letter_rate_zero` is the change of the verification consumer's `dead_letter_count`
(the sentinel excluded) over the last 60 s, from two samples of one consumer process. The query `dead_letter_in_per_s` is
removed from `run/thresholds.json`. The sentinel is written before the startup gate, not at the start of warm-up.

**Reason.** Kafka creates a topic's messages-in meter at the first append and closes it when the broker stops leading the
topic, so the series was missing at startup and after F1 or F2 on the sentinel's broker (Review 4 L3).

### C35. The redeploy resubmits before it waits for TaskManagers (1.6)

**Change.** After the restart nonce, the remediation waits for the new JobManager to be Ready and its REST API to answer,
resubmits every job, then waits for three new Ready TaskManagers registered with 12 slots, all within 600 s.

**Reason.** A native session cluster with no jobs starts no TaskManagers, so the old order could never finish (Review 4 L4).

### C36. The F8 sizing samples are recorded (2.4, 11.6)

**Change.** `f8.per_pod[]` gains `ws_samples` and `limit_samples` (arrays of `ts` and `value`, the samples the injector sized
from) and `export_stressor_bytes` (the size recomputed from the export, recorded only, nullable). `f8_stressor_size_rule`
recomputes the size from the recorded samples.

**Reason.** cAdvisor samples keep their own timestamps and can arrive after the online query, so the export could change the
size and fail an F8 attempt for no reason related to the fault (Review 4 L5).

### C37. The controller is read for every run type (2.3.4, 11.5)

**Change.** The controller read runs immediately before every apply and after every labelled end, for every run type.
`controller_at_onset` is set for every run type when the read succeeds. For F4 to F8 a failed read is recorded as null and
fails nothing. For F0, F1 and F2 it fails the attempt as before.

**Reason.** The read's admin requests reached broker features at the onset and end of F0, F1 and F2 only (Review 4 L6).

### C38. A stressor OOM event fails the F8 envelope (2.4)

**Change.** New ENVELOPE check `env_no_oom_event`: `container_oom_events_total` of `flink-main-container` may not increase on
any TaskManager from onset to the end of RECOVERY.

**Reason.** The kernel can kill the stressor without killing the container, which leaves no OOMKilled reason (Review 4 L8).

### C39. The `run` options the infra Job passes (11.11)

**Change.** `run` accepts `--resume`, `--fault <F>` and `--block <n>` in any combination, as the Job's args (`--fault` and
`--block` for the pilot only). Exit codes: 0 finished or a pilot subset finished, 1 defect, 2 refused to start, 3 halted, 4
resume refused, 143 SIGTERM.

**Reason.** Infra's deploy script could not pass them, so nothing could resume or rerun one pilot fault (Review 4 B6).

### C40. One submission path and a check of the job settings (REVIEW4 L1, with infra; 1.6, 1.8)

**Change.** The remediation resubmits a job with `/opt/flink/bin/run-job.sh <main-class> <jar> [<checkpoint path>]` in the
JobManager pod, the script the `flink-submit` Job also runs (infra C_infra.md). It passes `-p 4` and every checkpoint and
restart setting itself. A checkpoint path in the stored relative form (C24) is rebuilt under this campaign's scratch prefix.
At every start of the orchestrator, after the startup gate or the resume's recovery gate, and after every resubmission or
redeploy, every expected job's `/jobs/<id>/config` and `/jobs/<id>/checkpoints/config` must show the registered settings
(`run/thresholds.json` `flink_job_settings`: parallelism 4, `exactly_once`, interval 60000 ms, timeout 120000 ms, minimum
pause 30000 ms, externalized and kept on cancel). A mismatch or an unreadable answer halts with FLINK_JOB_SETTINGS_WRONG. A
test compares the registered values with `run-job.sh`, read with infra's own parser.

**Reason.** Jobs submitted with the distribution's client config ran at parallelism 1 with checkpointing off (Review 4 L1).
No orchestrator rule depends on the restart strategy, which infra set to failure-rate pending the author.

### C41. A CPU sample is the counter's increase over 60 s (4.1.2, from the pilot)

**Change.** The F4 signature's CPU sample at each 10 s step t is the increase of `container_cpu_usage_seconds_total` over
the 60 s before t, divided by 60, with t and t - 60 both inside the window. The pre and during values stay the medians of
these samples (4.1.2). A decrease drops that sample, and the maximum over a pod's series is kept per step, as before.

**Reason.** In the pilot of 14 October 2026 the counter moved only about every 15 s, so on the 10 s export grid about a third of
the steps read 0 and the step after each read a double step. The median of the 10 s rates read 2.33 to 2.93 cores on
containers limited to 2 cores, 20 to 45 percent above the counter's increase over the window. It passed every F4 attempt the
pilot ran, rightly, but it could pass a weak stress or fail a real one. On the pilot's e-commerce F4 export the 60 s samples
give 1.94 to 2.02 cores, within 5 percent of the increase over the whole window (PILOT_LOG.md, 08:50).

### C42. Three more instants are rounded to the millisecond (11.1.1, from the pilot)

**Change.** The orchestrator rounds three instants to the millisecond before it writes them: every recovery gate condition's
`at` and the F5 `all_jobs_running_at` in `verification.json`, and `newest_object_at` in `COMPLETE`. Every other instant was
already rounded. The test `test_loader_accepts_a_campaign_whose_clock_reads_carry_microseconds` runs a pilot on a fake clock
whose every read takes 7.3 microseconds, with probe samples stamped 0.36 ms into their step, and the preprocessing's loader
must accept it. With any one of the three roundings removed, the test fails.

**Reason.** Both pilot campaigns of 14 October 2026 completed, and `streamprep validate` refused both, because the recovery
gate's `at` (345 values) and the F5 `all_jobs_running_at` carried microseconds from `time.time()` and from the probes. The
fake clock reads whole simulated seconds, so no test had seen it. `newest_object_at` comes from S3's `LastModified`, which has
whole seconds, so it never failed in the pilot. It is rounded so the record does not depend on that. The pilot data are not
changed. With only this rule relaxed, `streamprep validate` accepts both pilot copies (PILOT_LOG.md, 13:10).

## Resolved after integration (13 October 2026)

1. No campaign record exists in any form, so C1 to C21 are pre-registration clarifications. `schema_version` stays "1", and
   they are not logged in `DEVIATIONS.md`, which holds changes made after registration. The registered protocol includes them.
2. Protocol 2.3.3 now says the controller metrics come from scrape job `kafka-brokers` and are kept out of every feature source
   at export (C10).
