# Rebuild protocol for the streaming anomaly-detection benchmark

Revised 13 October 2026 after three independent reviews (`REVIEW1_FAULTS.md`, `REVIEW2_PIPELINE.md`, `REVIEW3_INFRA.md`) and author decisions 1 to 20 in `REBUILD_DECISIONS.md`. The pilot ran on 14 October 2026 (`PILOT_LOG.md`) and passed. The author settled section 10 and decisions 21 to 47 the same day, and this version is the pre-registration (7.6). Any later change is a deviation, logged in `DEVIATIONS.md` and reported in the paper. Section 11 is the interface contract that the orchestrator and preprocessing builders code against.

It replaces the R1 data collection, not the research questions. It answers the breaks listed in `FOUNDATION_RECORD.md` and the defects the reviews found in the R1 setup and in the first patches. In R1, four of eight faults never took effect, fault and time were confounded, the e-commerce data came from two sessions, labels came from the schedule, pooled AUC-ROC rewarded drift, and partial exclusion biased the R2 draft. The reviews add that F3 could not run on ARM nodes, that time outside completed runs became NORMAL training data, that stateful jobs could not restore after a TaskManager loss, and that the first patched gate selected runs on the detectors' own features.

The protocol rests on five rules.

1. Every injection is verified at its point of application, from a channel the detectors never see, before any window gets a label.
2. Fault order is random inside every repetition, so time and fault cannot line up.
3. Every rule that touches labels, exclusions, metrics or tests is fixed before the data it applies to exist.
4. A headline claim needs to hold on independently provisioned clusters, not on one campaign.
5. One command rebuilds every table, figure and number in the paper from the raw metrics.

These design points are fixed by author decision.

1. Three fully independent campaigns per workload, six in total. Each runs on a fresh cluster with its own 24 h normal baseline before its 70 fault runs and 10 sham runs. All six run in parallel in us-east-2, in the shared staging account, inside one new VPC, with all nodes, volumes and the NAT gateway in one availability zone (1.7).
2. Nothing runs until the orchestrator, preprocessing, Flink setup and Terraform are rebuilt against section 11, tested offline and re-reviewed (1.8).
3. A short pilot comes first. Every fault runs once on each workload and must pass the injection gate. The pilot is approved up to $60. The full campaigns need a separate approval after the pilot passes (1.8).
4. F3 is dropped. Chaos Mesh's disk-fault tool does not run on ARM nodes, so F3 never ran in R1. Seven faults and the sham remain (1.2).
5. F1 is a forced pod kill of a broker that is not the active KRaft controller, drawn from the seed among the two followers. F2 uses the same follower rule. The controller's identity is recorded at onset (1.2, 2.4).
6. Cumulative counters become rates, and Flink TaskManagers are aggregated by role with minimum, maximum and mean, plus a count of TaskManagers reporting (4.1).
7. The hyperparameter search runs on campaign 1 of each workload and optimises AUC-ROC. The chosen settings are frozen for campaigns 2 and 3. Seeds are fixed and recorded (6.1, 6.2).
8. Cooldown is 300 s. The recovery gate must still pass before the next attempt (1.3, 1.6).
9. The compute budget is about $870 in total, split as about $680 for campaigns, $60 for the pilot, $100 for training and $30 for other items. Window-size sensitivity runs on campaign 1 only (6.1, 6.4).
10. The target is the 27 October 2026 revision date. Data plus paper will not fit, so the author sends the editor an extension request after the pilot. Section 8.4 marks what the submission needs and what can wait.
11. Zenodo v8 and the R1 manuscript stay as they are. The R2 version sets the record, and the cover letter tells the editor what was found and what changed.
12. No account names or IDs go into any public file.

## 1. Campaign design

### 1.1 Units

A **campaign** is one uninterrupted data collection for one workload on one freshly provisioned cluster. It has its own warm-up, leading baseline, ten blocks and trailing baseline.

A **block** is one repetition. It holds one run of each of the eight run types, in an order drawn at random for that block.

A **run** is one cell of the plan, identified by its block and position. A run can have more than one **attempt** if the gate fails (2.6). Every attempt has its own key and `run_id` (11.2).

There are eight **run types**. Seven are the faults F1, F2, F4, F5, F6, F7 and F8. The eighth is the **sham run**, F0. The orchestrator goes through every phase of a sham run and applies nothing. The fault IDs keep their R1 numbers, so F3 is absent rather than renumbered.

The sham run is a direct negative control for within-run metrics, for the injection gate and for episode-level false alarms.

### 1.2 Fault definitions

| ID | Name in the paper | Injection | Duration | Change from R1 |
|---|---|---|---|---|
| F0 | Sham | Nothing applied. Every fault's checks run against seeded pseudo-targets (2.5) | 180 or 300 s, drawn per run | New |
| F1 | Broker pod kill | PodChaos `pod-kill`, `gracePeriod: 0`, on one follower broker chosen by the seeded follower rank at onset | 300 s labelled span | R1 selector named a pod that does not exist. Target is now a follower, never the active controller. The broker returns in about a minute, and the rest of the span is catch-up, which the paper says |
| F2 | Broker network partition | NetworkChaos `partition`, both directions, one follower broker against the other two brokers only, so clients stay connected | 180 s | Same selector fix and follower rule |
| F3 | Dropped | None | None | Never ran in R1. The Chaos Mesh daemon ships only an x86 disk-fault binary and all node groups are ARM. The paper says so |
| F4 | TaskManager CPU stress | StressChaos CPU, 4 workers at 90%, on all TaskManagers, in `flink-main-container` | 300 s | Gate moves from Flink busy time to container CPU |
| F5 | TaskManager loss | Five PodChaos `pod-kill` resources, `gracePeriod: 0`, one Ready TaskManager each, at onset plus 0, 60, 120, 180 and 240 s, chosen by seeded ranks | 300 s labelled span | Renamed. R1 patched `spec.taskManager.replicas`, which the Flink operator treats as an upgrade and redeploys the whole cluster. Flink's Kafka source uses fixed split assignment, so no consumer-group rebalance occurs. Checkpoints move to S3 so stateful jobs can restore |
| F6 | Poison pill | Prebuilt injector image sends JSON fragments without a closing brace to `raw-events` at 5% of the baseline mean rate | 180 s planned, labels from first to last produce time | Not Avro, as R1 claimed. Injector pod starts before onset. RBAC fixed |
| F7 | TaskManager packet loss | NetworkChaos `loss` 50%, outbound, on all TaskManagers | 300 s | TaskManager loss during F7 is flagged, never a failure |
| F8 | TaskManager memory pressure | One StressChaos memory resource per TaskManager, sized per pod from its live working set and limit (2.4) | 180 s | R1 at 95% replaced three or four pods per run. Now pressure with no kill |

A fault's name and description in the paper match its verified effect. If the pilot cannot verify a fault, it leaves the taxonomy before any campaign starts.

### 1.3 Run protocol

Each attempt runs these segments in order (11.5).

1. **Settle gap.** Random length, uniform on 0 to 300 s, from the plan. Excluded. Together with the natural spread in gate time it spreads onsets over the load generator's 30-minute cycle. R1 runs had a fixed length, so the e-commerce F8 onsets were exactly phase-locked to the cycle. An attempt that follows a full Flink redeploy gets 600 s more settle time and the flag AFTER_REDEPLOY. An attempt that follows a remediation that resubmitted a job without a redeploy gets the same 600 s and the flag AFTER_RESUBMIT (C28).
2. **Pre-fault.** 600 s, labelled NORMAL if the attempt is valid. R1 used 300 s, which capped evaluable sequence length at 11. The length depends on decision 1 in section 10.
3. **Apply.** From the planned onset to the verified onset. Excluded. At most 30 s for Chaos Mesh faults and 5 s for F6.
4. **Injection.** From the verified onset to the labelled end (2.4). Labelled ANOMALOUS only if the attempt passes the gate.
5. **Recovery.** 300 s from the labelled end, labelled RECOVERY for a valid fault attempt. The labelled end is the observed end of the fault, so an overrunning fault never leaks into RECOVERY.
6. **Cooldown.** 300 s, excluded.
7. **Recovery gate.** Runs until it passes (1.6). Excluded.

Once anything has been applied, recovery and cooldown always run, even if the attempt has already failed. R1 code skipped both after a partial failure, so the retry's pre-fault could start a minute after a partial fault.

The mean attempt length is about 1,790 s, close to 30 minutes. A block of eight attempts takes about 4 hours.

### 1.4 Order and seeds

1. Six seeds per workload are generated before the pilot. Seed i is the integer value of the first 16 hex digits of SHA-256 of the string `rebuild/<workload>/campaign-<i>`. Index 0 is the pilot, 1 to 3 are the campaigns, and 4 and 5 are reserves for abandoned campaigns. The two workloads get different seeds by construction.
2. One NumPy PCG64 generator per campaign draws every random value in the fixed order of 11.3. Every cell gets every field, whether its run type uses it or not, so the random stream never depends on run type.
3. The plan is written to `plan/plan.json` in canonical form. Its SHA-256 is committed and tagged before the campaign's cluster is provisioned.
4. The orchestrator regenerates the plan from the seed, compares it byte for byte with the stored plan and with the hash in its configuration, and refuses to start if any differ. The plan is stored before any preflight, so a crash cannot draw a new seed.
5. Permutations are unconstrained. Each run's predecessor is recorded so carryover can be checked (5.9).

### 1.5 Baselines

1. **Warm-up.** One hour after the pipeline first passes the health gate. Excluded. The probes, the verification consumer and the dead-letter sentinel (2.3) start at the start of warm-up, so their traffic is part of every later segment.
2. **Leading baseline.** Exactly 24 hours, labelled NORMAL, used in training only. R1 recorded 56.8 hours on e-commerce, but Review 2 found that 3,661 of those windows lay inside campaign runs, so the true baseline was 26.3 hours. The orchestrator now starts block 1 when the 24 hours end, after the baseline export and baseline statistics are in S3 (11.12).
3. **Trailing baseline.** One hour after block 10, labelled TRAILING, used for evaluation only. It gives false alerts per hour on normal traffic far from training, and a drift check. It starts after the health gate and a fixed settle of 300 s, plus 600 s when the last remediation redeployed Flink or resubmitted a job. That time is IDLE (C29).
4. **Drift check.** A classifier two-sample test, run per campaign. A random forest is trained to tell the first six hours of the leading baseline from its last six hours, and separately the leading baseline from the trailing one. Five-fold cross-validated AUC above 0.70 counts as drift. It is reported, and every pooled result for that campaign carries a note.

### 1.6 Recovery gate between attempts

All conditions hold for 120 consecutive seconds before the next attempt's settle gap starts. Every value comes from the orchestrator's own queries and probes, which are stored in the verification record.

1. No resource or Job carrying the campaign label exists. Each deleted chaos resource was confirmed NotFound.
2. Three brokers, one JobManager and three TaskManagers are Ready. The pod UIDs are recorded.
3. Kafka under-replicated, offline and under-min-ISR partition counts are 0, preferred replica imbalance is 0, and the fenced broker count is 0.
4. Every expected Flink job for the workload is RUNNING, matched by name, with no extra jobs. Each has at least two completed checkpoints since the labelled end and no failed checkpoint in the last two minutes.
5. Consumer lag is below the leading baseline's mean plus three standard deviations, computed with the gate's own query (11.12).
6. Broker messages-in rate on `raw-events` is within 5% of the load generator's own acked produce rate over the same 60 s. A floor taken from the baseline would sit inside the load cycle and lock onsets to load phase.
7. The dead-letter topic rate is 0. It is read from the verification consumer's dead-letter count over 60 s, because Kafka creates a topic's messages-in meter only at the first append and closes it when the broker stops leading the topic (C34).
8. Every cause-level probe is back at its pre-fault pass level, with at least 95% of TCP connects succeeding for every source and destination pair and packet-probe loss at most 2% for every TaskManager. A probe whose `kubectl exec` failed is a missing sample, not a failed connect (C32).
9. The JobManager reports three registered TaskManagers and 12 task slots. This replaces the R1 draft's CPU and memory envelope, which replacement TaskManagers fail after every F5 because they start small.

If the gate has not passed after 900 s, one scripted remediation runs. It triggers preferred leader election, then resubmits any missing job by name from its latest S3 checkpoint, which flags the next attempt AFTER_RESUBMIT. A full Flink redeploy is the last step and flags the next attempt AFTER_REDEPLOY. A session cluster with no jobs starts no TaskManagers unless a minimum is set, so the redeploy waits for the new JobManager, resubmits every job, and then waits for three TaskManagers with 12 slots (C35). Every resubmission uses `run-job.sh`, the path of the first submission, and every start and every resubmission reads each job's parallelism and checkpoint settings back from the Flink REST API and halts on a mismatch (C40). The gate then gets another 900 s. If it still fails, the campaign halts (2.6). An attempt that needed remediation keeps its data with outcome VALID_REMEDIATED.

A health gate also runs before every settle gap. It uses the same conditions and adds two. The set of EC2 instance IDs must match the set recorded at campaign start, and the clock difference between the orchestrator and Prometheus must be under 1 s. Either failure halts the campaign.

### 1.7 Account, network and independence

The rebuild runs in the shared staging account in us-east-2. Other teams' VPCs and resources are never read or changed. No account name or ID appears in any public file. AWS sessions use the staging MFA session script, and the author is asked for the one-time password before each session.

All six clusters share one new VPC from the `network` Terraform root. Every node, every volume and the single NAT gateway sit in one availability zone, with two small control-plane-only subnets in two other zones because EKS requires them. Each cluster gets its own pre-cut /22 node subnet, so the VPC CNI cannot run out of addresses. This removes cross-zone traffic cost and the volume-attach trap that Review 3 found, where a node replaced in another zone leaves the Prometheus and manifest volumes unattachable. The paper states single-zone operation as a change from R1.

What differs between campaigns of one workload:

1. A fresh `terraform apply` of the `cluster` root, with new nodes, EBS volumes, Kafka topics, Flink state, S3 checkpoint prefix and Prometheus storage. A cluster is never reused.
2. Its own warm-up, 24 h leading baseline and trailing baseline.
3. Its own seed, so its own fault order, settle gaps, follower ranks and TaskManager kill ranks.

What the six share is the VPC, the NAT gateway, the zone, the region and calendar time. Their independence comes from cluster state, baseline and seed. A region-wide or zone-wide event would reach all six at once. Each campaign records AWS Health events for the region, and any attempt that overlaps one is flagged HEALTH_EVENT. The events come from a CloudWatch Logs group that an EventBridge rule on `aws.health` fills, because the Health API needs a support plan the account does not have. The orchestrator reads it every 10 minutes and before COMPLETE. A start halts if the group cannot be read for 15 minutes, so no campaign runs without the record (C30).

What is pinned and identical:

1. The three Terraform roots (`shared`, `network`, `cluster`) with exact module versions and committed lock files, node instance types, the AMI release, and the Kubernetes version.
2. EKS 1.35, within the author's standard-support range of 1.34 to 1.37 and the newest version with vendor testing for Strimzi 0.45.2 and Kafka 3.9.0 (Review 3). Strimzi 0.45.2, Kafka 3.9.0, Flink 1.19, Flink operator 1.10.0, Chaos Mesh 2.8.4 and kube-prometheus-stack 69.8.0. Every image is mirrored into ECR for arm64 and referenced by digest, and the digests go into `images.lock.yaml`.
3. Kafka, Flink, load generator and exporter configuration, with four changes from R1 that every campaign shares. Flink checkpoints and savepoints go to the S3 scratch prefix (11.7). `raw-events` and `enriched-events` keep 2 h of data on 300 Gi broker volumes so the IoT disks do not fill. Checkpoints are kept when a job is cancelled, so a remediation can restore from the last one. Jobs restart under a failure-rate rule (40 failures in 10 minutes, 30 s delay) instead of R1's fixed delay with 10 attempts, because a fixed budget counts over a job's whole life and the F5 runs of one campaign would use it up (decision 31). The pilot raised the limit from 20 to 40, because a job with no shuffle restarts four regions per lost TaskManager and five F5 kills on one such job reach 20 (decision 41). Parallelism 4, checkpoints every 60 s exactly once, a 120 s timeout and incremental RocksDB state stay as in R1.
4. The orchestrator image, the gate code and thresholds, the plan format and the phase lengths. Nothing installs packages at runtime.
5. Feature extraction and analysis code.

One orchestrator codebase serves both workloads through a required workload setting with no default. R1 had two copies, and the patched code defaulted to e-commerce, so a redeploy on an IoT cluster would have started e-commerce jobs and the gate would have accepted them.

EKS 1.30, which R1 used, reached the end of extended support on 23 July 2026. The paper records the version change.

### 1.8 Before the pilot, and the pilot

Before the pilot (decision 13 of the author):

1. Rebuild the orchestrator, the exporter, the preprocessing, the Flink setup and the Terraform roots against section 11.
2. Test them offline. The tests include the review test scripts in `review1_scripts/` and `review2_scripts/`, adversarial kills of the orchestrator in every segment, resume after each kill, the per-second timeline check of 3.1 on synthetic manifests, and the gate on replayed R1 runs where the expected answer is known.
3. Have the rebuilt code re-reviewed.

The pilot runs one pilot cluster per workload, both in parallel. Each pilot holds one block, so every fault runs once, plus one sham. Pilot data are never used for any claim.

1. Provision the two pilot clusters (`pilot-ec`, `pilot-iot`) from the pinned code.
2. Record the live pod names and labels, the TaskManager container limits and the broker volume mount, and confirm that every name in 11.11 resolves.
3. Collect a one-hour warm-up and a two-hour baseline.
4. Run the eight cells of the seed-0 plan with the full run protocol and the gate.
5. Each fault must pass the gate and the sham must fail every signature. Each attempt is also checked by hand against its raw verification record.
6. A fault that fails is fixed and run again. The thresholds in 2.4 are not loosened to make a fault pass. A threshold may change before registration only for a mechanical reason, such as a probe that cannot resolve the stated value, and the reason is logged.
7. A fault that cannot pass is dropped on both workloads before the campaigns start.
8. Check live what the reviews could not settle. That covers chaos resources reporting Injected and then AllRecovered on containerd 2 and cgroup v2, stressors charged to `flink-main-container`, F1 broker recovery time, F2 fencing and probe failure rate, F5 job recovery from S3 checkpoints, F6 onset delay and consumer count against planned, F7 probe loss, F8 sizing without an OOM kill, the recovery gate passing without remediation after each fault, the new controller metrics existing, broker volume use under 70% after 4 h at IoT load, and the load generator holding its target rate within 2%.
9. Run the analysis pipeline end to end on pilot data with Isolation Forest only, so every stage runs and every table builds.
10. Measure the real cost per cluster-hour and the time per training fit and per HPO trial, then requote 6.4.
11. Dry-run teardown on a pilot cluster. Destroy without `COMPLETE` must refuse, and the tag sweep must find nothing left.

The pilot passes when every retained fault has passed the gate once on each workload, both shams have failed every signature, the dry run has built every table, teardown behaved as specified and the author has approved the requote. Then the code, thresholds, seed list, plan hashes and this protocol are tagged and registered (7.6), and the six campaigns launch about 10 minutes apart.

One passing injection per fault shows the injector works. It does not show it works every time. The halt rule (2.6) covers that during the campaigns, and the sham false passes (2.5) measure the gate's specificity on 60 shams.

### 1.9 Length of one campaign

| Item | Value |
|---|---|
| Blocks | 10 |
| Attempts per block | 8 (7 faults and 1 sham) |
| Planned attempts per campaign | 70 fault runs and 10 shams, plus about 4 repeats |
| Mean attempt | about 1,790 s (settle 150, pre-fault 600, apply 10, injection 248, recovery 300, cooldown 300, gate about 180) |
| Attempt time | about 41.7 h |
| Warm-up, leading and trailing baselines | 26 h |
| Provisioning to warm-up, final export and teardown | about 1.5 h |
| Cluster time per campaign | about 69 h |
| Wall time, six in parallel | about 69 h after launch |
| Orchestrator Job deadline | None. The Job ends when the orchestrator exits. R1's 48 h deadline would kill the Job mid-campaign, and a deadline cannot stop billing, because the cluster bills whether the Job runs or not. The alert-only watchdog (6.5 item 4) and the pilot's wall-clock stop rule bound the cost instead |

## 2. Injection verification gate

### 2.1 What the gate tests

Each attempt must pass five checks.

1. **Mechanism.** The fault resource or injector acted on exactly the intended targets.
2. **Cause-level signature.** The fault exists at its point of application, measured by the probes and container metrics of the verification channel (2.3).
3. **Envelope.** The effect did not reach components the fault design cannot reach, and stateful jobs came back where the fault requires it.
4. **Timing.** Onset and end fall within tolerance of the plan.
5. **Telemetry.** Every scrape target the fault does not target was scraped (3.4.3).

The gate never uses an exported feature as a signature. Lag, throughput, busy time, checkpoint time and dead-letter counts are what the detectors see. Gating on them would keep only the runs detectors find easy, which selects on the outcome. Review 2 replayed the first patch's F4 rule on the R1 IoT runs and showed exactly that. Symptoms are recorded for every attempt and never decide anything.

One exception is allowed. F1 and F2 also require at least one under-replicated partition, which is an exported broker metric. It is used only as a presence test at the lowest possible threshold, which every real broker loss or partition passes by construction, so it selects nothing. It is read from the verification channel copy (11.8). Decision 4 in section 10 asks whether to keep it.

### 2.2 What each attempt records

Section 11.6 defines the verification record field by field. It holds the applied resources and their status snapshots, the expected and selected targets with pod UIDs, the controller identity at onset, the per-fault details (kills, injector counts, F8 sizing), every check with its pre and during values, threshold and result, telemetry coverage, the recovery gate conditions and any remediation.

### 2.3 Verification channel

The verification channel is kept apart from the feature data and never enters a detector.

1. **Container metrics.** kubelet cAdvisor CPU usage and memory working set for `flink-main-container` and the broker containers.
2. **Kubernetes state.** kube-state-metrics for pod UID, readiness, restarts, last termination reason and `kube_pod_container_resource_limits`. The cAdvisor memory-limit series is dropped by the Prometheus chart's relabelling, so limits come from kube-state-metrics.
3. **Controller metrics.** `kafka_controller_kafkacontroller_fencedbrokercount` and `kafka_controller_kafkacontroller_activecontrollercount`, plus a copy of the under-replicated partition series. They come from the broker scrape job `kafka-brokers`, because a second job on the same JMX endpoint would make every broker collect its MBeans twice. The separation is made at export: these series are in the verification allowlist only and never in a feature source (C10).
4. **Controller identity.** `kafka-metadata-quorum.sh describe --status` run in a broker immediately before every apply and after every labelled end, for every run type, and every 60 s, cross-checked with the active-controller metric. A failed read fails F0, F1 and F2 and is recorded only for the other faults (C37).
5. **TCP probes.** Every 10 s, from inside each broker pod, a connect to each other broker's ports 9090 and 9091 with a 0.5 s timeout. From the load generator pod, a connect to each broker's port 9092. R1's 2 s connect retries hid most of the loss.
6. **Packet probes.** Every 10 s, from inside each TaskManager's network namespace, ten connects to the JobManager's port 6123 with a 0.5 s timeout each. The loss fraction is failed connects over ten.
7. **Flink state.** Job names, states and checkpoint counts per job, and registered TaskManagers and slots, from the Flink REST API every 10 s.
8. **Verification consumer.** A separate consumer group reads `raw-events` and `dead-letter` and counts malformed records, attributed to an attempt by a record header.
9. **Load generator status.** The load generator reports its commanded rate, acked produce count and cycle phase every 10 s.
10. **Dead-letter sentinel.** Before the startup health gate the orchestrator writes one sentinel record to `dead-letter`, so the broker leading its partition creates the topic's metric series before the baseline. R1 lost the first scrape of every fresh dead-letter series. Consumers ignore the sentinel (C34).

Probes and the consumer run from the start of warm-up to the end of the trailing baseline, on the same schedule in every segment and run type, so their traffic is in every NORMAL window and cannot mark an injection.

### 2.4 Pass and fail rules

Every signature uses the same rule unless its row in the table gives its own window. Let P be the attempt's pre-fault samples and I its injection samples after a 30 s lead. The pre value is the median of P and the during value is the median of I, per target. For a cumulative counter, such as container CPU, one sample at each 10 s step is the counter's increase over the 60 s before it divided by 60, with both ends inside the window (C41). The check compares those values with the fault's threshold in the table below. A missing pre or during value fails the check. R1 code replaced a missing pre value with a tiny number, so any ratio rule passed. Every check runs on every target. One target meeting the threshold never stands in for the others.

The thresholds are fixed here from each fault's configured intensity, before the pilot. One pilot injection per fault is too few to calibrate from, so the pilot checks the thresholds and does not tune them.

| Fault | Mechanism | Cause-level signature (gates) | Envelope failure | Recorded, never gates |
|---|---|---|---|---|
| F1 | The controller is read just before apply and the target is the follower at the seeded rank among the two followers sorted by node ID. The resource's records list exactly that pod, and its Apply event succeeds within 30 s | Target pod UID changes. Within 60 s of onset the target is not Ready in at least one sample, and the TCP probes from both other brokers to its port 9091 fail at least once. Under-replicated partitions at least 1 in one sample (presence, 2.1) | Any other broker pod, the JobManager or a TaskManager is replaced | Broker unavailable time, ISR changes, leader elections, produce rate |
| F2 | Same follower rule. Records list the follower as source and the two other brokers as targets. AllInjected within 30 s | TCP probes from both other brokers to the follower's ports 9090 and 9091 fail in at least 80% of injection samples and succeed in at least 95% of pre-fault samples. Fenced broker count at least 1 in one injection sample. Under-replicated partitions at least 1 (presence). ISR shrinks are not used, since KRaft fences the broker before any shrink is counted | The follower's pod restarts, or the load generator's probes to the follower's port 9092 succeed in fewer than 95% of injection samples, since clients should stay connected | ISR changes, fencing time |
| F4 | Records list all three Ready TaskManagers | Container CPU at least 2 times pre-fault and at least 1.6 cores, on every TaskManager | A TaskManager is replaced | Busy time, lag |
| F5 | Each of the five kill resources records exactly the TaskManager at its seeded rank among the Ready TaskManagers sorted by name, with an Apply event within 30 s of its planned time | At least 5 new TaskManager UIDs appear between onset and 60 s after the labelled end | The JobManager restarts, or any expected job is not RUNNING within 180 s of the last kill, with checkpoints on S3 | Job restarts, lag, throughput |
| F6 | The injector pod is Ready at least 30 s before the planned onset. It exits 0 and reports acked records at least 95% of the planned count. The first produce ack comes within 5 s of the planned onset | The verification consumer counts at least 90% of the planned count of malformed records for this attempt. The threshold uses the planned count, so a sham, which sends nothing, fails | A Flink job restarts | Dead-letter rate, `raw-events` rate |
| F7 | Records list all three Ready TaskManagers | Packet-probe loss at least 30% during injection and at most 2% pre-fault, on every TaskManager | A broker or the JobManager is replaced | Checkpoint duration, failed checkpoints. Any TaskManager replaced sets the flag F7_TM_LOSS |
| F8 | One resource per TaskManager, each recording exactly its pod. Each stressor's size is 0.85 times the pod's memory limit minus its working set over the last 60 s of pre-fault, rounded down to MiB, computed from the samples the injector read and recorded (C36). A size under 256 MiB fails the mechanism | On every TaskManager, the working-set increase (during minus pre) is at least half that pod's stressor size | Any TaskManager is replaced or has an OOMKilled termination, or its `container_oom_events_total` increases between onset and the end of recovery (a killed stressor, C38) | Heap, GC time, lag, the size recomputed from the export |
| F0 | No resource or injector exists at any point in the attempt | Every fault's signature above, evaluated on the pseudo-targets of 2.5, fails. A sham on which one passes is GATE_FAILED and repeated (C23) | None | None |

Onset and labelled end come from fixed sources.

| Fault | Verified onset | Labelled end |
|---|---|---|
| F1 | Apply event of the kill | Onset plus 300 s |
| F2, F4, F7, F8 | The latest of the per-target first Apply events, so every target is affected at onset | The latest of the per-target Recover events |
| F5 | Apply event of the first kill | Onset plus 300 s |
| F6 | First produce ack | Last produce ack |
| F0 | Planned onset | Onset plus the sham length |

Timing applies to every fault. The labelled end for F2, F4, F7 and F8 must fall within 60 s of the onset plus the configured duration. An overrun of 60 to 120 s keeps the observed end as the labelled end and flags OVERRUN. An overrun above 120 s fails timing, and so does an end more than 60 s early (C25). After deletion, the orchestrator polls until the resource is NotFound, and NotFound later than 60 s after the delete call fails timing. R1 code swallowed failed deletions. A duration fault must show AllRecovered with every record Not Injected before or after the delete. A one-shot pod-kill never shows AllRecovered while it exists, so its proof is the NotFound within 60 s of the delete call, since its finalizer is released only after every record is Not Injected (C33). The onset and labelled end are recomputed from the stored chaos records or injector log and must equal the attempt record's, so the offline gate catches a moved time (C27).

Every attempt, the sham included, also fails its envelope unless its PRE_FAULT was healthy (C26). Over the PRE_FAULT segment, every expected Flink job stays RUNNING with the same job ID, three TaskManagers stay registered, kube-state shows no broker, JobManager or TaskManager pod replaced or restarted, the TCP probes reach at least 95% for every source and destination pair, and packet-probe loss stays at most 2% for every TaskManager. A segment with no sample of a kind fails.

Every rule the preprocessing applies to a valid attempt is also a gate check, so a broken record is repeated during the campaign and never stops the build afterwards (C25). This covers the controller read after injection for F1 and F2, the job list at the end of the attempt, the onset and end sources, the fixed span of F1 and F5 and the stored injector log of every F6 attempt that created its Job.

### 2.5 Sham runs and the gate's false passes

Each campaign has ten shams. The plan gives every sham the same fields a fault cell has. So every fault's signature runs on a sham with concrete pseudo-targets, which are the follower at the sham's seeded rank for F1 and F2, all TaskManagers for F4, F7 and F8, five seeded TaskManager ranks for F5 and the F6 planned count from the baseline statistics. A sham that passes a signature is a false pass of the gate. It fails the gate and is repeated, so it is never labelled (2.4). The false-pass count per signature per campaign is reported, counted over every sham attempt, the failed ones included (C23).

If one signature false-passes on two or more of a campaign's sham attempts, every run of that fault in that campaign is flagged SIGNATURE_FALSE_PASS by the analysis, never by the gate, and an exploratory sensitivity analysis drops those runs. Thresholds never change after registration.

### 2.6 Failed attempts, repeats and halts

1. An attempt that fails any check is never labelled. All its windows, from settle gap to recovery gate, are excluded. Its outcome is GATE_FAILED or TELEMETRY_FAILED with the failing checks named.
2. If anything was applied, recovery and cooldown run before the recovery gate (1.3).
3. The run is repeated in place, at the same block position, after the recovery gate passes. The repeat is a new attempt with the next attempt number and the next settle gap from the reserve stream. An attempt number is never reused, including after a resume.
4. A run is never repeated because its effect was stronger or weaker than expected inside the envelope, or because a symptom was missing.
5. The campaign halts after two consecutive failed attempts, counted across faults. R1 patch code counted per fault, so 16 failures in a row across faults would never have halted. It also halts after more than 8 failed attempts in total, if a sham shows any campaign resource or injector, if a node is replaced, if the clock check fails, if the recovery gate fails after remediation, if an S3 upload keeps failing for 15 minutes, if the Flink REST API does not answer the job list at the end of an attempt for 15 minutes (C25), or if the orchestrator stops.
6. After a halt, the campaign resumes on the same cluster only if the cause was transient, nothing in code, configuration, thresholds or the cluster changed, and the pause lasted under two hours. The paused time is an IDLE segment. The interrupted run gets a new attempt.
7. Otherwise the campaign is abandoned and its cluster torn down. A new campaign starts on a fresh cluster with the next reserve seed. Any code fix after registration goes in the deviations log (5.10).
8. Abandoned campaigns are reported with their cause and attempt counts, and deposited with a flag. They are never analysed.

### 2.7 Orchestrator requirements

1. No path marks an injection done without the gate. R1 code set `injection_success = True` for F5 and F6 unconditionally.
2. Every injector raises on any error and never returns quietly. Every query failure raises. An empty result is never read as zero.
3. Expected targets come from the live pod list by the labels in 11.11 at apply time. No pod names are hard-coded.
4. The gate is one module, `gate.py`. The orchestrator calls it online to decide repeats, and the analysis pipeline calls it offline on the stored records. `make check` fails if the two results differ for any attempt.
5. An attempt's validity is set only in its final outcome. A crash leaves the attempt RUNNING, and resume finishes it as ABORTED and starts a new attempt for the same cell.
6. The orchestrator runs as an in-cluster Job with a namespaced Role (Review 3, section F), no deadline (1.9) and no runtime installs, so laptop credentials and MFA expiry cannot stop a campaign.
7. The orchestrator supports a plan with one block, for the pilot.

## 3. Labelling and exclusion rules

### 3.1 Timeline

Each campaign has one timeline from the start of warm-up to the end of the trailing baseline. Every second belongs to exactly one segment, and EXCLUDED is the default label. The campaign-level segments are WARMUP, BASELINE, TRAILING and IDLE. Attempt segments are listed in 1.3 and 11.5. The preprocessing build fails if any second is unassigned or assigned twice.

R1 preprocessing labelled every second outside a completed run as NORMAL training data. That put failed and retried attempts, validation and remediation time, and outage windows into every fold's training set (Review 2).

### 3.2 Window grid

Each labelled segment has its own grid. PRE_FAULT windows are anchored at the segment's end, the planned onset, and run backwards. INJECTION windows are anchored at the verified onset, RECOVERY windows at the labelled end, and BASELINE and TRAILING windows at their own start. A window that would cross a segment boundary is dropped. R1 used one global grid assigned by midpoint, so every IoT run had a window straddling both injection edges.

### 3.3 Labels

| Segment | Valid fault attempt | Valid sham attempt | Failed attempt |
|---|---|---|---|
| SETTLE, APPLY, COOLDOWN, GATE | EXCLUDED | EXCLUDED | EXCLUDED |
| PRE_FAULT | NORMAL | NORMAL | EXCLUDED |
| INJECTION | ANOMALOUS | SHAM | EXCLUDED |
| RECOVERY | RECOVERY | SHAM_POST | EXCLUDED |

BASELINE windows are NORMAL. TRAILING windows are TRAILING. WARMUP and IDLE are EXCLUDED. A valid attempt is VALID or VALID_REMEDIATED. A BASELINE or TRAILING window that fails the NORMAL-time health rule of 3.4.7 is EXCLUDED.

NORMAL therefore comes only from the healthy leading baseline and the healthy pre-fault of valid attempts and shams. Only NORMAL windows train the semi-supervised models. The supervised random forest trains on NORMAL and ANOMALOUS. RECOVERY, SHAM, SHAM_POST and TRAILING never enter any fit.

In evaluation, ANOMALOUS is positive. NORMAL, RECOVERY, SHAM and SHAM_POST windows of the test block are negative. TRAILING windows are scored as an extra negative-only set (4.2). Every window carries its run type, so a sham can never be read as a fault or as baseline.

Labels come from the schedule only for attempts whose cause was verified, and the span runs from the verified onset to the observed end. For F1 and F5 the span is a fixed 300 s that includes catch-up after the kills, and the paper says so.

### 3.4 Exclusion

1. The unit of exclusion is the attempt. No rule may remove only the ANOMALOUS windows of a run, or any subset of a run's windows. The R2 draft removed only the F1 and F2 fault windows. That left positives later in the campaign than the negatives and moved a time-only score from 0.48 to 0.59 (`run_faults.py`).
2. Exclusions come only from the outcome codes and the telemetry rule, computed from the manifest and the verification channel. Nothing computed from features or detector scores can exclude a window.
3. An attempt is TELEMETRY_FAILED if a scrape target the fault does not target misses more than 3 scrapes in one pre-fault, injection or recovery segment (decision 21, C22). A scrape target is a broker, JobManager or TaskManager pod while kube-state-metrics shows it. A scrape is missed when Prometheus `up` for that pod is absent or 0 at a 10 s step. The fault's own targets may miss scrapes in injection and recovery: the target broker for F1 and F2, and every TaskManager, the replacements included, for F4, F5, F7 and F8. A TELEMETRY_FAILED attempt is repeated in place like a gate failure. Series presence decides nothing. A job restart empties task metrics and a stressed pod answers late, so a presence rule would repeat the runs with the strongest effects.
4. Task-level series presence per feature source is recorded in the verification record, and an untargeted source under 95% of its samples flags the attempt SERIES_GAP. Every window also carries `meta__telemetry_ok` and the per-source sample counts (11.9), so gaps that do not fail an attempt stay visible. Missing TaskManager series are never imputed silently. The count of TaskManagers reporting is a feature.
5. `make labels` writes the timeline and window labels per campaign with SHA-256 hashes. The hashes are committed before any model is trained on that campaign. Any later change is a deviation.
6. Sensitivity analyses that drop runs (flagged signatures, VALID_REMEDIATED, AFTER_REDEPLOY, AFTER_RESUBMIT, F7_TM_LOSS, SERIES_GAP, HEALTH_EVENT) drop whole runs.
7. NORMAL time must be healthy (decision 24, C26). The gate checks it for every PRE_FAULT (2.4). The preprocessing checks every BASELINE and TRAILING window on the same conditions, over the window plus 10 s on each side, from the probe and kube-state data of the baseline and trailing exports: every expected job RUNNING with an unchanged job ID, three TaskManagers registered, no broker, JobManager or TaskManager pod replaced or restarted, TCP probes at least 95% for every pair and packet-probe loss at most 2% for every TaskManager. A failing window is EXCLUDED, a window with no sample of a kind fails, and the counts per segment and condition are reported. This rule is fixed before any data exist and uses no feature.

### 3.5 Outcome codes

| Code | Meaning | Labelled |
|---|---|---|
| VALID | Gate passed, recovery gate passed without remediation | Yes |
| VALID_REMEDIATED | Gate passed, recovery needed the scripted remediation | Yes, flagged |
| GATE_FAILED | Mechanism, signature, envelope or timing failed | No, repeated in place |
| TELEMETRY_FAILED | Missing samples on an untargeted feature source | No, repeated in place |
| ABORTED | The orchestrator stopped mid-attempt | No, repeated in place |
| SKIPPED_UNHEALTHY | The health gate failed before the settle gap, so no attempt started | Not an attempt |

### 3.6 Secondary labellings

These are exploratory. Each is reported with its prevalence and its always-fire F1.

1. Cause-onset alignment from the verification channel, for example F1 restricted to the time the target broker was not Ready.
2. Symptom-onset alignment, the R1 statistic.
3. Three states (NORMAL, ANOMALOUS, RECOVERY scored separately).
4. RECOVERY windows dropped.

## 4. Evaluation

### 4.1 Features

These rules are fixed before the pilot ends. Section 11.9 gives the column names.

1. Same 10 s scrapes, 30 s windows and six aggregations (mean, standard deviation, minimum, maximum, slope, interquartile range) as R1.
2. Every cumulative counter becomes a per-second rate at 10 s resolution before windowing. A negative step is a reset, and that step is dropped. R1 kept 5 (e-commerce) and 6 (IoT) raw counter levels that tracked time with Spearman 0.97 to 1.0.
3. TaskManager series are aggregated by role at each 10 s step into minimum, maximum and mean, and the count of TaskManagers reporting is a feature. A replaced pod creates no new columns and leaves no empty ones. Broker series stay per broker, keyed by pod name and node ID, never by pod IP, which changes when a broker restarts. For each source that holds TaskManager series (consumer, Flink and JVM), the share of its TaskManager aggregate columns with no value at the step is also a feature. A job restart or a lost TaskManager therefore shows in the features even where 4.1.4 fills a window statistic with its training median (3.4.4, decision 25). The same holds per broker. For each broker identity and for the broker and JVM sources, the share of that broker's columns with no value at the step is a feature, and the number of brokers reporting is a feature (decision 34). A column counts as expected only from its first value in the campaign, and Kafka's lazily opened dead-letter meters are left out of the shares, so a meter that closed after a leadership change does not carry fault history into normal time.
4. Variance and correlation filters are fitted per fold on that fold's training NORMAL windows only. Imputation and scaling are fitted the same way. Protected columns are never removed by either filter, and they take no part in the correlation filter. They are the TaskManager count, the TaskManager missingness features of 4.1.3, the broker count, the broker missingness features, the eight derived features and a fixed list of event counters in `config/pipeline.json` (`fold_fit.protected`): under-replicated partitions, partitions under minimum ISR, offline partitions, preferred-replica imbalance, ISR shrinks and expands, failed checkpoints, failed offset commits, consumer rebalances, and records into the dead-letter sink. These are zero or constant in healthy operation, which is why the R1 variance filter removed them. A column that is constant on training NORMAL keeps unit scale and is centred on its value, so it is 0 there (Review 4 CAM-4, decision 25).
5. The eight derived features stay, computed only inside one contiguous group (11.9), so `lag_velocity` never differences across a run boundary as it did in R1. They are protected under 4.1.4. Two carry nothing on this deployment, as the pilot showed (decision 43). `watermark_lag` is undefined because the jobs assign no watermarks, so 4.1.4 drops it as empty. `rebalance_frequency` is always 0 because Flink's KafkaSource never joins a consumer-group rebalance. The paper reports six informative derived features.
6. Verification-channel series are never features.
7. Contiguous sequences are the default and the only mode. A sequence uses consecutive windows of one attempt's PRE_FAULT, INJECTION and RECOVERY, which may cross that attempt's APPLY gap of at most 30 s, or consecutive baseline windows. It never crosses any other excluded segment. The sequence-length range is 5 to 11 windows (decision 27, `evaluation.seq_length` in `config/pipeline.json`). With a 600 s pre-fault every model, at any length in that range, scores the last 10 pre-fault windows of every attempt, which are the within-run negatives of 4.5.2. R1 searched 5 to 20.
8. Some series never become features (decision 42, `counters.exclude_series` in `config/pipeline.json`). They are the JVM memory pools that grow with process age (Metaspace, CodeHeap, Compressed Class Space, G1 Old Gen, the non-heap area and Flink's NonHeap and Metaspace gauges), which the monotone guard stopped in the pilot, and two families of Kafka meters that open on first use and close on a restart or a leadership change (REVIEW4_LIVE_CONTRACT L3): the dead-letter topic's broker meters, and request types off the data path. Broker request rates keep Produce, Fetch, FetchConsumer, FetchFollower and Metadata, R1's produce and fetch set. The monotone guard stays an error for any other gauge that only grows over the BASELINE. The rule uses metric names and label values only, never a label or a detector score.

### 4.2 Folds

The R1 design stays. Ten folds, leave one block out, with four roles rotating as in Table 5 of the current draft (train seven blocks, `hpo_val` one, `cal` one, `test` one). Each campaign is analysed separately, and folds never mix windows from different campaigns. The block index replaces the repetition index.

Why keep it. Each block holds one run of every type at random positions, so a test fold is one contiguous segment with every fault and a sham in it. The time confound came from running faults in blocks across the campaign, not from the fold design. Keeping the folds keeps Table 4 of the current draft (where labels enter) valid and the tables comparable.

What it does not fix. Training uses blocks recorded after the test block. A forward-chaining variant, training only on earlier blocks, is an exploratory check.

BASELINE windows have the train role in every fold. TRAILING windows have the eval-only role in every fold. They feed false alerts per hour and the drift check, never fitting or pooled AUC.

### 4.3 Cross-campaign and cross-workload evaluation

1. **Same workload, new cluster.** Train the frozen configuration (6.1) on campaign X with blocks 1 to 8 for training, 9 for `hpo_val` and 10 for `cal`. Score every block of campaign Y. Exploratory.
2. **Cross-workload.** The R1 cross-train design on the features both workloads share, with the frozen configuration and the scaler refitted on the target's normal windows, which avoids the R1 source-scaler artefact. The within-workload control uses the same features and configuration. This feeds C6.

### 4.4 Models and baselines

The semi-supervised panel is unchanged. It has Isolation Forest, OC-SVM, LSTM-AE, Transformer-AE, CNN-AE, LSTM-VAE, Deep SVDD and DAGMM. Their scoring differs from the R1 code in three places, and the paper says so (Review 4 CAM-5, decision 26). Isolation Forest and OC-SVM scores use one min-max map per fold, fitted on the training NORMAL scores and applied to every split, where R1 rescaled each scored set by its own extremes. DAGMM's non-finite energies take the largest finite training energy, not the largest of the scored set. Deep SVDD's degenerate-encoder check runs once on the training NORMAL encodings, not per scoring batch.

The references also stay. They are the supervised random forest, the curated rules (raw and per-rule tuned), the static max-z score and EWMA exactly as coded in `classical_baselines.py`. The EWMA variants of Table 27 in the current draft and the other classical detectors (CUSUM, Matrix Profile, POT, COPOD, HBOS, LODA) are exploratory.

Three diagnostic rows need no training.

1. **Time-only.** The score is the window's start time. Inside one run it ranks every ANOMALOUS window above every pre-fault window, so its within-run AUC and its sham placebo are 1 by construction. It is therefore a pooled-AUC reference only. C5 uses block position as its time reference instead (5.8).
2. **Phase position.** The score is the seconds since the attempt's pre-fault start. It shows what a score that rises within a run earns on pooled AUC by construction. The value is computed per campaign.
3. **Always-fire.** Every window flagged. F1 is 2p/(1+p) at the evaluated prevalence p.

### 4.5 Metrics

Threshold-free metrics, per fold.

1. **Pooled AUC-ROC.** Positives are every ANOMALOUS window of the test block. Negatives are every NORMAL, RECOVERY, SHAM and SHAM_POST window of the block.
2. **Within-run AUC-ROC.** For each fault run, its ANOMALOUS windows against the same attempt's within-run negatives: its last 10 pre-fault windows, counted back from the planned onset. The preprocessing tags them in the `within_run` column (11.9) and every score file carries the tag, so every model uses the same negatives. With sequence lengths of at most 11 windows and a 600 s pre-fault, every model scores all ten, and `train.py` stops at the 30 s anchor if a model does not (decision 27). Each run contributes 10 negatives. The fold value is the mean over the block's seven fault runs. Per-fault values are kept.
3. **Sham placebo.** The same computation on the block's sham, its SHAM windows against its own 10 within-run negatives. It should be 0.5. The placebo-corrected within-run AUC is the within-run AUC minus the sham value of the same fold. The R1 placebo is also reported, fixed as the 5 late within-run negatives against the 5 early ones of each run (`within_run` values `late` and `early`), with the same windows for every model. R1 compared the later and the earlier half of the whole pre-fault, which sequence models above length 10 could not score.
4. **PR-AUC**, with the prevalence p beside it as its chance level.

Operating points.

5. **Fixed false-positive budgets without labels.** The threshold is the (1 minus b) quantile of the `cal` block's NORMAL scores, for b of 0.01 and 0.05. No labelled anomaly is needed. This is the primary operating point for alert and latency metrics.
6. **F1-optimal threshold on `cal`**, the R1 operating point, with F1, precision and recall, and the always-fire F1 at the same population beside them. Every distinct observed `cal` score is a candidate threshold (Review 4 MIN-8). R1 searched 0.01 to 0.99, which lies below almost every max-z score, so its rule-based F1 was the always-fire F1.
7. **IQR threshold** on training scores, as in Table 20 of the current draft. Exploratory.

Alert burden at each operating point.

8. Realised false-positive rate on test negatives. False alerts per hour, counted as alert onsets on negative windows per hour of negative time, on the test block and on the trailing baseline. Duty cycle, the share of negative windows in alert.
9. **Episode detection rate**, the share of verified fault runs with at least one alert in the labelled span. **Sham false-episode rate**, the share of shams with at least one alert in the sham span. The pair is an episode-level operating point.

Latency.

10. Time from verified onset to the first alert in the labelled span, at the fixed-budget thresholds, censored at the labelled end and estimated with Kaplan-Meier. Report the restricted mean time to detection up to the span length, which always exists, the Kaplan-Meier median where detection passes 50%, and the detection rate. A secondary version measures from cause-level onset. Resolution is one 30 s window. R1 measured latency at F1-optimal thresholds, where saturated detectors fire at once and report 0 s.
11. Computational cost. Training time per fit and scoring time per window, from one script on one recorded machine, with hardware and batch size stated.

### 4.6 Reporting rules

1. Every results table carries the three diagnostic rows.
2. A ranking or superiority statement between detectors goes into the abstract, highlights or conclusion only if it holds on both pooled and placebo-corrected within-run AUC and meets the replication criterion (5.5). If the two metrics disagree, the text reports both and calls the result metric-dependent. In R1 within-run AUC put EWMA last and the autoencoders near the top, the reverse of pooled AUC.
3. F1 values from different populations are never compared without the always-fire F1 beside each.
4. A pooled result is flagged if its campaign's time-only AUC has a 95% interval that excludes 0.5, or if its drift check (1.5) fails.
5. Table and figure captions stay at 20 words or fewer. Procedure goes into table notes or the text (R7.3). The table generator enforces it.

## 5. Statistics

### 5.1 Units

The fold, one test block, is the unit of estimation. Each campaign has ten. The campaign is the unit of replication. Windows are never units.

### 5.2 Within one campaign

For each contrast, take the ten fold-level paired differences. Report their mean, a t-based 95% interval with 9 degrees of freedom, the exact two-sided Wilcoxon signed-rank p, and the matched-pairs rank-biserial correlation. A block bootstrap interval (BCa, resampling test blocks) is a sensitivity check.

The smallest exact two-sided Wilcoxon p with ten folds is 2/1024, about 0.00195. A Holm correction over many pairs cannot pass inside one campaign, which is why decisions use the pooled estimate.

### 5.3 Across campaigns

1. **Stratified pooled estimate.** The mean of the campaign means. Its standard error is the square root of the summed per-campaign variances of the mean, divided by the number of campaigns K. It uses a t reference with the summed within-campaign degrees of freedom. It is conditional on these clusters, and it is the basis for decisions.
2. **Random-effects estimate.** The model is d = mu + u(campaign) + e, with campaign variance tau squared, fitted by REML. Report mu, tau and the intraclass correlation. With two or three campaigns tau is poorly estimated, so this is descriptive. A singular fit (tau estimated at zero) is reported as such. R1 hit the same problem with fold as a random effect.
3. **Variance decomposition** of absolute pooled AUC for each detector into between-campaign and between-fold parts.
4. **Stratified sign-flip permutation p** for the pooled mean. Exact over all sign vectors for two campaigns, one million random flips for three. Robustness only.

### 5.4 Decision rules and the margin

Every contrast ends in one of four outcomes, with margin Delta in AUC units.

| Outcome | Rule on the pooled confirmatory estimate |
|---|---|
| Superior | 95% interval lower bound above 0, after Holm (5.7) |
| Not superior | One-sided 95% upper bound below Delta |
| Equivalent | 90% interval inside minus Delta to plus Delta (two one-sided tests at 0.05) |
| Inconclusive | None of the above |

A difference that fails to reach p below 0.05 is never written as "no difference".

The recommended Delta is 0.03 AUC (decision 3 in section 10). It comes from R1 power. In `ewma_mechanism_maxz_contrasts.csv`, the standard error of the paired deep-minus-max-z AUC difference over ten folds is 0.008 to 0.028, with a median near 0.013. Pooled over the two confirmatory campaigns (2 and 3) the median case is about 0.009. With a true difference of 0, equivalence at Delta 0.03 then passes about 85% of the time, and at Delta 0.02 about 35%. Non-superiority at Delta 0.03 passes about 94% of the time. DAGMM is the noisiest model (0.028 on e-commerce, 0.021 on IoT), so its pooled error would be near 0.02 and its equivalence tests will often end inconclusive. The same calculation for within-run AUC uses the R1 per-fold within-run values and is done before registration. It uses the within-run negative set of 4.5.2, 10 pre-fault windows per run.

### 5.5 Replication criterion

This applies to each headline claim, per workload.

| Status | Rule | Where it can appear |
|---|---|---|
| Replicated | The decision rule holds on the pooled confirmatory estimate. Every confirmatory campaign's point estimate sits on the claim's side (above 0 for superiority, below Delta for non-superiority, inside the margin for equivalence). No campaign's 95% interval lies wholly on the other side | Abstract, highlights, conclusion |
| Replicated in each campaign | The rule holds in every confirmatory campaign analysed alone | Said wherever true |
| Direction consistent | Same sign everywhere, pooled rule not met | Results only |
| Not replicated | Signs differ, or one campaign's interval excludes the claim | Results only, said plainly |

Every headline claim carries one scope sentence. It says the claim holds for this panel, this feature representation and this protocol, on a stated number of independently provisioned clusters per workload. That is R7.1a in fixed form. Campaign 1, where the search ran (6.1), is reported beside the confirmatory result from campaigns 2 and 3, and the text says so when campaign 1 disagrees.

### 5.6 Reporting

1. Exact p-values with three digits of precision (for example 0.00195), never only a threshold.
2. The effect size in AUC points leads, followed by the rank-biserial correlation. Cliff's delta stays in the supplement for continuity with R1.
3. An interval for every estimate.
4. The number of folds and campaigns in every table note.

### 5.7 Multiplicity

1. The confirmatory family for each workload is the primary contrast of C1 to C6 (5.8), six hypotheses. Holm at 0.05 runs across them on the pooled p-values.
2. A claim that no member of a set beats B is an intersection-union test. Each member must pass non-superiority at 0.05, so no adjustment is needed. A claim that some member m beats B gets Holm within the set.
3. Requiring a claim on both pooled and within-run AUC is also an intersection-union test and needs no adjustment.
4. The two workloads are separate families. A statement about both needs both.
5. Exploratory analyses get no adjustment. Every exploratory p-value is labelled as such, and no exploratory result goes into the abstract, highlights or conclusion.

### 5.8 Confirmatory hypotheses

All use the 30 s anchor and the primary labels. Every one of them is needed for the R2 submission (8.4).

| ID | RQ | Contrast | Decision rule |
|---|---|---|---|
| C1 | RQ1 | Each of the six deep models minus static max-z, on pooled and placebo-corrected within-run AUC | "No deep model beats max-z" if all six pass non-superiority on both metrics. "Model m beats max-z" if m passes superiority on both, Holm over the six. Otherwise inconclusive |
| C2a | RQ1, R7.1 | EWMA as coded minus max-z, both metrics | Two-sided, superiority in either direction or equivalence |
| C2b | RQ1, R7.1b | EWMA with its fixed end-of-training reference minus EWMA referenced to the training mean, pooled AUC | Superiority. Tests the R1 explanation of EWMA's lead on new data |
| C3 | RQ2 | Mean of the six sequence models minus mean of IF and OC-SVM, on matched windows, per fold | Equivalence and superiority on pooled AUC, within-run AUC required for any headline. The R1 mixed model with a random intercept for model is secondary |
| C4 | RQ3 | Isolation Forest with category fusion weights learned leave-one-fold-out, minus Isolation Forest on all features, same windows, pooled AUC | Superiority |
| C5 | RQ4 | Fault-difficulty transfer. Per detector, Spearman rho between the workloads' per-fault placebo-corrected within-run AUC over the seven faults, each the mean over that workload's confirmatory campaigns. The test statistic is the median rho over the eleven detectors | Exact permutation over all 5,040 orderings of one workload's fault labels, applied to all detectors at once, so their correlation is kept. Reported against two references, the same-workload rho between campaigns (ceiling) and a time reference (should be near 0). The time reference is block position: each fault's value is the mean `position` (11.9) of its runs in their blocks, over that workload's confirmatory campaigns, and the reference is the Spearman rho between the two workloads' per-fault values. A start-time score cannot serve, because its within-run AUC is 1 in every run (4.4.1, decision 27) |
| C6 | RQ4 | Per panel model, within-workload AUC minus cross-workload AUC with the target scaler refitted | "Transfer costs less than Delta for all eight" if all pass equivalence. Per-model results otherwise |
| C7 | Benchmark | Per detector, the mean pooled AUC of each of the three campaigns under the frozen configuration (6.1) | Reproducible if all three lie within Delta of their pooled mean. Descriptive, no p-value, not in the Holm family |

### 5.9 Exploratory analyses

These are planned and labelled exploratory. Each item is marked **R2** if the R2 submission needs it, or **later** if it can wait (8.4).

1. Per-fault pooled and within-run AUC tables and heatmaps. **R2**
2. Window-size sensitivity on campaign 1 only, at 15, 60 and 120 s on both workloads and 300 s on IoT (6.1). **R2**, first to defer
3. Prevalence sweep at 40, 20, 10, 5 and 1%. **R2**
4. IQR thresholds without labels. **R2**
5. Curated-rule variants. **R2**
6. The other classical baselines. **R2**
7. The EWMA mechanism ablation (the Table 27 variants of the current draft and the saturation bound), and EWMA's per-fault within-run AUC on faults with direct magnitude shifts (F4, F7) against the others (R7.1b). **R2**
8. Random forest feature importance and confusion summaries. **R2**
9. Leave-one-fault-out with thresholds only. **R2**
10. Leave-one-fault-out with full retraining. **later**
11. Three-state evaluation, recovery dropped, and cause- and symptom-onset label alignment. **R2**
12. Forward-chaining folds. **later**
13. Same-workload cross-campaign transfer (4.3). **later**
14. Carryover. Max-z score of pre-fault windows grouped by the preceding run type, Kruskal-Wallis. **R2**
15. Load phase balance. Onset phase by fault, a Rayleigh test of uniformity, and onset phase as a covariate. **R2**
16. Baseline drift (1.5). **R2**
17. Seed variance (6.2). **later**
18. Computational cost and the chronological alert replay. **R2**
19. F7 runs with and without TaskManager loss, and runs flagged AFTER_REDEPLOY or VALID_REMEDIATED. **later**

Anything not listed here or in 5.8 is post hoc and labelled so.

### 5.10 Deviations

`diagnostics/DEVIATIONS.md` records every change after registration with its date, reason and the results it touches. The paper reports it in an appendix.

## 6. Compute plan

### 6.1 Hyperparameter search

Fixed by author decision. The search runs on campaign 1 of each workload and optimises AUC-ROC, and its settings are frozen for campaigns 2 and 3.

1. Campaign 1 runs full nested HPO per fold as in R1, TPE with 100 trials per model per fold at the 30 s anchor. The objective is pooled AUC-ROC on `hpo_val`. The R1 objective, F1 at the per-trial best threshold, favours configurations that fire on everything. Max-z reaches F1 0.484 that way.
2. From campaign 1's ten fold winners, one consensus configuration per model per workload is set. Each numeric hyperparameter takes the median, rounded to its grid. Each categorical one takes the mode, with ties going to the smaller model. The configurations are committed and tagged before any feature, label or score file of campaigns 2 and 3 is read.
3. Campaigns 2 and 3 use the frozen configuration with no search. `hpo_val` keeps its role for early stopping on the normal-only loss. Thresholds still come from `cal`.
4. Campaign 1 is also refitted with the frozen configuration, so C7 compares like with like.
5. Window-size sensitivity runs on campaign 1 only, with 30 trials per model per fold as in R1, at 15, 60 and 120 s on both workloads and 300 s on IoT. Those are the R1 grids. Running it on campaigns 2 and 3 as well would add about 1,100 frozen fits that the budget does not cover, and window results feed only exploratory statements. A window result therefore comes from one campaign per workload and is never called replicated.

Campaigns 2 and 3 are collected at the same time as campaign 1, so their data exist before the search ends. The pipeline refuses to read their features, labels or scores until the freeze tag exists (7.1). Before that, only the gate and label steps touch them, and both are fixed code.

With this design the replicates involve no model selection on their own data. Labelled anomalies enter them only through threshold calibration, a narrower label use than Table 4 of the current draft describes.

### 6.2 Seeds

1. Campaign seeds as in 1.4. The seed list, every plan, and every model and sampler seed are recorded in the deposit.
2. Model seed is 1000 times the campaign index, plus 42, plus the fold. The Optuna sampler gets the same seed. R1 used 42 plus the fold.
3. Confirmatory results use one seed per fit.
4. An exploratory seed check refits Isolation Forest, LSTM-AE and DAGMM with three extra seeds on every fold of campaign 2, on both workloads. It can wait until after the submission.
5. cuDNN deterministic flags are set where available. Remaining GPU nondeterminism is stated.

### 6.3 Training job counts

At the 30 s anchor unless stated. "Fit" means one model trained on one fold.

| Item | Formula | Count |
|---|---|---|
| Nested HPO trials, campaign 1 | 8 models x 10 folds x 100 trials x 2 workloads | 16,000 |
| Window sensitivity trials, campaign 1 | 8 models x 10 folds x 30 trials x (3 windows on e-commerce and 4 on IoT) | 16,800 |
| Random forest HPO, campaign 1 | 10 folds x 50 trials x 2 workloads | 1,000 (CPU) |
| Frozen fits, campaigns 2 and 3 | 9 models x 10 folds x 2 campaigns x 2 workloads | 360 |
| Frozen refit of campaign 1 | 9 x 10 x 2 | 180 |
| RQ3 category models, Isolation Forest | 8 configurations x 10 folds x 6 campaigns | 480 (CPU) |
| Cross-workload fits | 9 models x 6 source campaigns | 54 |
| Max-z, EWMA, curated rules, classical detectors | No training | None |
| Seed check (exploratory, later) | 3 models x 3 seeds x 10 folds x 2 workloads | 180 |

Campaign 1's search, 32,800 trials, is the same size as the R1 search.

### 6.4 Cost estimate and approval

The cap is about $870 in total. Nothing beyond the pilot launches without the author's separate approval of the requote after the pilot. The rate is $2.07 per cluster-hour (Review 3). That is $1.8144 for the nodes, $0.10 for the EKS control plane, $0.0658 for root volumes and $0.0899 for about 820 GiB of persistent volumes. With every node in one zone there is no cross-zone transfer.

| Item | Hours | Cost (USD) |
|---|---|---|
| One campaign | about 69.2 | about 143 |
| Six campaigns | about 415 | about 860 |
| Two pilots | about 10.5 each, with one fix-and-rerun cycle | about 43 |
| Training | Author's estimate. Campaign 1's 32,800 trials match the R1 search, which the current draft puts at about $80 of EC2 with its other training, and about 600 frozen and transfer fits come on top | about 100 |
| Other items (NAT gateway and data, Elastic IP, S3, ECR) | Author's estimate | about 30 |
| Total | | about 1,033 |

The design as written exceeds the cap by about $160. The campaign line alone is about $860 against the $680 the author set aside, which allows about 55 h per campaign at this rate. Three options are costed. Decision 1 in section 10 picks one.

| Option | Changes | Campaign length | Six campaigns | Total |
|---|---|---|---|---|
| A | As written | about 69.2 h | about $860 | about $1,033 |
| B | Monitoring node from r6g.2xlarge to r6g.xlarge if the pilot shows the load generator holds its rate and Prometheus keeps every scrape, so $1.87 per hour. Trailing baseline dropped | about 68.2 h | about $765 | about $938 |
| C | B, plus pre-fault 300 s instead of 600 s and settle gap 0 to 120 s | about 59.1 h | about $663 | about $836 |

Option C fits the cap, but decision 27 makes it unworkable as written. Its 300 s pre-fault holds 10 windows at 30 s. A sequence model of length L cannot score the first L minus 1 of them, so with lengths of 5 to 11 no sequence model scores all 10 within-run negatives of 4.5.2, and a length of 11 scores none. Option C would therefore drop the six sequence models from the within-run analysis, which C1, C3 and C5 use. Option B keeps the 600 s pre-fault and runs about $70 over. With option C out, the choice after the pilot is between A and B, against the requote.

Billing safeguards come before the pilot. The `Project` and `Campaign` cost allocation tags are activated and a tag-filtered budget alerts at 50, 80 and 100% of the cap.

### 6.5 Running protections

1. The orchestrator runs in-cluster with no deadline (1.9, 2.7).
2. The results bucket lives in the `shared` Terraform root with no forced destroy, versioning and a deny on deletes under `campaign/`, so tearing down a cluster cannot delete its data (Review 3, A1).
3. Exports and verification files reach S3 after every attempt and every baseline hour. A failed upload is retried and then halts the campaign (2.6). Uploads never exit 0 on failure, as the R1 exporter did.
4. A watchdog alerts if no attempt has finished within twice the longest expected attempt. It is the CronJob `campaign-watchdog`. Every 10 minutes it publishes to the alert topic when the orchestrator Job has failed, when the orchestrator container has ended with a non-zero code, or when a RUNNING manifest has not changed for 2 h. It never restarts or changes anything.
5. Teardown runs only after `COMPLETE` exists and is newer than every result object (11.10). The pre-destroy script deletes Kafka, Flink, services and volume claims while the operators still run, and a tag sweep then checks for leftover instances, volumes, network interfaces, load balancers and roles.
6. AWS sessions use the staging MFA session script, with the one-time password asked for first. Resource names use the `streaming-anomaly-detection-` prefix and no paper number.

## 7. Reproducibility

### 7.1 One-command pipeline

`make all` runs every stage from raw files. Each stage checks the hashes of its inputs and writes the hashes of its outputs.

1. `make verify-inputs` checks every raw file against `checksums/SHA256SUMS` and the `COMPLETE` marker.
2. `make gate` rebuilds every attempt's gate result from the verification records and compares it with the online result.
3. `make labels` builds the timeline and window labels, checks full coverage and checks them against the committed hashes.
4. `make features` builds the window features per campaign.
5. `make folds` writes the fold role files.
6. `make train` runs HPO and fits on GPU and writes per-window scores. `make scores` fetches the stored scores instead.
7. `make metrics` computes per-fold metrics.
8. `make stats` runs the confirmatory and exploratory tests and writes the replication decisions.
9. `make tables figures` writes every table as `.tex` and every figure as `.pdf`.
10. `make numbers` writes `numbers.tex` and `claims.json` (7.2).
11. `make check` runs the claims harness.

`make train` runs campaign 1 first. `make freeze` then writes the consensus configurations (6.1) and the freeze tag. Every step after `make labels` refuses to read campaigns 2 and 3 until that tag exists.

`make from-scores` runs steps 1 to 5 and 7 to 11 on CPU from the stored scores. It is the path a reader uses. `make all` adds the GPU training.

### 7.2 Numbers in the manuscript

Every number in the abstract, results, discussion and conclusion is a LaTeX macro from `numbers.tex`, generated by `make numbers`. `claims.json` maps each macro to the script and output cell that produced it. `make check` fails on a bare number in those sections that is not on an allowed list (section numbers, years, fixed design constants), on a caption over 20 words, and on any macro whose value changed without a regenerated PDF. This answers R5.1, so the text, tables, figures and supplement cannot drift apart.

### 7.3 Pinned environments

1. Python dependencies in a lock file with hashes. Training in a container pinned by digest, with CUDA and PyTorch versions recorded.
2. The three Terraform roots with exact module versions and lock files for linux_amd64, linux_arm64 and darwin_arm64. EKS add-on versions and the AMI release are captured for 1.35 and committed as `.auto.tfvars` with no defaults.
3. Every runtime image mirrored into ECR as arm64 by digest and listed in `images.lock.yaml`, which is also copied into each campaign's S3 prefix. kubectl in every image matches 1.35.
4. Docker builds target `linux/arm64`, and Gradle dependencies are locked.
5. The analysis machine's OS, CPU and Python version are recorded in every `make` log.

### 7.4 Checksums

1. A SHA-256 for every object under a campaign's S3 prefix, listed in `checksums/SHA256SUMS` (11.10).
2. A SHA-256 for every derived file (timeline, windows, folds, scores, tables).
3. Each generated table carries a comment with the hashes of its inputs.

### 7.5 Public deposit

The deposit holds data and code only. It is a new dataset version. Zenodo v8 stays as it is, and the new README says which earlier versions are superseded and why (decision 6 in section 10).

What goes in:

1. Every campaign's S3 prefix except scratch, which holds the plan, manifest, baseline statistics, raw exports, probes and verification records.
2. The gate code and thresholds.
3. The derived timeline, window, fold and per-window score files for every model, fold and campaign.
4. Frozen configurations and the Optuna trial tables.
5. Analysis code, the three Terraform roots, Helm values, manifests, the orchestrator, exporter and injector source with their Dockerfiles, lock files and `images.lock.yaml`.
6. `SHA256SUMS`, README and licence.
7. Pilot and abandoned-campaign data, flagged as not analysed (decision 5 in section 10).

What stays out:

1. The manuscript, response letters, cover letters and highlights.
2. Process notes, including everything in `diagnostics/`, trackers and project notes files.
3. Terraform state, tfvars, backend settings, kubeconfigs, keys and logs with credentials.
4. Account names, account IDs, profile names, bucket names and role ARNs. The results bucket name contains the account ID, so every key in the deposit is relative to the campaign prefix.

Before upload, a scan of the whole deposit tree checks for account identifiers, profile and bucket names, keys and tokens. The GitHub mirror gets the same scan.

### 7.6 Registration

After the pilot passes and before the six campaigns launch, this protocol with its thresholds, the seed list, the plan hashes and the analysis code tag is registered with a public timestamp (decision 5 in section 10). The registration says the pilot came first and what it changed (decisions 40 to 47). Code fixes found by running it: the F4 CPU sample over 60 s (C41), three instants rounded to the millisecond (C42), training on a one-block pilot, a refusal in the manual-marker script and the identifier scan. One system setting: the Flink restart limit from 20 to 40. One feature rule: the series of 4.1.8, chosen from how series behave over time (growth with process age, presence after a restart), never from labels, detection results or any score. The pilot tuned no gate threshold, `run/thresholds.json` is unchanged, and the Isolation Forest dry run on pilot data was a code test whose result chose nothing.

## 8. Mapping

### 8.1 Research questions

| RQ | Confirmatory | Exploratory | Notes |
|---|---|---|---|
| RQ1, semi-supervised performance | C1, C2a, C2b | Per-fault tables, classical baselines, random forest, curated rules, prevalence, IQR thresholds, operating points, latency, alert burden | The RQ1 wording lists "broker failure". It stays if F1 and F2 pass the pilot |
| RQ2, temporal context | C3 | Sequence-length sensitivity across the searched range of 5 to 11 windows, mixed model | The range stops at 11 so that, with the 600 s pre-fault, every model scores the same 10 within-run negatives (4.1.7, 4.5.2, decision 27). R1 searched 5 to 20 but its 300 s pre-fault left sequences above 11 without a pre-fault window to score. Option C of 6.4 would bring that problem back, so it is no longer workable |
| RQ3, telemetry categories and fusion | C4 | Single-category and leave-one-category-out models, the other fusion rules | The broker category is testable again if the broker faults work |
| RQ4, cross-workload generalisation | C5, C6, C7 | Window-size optimum transfer, same-workload cross-campaign transfer, operating-point transfer | C7 is new. It asks whether benchmark numbers reproduce on a fresh cluster of the same workload |

### 8.2 Reviewer points

| Point | What answers it |
|---|---|
| R5.1, consistency of text, tables, figures and supplement, with statistics, latency, false alerts and cross-workload claims documented | One pipeline and `numbers.tex` (7.1, 7.2). Reporting rules (5.6). Latency and alert definitions (4.5). Cross-workload wording fixed by the replication statuses (5.5) |
| R7.1a, scope the EWMA claim | The scope sentence and replication rule (5.5), applied to C1 and C2a |
| R7.1b, telemetry characteristics or model limits | C2b on new data. Exploratory item 5.9.7, the EWMA ablation, saturation bound, and per-fault within-run AUC on faults with direct magnitude shifts. Counters as rates (4.1) remove the step-valued counter levels R1 traced the lead to, so the answer may change |
| R7.2, highlights about the dataset, faults, cross-workload scope and reproducibility | Highlights written after the stats run, from replicated claims and the verified-fault dataset with its verification records |
| R7.3, shorter titles for Tables 12 to 16 | Captions of 20 words or fewer, enforced by the generator and `make check` (4.6) |

### 8.3 Existing claims expected to be retired or restated

| R1 or R2 draft claim | Expected status | Why |
|---|---|---|
| Eight working faults in three or four categories | Restated | Seven faults, named by verified effect, and F3 reported as never run |
| F3 disk I/O latency, effect unconfirmed | Restated | F3 never ran. The disk-fault binary does not run on ARM, and every R1 F3 deletion overran by 122 to 124 s |
| R1 explanation of why broker faults are hard to detect | Retired | No broker fault took effect in R1 |
| Bucket B below-chance AUC (0.32 and 0.31) | Retired | Bucket B held windows without a fault |
| Fault-difficulty transfer, rho 0.74 to 0.98 | Retired, replaced by C5 | The clock alone gives rho 1.0 under the R1 order |
| EWMA not beaten by any deep model | Retired, replaced by C2a | Depends on pooled AUC, fixed reference and invalid windows |
| EWMA's lead comes from its fixed reference | Retested, C2b | Mechanism claim, may survive |
| No deep model beats max-z | Retested, C1 | Held in every subset checked so far, but on invalid labels |
| Sequence models no better than point models | Retested, C3 | Now with sequence lengths of 5 to 11 windows, each scoring the same within-run negatives as the point models (4.5.2). Option C of 6.4 is no longer workable, because it would leave the sequence models without those negatives |
| Learned fusion raises AUC from 0.551 to 0.660 | Retested, C4 | |
| Cross-train transfer penalty 0.004 to 0.008 | Retested, C6 | |
| F5 is a rebalancing storm, F8 is memory pressure, F6 is malformed Avro | Restated | R1 data show a full Flink outage for F5 and pod loss for F8, and F6 sent JSON fragments |
| All runs completed and recovered, faults F6 to F8 self-healed | Restated | In 24 R1 runs a stateful job never restored after a TaskManager loss, and those RECOVERY windows hold a dead job (Review 1) |
| Labels are symptom-aligned (7 of 721 windows) | Retired as label evidence | The statistic passes for runs with no fault. The run-level gate replaces it |
| 56.8 h e-commerce baseline | Restated | 3,661 baseline windows lay inside campaign runs, 3,085 of them inside failed first-session runs, so the true baseline was 26.3 h (Review 2) |
| Window optima differ between workloads | Retested, exploratory | Three of the R1 faults behind it did not run |
| Duty cycles 0.54 to 1.00 at F1-optimal thresholds, low budgets unreachable under sigmoid saturation | Recomputed, exploratory | A score-function property, likely to persist, numbers change |
| F1 tracks prevalence | Kept as method, numbers recomputed | Does not depend on the faults |
| Label-free thresholds cost 0.13 to 0.44 F1 | Recomputed | |
| 160 controlled runs, two e-commerce sessions | Restated | New counts, one session per campaign |
| Feature selection fitted once, residual effect at most 0.008 | Retired | Selection now runs per fold on training windows only |
| Table 28 and every "without F1 and F2" analysis | Retired | No longer needed |
| Full leave-one-fault-out broker-crash hold-out | Retested, exploratory, after the submission | Its table leaves the R2 until then (8.4) |
| Training cost table | Recomputed | |

### 8.4 What the R2 submission needs

The R2 target is 27 October 2026. The work runs as one chain. The offline rebuild and re-review lead, followed by the pilot, about 69 hours of parallel campaigns, the search on campaign 1, the frozen fits on campaigns 2 and 3, and then statistics, tables and writing. Each step waits for the one before. The author expects to request an extension after the pilot, and the split below holds for whatever date the editor sets.

| Needed for the R2 submission | Can wait |
|---|---|
| Gate and verification summary per fault and campaign, with outcome counts and sham false passes (2.5) | Leave-one-fault-out with full retraining (5.9.10) |
| Label accounting per campaign, replacing Table 7 of the current draft | Forward-chaining folds (5.9.12) |
| C1 to C7 with per-campaign and pooled estimates, decisions and replication statuses (5.8) | Same-workload cross-campaign transfer (5.9.13) |
| RQ1 tables for both workloads with the three diagnostic rows, pooled and within-run AUC, PR-AUC, and F1 with always-fire F1 (4.5, 4.6) | Seed check (5.9.17) |
| Operating points at the fixed budgets, alert burden, episode and sham false-episode rates, and latency (4.5), for R5.1 | Flagged-run subgroups (5.9.19) |
| Computational cost (5.9.18), for R5.1 | Window sensitivity on campaigns 2 and 3, which the budget excludes anyway |
| The EWMA mechanism analysis (5.9.7), for R7.1b | |
| Exploratory items 5.9.1 to 5.9.9, 5.9.11, 5.9.14 to 5.9.16 | |
| `numbers.tex`, the claims harness and the caption rule (7.2, 4.6), for R5.1 and R7.3 | |
| Highlights from replicated claims (8.2), for R7.2 | |
| The deposit built and audited, with a DOI for the data availability statement (7.5) | |

If the chain slips, these are deferred in this order. Window sensitivity is deferred before anything else, and the window figure and the RQ4 window statement leave the submission with it. Then the other classical baselines beyond EWMA and max-z. Then the threshold-only leave-one-fault-out. Then the three-state and onset-alignment analyses. Then the random forest importance and confusion summaries. The confirmatory hypotheses, the gate summary, the RQ1 tables, the operating points, latency, cost and the EWMA mechanism analysis are never deferred, because the research questions and the reviewer points depend on them.

Two rules hold for the submission.

1. No R1 or R2-draft number appears in the rebuilt results. A table whose analysis was not rerun is removed, with one sentence saying the analysis is pending.
2. A deferred analysis keeps its place in this protocol and runs later on the same frozen code.

## 9. Risks and fallback

### 9.1 Risks

| Risk | Effect | Mitigation |
|---|---|---|
| A fault cannot be verified in the pilot (F5 job recovery from S3, F8 sizing without a kill, Chaos Mesh on containerd 2) | Smaller taxonomy | Drop it before any campaign and say so. Never drop after data exist |
| Gate too strict | Many repeats, longer campaigns | Thresholds set from configured intensity and checked in the pilot (2.4), halt rule (2.6) |
| Gate too lax | Fault-free windows labelled again | Cause-level signatures, sham false passes reported (2.5) |
| Gating selects on detectability | Inflated detection | Gate uses cause-level probes and container metrics, never features, with one presence-only exception (2.1) |
| Carryover between randomised runs | Noise in pre-fault windows | Recovery gate with job, checkpoint, imbalance and probe checks (1.6), carryover check (5.9) |
| Load cycle lines up with onsets | Load mimics faults | Settle gap, recorded load phase, balance check, sham runs |
| Between-campaign variance is large | Claims do not replicate | That is a finding and is reported. C7 measures it |
| Margin choice is disputed | Equivalence claims questioned | Margin fixed before data with a power basis (5.4). Intervals reported so readers can apply their own |
| Pinned versions untested together on EKS 1.35 | Setup fails in the pilot | Pilot canaries (1.8). The Flink operator falls back to 1.16.1, recorded as a deviation |
| Six clusters share one VPC, NAT gateway and zone | One network or zone event reaches all six, and image pulls share one public IP | AWS Health events recorded, overlapping attempts flagged (1.7). Images come from ECR, so no Docker Hub pulls go through the NAT |
| Gate thresholds fixed without a calibration sample | A threshold could be too strict or too lax | Thresholds set from configured intensity, checked by one injection and one sham per workload in the pilot, false passes measured on 60 shams (2.5) |
| The design exceeds the $870 cap at $2.07 per hour | Overrun | Options A to C (6.4), decision 1 in section 10, tag-filtered budget alerts |
| IoT broker disks fill during the baseline | Broker failure mid-campaign | 2 h retention on the two large topics, 300 Gi volumes, pilot check under 70% after 4 h (1.7, 1.8) |
| Analyst freedom after seeing data | Hidden forking paths | Frozen code and thresholds, label hashes, freeze tag, deviations log |
| A campaign is abandoned | Fewer campaigns for one workload | Reserve seeds, fallback rules (9.2) |
| Results no longer mostly negative | Paper framing changes | The protocol does not depend on the direction of any result. The framing follows the confirmatory outcomes |
| Zenodo v8 stays public with F1, F2, F3 and F6 windows labelled anomalous | Reuse of invalid labels | New version README, and the cover letter says what was found (author decision 2) |
| The R2 date | The chain is serial, so any halt or abandoned campaign moves every later step | Extension request after the pilot (author decision 20). Defer order (8.4). Fallback to the campaigns that finished (9.2) |

### 9.2 Fallback if a campaign is lost

The design is three campaigns per workload. If a campaign is abandoned (2.6) and its reserve cannot finish inside the budget or before the agreed date, that workload proceeds with the campaigns that finished.

1. With campaign 1 and one replicate, the replication criterion asks the claim to hold in both. Campaign 1 used the search and the replicate used the frozen settings, and the text states the difference.
2. If campaign 1 itself is lost, campaign 2 becomes the search campaign and campaign 3 the replicate. That switch happens before any of their data are read beyond the gate and labels, which the freeze rule (6.1) already guarantees.
3. With two campaigns, the random-effects estimate has one degree of freedom and is reported only as the two campaign means.
4. With one campaign left for a workload, nothing for that workload is called replicated, and the paper says "one campaign".

A single campaign per workload would still remove the confounds and verify every fault. It cannot support the word "replicated".

## 10. Decisions for the author

The fixed points at the top are settled. The ten decisions below were settled by the author on 14 October 2026, after the pilot, and the answers are decision 38 in `REBUILD_DECISIONS.md`: option A, the values as written, a margin of 0.03, the presence-only URP check, a signed public git tag with pilot data deposited and labelled, the reserved v9 DOI, RQ1 keeping "broker failure" and RQ4 naming reproducibility across clusters, operator 1.10.0, a fixed 300 s F1 label, and the campaigns approved at about $970 of new spend. The list keeps the original wording.

1. **Cost option**, before registration, because the phase lengths go into the plans. Option A as written (about $1,033), B with a smaller monitoring node and no trailing baseline (about $938), or C, which also cuts the pre-fault to 300 s and the settle gap to 0 to 120 s (about $836, within the cap). Option C no longer works under decision 27, because its 300 s pre-fault is too short for the sequence models to score the within-run negatives (6.4). The choice after the pilot is between A and B.
2. **Values the protocol fixes without data**, before registration. The F5 job-recovery limit of 180 s after the last kill, the F8 fill target of 85% of the memory limit with a 256 MiB minimum, and halting after two consecutive failed attempts or more than 8 in total. The pilot can reveal a mechanical problem with any of them, but not tune them.
3. **Margin**, before registration. Delta of 0.03 AUC (recommended) or another value.
4. **Under-replicated partitions in the F1 and F2 gate**, before registration. Keep it as a presence-only check (2.1, recommended), drop it and rely on the probes, fencing count and pod state alone, or keep it and remove the series from the features.
5. **Registration and extra data**, before launch. Where to register (an OSF registration or a signed public git tag), and whether pilot and abandoned-campaign data go into the deposit.
6. **Dataset version**, before submission. Reuse the unpublished v9 draft and its reserved DOI with the rebuilt files, or publish a new version under the concept DOI. Either way v8 stays as it is.
7. **Research question wording**, before writing. Whether RQ1 keeps "broker failure", and whether RQ4 names cross-cluster reproducibility (C7).
8. **Flink operator version**, before the pilot. 1.10.0 for continuity with R1, with 1.16.1 as the fallback if 1.10.0 fails on EKS 1.35.
9. **The F1 labelled span**, before registration. A fixed 300 s for continuity with R1 (as written, with the not-Ready span as a secondary labelling in 3.6), or the time the broker was not Ready as the primary span.
10. **Approval of the campaigns**, after the pilot requote (author decision 7).

## 11. Interface contract

This section is the contract between two builders, one for the orchestrator (including the exporter and probes) and one for the preprocessing. Each codes against it and tests against shared fixture files built from it. A change to anything here bumps `schema_version` and is logged in `DEVIATIONS.md`.

The integration of 13 October 2026 recorded clarifications C1 to C21 below, before any data existed. Review 4 added C22 to C40, also before any data existed. Each is marked "(C<n>)" where it applies, and `rebuild/CONTRACT_CHANGES.md` gives each one with its reason. They keep `schema_version` "1". Whether they also go into `DEVIATIONS.md` is the author's decision.

### 11.1 Conventions

1. Every instant is in epoch seconds, UTC, as a JSON number or a float64 column with millisecond precision. Instant fields end in `_at`. The one exception is the `ts` column of the exported metric files, an int64 that is a multiple of 10.
2. Durations are seconds and end in `_s`. Byte counts are integers and end in `_bytes`. CPU is in cores and ends in `_cores`. Rates are per second and end in `_per_s`. Fractions are in 0 to 1 and end in `_frac`. Counts are integers and end in `_count`.
3. Codes and segment types are upper case (`VALID`, `PRE_FAULT`). Tokens are lower case (`ecommerce`).
4. Hashes are SHA-256, lower-case hex, 64 characters.
5. Canonical JSON is the UTF-8 encoding of Python `json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)`. Plans contain no floats, so their bytes are identical across machines.
6. A field marked nullable may be JSON null. Every other field is always present. An unknown field is an error for the reader.
7. Parquet files are written with pyarrow, zstd compression and no pandas index. An absent sample has no row. NaN is never written.
8. Time for the manifest and verification records comes from the orchestrator pod's clock. Exported metric time comes from Prometheus. The health gate keeps the two within 1 s (1.6).
9. Every object key stored in a record (manifest, verification record, baseline statistics key) is relative to `campaign/<campaign_id>/` (C16).

### 11.2 Identifiers

| Name | Format | Example |
|---|---|---|
| `workload` | `ecommerce` or `iot` | `iot` |
| `campaign_index` | Integer 0 to 5. 0 is the pilot, 1 to 3 the campaigns, 4 and 5 reserves | `2` |
| `campaign_id` | `pilot-ec` or `pilot-iot` for index 0. Otherwise `ec` or `iot`, a hyphen, and the letter a to e for index 1 to 5. Matches the Terraform rule `^[a-z]{2,5}(-[a-z0-9]{1,3})$` | `iot-b` |
| `cluster_name` | `streaming-anomaly-detection-<campaign_id>` | `streaming-anomaly-detection-iot-b` |
| `cell_key` | `<workload>-c<i>-b<bb>-p<p>`, with `bb` from 01 to 10 and `p` from 1 to 8 | `iot-c2-b07-p3` |
| `attempt_key` | `<cell_key>-a<a>`, with `a` from 1 to 9. Regex `^(ecommerce|iot)-c[0-5]-b(0[1-9]|10)-p[1-8]-a[1-9]$` | `iot-c2-b07-p3-a1` |
| `run_id` | UUID version 4, lower case, one per attempt, created with the attempt record and never reused | `3f2b9c1e-8d4a-4c7e-9b1f-2a6d5e8c0b47` |
| `run_type` | `F0`, `F1`, `F2`, `F4`, `F5`, `F6`, `F7` or `F8` | `F5` |
| Kubernetes object name for an attempt | `<prefix>-<attempt_key>` or `<prefix>-<attempt_key>-<n>`, at most 63 characters | `pk-iot-c2-b07-p3-a1-2` |

The pilot uses `c0` and block `b01` only.

### 11.3 Plan file

Path `plan/plan.json`, written in canonical JSON by the orchestrator before anything else. `plan/plan.json.sha256` holds the 64-character hash of those bytes and a newline. The hash is committed to git and passed to the orchestrator as `PLAN_SHA256`.

| Field | Type | Rule |
|---|---|---|
| `schema_version` | string | `"1"` |
| `workload` | string | 11.2 |
| `campaign_index` | integer | 11.2 |
| `campaign_id` | string | 11.2 |
| `seed` | string | Decimal string of the unsigned 64-bit seed |
| `seed_rule` | string | `"int(sha256('rebuild/<workload>/campaign-<i>').hexdigest()[:16],16)"` |
| `generator` | string | `"numpy.random.Generator(numpy.random.PCG64(seed))"` |
| `numpy_version` | string | Version used to draw, for example `"2.1.3"`. Always the orchestrator image's NumPy: plans are drawn with `rebuild/integration/plan_tool.sh`, and `orchestrator plan` refuses any other version (C14) |
| `run_types` | array of objects | In the order F0, F1, F2, F4, F5, F6, F7, F8. Each has `run_type`, `name`, `kind`, `duration_s` (integer, 0 for F0), `params` (object of integers and strings) |
| `phase_s` | object | Integers `settle_max`, `pre_fault`, `recovery`, `cooldown`, `gate_hold`, `gate_timeout`, `remediation_timeout`, `apply_max`, `redeploy_extra_settle` |
| `blocks` | array of objects | One per block, each with `block` (integer) and `cells` (array in position order) |
| `cells[].position` | integer | 1 to 8 |
| `cells[].cell_key` | string | 11.2 |
| `cells[].run_type` | string | From the block permutation |
| `cells[].settle_gap_s` | integer | 0 to `settle_max` |
| `cells[].follower_rank` | integer | 0 or 1. Used by F1, F2 and as the sham pseudo-target |
| `cells[].sham_duration_s` | integer | 180 or 300. Used by F0 only |
| `cells[].tm_kill_ranks` | array of 5 integers | Each 0 to 2. Used by F5 and as the sham pseudo-target |
| `reserve_settle_gap_s` | array of 40 integers | Settle gaps for repeated attempts, used in order |
| `load_generator_seed` | integer | 0 to 2147483646. The load generator's `LOADGEN_SEED`, read from the plan by `deploy_workload.sh --plan-file`; no other seed reaches it (C12) |
| `f6_payload_seed` | integer | 0 to 2147483646 |

The generator draws in this order. Rebuilding the plan from the seed must give the same bytes.

1. For each block 1 to 10, `rng.permutation(["F0","F1","F2","F4","F5","F6","F7","F8"])`.
2. For each block in order and each position in order, `int(rng.integers(0, settle_max + 1))` for `settle_gap_s`.
3. Then, in the same cell order, `int(rng.integers(0, 2))` for `follower_rank`.
4. Then, in the same cell order, `int(rng.choice([180, 300]))` for `sham_duration_s`.
5. Then, in the same cell order, `[int(x) for x in rng.integers(0, 3, size=5)]` for `tm_kill_ranks`.
6. `int(rng.integers(0, 2147483647))` for `load_generator_seed`, then the same for `f6_payload_seed`.
7. `[int(x) for x in rng.integers(0, settle_max + 1, size=40)]` for `reserve_settle_gap_s`.

At run time, a follower rank indexes the two non-controller brokers sorted by node ID. A kill rank indexes the Ready TaskManagers sorted by pod name at the kill time, modulo their count.

### 11.4 Campaign manifest

Path `manifest/manifest.json`, rewritten by the orchestrator after every state change. It is written to a temporary file on the `campaign-manifest` volume, renamed into place as `/manifest/manifest.json`, then uploaded. The volume copy is the source of truth during the campaign and the S3 copy afterwards. Bucket versioning keeps every revision. That file is the only part of the volume that is part of the record. The orchestrator's resume state lives under `/manifest/state/` and is never uploaded or listed in `SHA256SUMS`, and infra's sidecar mirrors `manifest.json` alone (C1).

| Field | Type | Rule |
|---|---|---|
| `schema_version` | string | `"1"` |
| `campaign_id`, `workload`, `campaign_index`, `cluster_name` | string, string, integer, string | 11.2 |
| `region` | string | `"us-east-2"` |
| `availability_zone` | string | The zone name, for example `"us-east-2b"` |
| `kubernetes_version` | string | `"1.35"` |
| `node_ami_release` | string | From SSM, as applied: `scripts/capture_versions.py` captures it, and the orchestrator reads it from the mounted `versions.json` (C8) |
| `plan_sha256` | string | Matches `plan/plan.json.sha256` |
| `seed` | string | As in the plan |
| `orchestrator` | object | `git_commit` (string), `image_digest` (string) |
| `gate` | object | `git_commit` (string), `thresholds_sha256` (string) |
| `images_lock_sha256` | string | Hash of `run/images.lock.yaml`, which is `rebuild/infra/images/images.lock.yaml` as mounted at `/config` (C8) |
| `feature_allowlist_sha256` | string | Hash of `run/feature_allowlist.yaml` (11.8) |
| `flink_jobs_expected` | array of strings | The workload's three job names (11.11) |
| `created_at`, `updated_at` | number | Instants |
| `timeline_start_at` | number | Start of WARMUP |
| `timeline_end_at` | number, nullable | End of TRAILING, null until then |
| `clock_skew_s` | number | Orchestrator minus Prometheus `time()` at start |
| `campaign_segments` | array of objects | `segment_type` (WARMUP, BASELINE, TRAILING, IDLE), `start_at`, `end_at`. Together with every attempt's segments they cover the timeline exactly |
| `baseline_stats_key`, `baseline_stats_sha256` | string, nullable | Set when BASELINE ends (11.12) |
| `attempts` | array of objects | 11.5, in start order |
| `halts` | array of objects | `at`, `reason` (string), `attempt_key` (nullable), `resumed_at` (nullable), `resolution` (`RESUMED`, `ABANDONED` or null) |
| `node_inventory` | array of objects | `at`, `instance_ids` (array of strings, sorted) |
| `health_events` | array of objects | One per AWS Health event of the region: `at` (the latest publication time), `service`, `event_type_code`, `status`, `event_key` (first 16 hex of SHA-256 of the event ARN), `start_at` and `end_at` (null while open). No account field is stored (C30) |
| `health_events_polled_until` | number, nullable | End of the last successful poll of the Health log group. Null only before the first poll, which follows a successful read at the start (C30) |
| `status` | string | `RUNNING`, `COMPLETE` or `ABANDONED` |

### 11.5 Attempt record, segments and outcomes

Each element of `attempts` has these fields.

| Field | Type | Rule |
|---|---|---|
| `attempt_key`, `run_id`, `cell_key` | string | 11.2 |
| `block`, `position`, `attempt` | integer | From the key |
| `run_type`, `fault_name` | string | From the plan |
| `status` | string | `RUNNING` or `FINISHED` |
| `outcome` | string, nullable | 3.5 codes except SKIPPED_UNHEALTHY. Null while RUNNING |
| `failed_checks` | array of strings | The distinct `name` of every failing entry of the verification record's `checks`, in record order, without targets. Empty if none. A telemetry failure is named `telemetry_scrape_misses` (C2, C22) |
| `flags` | array of strings | Any of `AFTER_REDEPLOY`, `AFTER_RESUBMIT` (C28), `OVERRUN`, `F7_TM_LOSS`, `REMEDIATED`, `HEALTH_EVENT` (set when an event overlaps the attempt, also after it finished, C30), `SERIES_GAP` (C22). `SIGNATURE_FALSE_PASS` is never in the manifest: the preprocessing adds it to the windows of a fault that false-passed on two or more shams (C23) |
| `started_at`, `ended_at` | number, number nullable | Start of SETTLE, end of GATE |
| `planned_onset_at` | number | End of PRE_FAULT |
| `apply_called_at` | number, nullable | Null for F0 |
| `onset_verified_at` | number, nullable | 2.4 source. Null if nothing was applied |
| `onset_source` | string, nullable | `CHAOS_APPLY_EVENT`, `FIRST_PRODUCE_ACK` or `PLANNED` |
| `configured_duration_s` | integer | From the plan, or the sham length |
| `labelled_end_at` | number, nullable | 2.4 source |
| `end_source` | string, nullable | `CHAOS_RECOVER_EVENT`, `FIXED_SPAN`, `LAST_PRODUCE_ACK` or `PLANNED` |
| `overrun_s` | number, nullable | Labelled end minus onset minus configured duration, for F2, F4, F7 and F8 |
| `applied` | boolean | True once any resource, kill or injector was created |
| `aborted_at` | number, nullable | When an injection was cut short |
| `resources` | array of objects | `kind`, `name`, `created_at`, `delete_called_at` (nullable), `not_found_at` (nullable) |
| `controller_at_onset` | object, nullable | `node_id` (integer), `pod` (string). Every run type once something was read before apply; required for F0, F1 and F2, null for F4 to F8 when the read failed (C37) |
| `segments` | array of objects | `segment_type`, `start_at`, `end_at`, in order, with no gaps or overlaps from `started_at` to `ended_at` |
| `recovery_gate` | object | `started_at` (nullable, and null only for an ABORTED attempt that ended before its GATE segment, C3), `passed_at` (nullable), `remediation` (object with `performed` boolean, `actions` array of strings, `started_at` and `ended_at` nullable) |
| `flink_jobs_at_end` | array of objects | `name`, `job_id`, `state`. Every job the Flink REST API reports, terminal jobs kept in Flink's history too; "no extra jobs" (1.6.4) counts the jobs that are not terminal (C19). Retried until the API answers, and a gate check (C25) |
| `verification_key`, `verification_sha256` | string, nullable | Set when FINISHED |
| `export` | object | `features_key`, `features_sha256`, `verification_metrics_key`, `verification_metrics_sha256`, `probes_key`, `probes_sha256`, `status` (`PENDING`, `DONE` or `FAILED`) |

Segment types for an attempt, in order:

| Segment | Start | End | When present |
|---|---|---|---|
| `SETTLE` | `started_at` | Plus the settle gap, plus 600 s if AFTER_REDEPLOY or AFTER_RESUBMIT (C28) | Always |
| `PRE_FAULT` | End of SETTLE | `planned_onset_at`, `pre_fault` seconds later | Always |
| `APPLY` | `planned_onset_at` | `onset_verified_at`, or the failure time | Always, zero length allowed for F0 |
| `INJECTION` | `onset_verified_at` | `labelled_end_at`, or `aborted_at` | If anything was applied, and always for F0 |
| `RECOVERY` | End of INJECTION | 300 s later | If anything was applied, and always for F0 |
| `COOLDOWN` | End of RECOVERY | 300 s later | If anything was applied, and always for F0 |
| `GATE` | End of the previous segment | `ended_at` | Always |

If an attempt fails before anything is applied, its segments are SETTLE, PRE_FAULT, APPLY and GATE. Labels follow 3.3 from the segment type, the run type and the outcome alone. An ABORTED attempt's segments are a prefix of this order that ends with the segment that was running at the crash, cut at `ended_at` (C21).

A resumed orchestrator finishes every RUNNING attempt as ABORTED, with `ended_at` set to the restart time. The time since that attempt's last recorded segment boundary belongs to the segment that was running. The orchestrator then deletes any campaign resource still present, confirms NotFound, waits 300 s and runs the recovery gate. That time is an IDLE segment. The next attempt for that cell then starts with the next attempt number.

### 11.6 Verification record

Path `attempts/<attempt_key>/verification.json`, written by the orchestrator when the attempt finishes. `make gate` writes the offline version locally with `mode` `OFFLINE` and must reproduce every `pass` value.

| Field | Type | Rule |
|---|---|---|
| `schema_version` | string | `"1"` |
| `attempt_key`, `run_id`, `run_type` | string | 11.2 |
| `mode` | string | `ONLINE` or `OFFLINE` |
| `gate_git_commit`, `thresholds_sha256` | string | As in the manifest |
| `evaluated_at` | number | Instant |
| `pseudo_targets` | object, nullable | F0 only. `follower_pod`, `tm_pods`, `tm_kill_ranks`, `f6_planned_count` |
| `targets_expected`, `targets_selected` | array of objects | `pod`, `uid`, `node`, and `broker_id` (integer, nullable) |
| `controller` | object, nullable | `node_id_before_apply`, `node_id_after_injection`, `method` (`"metadata-quorum"`), `metric_agrees` (boolean) |
| `chaos_snapshots` | array of objects | `tag` (`APPLY_PLUS_10S`, `MID`, `BEFORE_DELETE`, `AFTER_DELETE`), `taken_at`, `kind`, `name`, `yaml_key` (S3 key), `conditions` (object keyed `Selected`, `AllInjected`, `AllRecovered`, `Paused`, each with `status` and `last_transition_at`), `records` (array of `id` as `namespace/pod`, `phase`, `first_apply_at`, `last_recover_at`) |
| `pods_before`, `pods_after` | array of objects | `pod`, `uid`, `node`, `ready` (boolean), `restart_count`, `last_termination_reason` (nullable), `started_at` |
| `f5` | object, nullable | `kills` (array of `k`, `planned_at`, `pod`, `uid`, `apply_at`), `new_tm_uid_count`, `all_jobs_running_at` (nullable) |
| `f6` | object, nullable | `injector_image_digest`, `pod_ready_at`, `rate_per_s`, `planned_count`, `sent_count`, `acked_count`, `first_produce_at`, `last_produce_at`, `consumer_count`. `acked_count` counts deliveries the brokers acknowledged, and `first_produce_at` and `last_produce_at` are the first and last acknowledgement times. `consumer_count` counts malformed records carrying this attempt's `rebuild-attempt` header (C6) |
| `f8` | object, nullable | `per_pod` (array of `pod`, `limit_bytes`, `ws_pre_bytes`, `stressor_bytes`, `ws_during_bytes`, `increase_bytes`, `ws_samples` and `limit_samples` (arrays of `ts` and `value`, the samples the injector sized from), `export_stressor_bytes` (nullable, the size recomputed from the export, recorded only)) (C36) |
| `checks` | array of objects | `name`, `layer` (`MECHANISM`, `SIGNATURE`, `ENVELOPE`, `TIMING`, `TELEMETRY`), `target` (pod or `"all"`), `series` (string), `pre_value` and `during_value` (number, nullable), `op` (`>=`, `<=`, `==`), `threshold` (number), `unit` (string), `pass` (boolean), `reason` (nullable, `MISSING` when a value is null). The 3.4.3 rule is one TELEMETRY check `telemetry_scrape_misses` per untargeted `scrapes` row, with target `<role>:<pod>`, `pre_value` the expected count, `during_value` the missed count and `<=` 3 (C2, C22). For F0, one SIGNATURE check `f0_signature_false_pass` per fault, which fails when that fault's signature passed (C23) |
| `signatures_all_faults` | array of objects | `run_type`, `pass`. Every fault's signature evaluated on this attempt's targets or pseudo-targets, used for sham false passes |
| `telemetry` | array of objects | `source`, `segment`, `expected_count`, `present_count`, `coverage_frac`, `targeted` (boolean). One row per source and PRE_FAULT, INJECTION or RECOVERY segment with at least one 10 s step (C2). Recorded values: an untargeted row under 0.95 flags SERIES_GAP and decides nothing (C22) |
| `scrapes` | array of objects | `target` (`<role>:<pod>`), `segment`, `expected_count` (10 s steps at which kube-state shows the pod), `missed_count` (those without `up` equal to 1), `targeted` (the fault's own target in INJECTION or RECOVERY). One row per broker, JobManager and TaskManager pod and segment (C22) |
| `result` | object | Booleans `mechanism`, `signature`, `envelope`, `timing`, `telemetry`, `gate`. `telemetry` is true when every TELEMETRY check passes and at least one `scrapes` row exists (C2, C22). For F0, `signature` is true when no fault's signature passed, and `gate` includes it (C23) |
| `recovery_gate_conditions` | array of objects | `name`, `value` (nullable), `threshold`, `pass`, `at` |

A value the orchestrator could not observe is null, and every check that needs it fails with reason `MISSING`. These fields are nullable on that ground: `pseudo_targets.follower_pod` and `f6_planned_count`, `controller.node_id_before_apply` and `node_id_after_injection`, `f5.kills[].apply_at` and `f5.new_tm_uid_count`, `f6.pod_ready_at`, `sent_count`, `acked_count`, `first_produce_at`, `last_produce_at` and `consumer_count`, every number in `f8.per_pod[]`, and `chaos_snapshots[].records[].first_apply_at` and `last_recover_at`. A VALID or VALID_REMEDIATED attempt has none of them null for its run type, except a record's `last_recover_at` in a snapshot taken before the recovery (C4), and except `f8.per_pod[].export_stressor_bytes`, which is recorded only (C36). The controller of F4 to F8 is recorded only and may be null (C37).

### 11.7 S3 layout

All objects for one campaign live under `campaign/<campaign_id>/` in the results bucket from the `shared` root. The bucket name stays out of public files. Keys in the deposit are relative to the campaign prefix.

```
campaign/<campaign_id>/
  plan/plan.json                                      orchestrator, first object written
  plan/plan.json.sha256                               orchestrator
  run/versions.json                                   orchestrator: Kubernetes, AMI, chart and add-on versions
  run/images.lock.yaml                                orchestrator
  run/feature_allowlist.yaml                          orchestrator
  run/thresholds.json                                 orchestrator: the gate thresholds as registered
  manifest/manifest.json                              orchestrator, after every state change; the only object under manifest/ (C1)
  stats/baseline_stats.json                           orchestrator, at the end of BASELINE (11.12)
  baseline/features/chunk-<start_epoch>.parquet       exporter, one per hour of WARMUP and BASELINE
  baseline/verification/chunk-<start_epoch>.parquet   exporter
  export/attempts/<attempt_key>/features.parquet      exporter, after each attempt finishes
  export/attempts/<attempt_key>/verification.parquet  exporter
  export/trailing/features/chunk-<start_epoch>.parquet
  export/trailing/verification/chunk-<start_epoch>.parquet
  probes/baseline/chunk-<start_epoch>.parquet         orchestrator, warm-up and baseline probes
  probes/trailing/chunk-<start_epoch>.parquet         orchestrator
  attempts/<attempt_key>/probes.parquet               orchestrator
  attempts/<attempt_key>/verification.json            orchestrator (11.6)
  attempts/<attempt_key>/chaos/<tag>-<name>.yaml      orchestrator, status snapshots
  attempts/<attempt_key>/injector.log                 orchestrator, F6 only: the injector pod log plus one orchestrator_observed line (C6); every applied F6 attempt has one, on abort the observed line may stand alone (C25)
  logs/orchestrator-<start_epoch>.log.gz              orchestrator
  checksums/SHA256SUMS                                orchestrator, by verify_complete
  COMPLETE                                            orchestrator, last object written (11.10)
scratch/<campaign_id>/flink-checkpoints/              Flink, 7-day expiry, never part of the record
scratch/<campaign_id>/flink-savepoints/               Flink, 7-day expiry
```

Writers are fixed by IAM. The exporter role may write only `baseline/` and `export/`. The orchestrator role may write anything under the campaign prefix, including `COMPLETE`, and may not delete. The Flink service account may write only its `scratch/` prefix. `chunk-<start_epoch>` names the integer epoch of the chunk's first second. A chunk covers 3,600 s or the remainder of its segment.

An attempt's export covers `started_at` rounded down to 10 s through `ended_at` rounded up to 10 s, for every finished attempt including failed ones, so the gate can be recomputed offline. The exporter runs as a Job named `export-<attempt_key>` (11.11). It exits non-zero on any query or upload error, and the orchestrator retries it. An export still failing at campaign end blocks `COMPLETE`.

The offline gate (`orchestrator gate-offline`, `make gate`) needs only objects of this layout: the manifest, the plan, `run/thresholds.json`, the baseline statistics, and per attempt `verification.json`, the chaos snapshots, `probes.parquet`, both export files and, for F6, `injector.log`. The F6 log's final line is `INJECTED sent=N delivered=A failed=F planned=P first_epoch=T0 last_epoch=T1 payload_sha256=H`, with `delivered` the acknowledged count and `T0` and `T1` the first and last acknowledgement times (C6).

### 11.8 Exported metric and probe files

Both exported metric files (`features.parquet` and `verification.parquet`) use one long layout. One row is one sample of one series.

| Column | Type | Rule |
|---|---|---|
| `ts` | int64 | Epoch seconds UTC, multiple of 10, the Prometheus range-query step |
| `series_id` | string | First 16 hex characters of SHA-256 of `metric` + `"|"` + canonical JSON of `{"pod": pod, "labels": <the labels object>}`. A key of the exported files only: it hashes volatile labels such as `job_id` and `task_attempt_id`, so it is never a feature identity (C17) |
| `metric` | string | Prometheus metric name, unchanged |
| `metric_type` | string | `counter`, `gauge`, `summary`, `histogram` or `unknown`, from the Prometheus metadata API |
| `source` | string | Feature file: `broker`, `consumer`, `flink` or `jvm`. Verification file: `cadvisor`, `kube_state`, `kafka_controller`, `kafka_broker_copy` or `scrape` (Prometheus `up` and `scrape_samples_scraped` of the scrape jobs `kafka-brokers` and `flink-metrics`, C22) |
| `role` | string | `broker`, `jobmanager`, `taskmanager`, `loadgen` or `other` |
| `pod` | string | Pod name, never an IP |
| `pod_uid` | string, nullable | From kube-state-metrics where available |
| `broker_id` | int32, nullable | Pod ordinal for broker pods |
| `container` | string, nullable | Container name where the series carries one |
| `labels` | string | Canonical JSON of the remaining labels. `instance`, `pod`, `pod_ip`, `host`, `tm_id`, `kubernetes_pod_name`, `uid`, `endpoint`, `service`, `namespace`, `job` and `prometheus` are removed. Values are never truncated. The registry host is stripped from the image reference labels `image`, `image_id` and `image_spec` before `series_id` is hashed, so no account ID is stored (C24) |
| `value` | float64 | Raw sample value |

Rows are sorted by `ts`, then `series_id`. A repeated (`ts`, `series_id`) pair fails the export. Which metrics go to which file is fixed by `run/feature_allowlist.yaml`, a list of metric-name patterns per source. The feature file holds only allow-listed broker, consumer, Flink and JVM metrics. The verification file holds cAdvisor, kube-state-metrics and the controller metrics, plus a copy of the under-replicated partition series for the presence check. The controller metrics of 2.3.3 are scraped by the broker JMX job `kafka-brokers`; the allowlist puts them in the verification source `kafka_controller` only, which is the separation 2.3.3 asks for (C10).

The probe file `probes.parquet` has one row per probe sample.

| Column | Type | Rule |
|---|---|---|
| `ts` | float64 | Probe start, epoch seconds UTC |
| `probe` | string | `tcp`, `tcp_burst`, `controller`, `flink_jobs`, `flink_tms`, `loadgen` or `consumer` |
| `src_pod` | string, nullable | Pod the probe ran in |
| `dst` | string, nullable | `<pod>:<port>` or `<service>:<port>` |
| `tried_count` | int32 | 1 for `tcp`, 10 for `tcp_burst`, 1 otherwise |
| `ok_count` | int32 | Tries that connected or answered |
| `timeout_s` | float64 | 0.5 for TCP probes |
| `latency_s` | float64, nullable | Mean over tries that connected |
| `value` | float64, nullable | Controller node ID, registered TaskManager count, cumulative acked produce count (the load generator's `/status` `acked_count`, C7), or cumulative malformed count, by probe |
| `detail` | string, nullable | Canonical JSON for structured results, for example job names, states and checkpoint counts. For `loadgen` and `consumer` it is the service's whole `/status`. The consumer's holds `started_at` and `malformed_by_attempt` (attempt key to cumulative count), which the F6 signature reads (C6), and `dead_letter_count`, which the recovery gate reads (C34). In stored `flink_jobs` details `latest_completed_path` is relative to `s3://<results-bucket>/scratch/<campaign_id>/`, a path elsewhere keeps its key with the bucket removed (C24) |
| `error` | string, nullable | Error text when a try failed |

### 11.9 Preprocessing outputs

The preprocessing reads only the S3 objects above and writes these files per campaign under `derived/<campaign_id>/`.

`timeline.parquet` has one row per segment, with `segment_index` (int32), `segment_type`, `start_at`, `end_at`, `attempt_key` (nullable), `run_id` (nullable), `run_type` (nullable), `outcome` (nullable) and `label`. The build fails if the rows do not cover `timeline_start_at` to `timeline_end_at` exactly.

`windows_<w>s.parquet`, for `w` of 30 on every campaign and 15, 60, 120 and, on IoT, 300 on campaign 1, has one row per window. Its key and meta columns are:

| Column | Type | Rule |
|---|---|---|
| `window_id` | string | `<campaign_id>-<w>-<start_at as integer milliseconds>` |
| `campaign_id`, `workload` | string | 11.2 |
| `window_s` | int32 | `w` |
| `start_at`, `end_at` | float64 | 3.2 grid |
| `segment_type`, `label` | string | 3.3 |
| `attempt_key`, `run_id`, `cell_key` | string, nullable | Null for BASELINE and TRAILING |
| `block`, `position` | int32 | 0 for BASELINE and TRAILING |
| `run_type` | string | `F0` to `F8`, `BASELINE` or `TRAILING` |
| `outcome` | string, nullable | Attempt outcome |
| `flags` | string | Canonical JSON array of the attempt's flags |
| `seq_group` | string | `<attempt_key>` for attempt windows, `baseline-<n>` or `trailing-<n>` for each contiguous stretch |
| `seq_index` | int32 | Position inside `seq_group` |
| `within_run` | string, nullable | `late` for the last 5 PRE_FAULT windows of a valid attempt (label NORMAL), counted back from the planned onset, `early` for the 5 before them, null otherwise. These are the within-run negatives of 4.5.2 and 4.5.3 (`evaluation.within_run`, decision 27). Also in `labels_<w>s.parquet` and in every score file |
| `meta__tm_reporting_min`, `meta__tm_reporting_mean` | float64 | TaskManagers with any sample at each 10 s step, over the window |
| `meta__samples__<source>` | int32 | 10 s steps with at least one sample, per feature source |
| `meta__telemetry_ok` | bool | Every feature source has samples in at least two thirds of the window's steps |

Feature columns are named `f__<source>__<metric>__<scope>__<stat>`.

1. `<source>` is `broker`, `consumer`, `flink`, `jvm` or `derived`.
2. `<metric>` is the Prometheus metric name. A counter's `_total` suffix becomes `_per_s`, and a counter without that suffix gets `_per_s` appended. The labels that enter a name come from a fixed per-metric list, `labels.naming` in `config/pipeline.json`, set before the pilot and matched on the Prometheus metric name, where the first rule wins. Each label in the metric's list that the series carries adds `.<key>-<value>` in sorted key order, whether or not it varies inside the campaign, so one series has one name in every campaign (Review 4 MIN-11). A feature metric that no rule matches fails the build. A label outside the list that is not dropped stays part of the series identity, so two series that differ only in such a label fail the build. Characters outside `[A-Za-z0-9_]` in values become `_`. Names are never truncated, and two series mapping to one name fail the build. The labels `job_id`, `task_attempt_id`, `task_attempt_num` and `node` are not retained: they are dropped before naming, so a job restart, a resubmission or a redeploy creates no new column. "Two series mapping to one name" means two series with samples at the same 10 s step inside one windowed segment, or two different label sets that sanitise to one name (C18). `<metric>` may itself contain `__` (for example the topic `__consumer_offsets`), so a name is read with `<source>` from the left and `<scope>` and `<stat>` from the right (C15).
3. `<scope>` is `b0`, `b1` or `b2` for broker node IDs, `jm` for the JobManager, `tm_min`, `tm_max` or `tm_mean` for the TaskManager role, and `cluster` for series with no pod.
4. `<stat>` is `mean`, `std`, `min`, `max`, `slope` or `iqr`, over the window's 10 s steps.

For TaskManager series, the minimum, maximum and mean are taken at each 10 s step across all series that share the metric and retained labels, with pod, subtask and TaskManager ID labels dropped. The count of TaskManagers reporting is also a feature, `f__flink__taskmanagers_reporting__cluster__<stat>`. For each of the sources `consumer`, `flink` and `jvm`, `f__<source>__tm_aggregates_missing_frac__cluster__<stat>` is the share of that source's TaskManager aggregate columns with no value at each 10 s step (4.1.3, decision 25). `f__broker__broker_series_missing_frac__b<n>__<stat>` and `f__jvm__broker_series_missing_frac__b<n>__<stat>` give the same share per broker identity, and `f__broker__brokers_reporting__cluster__<stat>` counts the broker identities with any sample at each 10 s step (decision 34). In every missingness share a column counts from its first value in the campaign, and the columns of `missingness.exclude_column_regex` are left out. Derived features are named `f__derived__<name>__cluster__<stat>` with the R1 names, and are computed inside one `seq_group` only.

`folds.parquet` has `window_id`, `fold` (int32, 1 to 10) and `role` (`train`, `hpo_val`, `cal`, `test` or `eval_only`). BASELINE windows are `train` in every fold, TRAILING windows are `eval_only` in every fold, and attempt windows take the role of their block under Table 5 of the current draft. Fitting code applies the label rules of 3.3 on top of the roles.

Each file has a `.sha256` beside it, committed before any model is trained on that campaign.

### 11.10 Completeness marker

`campaign/<campaign_id>/COMPLETE` is canonical JSON written once, last, by the orchestrator's `verify_complete` step.

| Field | Type | Rule |
|---|---|---|
| `schema_version` | string | `"1"` |
| `campaign_id` | string | 11.2 |
| `status` | string | `COMPLETE` or `ABANDONED` |
| `written_at` | number | Instant |
| `plan_sha256`, `manifest_sha256` | string | Hashes of the final plan and manifest objects |
| `checksums_key`, `checksums_sha256` | string | `checksums/SHA256SUMS` and its hash |
| `object_count`, `total_bytes` | integer | Over every object listed in SHA256SUMS |
| `newest_object_at` | number | Latest LastModified among listed objects |
| `checks` | array of objects | `name`, `pass`. Every FINISHED attempt has its verification record, probe file and both export files. Baseline and trailing chunks cover their segments. Every attempt export's rows lie inside its span. The timeline covers every second |

`checksums/SHA256SUMS` lists every object under the campaign prefix except `COMPLETE` and except itself (C20), one line per object, as the hash, two spaces and the key relative to the prefix, sorted by key. `verify_complete` refuses to write `COMPLETE` if any check fails. The teardown guard refuses to destroy a cluster unless `COMPLETE` exists, its `written_at` is later than every object under the prefix, and its `status` is `COMPLETE` or the author has confirmed an abandonment.

### 11.11 Kubernetes names

These are the R1 names, kept for both workloads.

| Object | Name or selector |
|---|---|
| Namespaces | `streaming` (Kafka, Flink, load generator, orchestrator, injectors, verification consumer), `observability` (Prometheus stack), `chaos-mesh` |
| Kafka cluster | Kafka resource `streaming-kafka`, KafkaNodePool `brokers` with roles controller and broker, 3 replicas |
| Broker pods | `streaming-kafka-brokers-0` to `-2`, labels `strimzi.io/cluster=streaming-kafka` and `strimzi.io/pool-name=brokers`. Node ID is the pod ordinal. Container `kafka` |
| Kafka ports | 9090 controller, 9091 replication, 9092 plain, 9093 TLS, 9404 JMX exporter |
| Bootstrap | `streaming-kafka-kafka-bootstrap.streaming.svc.cluster.local:9092` |
| Topics | `raw-events` (12 partitions), `enriched-events` (12), `aggregated-metrics` (6), `fraud-alerts` (3), `dead-letter` (3). Replication factor 3, `min.insync.replicas` 2 |
| Flink | FlinkDeployment `streaming-flink`, ServiceAccount `flink`, REST service `streaming-flink-rest.streaming.svc.cluster.local:8081`, JobManager RPC port 6123 |
| Flink pods | JobManager `app=streaming-flink,component=jobmanager`, TaskManagers `app=streaming-flink,component=taskmanager`, container `flink-main-container`. 3 TaskManagers with 4 slots each |
| E-commerce jobs | `Event Enrichment Job` (`com.phonotech.streaming.enrichment.EnrichmentJob`, `enrichment.jar`), `Windowed Aggregation Job` (`com.phonotech.streaming.aggregation.AggregationJob`, `aggregation.jar`), `Fraud Detection Job` (`com.phonotech.streaming.fraud.FraudDetectionJob`, `fraud.jar`) |
| IoT jobs | `IoT Event Enrichment Job` (`com.phonotech.streaming.iot.enrichment.IoTEnrichmentJob`, `iot-enrichment.jar`), `IoT Per-Location Aggregation Job` (`com.phonotech.streaming.iot.aggregation.IoTAggregationJob`, `iot-aggregation.jar`), `IoT Threshold Alert Job` (`com.phonotech.streaming.iot.alert.IoTAlertJob`, `iot-alert.jar`). Jars under `/opt/flink/usrlib/` |
| Load generator | Deployment and Service `load-generator`, label `app=load-generator`, `/status` (JSON, `acked_count` from delivery acknowledgements) and `/metrics` on port 9102 (C7) |
| Prometheus | `http://prometheus-kube-prometheus-prometheus.observability.svc.cluster.local:9090` |
| Orchestrator | Job `campaign-orchestrator`, ServiceAccount `campaign-orchestrator`, namespaced Role and RoleBinding `campaign-orchestrator`, volume claim `campaign-manifest`, environment `WORKLOAD` (`ECOMMERCE` or `IOT`, the upper-case form of the 11.2 token), `CAMPAIGN_ID`, `CAMPAIGN_INDEX`, `PLAN_SHA256`, `RESULTS_BUCKET` and `RESULTS_PREFIX` (`campaign/<campaign_id>`), all required with no default. ConfigMap `campaign-config` mounted at `/config` gives `PLAN_PATH`, `THRESHOLDS_PATH`, `VERSIONS_FILE` and `IMAGES_LOCK_FILE`. No other spelling is accepted (C5). Args `["run"]`, plus any of `--resume`, `--fault <F>` and `--block <n>` (the last two for the pilot only); exit codes 0 finished, 1 defect, 2 refused to start, 3 halted, 4 resume refused, 143 SIGTERM (C39). The role may call `logs:FilterLogEvents` on `/streaming-anomaly-detection/health` (C30) |
| Exporter | Job `export-<attempt_key>`, or `export-baseline-<chunk_start>` and `export-trailing-<chunk_start>`, ServiceAccount `metric-exporter`. The orchestrator creates every one; infra creates only the ServiceAccount (C11) |
| Verification consumer | Deployment `verification-consumer`, consumer group `rebuild-verification`, `/status` and `/metrics` on port 9103 (C7) |
| Verification probes | Deployment and Service `verification-probes`, `/status`, `/samples` and `/metrics` on port 8000, Prometheus job `rebuild-verification` (PodMonitor), in the infra kustomization (C9) |
| F6 injector | Job `poison-<attempt_key>`, image from ECR by digest, header `rebuild-attempt` set to the attempt key on every record. One implementation, `rebuild/orchestrator/orchestrator/f6_injector.py`, built into infra's `f6-injector` image; environment `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC`, `INJECT_RATE`, `INJECT_DURATION`, `INJECT_SEED`, `START_AT_UNIX`, `ATTEMPT_KEY` (C6). It opens no Kafka connection before `START_AT_UNIX` (C31) |
| Chaos resources | PodChaos `pk-<attempt_key>` (F1) and `pk-<attempt_key>-<k>` for k 1 to 5 (F5), NetworkChaos `nc-<attempt_key>` (F2, F7), StressChaos `sc-<attempt_key>` (F4) and `sc-<attempt_key>-<n>` for TaskManager n (F8), all in `streaming` |
| Labels on every campaign object | `rebuild/campaign=<campaign_id>`, `rebuild/attempt=<attempt_key>` where it applies, `rebuild/run-type=<run_type>` where it applies |
| Flink state | `state.checkpoints.dir` `s3://<results-bucket>/scratch/<campaign_id>/flink-checkpoints` and `state.savepoints.dir` `.../flink-savepoints` (11.7). The F5 envelope requires every expected job's latest checkpoint under this campaign's `flink-checkpoints/` (C13), which the stored probe records name relative to the campaign's scratch prefix (C24). Every job is submitted with `/opt/flink/bin/run-job.sh <main-class> <jar> [<checkpoint path>]` in the JobManager, by the `flink-submit` Job and by the remediation alike. The orchestrator reads `/jobs/<id>/config` and `/jobs/<id>/checkpoints/config` at every start and after every resubmission and halts unless each job runs at parallelism 4 with exactly-once checkpoints every 60000 ms, timeout 120000 ms, minimum pause 30000 ms, retained on cancel (C40) |

### 11.12 Baseline statistics

Path `stats/baseline_stats.json`, written by the orchestrator when BASELINE ends, from the gate's own queries over the whole BASELINE segment. Block 1 does not start until it exists.

| Field | Type | Rule |
|---|---|---|
| `schema_version` | string | `"1"` |
| `campaign_id` | string | 11.2 |
| `window` | object | `start_at`, `end_at` of BASELINE |
| `queries` | object | Name to the exact PromQL string used |
| `raw_events_in_per_s` | object | `mean`, `sd`, `p05`, `p95` of the 60 s rate |
| `consumer_lag_max` | object | `mean`, `sd` |
| `f6_rate_per_s` | number | 5% of `raw_events_in_per_s.mean` |
| `f6_planned_count` | integer | `round(f6_rate_per_s * 180)` |
| `tm_working_set_bytes` | object | `mean`, `sd` per TaskManager, recorded only, never a gate condition |
| `computed_at` | number | Instant |
