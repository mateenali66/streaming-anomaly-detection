# NAMES

Every name, label, port, path and environment variable that the orchestrator and the preprocessing may rely on. If a name is not
here, do not rely on it. A change to anything here is a change to the interface (protocol section 11) and needs the other builders told.
The integration of 13 October 2026 made this file, protocol section 11 and the code agree; every change is listed with its reason
in `rebuild/CONTRACT_CHANGES.md` (items C1 to C21) and recorded in section 11.

`<id>` is the campaign id (`ec-a`, `ec-b`, `ec-c`, `iot-a`, `iot-b`, `iot-c`, `pilot-ec`, `pilot-iot`, reserves `ec-d`, `iot-d`; at most 9 characters).
`<project>` is `streaming-anomaly-detection`. No account ID, profile name or e-mail address appears in any file of this tree.

## 1. Workload names

| Thing | ecommerce | iot |
|---|---|---|
| Terraform `workload` variable | `ecommerce` | `iot` |
| `WORKLOAD` environment variable in pods (the upper-case form of the 11.2 token; records carry the lower-case token) | `ECOMMERCE` | `IOT` |
| Campaign ids | `ec-*`, `pilot-ec` | `iot-*`, `pilot-iot` |
| Load profile | `LOW` = 1000 events/s base | `IOT_MEDIUM` = 10000 events/s base, 2x burst for 30 s every 300 s |
| Flink jobs (name, as shown by the REST API, in submit order) | `Event Enrichment Job`, `Windowed Aggregation Job`, `Fraud Detection Job` | `IoT Event Enrichment Job`, `IoT Per-Location Aggregation Job`, `IoT Threshold Alert Job` |

`WORKLOAD` has no default in any manifest or script. Both Terraform and the scripts refuse a campaign id that does not match the workload.

## 2. AWS names

| Thing | Name |
|---|---|
| Region | `us-east-2` |
| EKS cluster | `<project>-<id>` |
| Results bucket | `<project>-results-<bucket_suffix>`, a fixed random suffix from the gitignored shared tfvars, no account ID (decision 29; output `results_bucket` of `terraform/shared`) |
| Result prefix of one campaign | `campaign/<id>/` |
| Flink checkpoints, savepoints | `scratch/<id>/flink-checkpoints/`, `scratch/<id>/flink-savepoints/` (11.7; expire after 7 days) |
| SNS topic | `<project>-alerts` (one for all clusters; also the target of the Health rule) |
| AWS Health events | EventBridge rule `<project>-health` (source `aws.health`), log group `/<project>/health` (90 days), resource policy `<project>-health-events`. The orchestrator role may `logs:FilterLogEvents` on that group only |
| ECR repositories (immutable, scan on push) | built: `<project>/orchestrator`, `<project>/f6-injector`, `<project>/flink-jobs`, `<project>/load-generator`. Mirrors: `<project>/mirror/...` (list in `images/images.lock.yaml`) |
| IAM roles (Pod Identity) | `<project>-<id>-orchestrator`, `-exporter`, `-flink`, `-ebs-csi`; node roles `<project>-<id>-kafka-node`, `-flink-node`, `-monitoring-node` |
| Tags on everything | `Project=<project>`, `Campaign=<id>`, `Workload`, `Scope` (`cluster`, `pv`, `network`, `shared`), `ManagedBy=terraform`, `Owner`. Volumes created for PVCs: `Scope=pv` |
| Terraform state keys | `shared/terraform.tfstate`, `network/terraform.tfstate`, `campaigns/<id>/terraform.tfstate` |

### S3 layout under `campaign/<id>/` (protocol 11.7; what the orchestrator and the exporter write)

The layout is the contract's, object for object. Infrastructure enforces the writers through IAM; the preprocessing refuses any
object outside the layout (`rebuild/pipeline/streamprep/loader.py` LAYOUT).

| Key | Writer | Notes |
|---|---|---|
| `plan/plan.json`, `plan/plan.json.sha256` | orchestrator | first objects written |
| `run/versions.json`, `run/images.lock.yaml`, `run/feature_allowlist.yaml`, `run/thresholds.json` | orchestrator | written once; `versions.json` and `images.lock.yaml` are the mounted `/config` inputs (C8) |
| `manifest/manifest.json` | orchestrator, after every write; the `s3-sync` sidecar mirrors the same file every 60 s | the ONLY object under `manifest/` (C1) |
| `stats/baseline_stats.json` | orchestrator | at the end of BASELINE (11.12) |
| `baseline/features/chunk-<t>.parquet`, `baseline/verification/chunk-<t>.parquet` | exporter role (Jobs `export-baseline-<t>`) | one per hour of WARMUP and BASELINE |
| `export/attempts/<attempt_key>/features.parquet`, `.../verification.parquet` | exporter role (Job `export-<attempt_key>`) | every finished attempt |
| `export/trailing/features/chunk-<t>.parquet`, `export/trailing/verification/chunk-<t>.parquet` | exporter role (Jobs `export-trailing-<t>`) | |
| `probes/baseline/chunk-<t>.parquet`, `probes/trailing/chunk-<t>.parquet`, `attempts/<attempt_key>/probes.parquet` | orchestrator | from the probe service |
| `attempts/<attempt_key>/verification.json`, `attempts/<attempt_key>/chaos/<tag>-<name>.yaml`, `attempts/<attempt_key>/injector.log` (F6 only) | orchestrator | the offline gate re-evaluates every attempt from these alone |
| `logs/orchestrator-<start_epoch>.log.gz` | orchestrator | the log; the file itself stays on the volume under `state/` |
| `checksums/SHA256SUMS` | orchestrator (`verify_complete`) | every object above, not itself and not `COMPLETE` |
| `COMPLETE` | orchestrator role only | written once, last. `scripts/pre_destroy.sh` needs it newer than every other object under the prefix. JSON with `campaign_id` |

Not in the record: `scratch/<id>/flink-checkpoints/` and `flink-savepoints/` (Flink role, 7-day expiry).

Permissions per role (Pod Identity, scoped to this campaign):

| Role | Allowed |
|---|---|
| orchestrator | `PutObject`, `GetObject`, `AbortMultipartUpload` on `campaign/<id>/*`; `ListBucket` only for prefix `campaign/<id>`; `sns:Publish` on the alert topic. No delete. |
| exporter | `PutObject`, `AbortMultipartUpload` on `campaign/<id>/baseline/*` and `campaign/<id>/export/*`; `ListBucket` for the prefix. Cannot write `COMPLETE`, cannot read. |
| flink | `PutObject`, `GetObject`, `DeleteObject`, `AbortMultipartUpload` on `scratch/<id>/*`; `ListBucket` on the bucket (needed by Hadoop S3A probes, key names only) |
| bucket policy | Deny `DeleteObject`/`DeleteObjectVersion` on `campaign/*` and bucket delete or policy change to every principal tagged `Project=<project>`; TLS only |

Because `ListBucket` is limited to the prefix, a `HEAD` of a missing key can return 403 instead of 404 for the orchestrator and the exporter. Test for existence with `ListObjectsV2` and a prefix, or treat 403 like 404.

## 3. Kubernetes names

| Thing | Name |
|---|---|
| Namespaces | `streaming` (everything of the pipeline, the orchestrator, chaos resources), `observability` (Prometheus), `chaos-mesh` (Chaos Mesh) |
| Node label and taint | label `workload=<kafka\|flink\|monitoring>`, taint `workload=<same>:NoSchedule` |
| Nodes | 3 `kafka` (r6g.xlarge), 2 `flink` (r6g.2xlarge), 1 `monitoring` (r6g.2xlarge) |
| StorageClass | `gp3` (default, `WaitForFirstConsumer`, expandable, reclaim `Delete`) |
| PriorityClass | `monitoring-critical` (Prometheus) |
| Chaos resources | created in `streaming`, kinds `PodChaos`, `NetworkChaos`, `StressChaos` (no `IOChaos`: F3 is dropped) |

### Kafka (Strimzi 0.45.2, Kafka 3.9.0, KRaft)

| Thing | Name or value |
|---|---|
| Kafka resource | `streaming-kafka` |
| Node pool | `brokers`, 3 nodes, roles `controller` and `broker` (combined), PVC 300 Gi `gp3`, one broker per node |
| Pods | `streaming-kafka-brokers-0`, `-1`, `-2` (the name survives a restart) |
| Select the brokers | `strimzi.io/cluster=streaming-kafka,strimzi.io/pool-name=brokers` (also `strimzi.io/component-type=kafka`). `strimzi.io/kind=Kafka` also matches the entity operator: do not use it |
| Data directory | `/var/lib/kafka/data` is the mount point; the log directory is `/var/lib/kafka/data/kafka-log<id>` (check with `df` in the pilot) |
| Bootstrap | `streaming-kafka-kafka-bootstrap.streaming.svc.cluster.local:9092` (plain), `:9093` (TLS) |
| Ports in the broker pod | 9092 plain, 9093 TLS, 9090 and 9091 control-plane and replication (internal), 9404 JMX exporter (`/metrics`) |
| Entity operator | one pod, kept on the `monitoring` node |
| Topic | partitions, replicas, `retention.ms` |
| `raw-events` | 12, 3, 7200000 (2 h) |
| `enriched-events` | 12, 3, 7200000 (2 h) |
| `aggregated-metrics` | 6, 3, 86400000 |
| `fraud-alerts` | 3, 3, 86400000 |
| `dead-letter` | 3, 3, 604800000 (pre-created) |
| All topics | `min.insync.replicas=2`; broker `log.retention.bytes=10000000000`; `auto.create.topics.enable=false` |

### Flink (Flink Kubernetes Operator 1.10.0, Flink 1.19.3)

| Thing | Name or value |
|---|---|
| FlinkDeployment | `streaming-flink` (session cluster, native mode, no `spec.job`) |
| REST | service `streaming-flink-rest`, port 8081: `http://streaming-flink-rest.streaming.svc.cluster.local:8081` |
| Pods | label `app=streaming-flink`; JobManager `component=jobmanager`, TaskManagers `component=taskmanager` (names `streaming-flink-taskmanager-1-<n>`; a replacement gets a new `<n>`) |
| Container | `flink-main-container` (the container StressChaos is charged to) |
| Sizing | JobManager 1 CPU 4096m; 3 TaskManagers 2 CPU 8192m, 4 slots each; parallelism 4 |
| Metrics | reporter port 9249 |
| Checkpoints, savepoints | `s3://<bucket>/scratch/<id>/flink-checkpoints`, `.../flink-savepoints` (11.7, C13); interval 60 s, EXACTLY_ONCE, timeout 120 s, min pause 30 s, incremental RocksDB, **retained on cancel** |
| Restart strategy | `failure-rate`: at most 40 failures in 10 minutes, 30 s delay (R1: fixed-delay, 10 attempts, 30 s; README deviation 16) |
| Job settings | passed on every submit by `/opt/flink/bin/run-job.sh` with `-p 4` and `-D` (checkpoints, state backend, restart strategy), the same values as the session config. Checked after phase `submit` by `scripts/check_flink_jobs.py` |
| Minimum slots | `slotmanager.number-of-slots.min: 12`: three TaskManagers run even with no job |
| Service account | `flink` (created by the operator chart, namespaced Role, Pod Identity role `<project>-<id>-flink`) |
| Job submission | `/opt/flink/bin/submit-jobs.sh ECOMMERCE\|IOT` inside the `flink-jobs` image; the Job `flink-submit` runs it once. Idempotent. Sources start from the latest offsets. One job, for example a resubmission from a checkpoint: `/opt/flink/bin/run-job.sh <main-class> <jar> [<checkpoint path>]` |
| Job classes and jars | see `images/flink-jobs/submit-jobs.sh`; jars in `/opt/flink/usrlib/` |
| Consumer groups | `enrichment-job`, `aggregation-job`, `fraud-detection-job`, `iot-enrichment-job`, `iot-aggregation-job`, `iot-alert-job` |
| No healing watchdog | nothing but the orchestrator resubmits jobs. The CronJob `campaign-watchdog` only alerts (below) |

### Orchestrator and helpers

| Thing | Name |
|---|---|
| Job | `campaign-orchestrator` (no `activeDeadlineSeconds`, `backoffLimit: 0`, `restartPolicy: Never`), label `app=campaign-orchestrator`, containers `orchestrator` and native sidecar `s3-sync` |
| Service account | `campaign-orchestrator` (Pod Identity role `<project>-<id>-orchestrator`) |
| RBAC | Role `campaign-orchestrator` in `streaming` (`pods/exec` get and create; `configmaps` update and patch on `campaign-watchdog-state` only); Roles `campaign-orchestrator-read` in `observability` and `chaos-mesh` (pods get, list); ClusterRole `campaign-orchestrator-nodes-read` (nodes get, list). See `k8s/base/rbac/orchestrator-rbac.yaml` for the exact verbs |
| Watchdog | CronJob `campaign-watchdog` (every 10 min, label `app=campaign-watchdog`, no `rebuild/*` label), orchestrator image and service account, script from ConfigMap `campaign-watchdog`, send times in ConfigMap `campaign-watchdog-state`. Alerts only. Phase `run`; `deploy_workload.sh --phase stop` suspends it |
| PVC | `campaign-manifest` (10 Gi, `gp3`), mounted at `/manifest`: `manifest.json` (the record, mirrored to S3) and `state/` (resume state, never synced, C1) |
| ConfigMaps | `workload-config` (`WORKLOAD`, `FLINK_EXPECTED_JOBS`, `LOADGEN_BASE_RATE`), `campaign-config` (`plan.json`, `thresholds.json`, `versions.json`, `images.lock.yaml`, written by `scripts/campaign_inputs.py configmap`), `loadgen-config`, `loadgen-epoch` (`LOADGEN_EPOCH_UNIX`), `kafka-metrics-config` |
| Exporter | service account `metric-exporter` (Pod Identity role `<project>-<id>-exporter`, no Kubernetes token), phase `rbac`. The orchestrator creates every exporter Job itself: `export-<attempt_key>`, `export-baseline-<chunk_start>`, `export-trailing-<chunk_start>` (orchestrator image, command `export`). There is no infra export Job and no export scratch volume (C11) |
| Verification consumer | Deployment `verification-consumer`, label `app=verification-consumer`, command `consumer`, consumer group `rebuild-verification`, `/status` (JSON) and `/metrics` on 9103 (C7) |
| Verification probes | Deployment and Service `verification-probes`, label `app=verification-probes`, command `probes`, ServiceAccount and Role `verification-probes` (pods get, list; pods/exec get, create), `/status`, `/samples`, `/metrics` on 8000, PodMonitor `verification-probes` with job label `rebuild-verification`. Manifest `k8s/base/probes/` (phase `load`, C9) |
| Load generator | Deployment and Service `load-generator`, label `app=load-generator`, `/status` (JSON) and `/metrics` on 9102 (C7) |
| F6 injector | Job `poison-<attempt_key>` that the orchestrator creates, image from `F6_INJECTOR_IMAGE` (the `f6-injector` image, built from `rebuild/orchestrator/orchestrator/f6_injector.py`, C6); no ConfigMap needed |
| Deploy phases | label `infra.phase` = `rbac`, `kafka`, `flink`, `submit`, `load`, `workload`, `run` |

## 4. Environment variables and mounted paths

### Orchestrator container (set by `k8s/base/orchestrator/orchestrator-job.yaml`; protocol 11.11, C5)

One spelling only. The orchestrator refuses a start without every required name, and refuses the retired names `SCHEDULE_PATH`,
`SCHEDULE_SHA256`, `CAMPAIGN_SEED` and `LOADGEN_METRICS_URL` if they are set.

| Variable | Value | Notes |
|---|---|---|
| `WORKLOAD` | `ECOMMERCE` or `IOT` | from ConfigMap `workload-config`; **required, no default**; refuse anything else |
| `CAMPAIGN_ID` | `<id>` | required |
| `CAMPAIGN_INDEX` | `0` to `5` | required; from the plan (`scripts/campaign_inputs.py plan-facts`); must agree with `CAMPAIGN_ID` (11.2) |
| `PLAN_SHA256` | hex | required; SHA-256 of the committed `plan.json` (11.3) |
| `RESULTS_BUCKET`, `RESULTS_PREFIX` | bucket, `campaign/<id>` | required; no `s3://`, no slash at either end |
| `PLAN_PATH`, `THRESHOLDS_PATH`, `VERSIONS_FILE`, `IMAGES_LOCK_FILE` | `/config/plan.json`, `/config/thresholds.json`, `/config/versions.json`, `/config/images.lock.yaml` | required for a run; ConfigMap `campaign-config`. The plan must equal the plan regenerated from the seed byte for byte, the thresholds the thresholds the code registers |
| `FLINK_EXPECTED_JOBS` | comma-separated job names (section 1) | must equal the 11.11 names of the workload |
| `LOADGEN_BASE_RATE` | `1000` or `10000` | recorded only |
| `AWS_REGION`, `AWS_DEFAULT_REGION` | `us-east-2` | credentials come from Pod Identity; no key variables |
| `SNS_TOPIC_ARN` | the alert topic | |
| `PROMETHEUS_URL` | `http://prometheus-kube-prometheus-prometheus.observability.svc.cluster.local:9090` | |
| `KAFKA_BOOTSTRAP` | `streaming-kafka-kafka-bootstrap.streaming.svc.cluster.local:9092` | |
| `FLINK_REST_URL` | `http://streaming-flink-rest.streaming.svc.cluster.local:8081` | |
| `LOADGEN_STATUS_URL` | `http://load-generator.streaming.svc.cluster.local:9102/status` | the load generator seed check at preflight |
| `PROBES_URL` | `http://verification-probes.streaming.svc.cluster.local:8000` | |
| `NAMESPACE` | `streaming` | |
| `MANIFEST_DIR` | `/manifest` | |
| `F6_INJECTOR_IMAGE` | the injector image reference with digest | |
| `KUBECTL_BIN` | `/usr/local/bin/kubectl` | kubectl 1.35.x |
| `HOME` | `/tmp` | root filesystem is read-only; write only to `/manifest` and `/tmp` |

| Path | Mode | Content |
|---|---|---|
| `/manifest` | read-write, PVC `campaign-manifest` | `manifest.json`, the campaign manifest, mirrored to `campaign/<id>/manifest/manifest.json`. `state/`: the orchestrator's resume state (`plan.json` copy, `pending.json`, `run.lock`, `campaign.log`, `attempts/<key>/facts.json`), never synced and never checksummed (C1). The sidecar writes `/manifest/.s3sync.status` (`ok <epoch>` or `fail <epoch> rc=<n>`) and syncs `manifest.json` only |
| `/config` | read-only, ConfigMap `campaign-config` | `plan.json`, `thresholds.json`, `versions.json`, `images.lock.yaml` |
| `/tmp` | read-write, emptyDir 2 Gi | scratch |

### F6 injector (`images/f6-injector`, built from `rebuild/orchestrator/orchestrator/f6_injector.py`; the orchestrator sets these in the Job it creates)

`KAFKA_BOOTSTRAP` (required), `KAFKA_TOPIC` (default `raw-events`), `INJECT_RATE` (records/s), `INJECT_DURATION` (s), `INJECT_SEED`
(the plan's `f6_payload_seed`), `START_AT_UNIX` (required, the planned onset), `ATTEMPT_KEY` (required, the header value),
`LATE_TOLERANCE_SEC` (5), `MIN_DELIVERED_FRACTION` (0.9; the orchestrator passes 0.95). Every record carries the header
`rebuild-attempt=<ATTEMPT_KEY>`. stdout: JSON lines (`ready`, `first_produce_ack`, `summary`, `error`), then the final line
`INJECTED sent=N delivered=A failed=F planned=P first_epoch=T0 last_epoch=T1 payload_sha256=H`, where `delivered` counts the records
the brokers acknowledged and `T0`, `T1` are the first and last acknowledgement times. Exit 0 ok, 1 shortfall or delivery failure,
2 bad configuration or no topic metadata, 3 started too late. Payloads never contain a closing brace, so every record reaches
`dead-letter`. The orchestrator stores the pod log plus its own `orchestrator_observed` line as `attempts/<key>/injector.log` (C6).

### Load generator (`k8s/<workload>/loadgen-config.yaml` plus the Deployment)

`WORKLOAD`, `THROUGHPUT_PROFILE`, `SINUSOIDAL_AMPLITUDE` (0.2), `SINUSOIDAL_PERIOD_SEC` (1800), `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC` (`raw-events`), `PRODUCER_ACKS`, `PRODUCER_COMPRESSION`, IoT only `NUM_DEVICES`, `NUM_LOCATIONS`, `BURST_PERIOD_SEC`, `BURST_DURATION_SEC`, `BURST_MULTIPLIER`; new: `LOADGEN_SEED` (required: the plan's `load_generator_seed`, which `scripts/deploy_workload.sh` reads from `--plan-file`, C12), `LOADGEN_EPOCH_UNIX` (from ConfigMap `loadgen-epoch`), `LOADGEN_METRICS_PORT` (9102). The user, product, device and location pools use a fixed internal seed 42 (identical in every campaign by design); `LOADGEN_SEED` seeds the event stream.

`/status` on 9102 (JSON, protocol 2.3.9, C7): `acked_count` (records the brokers acknowledged), `produced_count`, `delivery_failures_count`,
`produce_errors_count`, `commanded_rate_per_s`, `cycle_phase_frac`, `epoch_at`, `started_at`, `seed`, `workload`, `profile`, `at`.

### Verification consumer, exporter

Verification consumer: `WORKLOAD`, `KAFKA_BOOTSTRAP`, `KAFKA_GROUP_ID` (`rebuild-verification`, the only accepted value), `METRICS_PORT` (9103). It subscribes `raw-events` and `dead-letter`. `/status` fields: `started_at`, `last_poll_at`, `raw_events_count`, `malformed_count`, `malformed_by_attempt` (attempt key to cumulative count), `dead_letter_count`, `dead_letter_by_attempt` (C6).
Exporter Jobs (created by the orchestrator): `WORKLOAD`, `CAMPAIGN_ID`, `RESULTS_BUCKET`, `RESULTS_PREFIX` (`campaign/<id>`), `AWS_REGION`, `AWS_DEFAULT_REGION`, `PROMETHEUS_URL`, `FEATURE_ALLOWLIST_SHA256`; arguments `export --kind attempt|baseline|trailing --key ... --start-at ... --end-at ...`; `/tmp` emptyDir 1 Gi.
Verification probes: `FLINK_REST_URL`, `LOADGEN_STATUS_URL`, `KUBECTL_BIN`.

## 5. Ports

| Port | What |
|---|---|
| 9090 | Prometheus (`prometheus-kube-prometheus-prometheus.observability`) |
| 9092, 9093 | Kafka plain, TLS |
| 9404 | Kafka JMX exporter, every broker |
| 8081 | Flink REST (`streaming-flink-rest`) |
| 9249 | Flink Prometheus reporter, every JobManager and TaskManager pod |
| 9102 | load generator `/status` and `/metrics` |
| 9103 | verification consumer `/status` and `/metrics` |
| 8000 | verification probes `/status`, `/samples`, `/metrics` (`verification-probes.streaming`) |

## 6. Prometheus

Release name `prometheus`, namespace `observability`, scrape and evaluation interval 10 s, retention 30 d capped at 85 GB on a 100 Gi `gp3` volume. No Grafana, no Alertmanager.

| Scrape job (`job` label) | Targets | `instance` and `pod` label |
|---|---|---|
| `kafka-brokers` | the three broker pods, port 9404 | the pod name, e.g. `streaming-kafka-brokers-1` (also `kafka_broker`, `node`) |
| `flink-metrics` | JobManager and TaskManager pods, port 9249 | the pod name; `flink_component` = `jobmanager` or `taskmanager` |
| `load-generator` | the generator pod, 9102 | the pod name |
| `verification-consumer` | the consumer pod, 9103 | the pod name |
| `rebuild-verification` | the probe service pod, 8000 (PodMonitor `verification-probes`) | the pod name. Never read by the feature export |
| kubelet (cAdvisor), `kube-state-metrics`, `node-exporter`, apiserver, coredns | chart defaults | cAdvisor and kube-state series carry `pod`, `container`, `namespace`, `node` |

No series is labelled by pod IP. Series from pods that are not Running are not scraped. A restarted broker keeps its name, so its counters reset inside one series.

No stored label carries the account ID (decision 29). The registry host `<account>.dkr.ecr.<region>.amazonaws.com/` is cut at scrape time from `image` (cAdvisor) and from `image`, `image_spec` and `image_id` (kube-state-metrics); the path, tag and digest stay. Labels that reach the export besides the metric's own: `node` (the EC2 private DNS name, which holds a private IP and the region; the preprocessing drops it), `host_ip` (`kube_pod_info`, a private IP), `id` and `name` (cAdvisor cgroup path and container ID), `created_by_name`. None of them names the account, a profile, a bucket or a role.

Kept on purpose: `container_cpu_usage_seconds_total`, `container_cpu_cfs_throttled_periods_total`, `container_cpu_cfs_throttled_seconds_total`, `container_memory_working_set_bytes`, `container_oom_events_total`, `container_spec_memory_limit_bytes`, `container_spec_cpu_quota`, and from kube-state-metrics `kube_pod_container_resource_limits`, `kube_pod_container_resource_requests`, `kube_pod_container_status_last_terminated_reason`, `kube_pod_container_status_restarts_total`, `kube_pod_start_time`, `kube_pod_status_ready`, `kube_pod_info` (carries `uid`). From node-exporter: `node_netstat_Tcp_RetransSegs`, `node_disk_*`, `node_network_*`. Volume use: `kubelet_volume_stats_used_bytes`, `kubelet_volume_stats_capacity_bytes`.

### Broker metrics (JMX exporter, `k8s/base/kafka/kafka-metrics-configmap.yaml`)

New in the rebuild (R1 exported only `Count` attributes):

| Metric | Labels |
|---|---|
| `kafka_network_requestmetrics_<stage>_<stat>` with stage in `totaltimems`, `requestqueuetimems`, `localtimems`, `remotetimems`, `responsequeuetimems`, `responsesendtimems` and stat in `mean`, `max`, `50thpercentile`, `95thpercentile`, `99thpercentile`, `999thpercentile` | `request` = `Produce`, `FetchConsumer`, `FetchFollower` |
| `kafka_log_logflushstats_logflushrateandtimems_<stat>` (same stats) | |
| `kafka_server_requesthandleravgidlepercent_oneminuterate`, `kafka_network_networkprocessoravgidlepercent` | |
| `kafka_server_delayedoperationpurgatory_purgatorysize` | `delayedoperation` |
| `kafka_controller_kafkacontroller_activecontrollercount` (1 on the active controller), `..._fencedbrokercount`, `..._activebrokercount`, `..._offlinepartitionscount`, `..._preferredreplicaimbalancecount`, `..._globalpartitioncount` | scraped by job `kafka-brokers` like every broker metric; `activecontrollercount` and `fencedbrokercount` go to the verification file only, by the allowlist (protocol 2.3.3 as clarified in 11.8, C10) |
| `kafka_server_raftmetrics_<attribute>` (`current_leader`, `current_epoch`, `high_watermark`, ... attribute names to be confirmed on a live broker) | |
| `kafka_server_kafkaserver_brokerstate`, `kafka_server_replicamanager_leadercount`, `..._offlinereplicacount`, `kafka_server_replicafetchermanager_maxlag`, `kafka_controller_controllerstats_leaderelectionrateandtimems_<count\|mean\|99thpercentile>` | |

Unchanged from R1: `kafka_server_brokertopicmetrics_<name>_total` (with `topic` or without), `kafka_network_requestmetrics_<name>_total` (`request`, `error`), `kafka_server_replicamanager_underreplicatedpartitions`, `..._underminisrpartitioncount`, `..._isrshrinks_total`, `..._isrexpands_total`, `..._partitioncount`, `kafka_log_logflushstats_logflushrateandtimems_total`, `kafka_server_fetcherlagmetrics_consumerlag`, plus the JVM metrics.

### Load generator metrics

`loadgen_target_rate_per_second`, `loadgen_actual_rate_per_second` (5 s window), `loadgen_produced_total`, `loadgen_acked_total` (acknowledged deliveries), `loadgen_produce_errors_total`, `loadgen_delivery_failures_total`, `loadgen_start_time_seconds`, `loadgen_epoch_seconds`, `loadgen_cycle_phase_seconds` (= (now - epoch) mod 1800), `loadgen_burst_active` (IoT), `loadgen_seed`, `loadgen_info{workload,profile,seed}`.

### Verification consumer metrics (contract, implemented by the orchestrator image)

`verify_consumer_records_total`, `verify_consumer_malformed_total`.
