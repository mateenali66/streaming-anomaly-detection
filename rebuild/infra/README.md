# Infrastructure for the re-run (EKS 1.35, Kafka and Flink, Chaos Mesh)

Built 13 October 2026 from `diagnostics/REVIEW3_INFRA.md` (main source), `REVIEW1_FAULTS.md`, `REVIEW2_PIPELINE.md`,
`REBUILD_INFRA_AUDIT.md`, `REBUILD_DECISIONS.md` (13 to 20) and `FOUNDATION_RECORD.md`, then fixed for `REVIEW4_OPERATIONS.md` and
`REVIEW4_LIVE_CONTRACT.md` (decisions 21 to 30, recorded in `rebuild/review4_fixes/C_infra.md`). Nothing here has been applied
or run against a cluster. Every root has been planned offline with mock providers. Section 7 says exactly what was checked on a
laptop and what only the pilot can show. `PILOT_RUNBOOK.md` is the pilot as exact commands.

`NAMES.md` lists every name, label, port, path and environment variable the orchestrator and the preprocessing may rely on. It agrees
with protocol section 11 name for name since the integration of 13 October 2026; every interface change and its reason is in
`rebuild/CONTRACT_CHANGES.md`.

## 1. Layout

```
terraform/shared/    once per account: protected results bucket, immutable ECR repos, one SNS topic, optional budget, AWS Health rule
terraform/network/   once: one new VPC, one NAT (one zone), S3 endpoint, 10 pre-cut /22 node subnets, 2 control-plane-only /24, destroy guard
terraform/cluster/   per campaign: EKS 1.35, 6 nodes, Pod Identity roles, operators, PriorityClass, teardown guard
k8s/base/            Helm values (helm-values/) and manifests for both workloads, the alert-only watchdog (watchdog/)
k8s/checks/          one-off pilot checks, never deployed by the scripts
k8s/ecommerce/       workload ConfigMaps, load profile LOW, Flink submit Job (kustomize overlay)
k8s/iot/             the same for IoT (profile IOT_MEDIUM, bigger load generator)
images/              four arm64 images (orchestrator, f6-injector, flink-jobs, load-generator), images.lock.yaml, Python and Gradle locks
scripts/             boto3 helpers, image mirroring and building, deploy, pre_destroy.sh, sweep, local validation
tests/               offline tests (section 7). tests/tf/ holds the mocked plans of the three roots
PILOT_RUNBOOK.md     the pilot, command by command
```

Each Terraform root has its own state (partial S3 backend, `backend.hcl` supplied at init, `use_lockfile = true`), exact provider pins,
and a committed `.terraform.lock.hcl` with hashes for linux_amd64, linux_arm64 and darwin_arm64. The EKS module is `= 21.29.0`.

## 2. Versions and the documentation they rest on

| Component | Pin | Checked against |
|---|---|---|
| Terraform | code `>= 1.10.0` (S3 `use_lockfile`, cross-variable validation). Validated and planned with mock providers on 1.10.5 and 1.16.5. 1.5.7 can no longer validate a copy: the cluster root's cross-variable validation needs 1.9 or later | developer.hashicorp.com/terraform/language/backend/s3, .../language/tests/mocking (context7) |
| AWS provider | `= 6.68.0` (newest on the registry on 13 Oct 2026). helm `= 2.17.0`, kubectl (alekc) `= 2.2.0`, null `3.3.2`, time `0.14.2`, tls `4.4.1`, cloudinit `2.4.1` | registry; lock files |
| EKS | 1.35, `AL2023_ARM_64_STANDARD`, module `terraform-aws-modules/eks/aws = 21.29.0` | module UPGRADE-21.0 (context7) |
| Add-ons and AMI release | **required variables, no defaults**, written by `scripts/capture_versions.py` | not captured yet |
| Pod Identity | trust `pods.eks.amazonaws.com` with `sts:AssumeRole` and `sts:TagSession`, `aws:SourceArn` = this cluster; SDK minimums boto3 1.34.41, AWS CLI v2 2.15.0, Java v1 1.12.746 | EKS user guide, pod-id-role and pod-id-minimum-sdk (context7) |
| EBS CSI | add-on config `controller.extraVolumeTags`; IAM scoped on `Campaign` | aws-ebs-csi-driver `docs/tagging.md` (context7) |
| Strimzi | chart 0.45.2, Kafka 3.9.0, KRaft, node pool `brokers` | strimzi.io/downloads (0.45.2: Kafka 3.8.0 to 3.9.2, Kubernetes 1.25 to 1.35); Strimzi docs (context7) |
| Chaos Mesh | 2.8.4, `chaosDaemon.runtime=containerd`, `socketPath=/run/containerd/containerd.sock` | chaos-mesh `helm/chaos-mesh/README.md` (context7) |
| Flink Kubernetes Operator | chart 1.10.0 (fallback 1.16.1, renders with the same values file) | operator docs (context7): session cluster = `FlinkDeployment` without `spec.job` |
| Flink | image `flink:1.19.3-java17` by digest; one version. `flink-s3-fs-hadoop-1.19.3` bundles AWS SDK for Java **1.12.779**, above the Pod Identity minimum (read from the jar) | Flink 1.19 docs, filesystems/plugins (context7) |
| kube-prometheus-stack | 69.8.0 (Prometheus v3.2.1, operator v0.80.1, kube-state-metrics v2.15.0, node-exporter v1.9.0) | chart read locally with `helm pull` |
| JMX exporter rules | first match wins; unmatched attributes are not collected; the pattern is not anchored | prometheus/jmx_exporter docs (context7) |
| kubectl in the orchestrator image | v1.35.9, sha256 checked against both a pinned value and the published `.sha256` | dl.k8s.io |

## 3. What the infrastructure does about each review finding

| Finding | Where |
|---|---|
| A1 results bucket outlives clusters, delete deny on `campaign/*`, `scratch/` expiry, versioning | `terraform/shared/main.tf` |
| A1 destroy guard, A5 PV tagging and sweep | `terraform/cluster/guard.tf`, `scripts/pre_destroy.sh`, `scripts/teardown_guard.py`, `scripts/sweep.py`, `extraVolumeTags` in `main.tf` |
| A2 module and provider pins, lock files | every `versions.tf`, `.terraform.lock.hcl` |
| A3 versions for 1.35, add-ons and AMI captured | `scripts/capture_versions.py`, variables without defaults |
| A4 PriorityClass before Prometheus | `terraform/cluster/platform.tf` (`kubectl_manifest.monitoring_critical`, `depends_on`) |
| A6 disk fill | `k8s/base/kafka/topics.yaml` (2 h on raw and enriched), 300 Gi in `kafka.yaml`, pilot check below |
| A7 one zone | `terraform/network` (nodes, NAT, volumes in `workload_az`; two small control-plane subnets elsewhere) |
| A8 48 h deadline | orchestrator Job has no deadline. Protocol 1.9, 2.7 item 6 and 6.5 item 1 now say so (review 4 M12) |
| A9 registry pulls and run-time installs | `images/` (everything by digest from ECR, nothing installs at start), `scripts/mirror_images.py` |
| C1 IAM names | explicit names, `iam_role_use_name_prefix = false`, a length `check` |
| C2 public API | `api_allowed_cidrs` required, `0.0.0.0/0` and ranges wider than /16 rejected (tested) |
| C3 EBS policy | scoped on `Campaign`, no snapshot actions |
| C4, F RBAC | `k8s/base/rbac/orchestrator-rbac.yaml` |
| C5, F S3 and SNS | prefix-scoped Pod Identity roles, one topic |
| C6 missing run inputs | Dockerfile, Flink submit script and Job, IoT overlay, WORKLOAD without default, stats are no longer files |
| C7 arm64 builds, Gradle locks | `images/*/Dockerfile`, `images/flink-jobs/locks/` (generated and a strict build tested locally) |
| C8 Flink checkpoints on S3 | `flink-session-cluster.yaml`, `aws_iam_role.flink` |
| REVIEW1 A1, A2, A3, B7 | S3 checkpoints, job-name checks via `FLINK_EXPECTED_JOBS`, the F6 injector exits non-zero on any shortfall, F3 gone |
| REVIEW2 item 6 | scrape labels by pod name; exporting by role is the preprocessing's job (`flink_component`, pod name) |

Review 4 (`diagnostics/REVIEW4_OPERATIONS.md`, `REVIEW4_LIVE_CONTRACT.md`; details and checks in `rebuild/review4_fixes/C_infra.md`):

| Finding | Where |
|---|---|
| B1 `coalesce` on a null profile | `terraform/cluster/guard.tf` (conditional instead) |
| B2 module `depends_on` broke the first plan | `terraform/cluster/main.tf` (postconditions on the VPC and subnet data sources), `variables.tf` (campaign and workload match as a cross-variable validation) |
| B3 role policies destroyed next to the guard | `guard.tf` `depends_on`; `tests/check_tf_graph.py` checks every resource is destroyed after the guard |
| B4 no MFA session without the AWS CLI | `scripts/mfa_session.py` |
| B5 long Terraform commands | `PILOT_RUNBOOK.md` (plan to a file, detached apply, polling, recovery) |
| B6 no resume, no pilot fault rerun | `scripts/deploy_workload.sh --resume --fault --block`, `scripts/orchestrator_args.py`, `--phase stop` |
| B7 no campaign download | `scripts/fetch_campaign.py` |
| C1 account ID in data | `terraform/shared` (bucket `<project>-results-<bucket_suffix>`), `k8s/base/helm-values/prometheus-values.yaml` (registry host cut from image labels), `scripts/mark_complete.py` (no user name) |
| C2 AMI pin ignored | `use_latest_ami_release_version = false` in `main.tf` |
| C3 new laptop address | `scripts/eks_api_cidrs.py` |
| C4 one data directory | per-campaign `TF_DATA_DIR` and state key (`terraform.tfvars.example`, `backend.hcl.example`, runbooks) |
| C5 vCPU use not measured | `scripts/discover.py` (CloudWatch `AWS/Usage`) |
| C6 no alert on a dead orchestrator | `k8s/base/watchdog/` (alert-only CronJob) |
| L1 jobs at parallelism 1, no checkpoints | `images/flink-jobs/run-job.sh` (the one submission path), `scripts/check_flink_jobs.py` |
| L4 no TaskManagers without jobs | `slotmanager.number-of-slots.min: 12` in `k8s/base/flink/flink-session-cluster.yaml` |
| L9 exec without `get` | both Roles |
| M1 the leak guard published its words | `tests/forbidden_words.sha256` (hashes), gitignored `tests/forbidden_words.local` |
| M2 sweep read other teams' resources | `scripts/sweep.py` |
| M3 EBS policy on one tag | `terraform/cluster/iam.tf` (`Project` as well) |
| M4 guard edge cases | `scripts/teardown_guard.py`, `scripts/mark_complete.py`, `scripts/delete_orphan_volumes.py`, `--phase stop` |
| M5 network destroy unguarded | `terraform/network` `terraform_data.network_guard`, `scripts/network_guard.py` |
| M6 account settings | `scripts/discover.py` |
| M7 cost allocation tags | resource removed from `terraform/shared`; the payer account activates the tags |
| M8 operator tools | `python_bin` required and absolute; kubectl version check in `deploy_workload.sh` |
| M9 placeholder versions | refused by name in `terraform/cluster/variables.tf` |
| M10, M11 runbook gaps | `PILOT_RUNBOOK.md` and section 4 (absolute paths, orchestrator start, both teardowns) |
| M12 Health events, 120 h deadline | `terraform/shared` (EventBridge rule, log group `/streaming-anomaly-detection/health`, SNS target), orchestrator role `logs:FilterLogEvents` on that group only; protocol text fixed |

## 4. Runbook (empty account to six campaigns and back)

The pilot, command by command, is `PILOT_RUNBOOK.md`. This is the short form, which also covers the six campaigns. `export
AWS_PROFILE=...` in the shell you use. No profile name, account ID or e-mail address goes into any file. The `*.tfvars` and
`backend.hcl` files are gitignored. No single command may wait longer than 10 minutes (decision 30): every Terraform apply and
destroy, and every long deploy, runs as `nohup caffeinate -i <command> > <log outside the repository> 2>&1 &` and is polled.

1. **Tools.** Terraform 1.10 or later (not the 1.5.7 on PATH), kubectl 1.34 to 1.36 (not the 1.37 on PATH), Helm 3 (embedded in the Terraform helm provider), Docker with buildx, and a Python environment with `scripts/requirements.txt`. The AWS CLI is not used anywhere. `python_bin` in the network and cluster tfvars is that interpreter's absolute path. It is required.
2. **Session.** Ask the author for the one-time password, then `scripts/mfa_session.py --source-profile ... --target-profile ... --otp ...` (36 h). Start every apply or destroy with at least 2 h left.
3. **Discovery (read only).** `scripts/discover.py --vpc-cidr 10.NN.0.0/16 --clusters N` must exit 0 (no BLOCK). It shows CIDRs, zones, Elastic IPs, vCPU quota AND current use, the default EBS key, GuardDuty Runtime Monitoring, the gp3 quota and whether roles can be created. DECIDE lines go to the author. Request quota increases now (6 campaigns x 36 vCPU, plus a reserve).
4. **Billing.** The payer account activates the `Project`, `Campaign` and `Workload` cost allocation tags. A member account cannot (review 4 M7). Set `budget_limit_usd`. Budget data lag 8 to 12 hours, so the pilot also has a wall-clock stop rule.
5. **State bucket by hand** (versioned, encrypted, public access blocked). Copy each `backend.hcl.example` to `backend.hcl`.
6. **`terraform/shared`.** Choose `bucket_suffix` ONCE, at random, in the gitignored tfvars. A bucket cannot be renamed. `init`, `plan -out`, apply the plan. Confirm the SNS e-mail subscription. The repositories come from `images/images.lock.yaml`; a new image needs a `shared` apply first.
7. **Images.** `scripts/mirror_images.py --resolve`, `--copy --dry-run`, `--copy`, `--verify`, then `scripts/build_images.sh --tag r2-NNN`. Commit `images/images.lock.yaml`. The cluster root refuses to plan while any digest is empty.
8. **`terraform/network`.** Read the plan: only creates. Its destroy guard refuses later while a project cluster uses the VPC.
9. **Versions.** `scripts/capture_versions.py --kubernetes-version 1.35 --check-config --out terraform/cluster/addon-versions.auto.tfvars`. Commit the file. The placeholders of the example file are refused by name.
10. **Plans and thresholds.** `rebuild/integration/plan_tool.sh` with ABSOLUTE `--out` and `--plan` paths (it runs inside `rebuild/orchestrator`). Commit `plans/<id>/plan.json`, its `.sha256` and `plans/thresholds.json`.
11. **Each cluster.** In its own shell: `export TF_DATA_DIR=.terraform/<id>`, `terraform init -reconfigure -backend-config=backend.hcl -backend-config="key=campaigns/<id>/terraform.tfstate"`, `terraform plan -var-file=<id>.tfvars -out=<id>.tfplan`, then the detached `terraform apply <id>.tfplan` (20 to 35 min). Then `scripts/eks_kubeconfig.py --cluster-name <project>-<id> --campaign <id> --output kubeconfig-<id>`. Check section 8, list A.
12. **Workload.** `scripts/deploy_workload.sh --workload ... --campaign-id <id> --results-bucket ... --sns-topic-arn ... --ecr-registry ... --plan-file <abs>/plans/<id>/plan.json --phase all`, detached (10 to 45 min). Phase `submit` ends with `scripts/check_flink_jobs.py`, so a cluster whose jobs do not run at parallelism 4 with 60 s exactly-once checkpoints stops here. Check section 8, list B. Run `k8s/checks/exporter-identity-check.yaml` once.
13. **Orchestrator.** The same command with `--phase run --thresholds-file <abs>/plans/thresholds.json`. It writes ConfigMap `campaign-config` (plan, thresholds, `versions.json`, `images.lock.yaml`), starts the orchestrator Job and the alert-only watchdog CronJob.
14. **Pilot protocol**, including the Chaos Mesh canaries (section 8, list C). A fixed fault reruns with `--phase stop`, then `--phase run --recreate-job --resume --fault <F>`. A transient halt resumes with `--phase run --recreate-job --resume`.
15. **Teardown dry run** on a pilot, early: `terraform plan -destroy` and apply the plan without `COMPLETE`. It must stop at the completeness gate and leave the state unchanged.
16. **Pilot data.** `scripts/fetch_campaign.py --bucket ... --campaign <id> --out <new dir outside the repository>`, then the pipeline dry run on the copy (protocol 1.8 item 9). A pilot has no orchestrator `COMPLETE`, which the script reports as a warning.
17. **Pilot teardown, both pilots.** `--phase stop`, `scripts/mark_complete.py ... --i-verified-the-data`, a final `fetch_campaign.py`, then the detached destroy. Then `scripts/sweep.py --campaign <id>` and `scripts/delete_orphan_volumes.py --campaign <id>` must find nothing. Stop rule: both pilots go 13 hours after the first cluster apply, whatever their state.
18. **Requote** from measured numbers (section 6) and get the author's approval.
19. **Six campaign clusters**, about 10 minutes apart, one data directory and state key each, as in 11 to 13. Baselines need nothing by hand: the orchestrator exports every hour of WARMUP and BASELINE itself and refuses to start block 1 until the baseline statistics are in S3.
20. **Watch.** The watchdog alerts on a failed orchestrator, a non-zero exit or a manifest unchanged for 2 h. By hand at +1 h and +6 h: objects under `attempts/` and `export/attempts/`, Kafka volumes below 70%, no unexpected node or Prometheus restart, `/manifest/.s3sync.status` says `ok`. A new laptop address: `scripts/eks_api_cidrs.py`, then the same CIDR in `<id>.tfvars`.
21. **Finish.** `verify_complete` writes `campaign/<id>/COMPLETE`. `scripts/fetch_campaign.py` copies the prefix and checks every hash against `SHA256SUMS` and the `COMPLETE` fields.
22. **Teardown per cluster.** The detached `terraform destroy` runs `scripts/pre_destroy.sh` first. Then `scripts/sweep.py --campaign <id>`.
23. **After the last cluster:** destroy `network`. Its guard checks that no project cluster, foreign endpoint or stray interface remains. Check the NAT, the Elastic IP and the VPC are gone and the VPC count is back. Keep `shared` until the deposit is verified. `shared` has `prevent_destroy` on the bucket and repositories on purpose.
24. **After 24 to 48 h** compare the bill with the budget and record the real cost per cluster hour.

## 5. Orchestrator image interface

The orchestrator code is written by another builder. The image must meet this contract; `images/orchestrator/Dockerfile` builds it.

| Item | Contract |
|---|---|
| Source | a Python package `orchestrator` with `__main__.py`, in a directory passed as build context `orchestrator_src` (`scripts/build_images.sh --orchestrator-src DIR`, default `rebuild/orchestrator`). The build fails if the package is missing |
| Entry point | `python -u -m orchestrator <command>`; image `CMD` is `run` |
| Commands | `run` (the campaign), `export` (the exporter Jobs the orchestrator creates), `consumer` (the verification consumer Deployment, protocol 2.3), `probes` (the verification probe Deployment), `verify-complete` (writes `COMPLETE`; `run` does it itself), `abandon`, `gate-offline`, `plan`, `thresholds`. The manifests that name a command are `k8s/base/orchestrator/orchestrator-job.yaml`, `k8s/base/loadgen/verification-consumer.yaml` and `k8s/base/probes/verification-probes.yaml` |
| Contents | Python 3.12.12, `kubectl` 1.35.9 at `/usr/local/bin/kubectl`, boto3 1.40.0, pyarrow 24.0.0, numpy 2.3.5, pandas 2.3.3, confluent-kafka 2.6.1, requests, PyYAML; `/opt/pip-freeze.txt` lists the rest. **No network installs at run time.** A needed library means editing `images/orchestrator/requirements.in`, `scripts/lock_python.py`, and rebuilding |
| User and filesystem | uid 10001, read-only root filesystem, writable `/tmp` and `/manifest`; `HOME=/tmp` |
| Environment | exactly the table in `NAMES.md` section 4 (protocol 11.11, C5). `WORKLOAD` must be `ECOMMERCE` or `IOT`; unset or anything else is a fatal error with a clear message. There is no default. Retired names (`SCHEDULE_*`, `CAMPAIGN_SEED`, `LOADGEN_METRICS_URL`) are refused |
| Credentials | boto3's default chain only (Pod Identity). No access keys. The role can read and write `campaign/<id>/*` and publish to the topic; it cannot delete |
| Existence tests in S3 | use `ListObjectsV2` with the prefix; a `HEAD` of a missing key may return 403 |
| Files | reads `/config/plan.json`, `/config/thresholds.json`, `/config/versions.json` and `/config/images.lock.yaml` (ConfigMap `campaign-config`); refuses to start if the plan differs from the plan it regenerates or from `PLAN_SHA256`, or the thresholds from what it registers. Writes `/manifest/manifest.json` (the sidecar mirrors that one file every 60 s and once more on exit) and its resume state under `/manifest/state/`, which never leaves the volume (C1). Writes every other object of 11.7 and `COMPLETE` itself to S3 |
| Kubernetes access | `kubectl` with the in-cluster service account; the allowed verbs are in `k8s/base/rbac/orchestrator-rbac.yaml`. Anything else is Forbidden, and **a Forbidden or failed fault action must fail the attempt**, never be logged and ignored (R1 F6 lesson) |
| Signals | on SIGTERM: delete any active chaos resource and injector Job, write the manifest, exit within 180 s (`terminationGracePeriodSeconds`) |
| Exit | 0 only when the campaign finished (or a pilot subset of `--fault`/`--block`); non-zero on a halt (protocol 2.6). The Job never restarts it (`backoffLimit: 0`); resume is `deploy_workload.sh --phase run --recreate-job --resume`, which renders `run --resume` into the Job. The watchdog CronJob alerts on a failed Job or a non-zero exit |
| F6 injector | the orchestrator creates a Job from `F6_INJECTOR_IMAGE` with the environment in `NAMES.md` section 4 and reads its `INJECTED ...` line and exit code. No ConfigMap is needed. The image is built from `rebuild/orchestrator/orchestrator/f6_injector.py` (named build context `orchestrator_src`, C6), the one injector implementation |
| Flink | `FLINK_REST_URL` for the REST API. A resubmission must use `/opt/flink/bin/run-job.sh <main-class> <jar> [<checkpoint path>]` inside the JobManager pod (`kubectl exec`), never a bare `flink run`: run-job.sh carries the job settings (parallelism 4, 60 s exactly-once checkpoints, RocksDB incremental, retained on cancel, failure-rate restarts). `/opt/flink/bin/submit-jobs.sh ECOMMERCE\|IOT` submits every missing job the same way. `FLINK_EXPECTED_JOBS` is the list of names that must all be RUNNING |

## 6. Cost and disk

Per cluster hour (on demand, us-east-2, prices from `REBUILD_DECISIONS.md`): nodes 3 x $0.2016 + 3 x $0.4032 = $1.8144; EKS $0.10;
root volumes 600 GB x $0.08 / 730 = $0.0658; PVCs 1,010 GiB (Kafka 3 x 300, Prometheus 100, manifest 10; the 50 Gi export scratch
volume is gone, C11) = $0.1107. Total about **$2.09 per cluster hour**, so $169 for an 81 h campaign and about $1,016 for six. The NAT gateway is shared:
$0.045 per hour plus $0.045 per GB, and no cross-zone Kafka traffic any more. The REBUILD_DECISIONS estimate of about $870 for the six
campaigns does not hold at 81 h; REVIEW3 D already said so. The pilot needs the author's approval before it starts (decision 6 caps it at $60).
Budget data lag 8 to 12 hours and the tag-filtered budget tracks nothing until the payer account activates the tags, so the pilot
cap is a wall-clock rule: both pilots are torn down 13 hours after the first cluster apply, whatever their state (about $55).

Kafka disk: raw-events and enriched-events keep 2 h. At IoT's about 11,000 events/s and an event of a few hundred bytes the live data is
tens of GB per broker, against 300 Gi; the 10 GB per-partition cap with 24 replicas per broker is a ceiling, not an expectation. Event size
has not been measured, so the pilot must show volume use below 70% after 4 h at full IoT load (`kubelet_volume_stats_used_bytes`).

## 7. What was validated locally (13 Oct 2026)

All of it with no AWS call and no cluster call. Run `scripts/validate_local.sh` to repeat it (it works on copies in a temp directory).
Result of the last run is in section 7.1.

| Check | Tool | Result |
|---|---|---|
| `terraform fmt -check` for each root | 1.10.5, 1.16.5 | pass |
| `terraform init -backend=false` and `validate` for each root, lock file unchanged by init | 1.10.5, 1.16.5 | pass |
| FULL offline plan of each root with mock providers (`terraform test`, `tests/tf/*/plan.tftest.hcl`): first plans of both pilots only create; no `Invalid count argument`, no `coalesce` error; a foreign VPC, a foreign subnet, a campaign of the other workload and an empty image digest are refused; the node groups take the pinned AMI release, never the SSM one; the bucket name has no account ID; the Health rule, log group and targets exist | `tests/test_tf_mock_plans.sh`, 1.10.5 and 1.16.5 | pass |
| Destroy order: every resource of the cluster root, the four role policies included, is destroyed after the teardown guard, and every resource of the network root after the network guard | `terraform graph` on the copy, `tests/check_tf_graph.py` | pass |
| Variable validations (0.0.0.0/0 and wide ranges rejected, campaign id shape and length, account id, owner not an e-mail, version variables have no defaults, one slot per campaign) with good and bad values through `terraform plan` on a stub | `tests/test_tf_variable_validation.sh` | pass |
| `helm template` of Strimzi 0.45.2, Flink operator 1.10.0 (and 1.16.1), kube-prometheus-stack 69.8.0, Chaos Mesh 2.8.4 with the rendered values files, then structural checks on the output (no admission webhooks or Jobs in the Prometheus stack, scrape jobs label by pod name, cAdvisor limits not dropped, every image from ECR by digest, containerd socket, no `edit` ClusterRoleBinding) | Helm 4.3.0, `tests/check_helm_output.py` | pass |
| `kubectl kustomize` of both workloads, all placeholders filled, every YAML parses, every image pinned by digest, no deadline and no default `WORKLOAD` on the orchestrator Job, only the node reader is cluster-wide, 300 Gi, retention, S3 checkpoint paths | `tests/check_manifests.py` | pass |
| No account ID, profile name, e-mail address or employer name anywhere in this tree. The word list itself is stored as SHA-256 hashes only | `tests/check_manifests.py` | pass |
| JMX rules against sample MBean strings (names, labels, near misses such as `MeanRate`) | `tests/test_jmx_rules.py` | 31 cases pass |
| `submit-jobs.sh` and `run-job.sh` with a stub `flink` CLI (idempotent, only missing jobs, no default workload, failure and timeout; every submit carries `-p 4` and the checkpoint, state and restart settings; a restore path goes in as `-s`) | `tests/test_submit_jobs.sh` | 31 checks pass |
| The Flink job check against REST answers in the 1.19.3 shapes (parallelism 1, checkpointing off, HashMap backend, at-least-once, missing or extra jobs all fail), and the session config agrees with `run-job.sh` | `tests/test_check_flink_jobs.py` | pass |
| F6 injector (`rebuild/orchestrator/orchestrator/f6_injector.py`, run as the image runs it) with a fake Kafka that acknowledges 0.3 s late: planned count met at 500 per second, `rebuild-attempt` header on every record, acknowledged counts and acknowledgement times, no brace, seeded, BufferError, late start, shortfall, bad configuration | `tests/test_f6_injector.py` | pass |
| Plan facts, `versions.json`, the `campaign-config` ConfigMap byte for byte, and `deploy_workload.sh --dry-run` putting `PLAN_SHA256`, `CAMPAIGN_INDEX` and the plan's load generator seed into the manifests as strings | `tests/test_campaign_inputs.py` | pass |
| Load generator wrapper against the real `generator.py` with a fake Kafka (seed required, all metrics, restart-safe phase) | `tests/test_loadgen_entry.py` | pass |
| `pre_destroy.sh` order and refusals with stubs | `tests/test_pre_destroy.sh` | 16 checks pass |
| boto3 helpers with botocore stubs: `capture_versions`, `eks_token` (token decodes to a presigned STS URL with the cluster header), `eks_kubeconfig`, `teardown_guard` (marker body and status, manual marker on an empty prefix), `sweep` (Project filter, no other team's cluster described, CNI interfaces), `discover` (vCPU use, default quota, EBS key, GuardDuty, gp3, IAM), `eks_api_cidrs`, `network_guard`, `delete_orphan_volumes` | `tests/test_aws_helpers.py` | pass |
| `mfa_session.py` with a stubbed STS client: serial from the config, the source profile signs, other profiles kept, mode 0600, long-term keys never overwritten, bad OTP or duration refused | `tests/test_mfa_session.py` | pass |
| `fetch_campaign.py` on an in-memory S3: a finished campaign verifies; a changed object, an unlisted object and wrong `COMPLETE` fields fail; a pilot without `COMPLETE` passes with a warning | `tests/test_fetch_campaign.py` | pass |
| The watchdog with fake kubectl, S3 and SNS: OOM kill, non-zero exit, node loss, stale manifest, reminders, nothing on success, read errors alerted, no mutating call | `tests/test_watchdog.py` | pass |
| `deploy_workload.sh --dry-run` renders `run --resume`, `--fault`, `--block` into the Job and nothing else changes; `--fault` outside the pilot, F3 and `--block 0` are refused | `tests/test_campaign_inputs.py` | pass |
| The registry host is cut from cAdvisor and kube-state-metrics image labels (relabel rules applied to sample label sets) | `tests/check_helm_output.py` | pass |
| Gradle dependency locks generated and a strict `shadowJar` build of all six jobs with them (Gradle 8.10, JDK 17) | local, scratch copy | six jars built |
| Hashed Python locks installable on linux/aarch64 CPython 3.12 | `pip download --require-hashes --platform ...` | pass for all three |
| Upstream digests of every mirrored image and build base, each with a linux/arm64 entry | `scripts/mirror_images.py --resolve` against the registries | 14 resolved |
| `shellcheck` on all shell scripts, `py_compile` on all Python | | pass |

### 7.1 Last full run

After the review 4 fixes of 13 October 2026, with the command in `rebuild/review4_fixes/C_infra.md`: Terraform 1.10.5 and 1.16.5
(both with the mocked plans and the graph checks), Helm 4.3.0, kubectl 1.37.1 client (kustomize 5.8.1), Python 3.14.8 with boto3
and PyYAML: 520 checks ok, 0 failed, "ALL LOCAL CHECKS PASSED" (and the 31 JMX rule cases). The flink-jobs image was also built
for linux/arm64 with `scripts/build_images.sh --no-push --only flink-jobs`, and the real Flink 1.19.3 CLI in it, against a local
standalone cluster in the container with no network, showed the review 4 L1 defect and its fix (C_infra.md, L1).

Before that, after the integration of 13 October 2026: `scripts/validate_local.sh` with Terraform 1.10.5 and 1.16.5, Helm, kubectl 1.37 client
(kustomize 5.8.1), Python 3.14: 318 checks ok, 0 failed, "ALL LOCAL CHECKS PASSED" (and the 31 JMX rule cases). The checks added at
integration are `tests/test_campaign_inputs.py`, the rewritten `tests/test_f6_injector.py`, the `/status` checks of
`tests/test_loadgen_entry.py`, and the 11.11 interface, string env value and probe-service checks of `tests/check_manifests.py`.
Earlier run, before integration: Terraform 1.10.5, 1.16.5 and 1.5.7, Helm 4.3.0, kubectl 1.35 client, Python 3.14 with the pinned
`scripts/requirements.txt` (boto3 1.40.0): 304 checks ok, 0 failed, "ALL LOCAL CHECKS PASSED". The Terraform binaries 1.10.5 and 1.16.5 were
downloaded from releases.hashicorp.com and their SHA-256 sums checked against the published SHA256SUMS file (the GPG signature was not
checked). The machine's own Terraform is 1.5.7; it cannot satisfy the committed `required_version`, which is why the script patches a copy.

### 7.2 Not run, and why

| Not run | Why | Consequence |
|---|---|---|
| `terraform plan` against AWS and `apply` of any root | needs AWS | the plans ran offline with mock providers (section 7). IAM policy contents, the real add-on configuration schemas, provider-side validation and the destroy-time provisioners have not met AWS |
| `capture_versions.py` against AWS | needs AWS | `terraform/cluster/addon-versions.auto.tfvars.example` holds placeholders. `--check-config` will check the add-on configuration keys |
| Docker builds of the four images | see `rebuild/review4_fixes/C_infra.md` for the `build_images.sh --no-push` run of this round | |
| `mirror_images.py --copy` and `--verify` | needs AWS and `crane` (not installed) | the copy path is untested; `--resolve` ran live |
| `helm install`, `kubectl apply`, any Kubernetes server-side validation | needs a cluster | CRD schemas of Kafka, FlinkDeployment and the Chaos Mesh kinds were not checked by an API server |
| Orchestrator and f6-injector images | the Docker daemon is not running here | both copy from `rebuild/orchestrator` (named build context `orchestrator_src`); the injector file's `--self-test`, which the f6-injector build runs, passes locally, and `tests/test_f6_injector.py` runs the file exactly as the image's ENTRYPOINT does |

## 8. What the pilot must show

**A. Infrastructure.** Nodes Ready in one zone, six node roles and taints as in NAMES.md. Every node on the pinned AMI release. The add-on `configuration_values` were accepted. PVs bound and every volume carries `Campaign` and `Scope=pv`. `kubectl auth can-i` matches the Roles. The Pod Identity role resolves in a pod; a put outside the prefix and any delete are denied (bucket policy and role). **Flink writes and restores a checkpoint on S3 through Pod Identity** (the Hadoop S3A path with the named credentials provider, not yet exercised), and a TaskManager kill during a stateful job restores from S3. All Prometheus targets are up. `k8s/checks/exporter-identity-check.yaml` gets the exporter role without a service account token. `kubectl auth can-i get pods/exec` is yes for both service accounts.

**B. Pipeline.** Kafka Ready; `df` on a broker shows `/var/lib/kafka/data`; three jobs RUNNING with the expected names (the `flink list` output format in `submit-jobs.sh` is assumed from the Flink CLI and must be confirmed), each at parallelism 4 with 60 s exactly-once checkpoints on RocksDB (`scripts/check_flink_jobs.py`), three TaskManagers before any job runs (`slotmanager.number-of-slots.min`); the TaskManagers are QoS Guaranteed; the load generator within 2% of target for 10 minutes at the base rate and in the IoT burst (the generator is a single Python loop; a 24,000 events/s peak is the risky point); the new JMX series exist (`kafka_network_requestmetrics_localtimems_mean`, `..._99thpercentile`, `kafka_log_logflushstats_*`, `kafka_controller_kafkacontroller_activecontrollercount` and `fencedbrokercount`, `kafka_server_raftmetrics_*`; the raft attribute names are unconfirmed); `container_oom_events_total`, `container_memory_working_set_bytes`, `container_spec_memory_limit_bytes` and `kube_pod_container_resource_limits` exist for TaskManagers; series are labelled by pod name; Kafka volume use below 70% after 4 h at full IoT load.

**C. Faults** (REVIEW1 section G, REVIEW2 pilot list): Chaos Mesh StressChaos, NetworkChaos and PodChaos canaries on arm64 with containerd 2.x and cgroup v2; chaos resources report `Injected` then `AllRecovered`; stressors are charged to `flink-main-container`; F1 target and the controller identity at onset; F2 fencing against ISR shrinks; F5 with S3 checkpoints, every job RUNNING after each kill; F6 delivered against planned and the onset delay; F7 probe loss; F8 stressor size from the real working set; shams fail every signature; the recovery gate passes without remediation.

**D. Teardown.** Destroy without `COMPLETE` refuses and leaves the state unchanged, the role policies included; with a marker it proceeds; `sweep.py` finds nothing; `delete_orphan_volumes.py` finds nothing; no PV volume remains. The watchdog's first runs succeed (`kubectl -n streaming get jobs -l app=campaign-watchdog`).

## 9. Deviations from the reviews, and why

| # | Deviation | Reason |
|---|---|---|
| 1 | ECR has 4 built repositories plus 11 mirror repositories created from `images.lock.yaml`, not "five" | every public image is mirrored (A9), so the count follows the lock file |
| 2 | The Flink role gets an **unconditioned `s3:ListBucket`** (object access stays on `scratch/<id>/*`) | Hadoop S3A probes parent paths with HEAD and LIST; with a prefix condition a HEAD of a missing key returns 403 and a job could fail at start. Key names only are exposed. The pilot checks it |
| 3 | Jobs are submitted by a script and a Job, not `FlinkSessionJob` | a session job needs a jar the operator can fetch, and an operator-managed job would be restarted behind the orchestrator, which is the watchdog problem |
| 4 | Grafana and Alertmanager off, default rules off, four control-plane scrape targets off | nobody watches six clusters, it removes a password and images, and "all targets up" can only hold if EKS-unreachable targets are not scraped. Monitoring-node load differs slightly from R1 |
| 5 | Chaos Mesh dashboard and DNS server not installed | unused; the dashboard had `securityMode: false` in R1 |
| 6 | The cluster root reads the network and shared outputs through `terraform_remote_state`, and the campaign-to-subnet map lives in the network root | the draft copied ids by hand; the map makes a shared subnet impossible |
| 7 | One `ClusterRole` that only reads nodes | protocol 1.6 needs the EC2 instance IDs. The review asked for Roles only |
| 8 | Orchestrator Role also has `events` (read), `persistentvolumeclaims` (read), `deployments` (get, list) | verification records need events (protocol 2.2 item 3), disk checks need PVCs, readiness needs deployments |
| 9 | Helpers use boto3 and a Python token script; the Terraform helm and kubectl providers call `scripts/eks_token.py`, not `aws eks get-token` | the AWS CLI on the author's machine does not run |
| 10 | `pre_destroy.sh` counts only `Scope=pv` volumes, and checks that the marker names this campaign | node root volumes carry the Campaign tag and exist until the nodes go |
| 11 | E-commerce load profile `LOW` (1,000 per second) | R1's YAML said `ULTRA_LOW` (500), the measured rate was about 992. **Author to confirm** |
| 12 | Strimzi `watchNamespaces` removed | the chart adds the release namespace itself; R1's list made it `streaming,streaming` |
| 13 | Added: verification consumer Deployment, `loadgen-epoch` ConfigMap, optional budget and cost allocation tags | protocol 2.3; restart-safe load phase; REVIEW3 D |
| 14 | Four transitive providers pinned exactly | "exact provider pins" |
| 15 | Checkpoints are retained on cancel; Flink HA is not enabled | a scripted remediation restores from the last checkpoint; R1 had no HA, and a JobManager loss is an envelope failure |
| 16 | Restart strategy `failure-rate` (40 failures in 10 min, 30 s delay), not R1's `fixed-delay` with 10 attempts and 30 s | fixed-delay is a budget for the whole life of a job. The rebuilt F5 kills five TaskManagers per attempt in ten blocks, so a 10-attempt budget runs out early in a campaign and every later job failure is permanent. **Author to confirm**; protocol 1.7 item 3 lists only two changes from R1 |
| 17 | Job settings are passed with `-p` and `-D` by `run-job.sh` on every submit, not read from a config file | the Flink CLI builds the JobGraph from its own process's config; the flink-submit pod read the distribution default (parallelism 1, no checkpoints). R1 submitted inside the JobManager, so its client read the session config; the values are R1's (review 4 L1) |
| 18 | One alert-only CronJob (`campaign-watchdog`) | protocol 6.5 item 4 needs a watchdog. It only publishes to SNS; it never heals, which was REVIEW1 A1's objection to R1's CronJob |
| 19 | `slotmanager.number-of-slots.min: 12` | keeps three TaskManagers without jobs, so a full redeploy can finish (review 4 L4). The steady state with three jobs at parallelism 4 is unchanged |
| 20 | Results bucket `<project>-results-<bucket_suffix>`, cost allocation tags not activated here | decision 29 (no account ID in data); only the payer account can activate tags (review 4 M7) |

## 10. Fallbacks

* **Flink operator 1.16.1.** In `terraform/cluster/variables.tf` set `chart_versions.flink_operator = "1.16.1"`, in `images/images.lock.yaml` set the `flink-operator` tag to `1c895a3` (that chart's image), run `scripts/mirror_images.py --resolve --accept-moved` and `--copy`, apply `shared` if the repository name changed. The values file renders unchanged with the 1.16.1 chart.
* **A tag that moved upstream.** `--resolve` stops; read the upstream change before `--accept-moved`.
* **Docker Hub, quay.io, ghcr.io and registry.k8s.io are not used by the clusters.** Only Helm chart downloads (from the machine that runs Terraform) and the build machine use them.

## 11. Open items for the author

1. Confirm the e-commerce profile `LOW` (deviation 11).
1. Confirm the restart strategy (deviation 16) and the protocol wording for it.
1. Ask the payer administrator to activate the `Project`, `Campaign` and `Workload` cost allocation tags.
2. Approve the pilot cost before any `apply` (decision 6).
3. Build the four images (`scripts/build_images.sh`; the orchestrator and f6-injector images both come from `rebuild/orchestrator`).
4. Decide the F1 variant and F5, F8 variants in the protocol; they do not change this infrastructure.
5. Run steps 5 to 8 of the runbook (they need AWS), then the pilot.
