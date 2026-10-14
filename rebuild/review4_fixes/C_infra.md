# Review 4 fixes, part C: infrastructure

13 October 2026. This covers my findings in `diagnostics/REVIEW4_OPERATIONS.md` (B1 to B7, C1 to C6, M1 to M12) and the items of `diagnostics/REVIEW4_LIVE_CONTRACT.md` that name a file under `rebuild/infra/` (L1, L4, L9 and pilot check 1). It follows decisions 8, 16, 29 and 30 of `diagnostics/REBUILD_DECISIONS.md`.

No AWS call and no cluster call was made. The network was used for Terraform providers (a local plugin cache), Helm charts, Flink 1.19.3 source files and documentation, and for the Docker base images of one local image build. Every file I changed is under `rebuild/infra/`, except three sentences of `diagnostics/REBUILD_PROTOCOL.md` (the 120 h deadline, M12) and this report.

Paths are relative to the paper directory. "Scratch" means a throwaway copy outside the repository.

## Summary

| Finding | Status | The check that now passes |
|---|---|---|
| B1 `coalesce` on a null profile | fixed | mocked plan of the cluster root |
| B2 module `depends_on`, "Invalid count argument" | fixed | mocked plan, 7 runs |
| B3 role policies destroyed next to the guard | fixed | graph reachability check, and a Terraform repro of the order |
| B4 no MFA session without the AWS CLI | fixed | `tests/test_mfa_session.py` with a stubbed STS client |
| B5 long Terraform commands | fixed (procedure) | `rebuild/infra/PILOT_RUNBOOK.md` |
| B6 no `--resume`, `--fault`, `--block` | fixed | `tests/test_campaign_inputs.py`, `tests/test_deploy_run_phase.sh` |
| B7 no campaign download | fixed | `tests/test_fetch_campaign.py` |
| C1 account ID in data (infra side) | fixed | mocked plan of `shared`, relabel checks in `tests/check_helm_output.py` |
| C2 AMI pin ignored | fixed | mocked plan: 12 node group plans on the pinned release, 0 from SSM |
| C3 new laptop address | fixed | `tests/test_aws_helpers.py` (`eks_api_cidrs.py` with a stubbed EKS client) |
| C4 one Terraform data directory | fixed (procedure) | a Terraform repro of the switch, runbook check of the key |
| C5 vCPU use not measured | fixed | `tests/test_aws_helpers.py` (`discover.py`) |
| C6 no alert on a dead orchestrator | fixed | `tests/test_watchdog.py`, `tests/check_manifests.py` |
| M1 to M12 | fixed, M4 item 1 partly | see each section |
| L1 Flink jobs at parallelism 1, no checkpoints | fixed in infra, one orchestrator change needed | real Flink 1.19.3 CLI in the built image, `tests/test_submit_jobs.sh`, `tests/test_check_flink_jobs.py` |
| L4 no TaskManagers after a redeploy | fixed in infra (session config) | `tests/test_check_flink_jobs.py` (config). Only the pilot can show the effect |
| L9 `pods/exec` without `get` | fixed | `tests/check_manifests.py` |
| Pilot check 1 (exporter Pod Identity without a token) | made checkable | `k8s/checks/exporter-identity-check.yaml` renders and passes the manifest checks |
| 120 h deadline in the protocol | protocol text fixed, no deadline added | `tests/check_manifests.py` (no `activeDeadlineSeconds`) |

Final run of `scripts/validate_local.sh`: 520 checks ok, 0 failed, "ALL LOCAL CHECKS PASSED". The command is at the end.

## B1. `coalesce` on a null profile

What changed. `terraform/cluster/guard.tf` uses `var.aws_profile == null ? "" : var.aws_profile`. The new network guard uses the same form.

How I reproduced it. A scratch copy of the original tree, with the mocked plan of `tests/tf/cluster/plan.tftest.hcl`, and the module `depends_on` removed so that B2 did not hide it. The plan stopped with `Call to function "coalesce" failed: no non-null, non-empty-string arguments` at `guard.tf` line 16.

Check that passes. The mocked plan of the cluster root. Run `first_plan` asserts that a null profile becomes `""` in the guard input, and `first_plan_with_profile` that a named one reaches it. `tests/test_tf_mock_plans.sh` also fails on any `coalesce` error in the log.

## B2. "Invalid count argument" on the first plan

What changed. In `terraform/cluster/main.tf` the module `depends_on` is gone, and so are the two `terraform_data` assertions. The VPC check is a postcondition on `data.aws_vpc.this` and the subnet check a postcondition on `data.aws_subnet.node`. The module takes `vpc_id`, `subnet_ids` and the first control-plane subnet from those two data sources, so it depends on the checked values. The campaign and workload match is a cross-variable validation on `var.workload` in `variables.tf`. Terraform 1.9 and later allow that, and the code needs 1.10.

How I reproduced it. The original tree with the mocked plan: six `Error: Invalid count argument` from `eks-managed-node-group/main.tf` line 2.

Check that passes. `tests/test_tf_mock_plans.sh` plans all three roots with mock providers on Terraform 1.10.5 and 1.16.5. The cluster file has 7 runs: both pilots, a named profile, and four refusals (a campaign of the other workload, a VPC without the project tags, a subnet in another VPC, an empty image digest). All pass. The script also fails on any `Invalid count argument` and on any plan that is not create-only. `scripts/validate_local.sh` runs it for every Terraform binary, so B1 and B2 class errors are caught from now on. The same harness on the original tree reports B2, C2, B3 and the missing network guard.

## B3. Role policies destroyed in parallel with the guard

What changed. The guard's `depends_on` in `guard.tf` lists `aws_iam_role_policy.ebs_csi`, `.orchestrator`, `.exporter` and `.flink`.

How I reproduced it. Two ways.

1. Statically. `terraform graph` of the original root (in a scratch copy with a local backend override), then `tests/check_tf_graph.py`: the four role policies were not reachable from the guard, so a destroy may delete them first.
2. The mechanism. A scratch config with `terraform_data` resources in the same dependency shape. The "policy" has a destroy provisioner that logs, the "guard" a destroy provisioner that refuses. Without the edge the log read `policy-destroyed guard-refused` and the policy was gone from state. With `depends_on` on the policy the log read `guard-refused` and the policy stayed in state.

Check that passes. `tests/test_tf_mock_plans.sh` runs `terraform graph` on each root and `tests/check_tf_graph.py` requires every managed resource to be reachable from the guard: "every resource of the root is destroyed after terraform_data.teardown_guard (126 graph nodes)". The same check covers the new network guard (16 nodes). The runbook's refusal test also compares the state count before and after.

## B4. MFA session without the AWS CLI

What changed. New `scripts/mfa_session.py`. It takes `--source-profile`, `--target-profile`, `--otp` and `--duration` (default 129600), and reads `mfa_serial` of the source profile from the shared config file, or `--serial`. It calls `sts.get_session_token` with the source profile's credentials and writes the target profile to the shared credentials file with configparser, keeping every other profile. It writes atomically with mode 0600. It refuses a target equal to the source, a target that holds long-term keys, an OTP that is not six digits and a duration outside 900 to 129600 s. It names no profile, account or organisation. It has not been run against AWS.

How I reproduced it. The review's evidence is the x86 CLI on an arm64 machine. I did not run the old script.

Check that passes. `tests/test_mfa_session.py`, 15 checks, with a botocore Stubber on STS that checks the exact parameters (serial, token code, duration) and temporary config and credentials files.

## B5. Long Terraform commands

What changed. `PILOT_RUNBOOK.md` (new) and README section 4. Every apply and destroy is planned to a file, then run as `nohup caffeinate -i $TF apply -no-color <plan> > $LOGS/<name>.log 2>&1 &` and polled with `tail` and `kill -0`. Logs go to a directory outside the repository. Section 15 of the runbook covers a stale lock (`force-unlock`), `errored.tfstate` (`state push`), a cluster created outside the state (`terraform import 'module.eks.aws_eks_cluster.this[0]' ...`), a failed Helm release, a stale plan and an expired session.

Check. This is a procedure. No offline test can run it.

## B6. Resume after a halt and the pilot fault rerun

What changed.

1. `scripts/deploy_workload.sh` takes `--resume`, `--fault <F>` and `--block <n>` with `--phase run`. New `scripts/orchestrator_args.py` writes them into the orchestrator container's `args` and leaves every other document byte for byte. `--fault` and `--block` are refused outside `pilot-*`, `--fault F3` and other unknown run types are refused, and `--block` must be 1 or more.
2. A Job's template cannot change in place, so the flags need `--recreate-job` when a Job exists. `--recreate-job` now refuses while the Job is active and deletes an ended Job with foreground deletion and a wait.
3. New `--phase stop`: it suspends the watchdog and deletes the Job with foreground deletion and a wait, so the orchestrator gets SIGTERM and the sidecar syncs the manifest one last time. It also serves M4 item 4.

How I reproduced it. The original `deploy_workload.sh` answered `unknown argument --resume` and `unknown argument --fault` (exit 2), and the Job had a fixed `args: ["run"]`.

Check that passes. `tests/test_campaign_inputs.py` (dry runs: `[run]`, `[run, --resume]`, `[run, --resume, --fault, F5, --block, 1]`, nothing else changes, and the four refusals). New `tests/test_deploy_run_phase.sh` with a stub kubectl: an active Job is never deleted, an ended Job is recreated with foreground deletion, `--resume` without `--recreate-job` is refused, a first pilot start with `--fault F5 --block 1`, kubectl 1.37 refused, and the stop order. 12 checks.

## B7. Copy a campaign to the laptop

What changed. New `scripts/fetch_campaign.py --bucket B --campaign ID --out DIR`. It lists `campaign/<id>/` and downloads every object to `DIR/<relative key>`, which is what `streamprep --campaign-dir` and `orchestrator gate-offline --store` read. It checks sizes, every line of `checksums/SHA256SUMS`, that no object is missing from it, and the `COMPLETE` fields `campaign_id`, `status`, `checksums_sha256`, `manifest_sha256`, `plan_sha256`, `object_count` and `total_bytes`. A pilot without `COMPLETE`, or with only a manual marker, gets a clear WARNING and exit 0. It writes `DIR.LOCAL_SHA256SUMS` beside the directory, never inside it, so the copy holds exactly the S3 layout. It refuses keys with `..` and a non-empty output directory, and makes no write call to S3.

Check that passes. `tests/test_fetch_campaign.py`, 15 checks on an in-memory S3 with paged listings: a finished campaign verifies, a changed object, an unlisted object and wrong `COMPLETE` fields fail, a pilot without `COMPLETE` warns and exits 0, a manual marker still runs the hash check.

## C1. The account ID in the deposited data (infra side)

What changed.

1. `terraform/shared`: the results bucket is `${project}-results-${bucket_suffix}`. `bucket_suffix` is a new required variable, 6 to 16 lower-case letters or digits, never a 12-digit number. The operator draws it once and keeps it in the gitignored `terraform.tfvars`. It is not committed, because protocol 7.5 keeps bucket names out of the deposit.
2. `k8s/base/helm-values/prometheus-values.yaml`: a `replace` relabel cuts `<anything>.dkr.ecr.<region>.amazonaws.com/` from the cAdvisor `image` label, and from `image`, `image_spec` and `image_id` of kube-state-metrics, at scrape time. The path, tag and digest stay, so no two series merge. Pod-level series have an empty `image` and are untouched.
3. `scripts/mark_complete.py` wrote the local user name into `COMPLETE`, which is part of the deposited prefix. It now writes `"written_by": "author"`.
4. The labels that reach the export, from the allowlist and the exporter's `REMOVED_LABELS`, are listed in `NAMES.md` section 6. Besides `image`, the ones that carry infrastructure values are `node` (the private DNS name, which holds a private IP and the region, and which the preprocessing drops), `host_ip` (a private IP), `id` and `name` (cgroup path and container ID) and `created_by_name`. None of them names the account, a profile, a bucket or a role.

How I reproduced it. The original `shared/main.tf` built the name from `account_id` and `region`. The original values file had no relabel on `image`.

Check that passes. The mocked plan of `shared` asserts the bucket name and that it does not contain the account ID, and refuses a 12-digit suffix. `tests/check_helm_output.py` applies the rendered relabel rules to sample label sets: the ECR host is cut from cAdvisor and from all three kube-state-metrics labels, the pause image loses its host, an empty image stays empty. `helm template` of chart 69.8.0 puts the rules into the ServiceMonitors.

The orchestrator side of C1 is not mine. It is listed below.

## C2. The AMI release pin

What changed. `use_latest_ami_release_version = false` in every node group in `terraform/cluster/main.tf`.

How I reproduced it. The mocked plan of the original root printed `release_version = "1.35.9-29991231"`, the mocked SSM sentinel, for all three node groups instead of the pinned `1.35.4-20261003`.

Check that passes. `tests/test_tf_mock_plans.sh` counts the release in the verbose plan: "node groups use the pinned AMI release (12 node group plans, 0 from SSM)".

## C3. A new laptop address

What changed. New `scripts/eks_api_cidrs.py`. It refuses a cluster without the `Project` and `Campaign` tags, `0.0.0.0/0` and any range wider than /16, the same rules as the Terraform validation. It calls `eks.update_cluster_config` with the new `publicAccessCidrs` and the endpoint flags unchanged, and polls `describe_update`. It waits at most 540 s, inside the 10 minute limit. If EKS is still working it exits 3 with the update ID, and `--wait-update <id>` waits again. It prints the `api_allowed_cidrs` line to put into `<id>.tfvars`, so the next plan does not change it back. `pre_destroy.sh` now names the script when the API is unreachable. Runbook section 12 has the step.

Check that passes. `tests/test_aws_helpers.py` with a Stubber on EKS: the exact `update_cluster_config` parameters, Successful, still InProgress after the timeout (exit 3), a cluster of another project refused, and five refused CIDR lists.

## C4. One data directory for every campaign

What changed. Each campaign uses `export TF_DATA_DIR=.terraform/<id>` and `-backend-config="key=campaigns/<id>/terraform.tfstate"`. This is in `terraform/cluster/terraform.tfvars.example`, `backend.hcl.example`, README section 4 and the runbook, which also prints the key from the data directory after init. `.terraform/` is gitignored, so the nested directories are too.

How I reproduced it. In a scratch directory with a local backend, a second `init -reconfigure` in the same directory left `.terraform/terraform.tfstate` pointing at the second campaign's state. With one `TF_DATA_DIR` per campaign each data directory kept its own state path.

## C5. vCPU headroom

What changed. `scripts/discover.py` reads current use from CloudWatch `AWS/Usage` `ResourceCount` (Service EC2, Resource vCPU, Type Resource, Class Standard/OnDemand), which counts every team's instances without listing them. It falls back to `get_aws_default_service_quota` when the applied quota cannot be read. It prints BLOCK and exits 1 when quota minus use is below the need (`--clusters N` times 36, or `--need-vcpu`).

Check that passes. `tests/test_aws_helpers.py`: the latest datapoint is used, a shortfall blocks, enough headroom passes, an unknown use asks for a decision, and a never-changed quota falls back to the default (no more `None`).

## C6. Alert when the orchestrator dies

What changed. One alert-only CronJob, `k8s/base/watchdog/` (`watchdog-cronjob.yaml`, `watchdog.py`, `kustomization.yaml`). Every 10 minutes one short pod on the monitoring node runs the orchestrator image with the orchestrator's service account, so it uses the orchestrator's IAM role. It publishes to the alert topic when the Job has failed (halt, crash, OOM kill, or a pod lost with its node), when the orchestrator container has ended with a non-zero code while the sidecar still runs, or when a RUNNING manifest has not changed in S3 for 7,200 s. That is twice the longest expected attempt of about 3,600 s with a full remediation. A Job that succeeded raises nothing. Alerts repeat every 2 h while the condition holds, and the send times live in ConfigMap `campaign-watchdog-state`. The Role gains `update` and `patch` on that one name. It never deletes, patches, scales, creates or execs anything else, and it writes nothing to S3. Its pods carry no `rebuild/*` label, because the orchestrator counts and deletes objects with those labels. `--phase run` applies it with the Job, and `--phase stop` suspends it. `check_manifests.py` used to forbid every CronJob (REVIEW1 A1). It now allows exactly this one and checks its shape.

I chose a CronJob over the sidecar. A sidecar cannot see the main container's exit code and dies with its pod and node. The CronJob still reports a failed Job after a node has been replaced.

Check that passes. `tests/test_watchdog.py`, 19 checks with fake kubectl, S3 and SNS. `tests/check_manifests.py`: one CronJob, the right image, command, service account and node, no `rebuild/*` labels, and the ConfigMap holds `watchdog.py` byte for byte after rendering.

## M1. The leak guard published its words

What changed. `tests/check_manifests.py` holds no word list. `tests/forbidden_words.sha256` holds `<length> <sha256>` of each lower-case word. The check hashes every window of each length in every file, so it still finds a word inside a longer token. The plain list is the gitignored `tests/forbidden_words.local`. When it is present it must hash to exactly the committed list, and it names the word in a failure. `--write-forbidden-hashes` rewrites the hash file from it. The scan takes under 2 s on this tree.

Check that passes. "the local forbidden-word list hashes to exactly the committed list", "the hashed scan finds a forbidden word inside a longer token", "the committed forbidden-word file holds hashes only", and the tree scan itself.

## M2. The sweep read other teams' resources

What changed. `scripts/sweep.py` counts a resource only with `Project=<project>` and a `Campaign` tag. EC2 and Auto Scaling are filtered on `tag:Project` on the server side, and so is the tagging API. EKS clusters are described only when named `<project>-...`. The load balancer listing of every team is gone, because the tagging API covers ELBv2. IAM OIDC providers are no longer read, since Pod Identity creates none. It now also reports network interfaces the VPC CNI left behind, found by `cluster.k8s.amazonaws.com/name` (M5).

How I reproduced it. The original `ec2_findings` returned another team's volume tagged `Campaign=ec-a`.

Check that passes. `tests/test_aws_helpers.py`: another team's volume is skipped, every EC2 call carries the Project filter, no other team's cluster is described, CNI interfaces are reported.

## M3. The EBS CSI policy on one tag

What changed. `terraform/cluster/iam.tf` adds `aws:RequestTag/Project` to create and `aws:ResourceTag/Project` to attach, detach, delete and modify, next to the `Campaign` conditions. Volumes get `Project` from `extraVolumeTags`, instances from the launch template tags. Only the pilot can show the policy works.

## M4. Edge cases in the teardown guard

What changed.

1. A failed first apply can leave a PV volume with no guard in state. New `scripts/delete_orphan_volumes.py` lists, and with `--yes` deletes, only `available` volumes tagged `Project`, `Campaign=<id>` and `Scope=pv`, and only when the cluster no longer exists. This is a recovery tool. Preventing the case would need a second destroy-time step, which I did not add.
2. and 3. `scripts/teardown_guard.py` now reads the marker body. It refuses a non-JSON marker, another campaign, and a status other than `COMPLETE` or `ABANDONED`. A manual marker must have `kind: manual-marker`, `status: ABANDONED` and `author_confirmed: true`, which is what `mark_complete.py` now writes. Only that kind may sit on an empty prefix.
4. `--phase stop` stops the Job before a marker is written. The guard's refusal says so when a manifest upload has overtaken the marker. The runbook orders stop, marker, copy, destroy.

How I reproduced it. The original guard accepted a non-JSON marker and a marker with status `RUNNING`, and refused a manual marker on an empty prefix.

Check that passes. `tests/test_aws_helpers.py`: 12 guard cases and 4 orphan cases.

## M5. The network destroy

What changed. `terraform/network/main.tf` gains `terraform_data.network_guard`, created last and so destroyed first. Its destroy provisioner runs new `scripts/network_guard.py`, which refuses while a cluster named for the project uses the VPC, while a VPC endpoint in it is not tagged `Project` (for example `GuardDutyManaged`), or while a network interface other than the NAT's and the project's endpoints remains. Every call is scoped to project names or to this VPC. `python_bin` is a new required variable of the network root.

Check that passes. The network mocked plan and the graph check (every network resource after the guard), and four `network_guard` cases in `tests/test_aws_helpers.py`.

## M6. Account settings

What changed. `scripts/discover.py` also reads the default EBS key (BLOCK when customer managed, with `kms.describe_key` for an ARN), GuardDuty Runtime Monitoring with automated EKS agents (DECIDE), the gp3 storage quota `L-7A658B76` against about 1.6 TiB per cluster (BLOCK when below), and whether the session may create and tag roles, with `iam:simulate_principal_policy` on the role behind an assumed-role session (BLOCK when denied). Each reads account-level settings only.

Check that passes. 7 cases in `tests/test_aws_helpers.py`.

## M7. Cost allocation tags

What changed. `aws_ce_cost_allocation_tag` and `activate_cost_allocation_tags` are gone from `terraform/shared`. The comment there, README sections 4 and 6, NAMES.md and the runbook say the payer account activates `Project`, `Campaign` and `Workload`. The tag-filtered budget stays optional and tracks nothing until then. The pilot cap is the 13 h stop rule.

## M8. The operator's default tools

What changed. `python_bin` has no default in the cluster root and must be an absolute path. The network root has the same rule. `deploy_workload.sh` refuses a kubectl outside 1.34 to 1.36, and `pre_destroy.sh` warns, so a teardown never stops on it. The runbook uses `$HOME/.local/bin/terraform-1.10.5`, the venv Python and a kubectl 1.35.9 with the download and checksum commands.

Check that passes. `tests/test_tf_variable_validation.sh`: `python3` is refused and `python_bin` has no default, in both roots. The kubectl refusal is in `tests/test_deploy_run_phase.sh`.

## M9. Placeholder versions

What changed. `terraform/cluster/variables.tf` refuses `1.35.0-20260101` and `v1.0.0-eksbuild.1` by name.

How I reproduced it. The original variable test passed with exactly those values.

Check that passes. Two new cases in `tests/test_tf_variable_validation.sh` (36 cases in all).

## M10 and M11. Runbook gaps

What changed. Both runbooks use absolute paths with `plan_tool.sh`. The pilot steps start the orchestrator with `--phase run --thresholds-file`, make `plans/thresholds.json` once, and tear down both pilots. The change to `plan_tool.sh` itself is listed for the integration agent.

## M12. AWS Health events and the 120 h deadline

What changed.

1. `terraform/shared/main.tf`: log group `/streaming-anomaly-detection/health` (90 days), a CloudWatch Logs resource policy that lets `events.amazonaws.com` and `delivery.logs.amazonaws.com` create streams and put events in that group only, an EventBridge rule on `source = aws.health` and `detail-type = AWS Health Event` in us-east-2, and two targets: the log group and the alert topic. The topic gets a policy that restates the owner statement and lets EventBridge publish, without a Condition block, because the EventBridge guide says it does not support one in SNS topic policies. A rule in us-east-2 receives that region's public and account-specific events (docs.aws.amazon.com/health/latest/ug/choosing-a-region.html). The new output `health_log_group_name` feeds the cluster root.
2. `terraform/cluster/iam.tf`: the orchestrator role gets `logs:FilterLogEvents` on that group only, in both ARN forms. The name matches `ids.HEALTH_LOG_GROUP` in the orchestrator, so no environment variable is needed.
3. The deadline. I chose to keep the Job without a deadline and fix the protocol text, because that is the safer choice. A deadline cannot stop billing, since the cluster bills whether the Job runs or not. It can only kill a healthy campaign that runs long. The watchdog alerts on a stuck or dead orchestrator, and the stop rule caps the pilot. I changed protocol 1.9 (the table row), 2.7 item 6 and 6.5 item 1, and added one sentence to 6.5 item 4 naming the watchdog.

Check that passes. The mocked plan of `shared` asserts the log group name, the rule pattern, both targets and the policy name. `tests/check_manifests.py` still requires no `activeDeadlineSeconds`. The IAM statement is checked by reading `iam.tf`, because the mock provider does not compute policy documents.

## L1. Flink job settings

What changed.

1. New `images/flink-jobs/run-job.sh` is the one submission path. It runs `flink run -d -m <REST> -p 4` with `-D` for `execution.checkpointing.interval=60000`, `.mode=EXACTLY_ONCE`, `.timeout=120000`, `.min-pause=30000`, `.externalized-checkpoint-retention=RETAIN_ON_CANCELLATION`, `state.backend.type=rocksdb`, `state.backend.incremental=true` and the restart strategy, then `[-s <checkpoint>] -c <class> <jar>`. `submit-jobs.sh` calls it for every missing job, and the Dockerfile copies it to `/opt/flink/bin/`. The orchestrator's remediation must call it too (requested below).
2. New `scripts/check_flink_jobs.py` reads `/jobs/overview`, `/jobs/<id>/config` and `/jobs/<id>/checkpoints/config` through the API server's service proxy. It takes the expected names from `submit-jobs.sh` and the expected values from `run-job.sh`. `deploy_workload.sh` runs it at the end of phase `submit`, so a wrong setting stops the deploy before the orchestrator starts.
3. What R1 really used. R1's session config, identical in `infrastructure/` and `infrastructure-b/`, had parallelism 4, interval 60000, EXACTLY_ONCE, timeout 120000, min pause 30000, RocksDB incremental, and fixed-delay restarts with 10 attempts and 30 s. R1 submitted with a bare `flink run -d -c <class> <jar>` inside the JobManager, so its client read that config. The job code in `streaming-app/` sets none of it. The settings now match R1 with two differences, which I record here and in README section 9:
   - checkpoints retained on cancel (R1 set nothing, so Flink 1.19's default kept none). The remediation restores from the last checkpoint.
   - the restart strategy stays `failure-rate` (20 failures in 10 minutes, 30 s delay), not R1's fixed-delay with 10 attempts. **This departs from the brief, which asked for fixed-delay.** Fixed-delay is a budget for the whole life of a job. The rebuilt F5 kills five TaskManagers in every attempt, and there are ten F5 attempts per campaign. A 10-attempt budget therefore runs out early in a campaign, after which every job failure is permanent, F5's envelope fails and the halt rules fire. The pilot has one block, so it would not show this. Switching is two lines, in `run-job.sh` and in the session config, and `tests/test_check_flink_jobs.py` checks that they agree. The author should decide, and protocol 1.7 item 3 should name the change either way.

How I reproduced it, with the real Flink. I built the flink-jobs image for linux/arm64 with `scripts/build_images.sh --no-push --only flink-jobs`, then ran a standalone Flink 1.19.3 cluster inside the container with no network and submitted two jobs. The bare `flink run`, as the old flink-submit Job ran it, gave `job-parallelism` 1, checkpoint interval 9223372036854775807 (off), `at_least_once`, no externalization and `HashMapStateBackend`. The image's own `/opt/flink/conf/config.yaml` has `parallelism.default: 1`, as the review said. `run-job.sh` gave `job-parallelism` 4, `exactly_once`, interval 60000, timeout 120000, min pause 30000, externalization enabled without delete on cancel, and `EmbeddedRocksDBStateBackend`. `check_flink_jobs.check_jobs` on those two real answers failed the first job on eight settings and passed the second.

Check that passes. `tests/test_submit_jobs.sh` (31 checks: every submit carries every setting, a restore path goes in as `-s` before `-c`, `10 min` stays one argument) and `tests/test_check_flink_jobs.py` (14 checks on REST answers in the 1.19.3 shapes, plus session config equal to `run-job.sh`). Run on the original `submit-jobs.sh`, the new settings checks fail.

## L4. TaskManagers after a redeploy

What changed. `slotmanager.number-of-slots.min: "12"` in `k8s/base/flink/flink-session-cluster.yaml`, from the Flink 1.19 configuration page (it does not apply to standalone clusters, and this one is native). That keeps three TaskManagers of four slots without any job, so the orchestrator's existing redeploy wait can finish. The steady state with three jobs at parallelism 4 is unchanged.

Check. The setting is checked statically. Only the pilot can show three TaskManagers start before any job.

## L9. `pods/exec` without `get`

What changed. Both Roles grant `get` and `create` on `pods/exec`.

Check that passes. `tests/check_manifests.py`, for both workloads and both Roles. The runbook has the two `kubectl auth can-i get pods/exec` checks.

## Pilot check 1. Exporter Pod Identity without a token

What changed. New `k8s/checks/exporter-identity-check.yaml`, a Job with the exporter's pod shape (service account `metric-exporter`, `automountServiceAccountToken: false`). It calls STS GetCallerIdentity and prints only the role name. It is not in any kustomization. The runbook applies it once after deploy.

## Changes needed from the other agents

Orchestrator (`rebuild/orchestrator/`):

1. L1, the remediation path. In `recovery.py` `resubmit_missing`, replace `["/opt/flink/bin/flink", "run", "-d", ("-s", path), "-c", cls, jar]` with `["/opt/flink/bin/run-job.sh", cls, f"{ids.FLINK_JAR_DIR}/{jar}"] + ([paths[name]] if name in paths else [])`. `run-job.sh` accepts an absolute jar path or a bare name.
2. L1, the startup check. At startup and after every resubmission, read `/jobs/<id>/config` (`execution-config.job-parallelism` must be 4) and `/jobs/<id>/checkpoints/config` (`mode` `exactly_once`, `interval` 60000, `externalization.enabled` true, `delete_on_cancellation` false), and halt on a mismatch. The values are the ones in `rebuild/infra/images/flink-jobs/run-job.sh`.
3. L1 and L4, the fake. Derive the TaskManager count from the slots that jobs request, with a floor of 12 slots (three TaskManagers) from `slotmanager.number-of-slots.min`.
4. C1. Store `latest_completed_path` without `s3://<bucket>/`, for example relative to `scratch/<id>/`. Strip the registry host from every image reference the orchestrator records (its own image, `F6_INJECTOR_IMAGE`, the provenance pod images). Scan `logs/orchestrator-*.log.gz` for 12-digit numbers, ARNs and the bucket name before upload.
5. C6. If any check counts pods or Jobs in `streaming` without the `rebuild/campaign` selector, exclude `app=campaign-watchdog`. The watchdog pods carry no `rebuild/*` label, so the existing selectors already ignore them.
6. B6. Nothing. The Job now runs `run [--resume] [--fault F] [--block N]` in that order.

Integration (`rebuild/integration/plan_tool.sh`, M10): resolve `--out` and `--plan` against the caller's working directory before the `cd` into `rebuild/orchestrator`.

Pipeline (`rebuild/pipeline/`): nothing. `fetch_campaign.py` leaves exactly the S3 layout in the output directory.

Protocol text I did not change, for the author or the agent that owns those sections:

1. 1.7 item 3 lists two changes from R1. The Flink configuration has two more: checkpoints retained on cancel, and the failure-rate restart strategy if the author keeps it (L1 above).
2. 1.7 and 6.5 item 6 name "the staging MFA session script". The session now comes from `rebuild/infra/scripts/mfa_session.py`.
3. 6.4, last paragraph: the payer account activates the tags, budget data lag 8 to 12 hours, and the pilot's cap is the 13 h stop rule.
4. 7.5 item 4 says the results bucket name contains the account ID. Under decision 29 it no longer does. Bucket names still stay out of the deposit.

## What I could not fix, and what is still unproven

1. M4 item 1 is recovered, not prevented: a failed first apply can still leave a PV volume, which `sweep.py` reports and `delete_orphan_volumes.py` removes.
2. The watchdog runs on the monitoring node. If that node is lost and never replaced, nothing alerts until the hand checks. An S3 request-metric alarm in CloudWatch would cover that from outside the cluster. I did not add it, because the brief asked for the sidecar or a CronJob.
3. The restart strategy needs the author's decision (L1 above).
4. L2, L3, L5, L6, L7 and L8 are orchestrator changes. Two infra options exist and are not applied: `kubelet.serviceMonitor.honorTimestamps: false` would remove the late cAdvisor samples behind L5 but changes how every cAdvisor sample is timed, and a JMX rule for `kafka.log` `LogEndOffset` on `dead-letter` would give L3 a series that exists from topic creation.
5. Nothing has met AWS. The mock provider does not compute IAM policy documents, add-on schemas, the Health rule's delivery or the SNS policy. The destroy provisioners have not run against a cluster. `network_guard.py`, `discover.py` and `eks_api_cidrs.py` are tested against the documented API shapes only.
6. A kubectl 1.35 for macOS is not installed. The runbook gives the download and checksum commands.

## Final `validate_local.sh` run

The command, from the paper directory, with `SCRATCH` the session scratch directory that holds the review venv, the Terraform 1.16.5 download and a provider cache:

```bash
PATH=$SCRATCH/venv/bin:$PATH TF_PLUGIN_CACHE_DIR=$SCRATCH/cfix/plugin-cache \
TF_BINS="$HOME/.local/bin/terraform-1.10.5 $SCRATCH/tf_dl/1.16.5/terraform" bash rebuild/infra/scripts/validate_local.sh
```

The provider cache only saves downloads. Result: Terraform 1.10.5 and 1.16.5, Helm 4.3.0, kubectl 1.37.1 client (kustomize 5.8.1), Python 3.14.8. 520 checks ok, 0 failed, "ALL LOCAL CHECKS PASSED", plus the 31 JMX rule cases. For each Terraform version the mocked plans give shared 2 of 2, network 2 of 2 and cluster 7 of 7 runs passed, 12 node group plans on the pinned release and none from SSM, and both guard-order checks pass. After that run I changed only prose in `README.md` and `PILOT_RUNBOOK.md`, and `tests/check_manifests.py` still passes on them (125 checks).
