# Pilot runbook: two pilot clusters, from session to teardown

This is protocol 1.8 as commands. It covers both pilots, `pilot-ec` (e-commerce) and `pilot-iot` (IoT). Each pilot runs in its own shell. It follows the pilot command sequence of `diagnostics/REVIEW4_OPERATIONS.md`, with every gap in that sequence now closed by a script. Nothing here has been run against AWS yet. README section 4 is the short form for the six campaigns.

Rules that hold for every step:

1. Ask the author for the one-time password before the session step. Never store it.
2. No account ID, profile name or e-mail address goes into any committed file. They live in the shell, or in the gitignored `*.tfvars` and `backend.hcl` files.
3. No single command may wait longer than 10 minutes (decision 30). Terraform applies and destroys, and the workload deploy, run detached with a log. You poll them with short commands.
4. Stop rule: both pilots are torn down 13 hours after the first cluster apply started, whatever their state. Section 11 says how.
5. Every path is absolute or starts with `$P`, the paper directory, set in step 1.

## 0. Before you start

1. The author has approved the pilot cost (decision 6, up to $60).
2. `scripts/validate_local.sh` passes on this commit (README section 7.1).
3. Docker with buildx is running. The images are built in step 6.

## 1. Values you supply

| Value | Example | Where it lives |
|---|---|---|
| One-time password | from the author | typed, never stored |
| `SOURCE_PROFILE` | the profile with the long-term keys and `mfa_serial` | the shell |
| `SESSION_PROFILE` | any name for the 36 h session profile | the shell, and `~/.aws/credentials` (written by `mfa_session.py`) |
| `account_id` | 12 digits | gitignored `terraform.tfvars` (shared, network) and `<id>.tfvars` (cluster) |
| `owner` | a team alias, not an address | the same files |
| `alert_email` | the address for the SNS topic | `terraform/shared/terraform.tfvars` |
| `budget_limit_usd` | the approved cap, for example 60 | `terraform/shared/terraform.tfvars` |
| `bucket_suffix` | 8 random characters, made ONCE (step 5) | `terraform/shared/terraform.tfvars`. The bucket can never be renamed |
| State bucket | made by hand (step 4) | `backend.hcl` of each root, `state_bucket` in `<id>.tfvars` |
| `vpc_cidr`, `workload_az`, `control_plane_azs` | from `discover.py` | `terraform/network/terraform.tfvars` |
| `MY_CIDR` | the laptop's public address as a /32 | `api_allowed_cidrs` in `<id>.tfvars` |
| `python_bin` | `$PY`, an absolute path | `terraform/network/terraform.tfvars` and `<id>.tfvars` |
| `IMAGE_TAG` | `r2-001`, usable once (immutable repositories) | the command line of step 6 |

## 2. Session (36 hours)

Ask the author for the OTP first.

```bash
cd <the paper directory, the one that holds rebuild/> && export P="$PWD"
INFRA=$P/rebuild/infra
PY=$HOME/.venvs/stream-rebuild-infra/bin/python      # boto3 and PyYAML. To make one: python3 -m venv DIR && DIR/bin/pip install -r $INFRA/scripts/requirements.txt
export SOURCE_PROFILE='<long-term profile>' SESSION_PROFILE='<session profile>'
$PY $INFRA/scripts/mfa_session.py --source-profile "$SOURCE_PROFILE" --target-profile "$SESSION_PROFILE" --otp '<OTP>' --duration 129600
export AWS_PROFILE="$SESSION_PROFILE"
```

Write down the expiry it prints. Start every apply or destroy with at least 2 hours left. A new session needs a new OTP from the author. The clusters themselves never use this session: everything in the cluster runs on Pod Identity, so an expired session cannot stop a running pilot.

## 3. Tools and paths

```bash
TF=$HOME/.local/bin/terraform-1.10.5               # the terraform on PATH is 1.5.7 and too old (README M8)
export PYTHON_BIN=$PY                               # deploy_workload.sh, pre_destroy.sh, build_images.sh
LOGS=$HOME/tf-logs && mkdir -p "$LOGS"             # OUTSIDE the repository: Terraform output quotes ARNs with the account ID
COPIES=$HOME/campaign-copies && mkdir -p "$COPIES" # downloaded campaign data, also outside the repository
# kubectl 1.34 to 1.36 against EKS 1.35 (the one on PATH is 1.37). Once:
#   curl -fsSLo $HOME/.local/bin/kubectl-1.35.9 https://dl.k8s.io/release/v1.35.9/bin/darwin/arm64/kubectl
#   curl -fsSL https://dl.k8s.io/release/v1.35.9/bin/darwin/arm64/kubectl.sha256   (compare with: shasum -a 256 $HOME/.local/bin/kubectl-1.35.9)
#   chmod +x $HOME/.local/bin/kubectl-1.35.9 && mkdir -p $HOME/.local/kubectl-1.35/bin && ln -sf $HOME/.local/bin/kubectl-1.35.9 $HOME/.local/kubectl-1.35/bin/kubectl
export PATH=$HOME/.local/kubectl-1.35/bin:$PATH && kubectl version --client
MY_CIDR="$(curl -s https://checkip.amazonaws.com)/32" && echo "$MY_CIDR"
```

`deploy_workload.sh` refuses a kubectl outside 1.34 to 1.36. `pre_destroy.sh` only warns, so a teardown never stops on it.

## 4. Discovery and the state bucket (read only, then by hand)

```bash
$PY $INFRA/scripts/discover.py --vpc-cidr 10.NN.0.0/16 --clusters 2
```

It must exit 0. Every BLOCK line stops the pilot until it is fixed (vCPU headroom below 72, a customer-managed default EBS key, a gp3 quota too small, no right to create IAM roles). Every DECIDE line goes to the author, for example GuardDuty Runtime Monitoring. Request quota increases now if needed.

Make the state bucket in the console: versioning on, SSE-S3, all four public access blocks on, tag `Project=streaming-anomaly-detection`. Put its name into each root's `backend.hcl` (copied from `backend.hcl.example`).

## 5. `shared` (1 to 3 minutes)

```bash
cd $INFRA/terraform/shared
cp backend.hcl.example backend.hcl && cp terraform.tfvars.example terraform.tfvars
python3 -c 'import secrets; print(secrets.token_hex(4))'   # bucket_suffix: run ONCE, put it in terraform.tfvars, never change it
# edit terraform.tfvars: account_id, owner, alert_email, bucket_suffix, budget_limit_usd
$TF init -backend-config=backend.hcl
$TF plan -out=shared.tfplan > $LOGS/plan-shared.txt; tail -n 3 $LOGS/plan-shared.txt
$TF apply -no-color shared.tfplan > $LOGS/apply-shared.log 2>&1; tail -n 3 $LOGS/apply-shared.log
```

Read the plan first: only creates. That is the bucket and its settings, 15 ECR repositories, the SNS topic and its policy, the budget, and the Health rule with its log group, log resource policy and two targets. Confirm the SNS e-mail subscription. The Project, Campaign and Workload cost allocation tags are activated by the payer account, not here (README M7). Until then the budget tracks nothing, which is why the stop rule exists.

## 6. Images

```bash
cd $INFRA
$PY scripts/mirror_images.py --resolve
$PY scripts/mirror_images.py --copy --dry-run && $PY scripts/mirror_images.py --copy && $PY scripts/mirror_images.py --verify
scripts/build_images.sh --tag "$IMAGE_TAG"
git -C "$P" add rebuild/infra/images/images.lock.yaml && git -C "$P" commit -m "build: pin image digests for $IMAGE_TAG"
```

A long `--copy` or build runs the same way as an apply if it nears 10 minutes: `nohup caffeinate -i <command> > $LOGS/<name>.log 2>&1 &`, then poll the log.

## 7. `network` (3 to 6 minutes)

```bash
cd $INFRA/terraform/network
cp backend.hcl.example backend.hcl && cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars: account_id, owner, vpc_cidr, workload_az, control_plane_azs, python_bin = "<$PY>"
$TF init -backend-config=backend.hcl
$TF plan -out=network.tfplan > $LOGS/plan-network.txt; tail -n 3 $LOGS/plan-network.txt
nohup caffeinate -i $TF apply -no-color network.tfplan > $LOGS/apply-network.log 2>&1 &
tail -n 3 $LOGS/apply-network.log                    # repeat until "Apply complete!" or "Error"
```

The plan creates 1 VPC, 1 internet gateway, 1 Elastic IP, 1 NAT gateway, 13 subnets, 2 route tables, 13 associations, the S3 endpoint and the destroy guard.

## 8. Versions, plans and thresholds

```bash
cd $INFRA && $PY scripts/capture_versions.py --kubernetes-version 1.35 --check-config --out terraform/cluster/addon-versions.auto.tfvars
git -C "$P" add rebuild/infra/terraform/cluster/addon-versions.auto.tfvars && git -C "$P" commit -m "build: capture add-on and AMI versions"
cd $P
rebuild/integration/plan_tool.sh verify-plan --workload ecommerce --index 0 --plan $P/rebuild/plans/pilot-ec/plan.json --sha "$(cat $P/rebuild/plans/pilot-ec/plan.json.sha256)"
rebuild/integration/plan_tool.sh verify-plan --workload iot --index 0 --plan $P/rebuild/plans/pilot-iot/plan.json --sha "$(cat $P/rebuild/plans/pilot-iot/plan.json.sha256)"
rebuild/integration/plan_tool.sh thresholds --out $P/rebuild/plans/thresholds.json     # once, then commit it
```

Use absolute paths with `plan_tool.sh`: it runs inside `rebuild/orchestrator`, so a relative `--out` lands there (README M10).

## 9. The two clusters (one shell each, about 25 to 35 minutes)

Shell A sets `ID=pilot-ec W=ecommerce WL=ECOMMERCE`. Shell B sets `ID=pilot-iot W=iot WL=IOT`. Run steps 2 and 3 in both shells first.

```bash
ID=pilot-ec; W=ecommerce; WL=ECOMMERCE              # shell B: ID=pilot-iot; W=iot; WL=IOT
cd $INFRA/terraform/cluster
cp -n backend.hcl.example backend.hcl                # once, bucket only. The key is given per campaign below
cp terraform.tfvars.example $ID.tfvars               # edit: account_id, owner, state_bucket, campaign_id=$ID, workload=$W,
                                                     #       api_allowed_cidrs = ["<MY_CIDR>"], python_bin = "<$PY>"
export TF_DATA_DIR=.terraform/$ID                    # one data directory per campaign (README C4)
$TF init -reconfigure -backend-config=backend.hcl -backend-config="key=campaigns/$ID/terraform.tfstate"
grep -o '"key": *"[^"]*"' $TF_DATA_DIR/terraform.tfstate   # must show campaigns/$ID/terraform.tfstate
$TF plan -var-file=$ID.tfvars -out=$ID.tfplan > $LOGS/plan-$ID.txt; tail -n 5 $LOGS/plan-$ID.txt   # only creates; read it
[ -f $LOGS/T0 ] || date +%s > $LOGS/T0               # the stop rule clock starts at the FIRST cluster apply
nohup caffeinate -i $TF apply -no-color $ID.tfplan > $LOGS/apply-$ID.log 2>&1 &
echo $! > $LOGS/apply-$ID.pid
```

Poll with short commands until it ends:

```bash
tail -n 3 $LOGS/apply-$ID.log
kill -0 "$(cat $LOGS/apply-$ID.pid)" 2>/dev/null && echo running || echo finished
```

It is done at `Apply complete!`. On `Error`, read section 12 before you run anything else. Keep the laptop on mains power: `caffeinate -i` stops idle sleep, and a sleeping laptop can fail a Helm wait.

Then the kubeconfig and the first checks (README section 8, list A):

```bash
cd $INFRA
$PY scripts/eks_kubeconfig.py --cluster-name streaming-anomaly-detection-$ID --campaign $ID --output $INFRA/kubeconfig-$ID
export KUBECONFIG=$INFRA/kubeconfig-$ID
kubectl get nodes -L workload,topology.kubernetes.io/zone          # 6 Ready nodes, ONE zone, labels kafka x3, flink x2, monitoring x1
kubectl get nodes -o custom-columns=NAME:.metadata.name,TAINTS:.spec.taints[*].value
kubectl -n kube-system get deploy coredns ebs-csi-controller       # the add-on configuration was accepted (both on the monitoring node)
kubectl -n observability get pods; kubectl -n chaos-mesh get pods; kubectl -n streaming get pods
cd $INFRA/terraform/cluster
BUCKET=$($TF output -raw results_bucket); SNS=$($TF output -raw alerts_topic_arn); REG=$($TF output -raw ecr_registry)
```

## 10. Workload, checks, orchestrator

```bash
cd $INFRA
nohup caffeinate -i scripts/deploy_workload.sh --workload $W --campaign-id $ID --results-bucket "$BUCKET" \
  --sns-topic-arn "$SNS" --ecr-registry "$REG" --plan-file $P/rebuild/plans/$ID/plan.json \
  --phase all > $LOGS/deploy-$ID.log 2>&1 &
tail -n 5 $LOGS/deploy-$ID.log                       # 10 to 45 minutes; the Kafka wait alone allows 1,200 s
```

It ends with `done: phases ...`. Phase `submit` stops it if the Flink jobs do not run with parallelism 4 and checkpoints every 60 s exactly once on RocksDB (`check_flink_jobs.py`). The checks of README section 8, list B, plus three from the reviews:

```bash
kubectl -n streaming exec streaming-kafka-brokers-0 -c kafka -- df -h /var/lib/kafka/data
$PY scripts/check_flink_jobs.py --context streaming-anomaly-detection-$ID --workload $W
kubectl -n streaming get pods -l component=taskmanager -o custom-columns=NAME:.metadata.name,QOS:.status.qosClass   # Guaranteed (L8)
kubectl -n streaming get --raw /api/v1/namespaces/streaming/services/load-generator:9102/proxy/status            # rate within 2% for 10 min
kubectl auth can-i get pods/exec -n streaming --as=system:serviceaccount:streaming:verification-probes           # yes (L9)
kubectl auth can-i get pods/exec -n streaming --as=system:serviceaccount:streaming:campaign-orchestrator         # yes (L9)
# Pod Identity without a service account token, as the exporter Jobs run (review 4 pilot check 1)
$PY scripts/render.py --ecr-registry "$REG" --campaign-id $ID --workload $WL --region us-east-2 \
  k8s/checks/exporter-identity-check.yaml | kubectl -n streaming apply -f -
kubectl -n streaming wait job/exporter-identity-check --for=condition=complete --timeout=180s
kubectl -n streaming logs job/exporter-identity-check && kubectl -n streaming delete job exporter-identity-check
# the new broker and controller series exist (README section 8, list B)
kubectl get --raw '/api/v1/namespaces/observability/services/prometheus-kube-prometheus-prometheus:http-web/proxy/api/v1/query?query=count%20by%20(__name__)(%7B__name__%3D~%22kafka_network_requestmetrics_localtimems_mean%7Ckafka_controller_kafkacontroller_fencedbrokercount%7Ckafka_server_raftmetrics_.*%7Ccontainer_oom_events_total%22%7D)' \
  | $PY -c 'import json,sys; print(sorted(r["metric"]["__name__"] for r in json.load(sys.stdin)["data"]["result"]))'
# every Prometheus target up
kubectl get --raw '/api/v1/namespaces/observability/services/prometheus-kube-prometheus-prometheus:http-web/proxy/api/v1/targets?state=active' \
  | $PY -c 'import json,sys; t=json.load(sys.stdin)["data"]["activeTargets"]; d=[x["labels"].get("job") for x in t if x["health"]!="up"]; print(len(t), "targets, down:", d)'
```

Then start the orchestrator. It also starts the alert-only watchdog CronJob:

```bash
scripts/deploy_workload.sh --workload $W --campaign-id $ID --results-bucket "$BUCKET" --sns-topic-arn "$SNS" --ecr-registry "$REG" \
  --plan-file $P/rebuild/plans/$ID/plan.json --thresholds-file $P/rebuild/plans/thresholds.json --phase run
kubectl -n streaming logs job/campaign-orchestrator -c orchestrator --tail=50     # poll; never -f in a timed command
kubectl -n streaming get cronjob campaign-watchdog
```

## 11. Teardown refusal test, early in the pilot

This must refuse and change nothing. The guard now runs before every other destroy, the IAM role policies included (README B3).

```bash
cd $INFRA/terraform/cluster && export TF_DATA_DIR=.terraform/$ID
$TF state list | wc -l > $LOGS/state-count-$ID
$TF plan -destroy -var-file=$ID.tfvars -out=$ID-destroy.tfplan > $LOGS/plan-destroy-$ID.txt
$TF apply -no-color $ID-destroy.tfplan > $LOGS/refuse-$ID.log 2>&1; grep -E '1/3 completeness gate|REFUSING' $LOGS/refuse-$ID.log
[ "$($TF state list | wc -l)" = "$(cat $LOGS/state-count-$ID)" ] && echo "nothing destroyed" || echo "STATE CHANGED: stop and read the log"
rm -f $ID-destroy.tfplan
```

## 12. The pilot itself

Warm-up 1 h, baseline 2 h, then the 8 cells of block 1 at about 30 minutes each, then a 1 h trailing baseline. Per pilot that is about 10 hours. Check every attempt by hand against its verification record (protocol 1.8 item 5).

| Situation | Command |
|---|---|
| A fault failed and has been fixed (1.8 item 6) | `scripts/deploy_workload.sh ... --phase stop`, then `... --phase run --recreate-job --resume --fault F5` (the run type that failed) |
| One block again | `... --phase run --recreate-job --resume --block 1` |
| A transient halt, under 2 hours, nothing changed (2.6 item 6) | `... --phase run --recreate-job --resume` |
| The watchdog alerts | read the alert, then `kubectl -n streaming describe job campaign-orchestrator` and the orchestrator log |
| The laptop's address changed, kubectl times out | `$PY scripts/eks_api_cidrs.py --cluster-name streaming-anomaly-detection-$ID --campaign $ID --cidr "$(curl -s https://checkip.amazonaws.com)/32"`, then the same CIDR in `api_allowed_cidrs` of `$ID.tfvars`. Exit 3 means still running: run it again with `--wait-update <id>` |

`...` stands for `--workload $W --campaign-id $ID --results-bucket "$BUCKET" --sns-topic-arn "$SNS" --ecr-registry "$REG" --plan-file $P/rebuild/plans/$ID/plan.json --thresholds-file $P/rebuild/plans/thresholds.json`. `--recreate-job` refuses while the Job still runs, so a running orchestrator is never deleted by accident.

When the pilot data are in, copy them and run the pipeline (protocol 1.8 item 9):

```bash
$PY $INFRA/scripts/fetch_campaign.py --bucket "$BUCKET" --campaign $ID --out $COPIES/$ID-$(date +%Y%m%dT%H%M)
```

A pilot that ran to its end has the orchestrator's own `COMPLETE`, and the copy is checked against it. A pilot stopped early has none, so it prints a WARNING that the copy is not checked against the orchestrator's record, and still exits 0. Sizes, and `SHA256SUMS` where it exists, are checked. Then run `python3 -m streamprep validate --campaign-dir <the copy>`, `streamprep build`, and the Isolation Forest dry run of `rebuild/pipeline/README.md` on the build with `--frozen-params`. A pilot has one block, so there is no hpo_val block to search on.

## 13. Teardown of each pilot

```bash
cd $INFRA
scripts/deploy_workload.sh --workload $W --campaign-id $ID --phase stop    # SIGTERM, last manifest sync, watchdog suspended
$PY scripts/mark_complete.py --bucket "$BUCKET" --campaign $ID --reason "pilot teardown" --i-verified-the-data   # only a pilot stopped early. It refuses when the orchestrator wrote COMPLETE
$PY scripts/fetch_campaign.py --bucket "$BUCKET" --campaign $ID --out $COPIES/$ID-final   # the last copy, marker included
cd $INFRA/terraform/cluster && export TF_DATA_DIR=.terraform/$ID
$TF plan -destroy -var-file=$ID.tfvars -out=$ID-destroy.tfplan > $LOGS/plan-destroy-$ID.txt
nohup caffeinate -i $TF apply -no-color $ID-destroy.tfplan > $LOGS/destroy-$ID.log 2>&1 &
echo $! > $LOGS/destroy-$ID.pid
tail -n 3 $LOGS/destroy-$ID.log; kill -0 "$(cat $LOGS/destroy-$ID.pid)" 2>/dev/null && echo running || echo finished
```

The destroy first runs `pre_destroy.sh` (completeness gate, then Kafka, Flink and every PVC while the operators and the CSI driver still run). That can take up to about 80 minutes in the worst case, then 15 to 40 minutes for the cluster. Afterwards:

```bash
$PY $INFRA/scripts/sweep.py --campaign $ID                     # must print "clean"
$PY $INFRA/scripts/delete_orphan_volumes.py --campaign $ID     # must print "nothing to do"; with --yes it deletes left-over PV volumes
```

Do both pilots. The network stays for the campaigns, at about $0.05 per hour. If the campaigns are not approved, destroy it the same detached way from `terraform/network`. Its guard refuses while a project cluster still uses the VPC, or while a foreign endpoint or a stray network interface would block the delete.

## 14. Stop rule: 13 hours after the first cluster apply

```bash
echo "stop at $(date -r $(( $(cat $LOGS/T0) + 13*3600 )))"
```

At that time tear down BOTH pilots with section 13, whatever their state. If an orchestrator is mid-attempt, `--phase stop` still ends it cleanly. If an apply is still running, wait for it to end, then destroy. This caps the pilot near $55: two clusters at about $2.09 per hour, plus the network and S3. Budget data lag 8 to 12 hours, so the budget cannot do this. A fix rerun after the stop needs a new approval.

## 15. Recovery

| What you see | What to do |
|---|---|
| `Error acquiring the state lock` and no Terraform process is running (`pgrep -fl terraform`) | `$TF force-unlock <Lock ID from the message>` in the same directory, with the same `TF_DATA_DIR` |
| `errored.tfstate` in the root directory (the final state write failed) | `$TF state push errored.tfstate`, then `rm errored.tfstate`. Never push it while another apply runs |
| `already exists` for the EKS cluster, so it was created outside the state | `$TF import -var-file=$ID.tfvars 'module.eks.aws_eks_cluster.this[0]' streaming-anomaly-detection-$ID`, then plan again. For another resource, import it under the address the error names |
| A Helm release `failed` after an interrupted apply | plan and apply again. Helm replaces the release |
| `Saved plan is stale` | the state changed since the plan: make a new plan |
| The session expired mid-apply (EKS answers 401) | a new OTP and `mfa_session.py`, then plan and apply again |
| A destroy stops in `pre_destroy.sh` with persistent volumes left | `kubectl get pv,pvc -A`. Delete what is left, then run the same destroy again |
| A destroy refuses at the completeness gate | a marker is missing or older than a result object: `--phase stop`, then `mark_complete.py` again |
