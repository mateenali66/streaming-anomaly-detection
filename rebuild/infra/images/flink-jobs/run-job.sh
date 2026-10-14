#!/usr/bin/env bash
# Submit ONE Flink job with the campaign's job settings. Baked into the flink-jobs image at /opt/flink/bin/run-job.sh. It is the
# ONE submission path (REVIEW4_LIVE_CONTRACT L1):
#   - submit-jobs.sh, which the `flink-submit` Job runs in its own pod, calls it for every missing job;
#   - the orchestrator's remediation runs it inside the JobManager pod:
#       kubectl exec <jobmanager> -c flink-main-container -- /opt/flink/bin/run-job.sh <main-class> <jar> [<checkpoint path>]
#
# Why the settings are on the command line: the Flink CLI builds the JobGraph from the configuration of the process it runs
# in. In the flink-submit pod that is the distribution's /opt/flink/conf/config.yaml, which sets parallelism.default: 1 and
# no checkpointing, so the jobs ran at parallelism 1 with checkpointing off. `-p` and `-D` override every config file, so both
# paths give the same JobGraph. The job code sets none of these (streaming-app/flink-jobs/*: getExecutionEnvironment() only).
#
# The values are R1's (infrastructure/manifests/flink-session-cluster.yaml and infrastructure-b/, identical: R1 submitted with
# `flink run -d -c <class> <jar>` inside the JobManager, so the client read the session config), with two recorded changes:
#   1. execution.checkpointing.externalized-checkpoint-retention RETAIN_ON_CANCELLATION (R1: not set, Flink 1.19 default
#      NO_EXTERNALIZED_CHECKPOINTS). The remediation restores a job from its last checkpoint (README section 9, item 15).
#   2. restart-strategy failure-rate, 40 failures in 10 min, 30 s delay (R1: fixed-delay, 10 attempts, 30 s). Fixed-delay is a
#      budget for the whole life of a job. The rebuilt F5 kills five TaskManagers per attempt and there are ten F5 attempts per
#      campaign, so a 10-attempt budget runs out within the first blocks and every later job failure is permanent. A job with
#      no shuffle restarts four regions when its TaskManager dies, and each counts, so five kills on one such job reach 20.
#      The pilot of 14 October 2026 saw 12 restarts in one F5 cell, and the limit is 40 (decision 38).
# The checkpoint and savepoint directories are not passed: the JobManager resolves them from the session config
# (k8s/base/flink/flink-session-cluster.yaml, scratch/<campaign>/ on S3).
#
# Usage: run-job.sh <main-class> <jar name in /opt/flink/usrlib, or an absolute path> [<checkpoint or savepoint path>]
# Env:   FLINK_BIN   flink CLI (default /opt/flink/bin/flink; tests point it at a stub)
#        FLINK_REST  host:port of the REST endpoint (default streaming-flink-rest.streaming.svc.cluster.local:8081)
#        USRLIB      jar directory (default /opt/flink/usrlib)
# Exit:  the exit code of `flink run`; 2 bad usage.
set -u
FLINK_BIN="${FLINK_BIN:-/opt/flink/bin/flink}"
FLINK_REST="${FLINK_REST:-streaming-flink-rest.streaming.svc.cluster.local:8081}"
USRLIB="${USRLIB:-/opt/flink/usrlib}"

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
  echo "usage: run-job.sh <main-class> <jar> [<checkpoint path>]" >&2
  exit 2
fi
CLASS="$1"; JAR="$2"; RESTORE="${3:-}"
case "$JAR" in /*) ;; *) JAR="$USRLIB/$JAR";; esac

# The job settings. tests/test_submit_jobs.sh and scripts/check_flink_jobs.py read this list; keep one setting per line.
SETTINGS=(
  -p 4
  -Dexecution.checkpointing.interval=60000
  -Dexecution.checkpointing.mode=EXACTLY_ONCE
  -Dexecution.checkpointing.timeout=120000
  -Dexecution.checkpointing.min-pause=30000
  -Dexecution.checkpointing.externalized-checkpoint-retention=RETAIN_ON_CANCELLATION
  -Dstate.backend.type=rocksdb
  -Dstate.backend.incremental=true
  -Drestart-strategy.type=failure-rate
  -Drestart-strategy.failure-rate.max-failures-per-interval=40
  "-Drestart-strategy.failure-rate.failure-rate-interval=10 min"
  "-Drestart-strategy.failure-rate.delay=30 s"
)

ARGS=(run -d -m "$FLINK_REST" "${SETTINGS[@]}")
[ -n "$RESTORE" ] && ARGS+=(-s "$RESTORE")
ARGS+=(-c "$CLASS" "$JAR")
echo "run-job: $FLINK_BIN ${ARGS[*]}"
exec "$FLINK_BIN" "${ARGS[@]}"
