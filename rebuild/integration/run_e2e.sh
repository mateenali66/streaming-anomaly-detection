#!/usr/bin/env bash
# End-to-end proof of the interface: orchestrator dry run against the simulated cluster, its S3 objects written to disk,
# the offline gate, then the preprocessing's validator and build, a second build checked against the first (outputs are
# deterministic), and the assertions of check_e2e.py. No AWS, no cluster, no network.
#
#   rebuild/integration/run_e2e.sh [--work DIR] [--only ec0,fail,...]
#
# Scenarios (all must pass):
#   ec0          ecommerce, campaign index 0 (pilot), shortened phases
#   iot1         iot, campaign index 1, shortened phases, ten blocks, five window sizes; training/train.py validate_data
#   full         iot, campaign index 1 at full length: 1 h warm-up, 24 h baseline, ten blocks, 1 h trailing
#   fail         ecommerce index 0: the first attempt of F1, F4, F5, F6 and F7 has no effect (GATE_FAILED) and the first F2
#                loses an untargeted feature source (TELEMETRY_FAILED); each is repeated in place and passes
#   resume       ecommerce index 0: SIGKILL 20 s into F5's INJECTION, a restart without --resume is refused (exit 4), the
#                restart with --resume finishes the attempt as ABORTED, repeats it and completes the campaign
#   remediation  ecommerce index 0: F5's jobs cannot restore until Flink is redeployed (scripted remediation, full redeploy,
#                new JobManager pod, next attempt AFTER_REDEPLOY) and a job fails after F4 recovers (VALID_REMEDIATED)
#   Review 4 (rebuild/review4_fixes/A_orchestrator.md):
#   sham1        packet loss from no campaign fault during the first sham: its F7 signature false-passes, the sham is
#                GATE_FAILED and repeated (CAM-1); one false pass flags nothing
#   sham2        the same on the first two sham attempts: two failures in a row halt the pilot, it is resumed, and the
#                analysis flags every F7 window SIGNATURE_FALSE_PASS (2.5)
#   ctrlread     the controller read after the first F1 fails: GATE_FAILED and repeated (CAM-2 item 1)
#   jobsend      the Flink job list right after the first F4's recovery gate fails twice: retried, the attempt stays valid
#                and the build accepts it (CAM-2 item 2)
#   underrun     the first F4 recovers 70 s early: GATE_FAILED and repeated (CAM-2 item 3)
#   f6term       SIGTERM 60 s into F6: the halt path stores the injector log, the resumed run finishes the attempt as
#                ABORTED and repeats it, and the build accepts the ABORTED F6 (CAM-2 item 4)
#   prefault     every Flink job restarts 300 s before the first F2's onset: the PRE_FAULT envelope fails it (CAM-3)
#   basegap      every Flink job restarts inside the BASELINE: the NORMAL-time health rule EXCLUDES those windows (CAM-3)
#   Every scenario also checks the labelled windows against the simulated cluster's own ground truth (MIN-1).
#
# Environment:
#   ORCH_PY   CPython 3.12 with numpy, pyarrow and PyYAML (default python3.12)
#   PIPE_PY   Python with numpy, pandas and pyarrow for rebuild/pipeline, plus scikit-learn, scipy, optuna and torch for
#             the train.py validator (default python3)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
REBUILD="$(cd "$HERE/.." && pwd)"
ORCH_PY="${ORCH_PY:-python3.12}"
PIPE_PY="${PIPE_PY:-python3}"
WORK=""; ONLY=""
while [ $# -gt 0 ]; do
  case "$1" in
    --work) WORK="$2"; shift 2;;
    --only) ONLY="$2"; shift 2;;
    -h|--help) sed -n '2,40p' "$0"; exit 0;;
    *) echo "unknown argument $1" >&2; exit 2;;
  esac
done
[ -n "$WORK" ] || WORK="$(mktemp -d "${TMPDIR:-/tmp}/rebuild-e2e.XXXXXX")"
mkdir -p "$WORK"
echo "work directory: $WORK"
"$ORCH_PY" -c 'import sys, numpy, pyarrow, yaml; assert sys.version_info[:2] == (3, 12), sys.version' \
  || { echo "ORCH_PY=$ORCH_PY must be CPython 3.12 with numpy, pyarrow and PyYAML" >&2; exit 2; }
"$PIPE_PY" -c 'import numpy, pandas, pyarrow' || { echo "PIPE_PY=$PIPE_PY needs numpy, pandas and pyarrow" >&2; exit 2; }

declare -a NAMES=(ec0 iot1 full fail resume remediation sham1 sham2 ctrlread jobsend underrun f6term prefault basegap)
declare -A ARGS=(
  [ec0]="--workload ecommerce --index 0"
  [iot1]="--workload iot --index 1"
  [full]="--workload iot --index 1 --full-length"
  [fail]="--workload ecommerce --index 0 --knob F1.fail_first_n=1 --knob F4.fail_first_n=1 --knob F5.fail_first_n=1 --knob F6.fail_first_n=1 --knob F7.fail_first_n=1 --world telemetry_gap_once=\"F2\""
  [resume]="--workload ecommerce --index 0 --kill INJECTION:F5:20 --restart-after 45"
  [remediation]="--workload ecommerce --index 0 --world f5_restore_fails_until_redeploy=true --world submit_fails_until_redeploy=true --world fail_job_after_recover_once=\"F4\""
  [sham1]="--workload ecommerce --index 0 --world sham_disturbance=[1]"
  [sham2]="--workload ecommerce --index 0 --world sham_disturbance=[1,2] --resume-halts 1"
  [ctrlread]="--workload ecommerce --index 0 --world quorum_fail_after_end_once=\"F1\""
  [jobsend]="--workload ecommerce --index 0 --world flink_snapshot_fail_after_gate_once=\"F4\""
  [underrun]="--workload ecommerce --index 0 --world early_recover_once=\"F4\""
  [f6term]="--workload ecommerce --index 0 --term INJECTION:F6:60 --restart-after 45"
  [prefault]="--workload iot --index 0 --world prefault_job_restart_once=\"F2\""
  [basegap]="--workload ecommerce --index 0 --world job_restart_at_offset_s=[1300]"
)
declare -A EXPECT=(
  [ec0]="--expect GATE_FAILED=0 --expect TELEMETRY_FAILED=0 --expect ABORTED=0"
  [iot1]="--expect GATE_FAILED=0 --expect TELEMETRY_FAILED=0 --expect ABORTED=0 --train-validate"
  [full]="--expect GATE_FAILED=0 --expect TELEMETRY_FAILED=0 --expect ABORTED=0 --train-validate"
  [fail]="--expect GATE_FAILED=5 --expect TELEMETRY_FAILED=1 --expect ABORTED=0"
  [resume]="--expect ABORTED=1 --expect GATE_FAILED=0 --expect-runs killed,4,0"
  [remediation]="--expect GATE_FAILED=1 --expect-outcome VALID_REMEDIATED --expect-flag AFTER_REDEPLOY --expect-flag REMEDIATED --expect-redeploy"
  [sham1]="--expect GATE_FAILED=1 --expect TELEMETRY_FAILED=0 --expect-failed-check f0_signature_false_pass --expect-no-window-flag SIGNATURE_FALSE_PASS"
  [sham2]="--expect GATE_FAILED=2 --expect-failed-check f0_signature_false_pass --expect-window-flag F7:SIGNATURE_FALSE_PASS --expect-runs 3,0"
  [ctrlread]="--expect GATE_FAILED=1 --expect-failed-check controller_read_after_injection"
  [jobsend]="--expect GATE_FAILED=0 --expect TELEMETRY_FAILED=0 --expect ABORTED=0"
  [underrun]="--expect GATE_FAILED=1 --expect-failed-check end_underrun"
  [f6term]="--expect ABORTED=1 --expect GATE_FAILED=0 --expect-runs 143,4,0"
  [prefault]="--expect GATE_FAILED=1 --expect-failed-check pre_fault_jobs_running_same_id"
  [basegap]="--expect GATE_FAILED=0 --expect-health-excluded BASELINE"
)

failed=()
summary=()
for name in "${NAMES[@]}"; do
  if [ -n "$ONLY" ] && [[ ",$ONLY," != *",$name,"* ]]; then continue; fi
  d="$WORK/$name"
  rm -rf "$d"
  echo
  echo "================ $name: ${ARGS[$name]}"
  t0=$(date +%s)
  ok=1
  # shellcheck disable=SC2086  # ARGS holds several words on purpose
  eval "(cd \"$REBUILD/orchestrator\" && \"$ORCH_PY\" \"$HERE/dump_dry_run.py\" --out \"$d\" ${ARGS[$name]})" || ok=0
  cid="$("$PIPE_PY" -c 'import json,sys; print(json.load(open(sys.argv[1]))["campaign_id"])' "$d/summary.json" 2>/dev/null || echo "")"
  if [ "$ok" = 1 ] && [ -n "$cid" ]; then
    echo "---- offline gate (orchestrator gate-offline --store, from the S3 mirror alone)"
    (cd "$REBUILD/orchestrator" && "$ORCH_PY" -m orchestrator gate-offline --store "$d/s3" > "$d/gate-offline.txt") || ok=0
    echo "     $(grep -c ' MATCH$' "$d/gate-offline.txt" || true) attempts MATCH, $(grep -c ' DIFFER$' "$d/gate-offline.txt" || true) DIFFER"
    echo "---- streamprep validate"
    (cd "$REBUILD/pipeline" && "$PIPE_PY" -m streamprep validate --campaign-dir "$d/s3") || ok=0
    echo "---- streamprep build"
    (cd "$REBUILD/pipeline" && "$PIPE_PY" -m streamprep build --campaign-dir "$d/s3" --out "$d/out" > "$d/build.txt") || { cat "$d/build.txt"; ok=0; }
    echo "---- streamprep build again, checked against the first build's hashes"
    (cd "$REBUILD/pipeline" && "$PIPE_PY" -m streamprep build --campaign-dir "$d/s3" --out "$d/out2" \
       --check-against "$d/out/derived/$cid" > "$d/build2.txt") || { cat "$d/build2.txt"; ok=0; }
    echo "---- assertions"
    # shellcheck disable=SC2086
    "$PIPE_PY" "$HERE/check_e2e.py" --dump "$d" --derived "$d/out/derived/$cid" ${EXPECT[$name]} || ok=0
  else
    ok=0
  fi
  secs=$(( $(date +%s) - t0 ))
  if [ "$ok" = 1 ]; then summary+=("PASS $name (${secs}s)"); else summary+=("FAIL $name (${secs}s)"); failed+=("$name"); fi
done

echo
echo "================ end-to-end summary ($WORK)"
printf '%s\n' "${summary[@]}"
if [ "${#failed[@]}" -gt 0 ]; then echo "FAILED: ${failed[*]}"; exit 1; fi
echo "ALL SCENARIOS PASSED"
