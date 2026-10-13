#!/bin/bash
# V3 e-commerce DSVDD + DAGMM gap-fill at the 15s, 60s and 120s windows.
#
# bootstrap_v3_ecom_gapfill.sh trains DeepSVDD and DAGMM on e-commerce at the
# 30s window only (results/ecom-gapfill/). This script trains the same two
# models at 15s, 60s and 120s, so they can appear in the window-sensitivity
# tables (detection latency, FPR against latency, cooldown and cost) next to
# the other 6 panel models. It uses 30 trials per model instead of the 100
# used at 30s, because these windows feed the sensitivity tables and not the
# headline evaluation. training.py saves the raw scores for each fold.
#
# Output: s3://$BUCKET/v3/results/ecom-gapfill-mw/{15s,60s,120s}/
#
# Usage on the EC2 instance (set BUCKET before invoking):
#   BUCKET=<your-bucket> INSTANCE_ROLE=ecom-gapfill-mw bash bootstrap_v3_ecom_gapfill_multiwindow.sh

set -euo pipefail

BUCKET="${BUCKET:?set BUCKET to your S3 bucket}"
REGION="us-east-2"
PREFIX="v3"
WORK_DIR="/home/ubuntu/training"
INSTANCE_ROLE="${INSTANCE_ROLE:-ecom-gapfill-mw}"

TRIALS="${TRIALS:-30}"
WINDOWS=("15" "60" "120")
MODELS=(DeepSVDD DAGMM)

echo "=========================================="
echo "V3 ecom gap-fill multi-window"
echo "Started:    $(date -u)"
echo "Windows:    ${WINDOWS[*]}s"
echo "Models:     ${MODELS[*]}"
echo "Trials:     $TRIALS"
echo "Output S3:  s3://$BUCKET/$PREFIX/results/$INSTANCE_ROLE/"
echo "=========================================="

mkdir -p "$WORK_DIR"/{input,results}
cd "$WORK_DIR"

echo "Downloading e-commerce features at all windows + scripts..."
for W in "${WINDOWS[@]}"; do
    aws s3 cp "s3://$BUCKET/$PREFIX/input/features_${W}s.parquet" \
              "$WORK_DIR/input/features_${W}s.parquet" --region "$REGION"
done
aws s3 cp "s3://$BUCKET/$PREFIX/iot/input/training.py" "$WORK_DIR/training.py" --region "$REGION"

PYTHON="/opt/pytorch/bin/python3"
PIP="/opt/pytorch/bin/pip"
echo "Python: $($PYTHON --version)"
$PIP install --quiet optuna pandas scikit-learn scipy pyarrow 2>&1 | tail -3

$PYTHON -c "import torch; print(f'CUDA: {torch.cuda.is_available()}, GPU: {torch.cuda.get_device_name(0) if torch.cuda.is_available() else \"N/A\"}')"

SYNC_CMD="aws s3 sync $WORK_DIR/results/ s3://$BUCKET/$PREFIX/results/$INSTANCE_ROLE/ --region $REGION --quiet"
{ crontab -l 2>/dev/null || true; echo "*/15 * * * * $SYNC_CMD"; } | crontab -

sync_results() {
    aws s3 sync "$WORK_DIR/results/" "s3://$BUCKET/$PREFIX/results/$INSTANCE_ROLE/" \
        --region "$REGION" --quiet 2>/dev/null || true
}

for W in "${WINDOWS[@]}"; do
    DATA="$WORK_DIR/input/features_${W}s.parquet"
    OUTPUT="$WORK_DIR/results/${W}s"
    mkdir -p "$OUTPUT"

    for MODEL in "${MODELS[@]}"; do
        echo ""
        echo "=========================================="
        echo "Model: $MODEL | Window: ${W}s | n_trials=$TRIALS"
        echo "Time: $(date -u)"
        echo "=========================================="
        $PYTHON "$WORK_DIR/training.py" \
            --model "$MODEL" \
            --data "$DATA" \
            --output "$OUTPUT" \
            --n-trials "$TRIALS" \
            --n-workers 1 \
            2>&1 | tee -a "$WORK_DIR/training_${MODEL}_${W}s.log"
        echo "Completed: $MODEL ${W}s at $(date -u)"
        sync_results
    done
done

echo ""
echo "=========================================="
echo "All windows complete"
echo "Final S3 sync..."
echo "=========================================="
sync_results

touch "$WORK_DIR/DONE"
aws s3 cp "$WORK_DIR/DONE" "s3://$BUCKET/$PREFIX/results/$INSTANCE_ROLE/DONE" --region "$REGION"
echo "Bootstrap finished: $(date -u)"
