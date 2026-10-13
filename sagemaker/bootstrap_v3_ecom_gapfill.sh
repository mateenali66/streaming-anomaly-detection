#!/bin/bash
# V3 e-commerce DSVDD + DAGMM gap-fill at the 30s window.
#
# The IoT run trains the full 8-model panel. The cross-workload analysis
# needs the same 8 models on e-commerce. The other 6 models (IF, OC_SVM,
# LSTM_AE, Trans_AE, CNN_AE, LSTM_VAE) come from the V3 run at v3/results/.
# This script trains only DeepSVDD and DAGMM on the e-commerce 30s features,
# with 100 trials per model.
#
# Output: s3://$BUCKET/v3/results/ecom-gapfill/
# bootstrap_v3_ecom_gapfill_multiwindow.sh covers the 15s, 60s and 120s windows.
#
# Usage on the EC2 instance (set BUCKET before invoking):
#   BUCKET=<your-bucket> bash bootstrap_v3_ecom_gapfill.sh

set -euo pipefail

BUCKET="${BUCKET:?set BUCKET to your S3 bucket}"
REGION="us-east-2"
PREFIX="v3"
WORK_DIR="/home/ubuntu/training"
INSTANCE_ROLE="ecom-gapfill"

TRIALS=100

echo "=========================================="
echo "V3 e-commerce DSVDD+DAGMM gap-fill"
echo "Started: $(date -u)"
echo "Trials: $TRIALS"
echo "=========================================="

mkdir -p "$WORK_DIR"/{input,results}
cd "$WORK_DIR"

echo "Downloading e-commerce 30s features + scripts..."
aws s3 cp "s3://$BUCKET/$PREFIX/input/features_30s.parquet" "$WORK_DIR/input/features_30s.parquet" --region "$REGION"
# The training.py with DeepSVDD and DAGMM support is uploaded under the iot/ prefix
aws s3 cp "s3://$BUCKET/$PREFIX/iot/input/training.py" "$WORK_DIR/training.py" --region "$REGION"

PYTHON="/opt/pytorch/bin/python3"
PIP="/opt/pytorch/bin/pip"
echo "Python: $($PYTHON --version)"
$PIP install --quiet optuna pandas scikit-learn scipy pyarrow 2>&1 | tail -3

$PYTHON -c "import torch; print(f'CUDA: {torch.cuda.is_available()}, GPU: {torch.cuda.get_device_name(0) if torch.cuda.is_available() else \"N/A\"}')"

# Periodic S3 sync
SYNC_CMD="aws s3 sync $WORK_DIR/results/ s3://$BUCKET/$PREFIX/results/$INSTANCE_ROLE/ --region $REGION --quiet"
{ crontab -l 2>/dev/null || true; echo "*/15 * * * * $SYNC_CMD"; } | crontab -

sync_results() {
    aws s3 sync "$WORK_DIR/results/" "s3://$BUCKET/$PREFIX/results/$INSTANCE_ROLE/" \
        --region "$REGION" --quiet 2>/dev/null || true
}

DATA="$WORK_DIR/input/features_30s.parquet"
OUTPUT="$WORK_DIR/results"

for MODEL in DeepSVDD DAGMM; do
    echo ""
    echo "=========================================="
    echo "Model: $MODEL | Data: features_30s.parquet | n_trials=$TRIALS"
    echo "Time: $(date -u)"
    echo "=========================================="
    $PYTHON "$WORK_DIR/training.py" \
        --model "$MODEL" \
        --data "$DATA" \
        --output "$OUTPUT" \
        --n-trials "$TRIALS" \
        --n-workers 1 \
        2>&1 | tee -a "$WORK_DIR/training_${MODEL}_features_30s.log"
    echo "Completed: $MODEL at $(date -u)"
    sync_results
done

echo ""
echo "=========================================="
echo "Gap-fill complete"
echo "Final S3 sync..."
echo "=========================================="
sync_results

touch "$WORK_DIR/DONE"
aws s3 cp "$WORK_DIR/DONE" "s3://$BUCKET/$PREFIX/results/$INSTANCE_ROLE/DONE" --region "$REGION"
echo "Bootstrap finished: $(date -u)"
