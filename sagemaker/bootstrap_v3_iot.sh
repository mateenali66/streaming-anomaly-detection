#!/bin/bash
# V3 IoT Bootstrap: 8-model panel × 5-window sensitivity grid.
#
# Mirrors bootstrap_v3.sh but pulls from s3://$BUCKET/v3/iot/features/
# and adds DeepSVDD + DAGMM for cross-workload analysis.
#
# Instance roles:
#   gpu-1: LSTM_AE, Trans_AE, CNN_AE  × 30s primary + 15/60/120/300s sensitivity
#   gpu-2: LSTM_VAE, DeepSVDD, DAGMM  × 30s primary + 15/60/120/300s sensitivity
#   cpu-1: IF, OC_SVM, RF, RuleBased  × 30s primary + 15/60/120/300s sensitivity
#
# n-trials: 100 at 30s (anchor / HPO) + 30 at sensitivity windows (sweep)

set -euo pipefail

BUCKET="${BUCKET:?set BUCKET to your S3 bucket}"
REGION="us-east-2"
PREFIX="v3/iot"
WORK_DIR="/home/ubuntu/training"
INSTANCE_ROLE="${INSTANCE_ROLE:-gpu-1}"

PRIMARY_WS=30
SENSITIVITY_WS=(15 60 120 300)
TRIALS_PRIMARY=100
TRIALS_SENS=30

echo "=========================================="
echo "V3 IoT Bootstrap: $INSTANCE_ROLE"
echo "Started: $(date -u)"
echo "Primary window: ${PRIMARY_WS}s, trials=$TRIALS_PRIMARY"
echo "Sensitivity windows: ${SENSITIVITY_WS[*]}, trials=$TRIALS_SENS"
echo "=========================================="

mkdir -p "$WORK_DIR"/{input,results}
cd "$WORK_DIR"

# Download features + scripts
echo "Downloading from s3://$BUCKET/$PREFIX/..."
aws s3 sync "s3://$BUCKET/$PREFIX/features/" "$WORK_DIR/input/" --region "$REGION"
aws s3 cp "s3://$BUCKET/$PREFIX/input/training.py" "$WORK_DIR/training.py" --region "$REGION"

# Python setup
PYTHON="/opt/pytorch/bin/python3"
PIP="/opt/pytorch/bin/pip"
echo "Python: $($PYTHON --version)"
echo "Installing dependencies..."
$PIP install --quiet optuna pandas scikit-learn scipy pyarrow 2>&1 | tail -3

# CUDA check for GPU instances
if [[ "$INSTANCE_ROLE" == gpu-* ]]; then
    $PYTHON -c "import torch; print(f'CUDA: {torch.cuda.is_available()}, GPU: {torch.cuda.get_device_name(0) if torch.cuda.is_available() else \"N/A\"}')"
fi

# Periodic S3 sync (every 15 min) so partial results survive instance crash
SYNC_CMD="aws s3 sync $WORK_DIR/results/ s3://$BUCKET/$PREFIX/results/$INSTANCE_ROLE/ --region $REGION --quiet"
{ crontab -l 2>/dev/null || true; echo "*/15 * * * * $SYNC_CMD"; } | crontab -

sync_results() {
    aws s3 sync "$WORK_DIR/results/" "s3://$BUCKET/$PREFIX/results/$INSTANCE_ROLE/" \
        --region "$REGION" --quiet 2>/dev/null || true
}

run_model() {
    local model=$1
    local data=$2
    local output=$3
    local n_trials=$4
    echo ""
    echo "=========================================="
    echo "Model: $model | Data: $(basename $data) | n_trials=$n_trials | Output: $output"
    echo "Time: $(date -u)"
    echo "=========================================="
    $PYTHON "$WORK_DIR/training.py" \
        --model "$model" \
        --data "$data" \
        --output "$output" \
        --n-trials "$n_trials" \
        --n-workers 1 \
        2>&1 | tee -a "$WORK_DIR/training_${model}_$(basename $data .parquet).log"
    echo "Completed: $model on $(basename $data) at $(date -u)"
    sync_results
}

DATA_PRIMARY="$WORK_DIR/input/features_${PRIMARY_WS}s.parquet"
OUTPUT="$WORK_DIR/results"

case "$INSTANCE_ROLE" in
    gpu-1)
        MODELS=(LSTM_AE Trans_AE CNN_AE)
        ;;
    gpu-2)
        MODELS=(LSTM_VAE DeepSVDD DAGMM)
        ;;
    cpu-1)
        MODELS=(IF OC_SVM RF RuleBased)
        ;;
    *)
        echo "ERROR: Unknown INSTANCE_ROLE: $INSTANCE_ROLE"
        exit 1
        ;;
esac

# Primary: 30s anchor with full HPO budget
echo ""
echo "### Primary 30s anchor (n_trials=$TRIALS_PRIMARY) ###"
for MODEL in "${MODELS[@]}"; do
    run_model "$MODEL" "$DATA_PRIMARY" "$OUTPUT" "$TRIALS_PRIMARY"
done

# Sensitivity sweep at 15/60/120/300s with reduced HPO budget
for ws in "${SENSITIVITY_WS[@]}"; do
    echo ""
    echo "### Sensitivity window=${ws}s (n_trials=$TRIALS_SENS) ###"
    DATA_SENS="$WORK_DIR/input/features_${ws}s.parquet"
    if [ ! -f "$DATA_SENS" ]; then
        echo "WARNING: $DATA_SENS not found, skipping"
        continue
    fi
    for MODEL in "${MODELS[@]}"; do
        run_model "$MODEL" "$DATA_SENS" "$OUTPUT/sensitivity_ws_${ws}s" "$TRIALS_SENS"
    done
done

echo ""
echo "=========================================="
echo "All training complete for $INSTANCE_ROLE"
echo "Final S3 sync..."
echo "=========================================="
sync_results

touch "$WORK_DIR/DONE"
aws s3 cp "$WORK_DIR/DONE" "s3://$BUCKET/$PREFIX/results/$INSTANCE_ROLE/DONE" --region "$REGION"

echo "Bootstrap finished: $(date -u)"
