#!/bin/bash
# V3 Bootstrap: 10-fold nested CV, all 8 models at all window sizes.
#
# Instance roles:
#   gpu-1: LSTM_AE, Trans_AE, CNN_AE (primary 30s + sensitivity 15/60/120s)
#   gpu-2: LSTM_VAE (primary + sensitivity) + ablation (IF on 4 categories)
#   cpu-1: IF, OC_SVM, RF, RuleBased (primary + sensitivity)

set -euo pipefail

BUCKET="${BUCKET:?set BUCKET to your S3 bucket}"
REGION="us-east-2"
PREFIX="v3"
WORK_DIR="/home/ubuntu/training"
INSTANCE_ROLE="${INSTANCE_ROLE:-gpu-1}"

echo "=========================================="
echo "V3 Bootstrap: $INSTANCE_ROLE"
echo "Started: $(date -u)"
echo "=========================================="

mkdir -p "$WORK_DIR"/{input,results}
cd "$WORK_DIR"

# Download data and scripts
echo "Downloading from s3://$BUCKET/$PREFIX/input/..."
aws s3 sync "s3://$BUCKET/$PREFIX/input/" "$WORK_DIR/input/" --region "$REGION"
cp "$WORK_DIR/input/training.py" "$WORK_DIR/training.py"

# Python setup
PYTHON="/opt/pytorch/bin/python3"
PIP="/opt/pytorch/bin/pip"

echo "Python: $($PYTHON --version)"
echo "Installing dependencies..."
$PIP install --quiet optuna pandas scikit-learn scipy pyarrow 2>&1 | tail -3

# Verify CUDA for GPU instances
if [[ "$INSTANCE_ROLE" == gpu-* ]]; then
    $PYTHON -c "import torch; print(f'CUDA: {torch.cuda.is_available()}, GPU: {torch.cuda.get_device_name(0) if torch.cuda.is_available() else \"N/A\"}')"
fi

# S3 sync function (called after each model)
sync_results() {
    aws s3 sync "$WORK_DIR/results/" "s3://$BUCKET/$PREFIX/results/$INSTANCE_ROLE/" \
        --region "$REGION" --quiet 2>/dev/null || true
}

# Setup periodic sync (every 15 min)
SYNC_CMD="aws s3 sync $WORK_DIR/results/ s3://$BUCKET/$PREFIX/results/$INSTANCE_ROLE/ --region $REGION --quiet"
{ crontab -l 2>/dev/null || true; echo "*/15 * * * * $SYNC_CMD"; } | crontab -

# Training wrapper
run_model() {
    local model=$1
    local data=$2
    local output=$3
    local extra_args="${4:-}"

    echo ""
    echo "=========================================="
    echo "Model: $model | Data: $(basename $data) | Output: $output"
    echo "Time: $(date -u)"
    echo "=========================================="

    $PYTHON "$WORK_DIR/training.py" \
        --model "$model" \
        --data "$data" \
        --output "$output" \
        --n-workers 1 \
        $extra_args \
        2>&1 | tee -a "$WORK_DIR/training_${model}_$(basename $data .parquet).log"

    echo "Completed: $model at $(date -u)"
    sync_results
}

DATA_30="$WORK_DIR/input/features_30s.parquet"
OUTPUT="$WORK_DIR/results"

case "$INSTANCE_ROLE" in
    gpu-1)
        # Primary 30s: 3 deep models x 10 folds
        for MODEL in LSTM_AE Trans_AE CNN_AE; do
            run_model "$MODEL" "$DATA_30" "$OUTPUT"
        done

        # Sensitivity: 3 deep models x 3 window sizes x 10 folds
        for ws in 15 60 120; do
            for MODEL in LSTM_AE Trans_AE CNN_AE; do
                run_model "$MODEL" "$WORK_DIR/input/features_${ws}s.parquet" \
                    "$OUTPUT/sensitivity_ws_${ws}s"
            done
        done
        ;;

    gpu-2)
        # Primary 30s: LSTM_VAE x 10 folds
        run_model LSTM_VAE "$DATA_30" "$OUTPUT"

        # Sensitivity: LSTM_VAE x 3 window sizes x 10 folds
        for ws in 15 60 120; do
            run_model LSTM_VAE "$WORK_DIR/input/features_${ws}s.parquet" \
                "$OUTPUT/sensitivity_ws_${ws}s"
        done

        # RQ3 Ablation: IF on single categories + leave-one-out
        echo ""
        echo "=========================================="
        echo "RQ3 Ablation: Category (IF model)"
        echo "=========================================="
        for cat in broker consumer flink jvm; do
            run_model IF "$DATA_30" "$OUTPUT/ablation_category" "--category $cat"
            run_model IF "$DATA_30" "$OUTPUT/ablation_category" "--exclude-category $cat"
        done

        # RQ3 Ablation: derived feature exclusion (IF + RF)
        for MODEL in IF RF; do
            run_model "$MODEL" "$DATA_30" "$OUTPUT/ablation_derived" "--exclude-derived"
        done
        ;;

    cpu-1)
        # Primary 30s: 4 classical/baseline models x 10 folds
        for MODEL in IF OC_SVM RF RuleBased; do
            run_model "$MODEL" "$DATA_30" "$OUTPUT"
        done

        # Sensitivity: 4 models x 3 window sizes x 10 folds
        for ws in 15 60 120; do
            for MODEL in IF OC_SVM RF RuleBased; do
                run_model "$MODEL" "$WORK_DIR/input/features_${ws}s.parquet" \
                    "$OUTPUT/sensitivity_ws_${ws}s"
            done
        done
        ;;

    *)
        echo "ERROR: Unknown INSTANCE_ROLE: $INSTANCE_ROLE"
        exit 1
        ;;
esac

# Final sync + completion signal
echo ""
echo "=========================================="
echo "All training complete for $INSTANCE_ROLE"
echo "Final S3 sync..."
echo "=========================================="
sync_results

touch "$WORK_DIR/DONE"
aws s3 cp "$WORK_DIR/DONE" "s3://$BUCKET/$PREFIX/results/$INSTANCE_ROLE/DONE" --region "$REGION"

echo "Bootstrap finished: $(date -u)"
