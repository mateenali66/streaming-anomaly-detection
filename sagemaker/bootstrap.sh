#!/bin/bash
# Bootstrap script for EC2 GPU instances (g5.2xlarge with Deep Learning AMI).
# Runs as user-data on instance launch. Installs deps, downloads data, starts training.
#
# Usage: Passed as user-data during instance launch by setup_instances.sh
# Logs: /var/log/cloud-init-output.log and /home/ubuntu/training.log

set -euo pipefail

BUCKET="${BUCKET:?set BUCKET to your S3 bucket}"
REGION="us-east-2"
WORK_DIR="/home/ubuntu/training"
INSTANCE_ROLE="${INSTANCE_ROLE:-gpu-1}"  # gpu-1, gpu-2, or gpu-3

echo "=========================================="
echo "Bootstrap: stream-ad $INSTANCE_ROLE"
echo "Started: $(date -u)"
echo "=========================================="

# Create working directory
mkdir -p "$WORK_DIR"/{input,results,optuna_dbs,models,raw_scores}
cd "$WORK_DIR"

# Download data and scripts from S3
echo "Downloading from s3://$BUCKET/input/..."
aws s3 sync "s3://$BUCKET/input/" "$WORK_DIR/input/" --region "$REGION"
cp "$WORK_DIR/input/training.py" "$WORK_DIR/training.py"

# Use the PyTorch venv from the Deep Learning AMI (has PyTorch + CUDA pre-installed)
PYTHON="/opt/pytorch/bin/python3"
PIP="/opt/pytorch/bin/pip"

echo "Python: $($PYTHON --version)"
echo "Installing Python dependencies..."
$PIP install --quiet optuna pandas scikit-learn scipy pyarrow

# Verify CUDA
$PYTHON -c "import torch; print(f'CUDA: {torch.cuda.is_available()}, Device: {torch.cuda.get_device_name(0) if torch.cuda.is_available() else \"N/A\"}')"

# Setup S3 sync cron (every 30 min)
SYNC_CMD="aws s3 sync $WORK_DIR/results/ s3://$BUCKET/results/$INSTANCE_ROLE/ --region $REGION --quiet"
(crontab -l 2>/dev/null; echo "*/30 * * * * $SYNC_CMD") | crontab -

# Define training commands based on instance role
DATA="$WORK_DIR/input/features_30s.parquet"
OUTPUT="$WORK_DIR/results"

run_model() {
    local model=$1
    local workers=${2:-1}
    echo ""
    echo "=========================================="
    echo "Starting: $model (workers=$workers)"
    echo "Time: $(date -u)"
    echo "=========================================="
    $PYTHON "$WORK_DIR/training.py" \
        --model "$model" \
        --data "$DATA" \
        --output "$OUTPUT" \
        --n-workers "$workers" \
        2>&1 | tee -a "$WORK_DIR/training_${model}.log"
    echo "Completed: $model at $(date -u)"
    # Sync results after each model
    $SYNC_CMD
}

case "$INSTANCE_ROLE" in
    gpu-1)
        # Deep models A: LSTM-AE + Trans-AE (4 Optuna workers, full A10G)
        run_model LSTM_AE 4
        run_model Trans_AE 4
        ;;
    gpu-2)
        # Deep models B: CNN-AE + LSTM-VAE (4 Optuna workers, full A10G)
        run_model CNN_AE 4
        run_model LSTM_VAE 4
        ;;
    gpu-3)
        # Classical models (CPU-bound, sklearn uses all cores internally)
        run_model IF 1
        run_model OC_SVM 1
        run_model RF 1
        run_model RuleBased 1

        # Sensitivity: window sizes (15s, 60s, 120s) on best classical model (IF)
        echo ""
        echo "=========================================="
        echo "Sensitivity: Window sizes"
        echo "=========================================="
        for ws in 15 60 120; do
            $PYTHON "$WORK_DIR/training.py" \
                --model IF \
                --data "$WORK_DIR/input/features_${ws}s.parquet" \
                --output "$OUTPUT/sensitivity_ws_${ws}s" \
                2>&1 | tee -a "$WORK_DIR/training_sensitivity_ws.log"
        done
        $SYNC_CMD

        # RQ2 Ablation: category (single + leave-one-out) on IF
        echo ""
        echo "=========================================="
        echo "RQ2 Ablation: Category"
        echo "=========================================="
        for cat in broker consumer flink jvm; do
            # Single category
            $PYTHON "$WORK_DIR/training.py" \
                --model IF \
                --data "$DATA" \
                --output "$OUTPUT/ablation_category" \
                --category "$cat" \
                2>&1 | tee -a "$WORK_DIR/training_ablation.log"
            # Leave-one-out
            $PYTHON "$WORK_DIR/training.py" \
                --model IF \
                --data "$DATA" \
                --output "$OUTPUT/ablation_category" \
                --exclude-category "$cat" \
                2>&1 | tee -a "$WORK_DIR/training_ablation.log"
        done
        $SYNC_CMD
        ;;
    *)
        echo "ERROR: Unknown INSTANCE_ROLE: $INSTANCE_ROLE"
        exit 1
        ;;
esac

# Final S3 sync
echo ""
echo "=========================================="
echo "All training complete. Final sync..."
echo "=========================================="
$SYNC_CMD

# Signal completion
touch "$WORK_DIR/DONE"
aws s3 cp "$WORK_DIR/DONE" "s3://$BUCKET/results/$INSTANCE_ROLE/DONE" --region "$REGION"

echo "Bootstrap finished: $(date -u)"
