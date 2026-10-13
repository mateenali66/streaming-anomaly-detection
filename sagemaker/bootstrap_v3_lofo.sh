#!/bin/bash
# V3 e-commerce LOFO sanity-check.
#
# For each held-out fault, retrain the 8-model panel with anomalous windows
# of that fault dropped from train/hpo_val/cal and the test set restricted
# to NORMAL/RECOVERY plus that fault. Validates whether threshold-only LOFO
# (the post-hoc analysis in analysis/cross_workload/calibrated_threshold_tables.py)
# is a faithful proxy for full-HPO LOFO.
#
# Held-out faults: F1 (broker crash), F4 (consumer lag spike) and F7 (Flink
# checkpoint failure and backpressure). One fault per category from the taxonomy.
#
# Each held-out fault runs 8 models x 5 folds x 30 trials. Launch one
# instance per fault in parallel, about 3 to 4 hours of wall time per instance.
#
# Usage on the EC2 instance (set BUCKET and HELD_OUT before invoking):
#   BUCKET=<your-bucket> HELD_OUT=F1 INSTANCE_ROLE=lofo-F1 bash bootstrap_v3_lofo.sh

set -euo pipefail

BUCKET="${BUCKET:?set BUCKET to your S3 bucket}"
REGION="us-east-2"
PREFIX="v3"
WORK_DIR="/home/ubuntu/training"
INSTANCE_ROLE="${INSTANCE_ROLE:-lofo-${HELD_OUT:?HELD_OUT must be set to F1..F8}}"
HELD_OUT="${HELD_OUT:?HELD_OUT must be set to F1..F8}"

TRIALS="${TRIALS:-30}"
FOLDS="${FOLDS:-5}"
MODELS=(IF OC_SVM LSTM_AE Trans_AE CNN_AE LSTM_VAE DeepSVDD DAGMM)

echo "=========================================="
echo "V3 e-commerce LOFO sanity-check"
echo "Started:    $(date -u)"
echo "Held-out:   $HELD_OUT"
echo "Trials:     $TRIALS"
echo "Folds:      1..$FOLDS"
echo "Models:     ${MODELS[*]}"
echo "Output S3:  s3://$BUCKET/$PREFIX/results/$INSTANCE_ROLE/"
echo "=========================================="

mkdir -p "$WORK_DIR"/{input,results}
cd "$WORK_DIR"

echo "Downloading e-commerce 30s features + scripts..."
aws s3 cp "s3://$BUCKET/$PREFIX/input/features_30s.parquet" "$WORK_DIR/input/features_30s.parquet" --region "$REGION"
aws s3 cp "s3://$BUCKET/$PREFIX/iot/input/training.py" "$WORK_DIR/training.py" --region "$REGION"

PYTHON="/opt/pytorch/bin/python3"
PIP="/opt/pytorch/bin/pip"
echo "Python: $($PYTHON --version)"
$PIP install --quiet optuna pandas scikit-learn scipy pyarrow 2>&1 | tail -3

$PYTHON -c "import torch; print(f'CUDA: {torch.cuda.is_available()}, GPU: {torch.cuda.get_device_name(0) if torch.cuda.is_available() else \"N/A\"}')"

# Check that the uploaded training.py supports --lofo-held-out
if ! $PYTHON "$WORK_DIR/training.py" --help 2>&1 | grep -q -- "--lofo-held-out"; then
    echo "ERROR: training.py does not expose --lofo-held-out. Upload sagemaker/training.py from this repository to s3://$BUCKET/$PREFIX/iot/input/training.py."
    exit 1
fi

SYNC_CMD="aws s3 sync $WORK_DIR/results/ s3://$BUCKET/$PREFIX/results/$INSTANCE_ROLE/ --region $REGION --quiet"
{ crontab -l 2>/dev/null || true; echo "*/15 * * * * $SYNC_CMD"; } | crontab -

sync_results() {
    aws s3 sync "$WORK_DIR/results/" "s3://$BUCKET/$PREFIX/results/$INSTANCE_ROLE/" \
        --region "$REGION" --quiet 2>/dev/null || true
}

DATA="$WORK_DIR/input/features_30s.parquet"
OUTPUT="$WORK_DIR/results"

for MODEL in "${MODELS[@]}"; do
    for FOLD in $(seq 1 "$FOLDS"); do
        echo ""
        echo "=========================================="
        echo "Model: $MODEL | Fold: $FOLD | Held-out: $HELD_OUT | Trials: $TRIALS"
        echo "Time: $(date -u)"
        echo "=========================================="
        $PYTHON "$WORK_DIR/training.py" \
            --model "$MODEL" \
            --data "$DATA" \
            --output "$OUTPUT" \
            --fold "$FOLD" \
            --n-trials "$TRIALS" \
            --n-workers 1 \
            --lofo-held-out "$HELD_OUT" \
            2>&1 | tee -a "$WORK_DIR/training_${MODEL}_lofo_${HELD_OUT}_fold${FOLD}.log"
        echo "Completed: $MODEL fold $FOLD held_out=$HELD_OUT at $(date -u)"
        sync_results
    done
done

echo ""
echo "=========================================="
echo "LOFO held_out=$HELD_OUT complete"
echo "Final S3 sync..."
echo "=========================================="
sync_results

touch "$WORK_DIR/DONE"
aws s3 cp "$WORK_DIR/DONE" "s3://$BUCKET/$PREFIX/results/$INSTANCE_ROLE/DONE" --region "$REGION"
echo "Bootstrap finished: $(date -u)"
