#!/bin/bash
# V3 setup for Phase A, the e-commerce run at 5,000 events/s.
# Uploads features + code to S3, launches 3 EC2 training instances.
#
# Run layout:
#   - 10-fold CV on data/features_v3, S3 prefix v3/
#   - gpu-1: LSTM_AE + Trans_AE + CNN_AE (primary + sensitivity)
#   - gpu-2: LSTM_VAE (primary + sensitivity) + ablation
#   - cpu-1: IF + OC_SVM + RF + RuleBased (primary + sensitivity) on c5.4xlarge
#
# Usage:
#   export AWS_PROFILE=<your-aws-profile>
#   ./sagemaker/setup_v3.sh           # upload only
#   ./sagemaker/setup_v3.sh --launch  # upload + launch instances

set -euo pipefail

BUCKET="${BUCKET:?set BUCKET to your S3 bucket}"
REGION="us-east-2"
PREFIX="v3"
KEY_NAME="stream-ad-key"
SECURITY_GROUP_NAME="stream-ad-sg"

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SAGEMAKER_DIR="$PROJECT_DIR/sagemaker"
DATA_DIR="$PROJECT_DIR/data/features_v3"

echo "=========================================="
echo "V3: Setup Training Infrastructure"
echo "Region:  $REGION"
echo "Bucket:  s3://$BUCKET/$PREFIX/"
echo "=========================================="

# --- Step 1: Verify bucket exists ---
echo ""
echo "Step 1: Verify S3 bucket..."
if aws s3api head-bucket --bucket "$BUCKET" --region "$REGION" 2>/dev/null; then
    echo "  Bucket exists: s3://$BUCKET"
else
    echo "ERROR: Bucket s3://$BUCKET does not exist. Run setup_instances.sh first."
    exit 1
fi

# --- Step 2: Upload data and scripts ---
echo ""
echo "Step 2: Uploading data and scripts to s3://$BUCKET/$PREFIX/input/..."

for ws in 15 30 60 120; do
    FEATURE_FILE="$DATA_DIR/features_${ws}s.parquet"
    if [ -f "$FEATURE_FILE" ]; then
        SIZE=$(ls -lh "$FEATURE_FILE" | awk '{print $5}')
        echo "  Uploading features_${ws}s.parquet ($SIZE)..."
        aws s3 cp "$FEATURE_FILE" "s3://$BUCKET/$PREFIX/input/features_${ws}s.parquet" --region "$REGION"
    else
        echo "  WARNING: $FEATURE_FILE not found, skipping"
    fi
done

echo "  Uploading training.py..."
aws s3 cp "$SAGEMAKER_DIR/training.py" "s3://$BUCKET/$PREFIX/input/training.py" --region "$REGION"

echo "  Uploading bootstrap_v3.sh..."
aws s3 cp "$SAGEMAKER_DIR/bootstrap_v3.sh" "s3://$BUCKET/$PREFIX/input/bootstrap_v3.sh" --region "$REGION"

echo ""
echo "  Upload complete:"
aws s3 ls "s3://$BUCKET/$PREFIX/input/" --region "$REGION" --human-readable

# --- Step 3: Launch instances ---
if [[ "${1:-}" != "--launch" ]]; then
    echo ""
    echo "Dry run complete. Run with --launch to create EC2 instances."
    exit 0
fi

echo ""
echo "Step 3: Resolving AMI..."
AMI_ID=$(aws ec2 describe-images \
    --region "$REGION" \
    --owners amazon \
    --filters "Name=name,Values=Deep Learning OSS Nvidia Driver AMI GPU PyTorch*Ubuntu 22.04*" \
    --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' \
    --output text)
echo "  AMI: $AMI_ID"

# Resolve existing infra
VPC_ID=$(aws ec2 describe-vpcs --region "$REGION" --filters "Name=isDefault,Values=true" --query 'Vpcs[0].VpcId' --output text)
SG_ID=$(aws ec2 describe-security-groups \
    --region "$REGION" \
    --filters "Name=group-name,Values=$SECURITY_GROUP_NAME" "Name=vpc-id,Values=$VPC_ID" \
    --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || echo "None")
PROFILE_NAME="stream-ad-ec2-profile"

if [ "$SG_ID" = "None" ] || [ -z "$SG_ID" ]; then
    echo "ERROR: Security group $SECURITY_GROUP_NAME not found. Run setup_instances.sh first."
    exit 1
fi

echo "  SG: $SG_ID"
echo ""
echo "Step 4: Launching instances..."

launch_instance() {
    local name=$1
    local role=$2
    local itype=$3

    USER_DATA=$(cat <<USERDATA
#!/bin/bash
export INSTANCE_ROLE="$role"
export AWS_DEFAULT_REGION="$REGION"
aws s3 cp "s3://$BUCKET/$PREFIX/input/bootstrap_v3.sh" /home/ubuntu/bootstrap_v3.sh --region "$REGION"
chmod +x /home/ubuntu/bootstrap_v3.sh
su - ubuntu -c "BUCKET=$BUCKET INSTANCE_ROLE=$role /home/ubuntu/bootstrap_v3.sh" &> /home/ubuntu/training.log &
USERDATA
)

    INSTANCE_ID=$(aws ec2 run-instances \
        --image-id "$AMI_ID" \
        --instance-type "$itype" \
        --key-name "$KEY_NAME" \
        --security-group-ids "$SG_ID" \
        --iam-instance-profile Name="$PROFILE_NAME" \
        --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":50,"VolumeType":"gp3"}}]' \
        --user-data "$USER_DATA" \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$name},{Key=Project,Value=stream-ad-v3}]" \
        --region "$REGION" \
        --query 'Instances[0].InstanceId' \
        --output text)

    echo "  $name ($itype): $INSTANCE_ID"
}

launch_instance "v3-gpu-1" "gpu-1" "g5.2xlarge"
launch_instance "v3-gpu-2" "gpu-2" "g5.2xlarge"
launch_instance "v3-cpu-1" "cpu-1" "c5.4xlarge"

echo ""
echo "=========================================="
echo "All 3 instances launched."
echo "=========================================="
echo ""
echo "Monitor:"
echo "  aws ec2 describe-instances --region $REGION \\"
echo "    --filters 'Name=tag:Project,Values=stream-ad-v3' 'Name=instance-state-name,Values=running' \\"
echo "    --query 'Reservations[].Instances[].[Tags[?Key==\`Name\`].Value|[0],InstanceId,PublicIpAddress,State.Name]' \\"
echo "    --output table"
echo ""
echo "Check completion:"
echo "  aws s3 ls s3://$BUCKET/$PREFIX/results/ --region $REGION --recursive | grep DONE"
echo ""
echo "Terminate:"
echo "  aws ec2 describe-instances --region $REGION \\"
echo "    --filters 'Name=tag:Project,Values=stream-ad-v3' \\"
echo "    --query 'Reservations[].Instances[].InstanceId' --output text | \\"
echo "    xargs aws ec2 terminate-instances --region $REGION --instance-ids"
