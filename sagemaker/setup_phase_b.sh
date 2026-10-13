#!/bin/bash
# Setup for Phase B, the e-commerce run at 500 events/s.
# Uploads features + code to S3, launches 3 EC2 training instances.
#
# Usage:
#   export AWS_PROFILE=<your-aws-profile>
#   ./sagemaker/setup_phase_b.sh           # upload only
#   ./sagemaker/setup_phase_b.sh --launch  # upload + launch instances

set -euo pipefail

BUCKET="${BUCKET:?set BUCKET to your S3 bucket}"
REGION="us-east-2"
PREFIX="v3/phase_b"
KEY_NAME="stream-ad-key"
SECURITY_GROUP_NAME="stream-ad-sg"
PROJECT_TAG="stream-ad-phase-b"

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SAGEMAKER_DIR="$PROJECT_DIR/sagemaker"
DATA_DIR="$PROJECT_DIR/data/features_v3_phase_b"

echo "=========================================="
echo "Phase B: Setup Training Infrastructure"
echo "Region:  $REGION"
echo "Bucket:  s3://$BUCKET/$PREFIX/"
echo "=========================================="

# --- Step 1: Verify bucket exists ---
echo ""
echo "Step 1: Verify S3 bucket..."
if aws s3api head-bucket --bucket "$BUCKET" --region "$REGION" 2>/dev/null; then
    echo "  Bucket exists: s3://$BUCKET"
else
    echo "ERROR: Bucket s3://$BUCKET does not exist."
    exit 1
fi

# --- Step 2: Upload data and scripts ---
echo ""
echo "Step 2: Uploading data and scripts to s3://$BUCKET/$PREFIX/input/..."

FEATURE_FILE="$DATA_DIR/features_30s.parquet"
if [ -f "$FEATURE_FILE" ]; then
    SIZE=$(ls -lh "$FEATURE_FILE" | awk '{print $5}')
    echo "  Uploading features_30s.parquet ($SIZE)..."
    aws s3 cp "$FEATURE_FILE" "s3://$BUCKET/$PREFIX/input/features_30s.parquet" --region "$REGION"
else
    echo "ERROR: $FEATURE_FILE not found"
    exit 1
fi

echo "  Uploading training.py..."
aws s3 cp "$SAGEMAKER_DIR/training.py" "s3://$BUCKET/$PREFIX/input/training.py" --region "$REGION"

echo "  Uploading bootstrap_phase_b.sh..."
aws s3 cp "$SAGEMAKER_DIR/bootstrap_phase_b.sh" "s3://$BUCKET/$PREFIX/input/bootstrap_phase_b.sh" --region "$REGION"

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

VPC_ID=$(aws ec2 describe-vpcs --region "$REGION" --filters "Name=isDefault,Values=true" --query 'Vpcs[0].VpcId' --output text)
SG_ID=$(aws ec2 describe-security-groups \
    --region "$REGION" \
    --filters "Name=group-name,Values=$SECURITY_GROUP_NAME" "Name=vpc-id,Values=$VPC_ID" \
    --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || echo "None")
PROFILE_NAME="stream-ad-ec2-profile"

if [ "$SG_ID" = "None" ] || [ -z "$SG_ID" ]; then
    echo "ERROR: Security group $SECURITY_GROUP_NAME not found."
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
aws s3 cp "s3://$BUCKET/$PREFIX/input/bootstrap_phase_b.sh" /home/ubuntu/bootstrap_phase_b.sh --region "$REGION"
chmod +x /home/ubuntu/bootstrap_phase_b.sh
su - ubuntu -c "BUCKET=$BUCKET INSTANCE_ROLE=$role /home/ubuntu/bootstrap_phase_b.sh" &> /home/ubuntu/training.log &
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
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$name},{Key=Project,Value=$PROJECT_TAG}]" \
        --region "$REGION" \
        --query 'Instances[0].InstanceId' \
        --output text)

    echo "  $name ($itype): $INSTANCE_ID"
}

launch_instance "phase-b-gpu-1" "gpu-1" "g5.2xlarge"
launch_instance "phase-b-gpu-2" "gpu-2" "g5.2xlarge"
launch_instance "phase-b-cpu-1" "cpu-1" "c5.4xlarge"

echo ""
echo "=========================================="
echo "All 3 Phase B instances launched."
echo "=========================================="
echo ""
echo "Monitor:"
echo "  aws ec2 describe-instances --region $REGION \\"
echo "    --filters 'Name=tag:Project,Values=$PROJECT_TAG' 'Name=instance-state-name,Values=running' \\"
echo "    --query 'Reservations[].Instances[].[Tags[?Key==\`Name\`].Value|[0],InstanceId,PublicIpAddress,State.Name]' \\"
echo "    --output table"
echo ""
echo "Check completion:"
echo "  aws s3 ls s3://$BUCKET/$PREFIX/results/ --region $REGION --recursive | grep DONE"
echo ""
echo "Terminate:"
echo "  aws ec2 describe-instances --region $REGION \\"
echo "    --filters 'Name=tag:Project,Values=$PROJECT_TAG' 'Name=instance-state-name,Values=running' \\"
echo "    --query 'Reservations[].Instances[].InstanceId' --output text | \\"
echo "    xargs aws ec2 terminate-instances --region $REGION --instance-ids"
