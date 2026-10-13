#!/bin/bash
# V3 IoT setup: upload IoT features + 8-model training.py + bootstrap_v3_iot.sh,
# launch 3 EC2 training instances (g5.2xlarge × 2 + c5.4xlarge × 1).
#
# Reuses infra (VPC default SG, key pair, instance profile) created by
# setup_instances.sh.
#
# Usage:
#   export AWS_PROFILE=<your-aws-profile>
#   ./sagemaker/setup_v3_iot.sh           # upload only
#   ./sagemaker/setup_v3_iot.sh --launch  # upload + launch instances

set -euo pipefail

BUCKET="${BUCKET:?set BUCKET to your S3 bucket}"
REGION="us-east-2"
PREFIX="v3/iot"
KEY_NAME="stream-ad-key"
SECURITY_GROUP_NAME="stream-ad-sg"
PROJECT_TAG="stream-ad-v3-iot"

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SAGEMAKER_DIR="$PROJECT_DIR/sagemaker"

echo "=========================================="
echo "V3 IoT: 8-model training launch"
echo "Region:  $REGION"
echo "Bucket:  s3://$BUCKET/$PREFIX/"
echo "=========================================="

# Verify bucket
if ! aws s3api head-bucket --bucket "$BUCKET" --region "$REGION" 2>/dev/null; then
    echo "ERROR: Bucket s3://$BUCKET does not exist. Run setup_instances.sh first."
    exit 1
fi

# Verify the IoT feature files are in S3. sagemaker/preprocessing.py
# --workload iot builds them, then they are uploaded to $PREFIX/features/.
echo ""
echo "Step 1: Verify IoT features in S3..."
for ws in 15 30 60 120 300; do
    if aws s3 ls "s3://$BUCKET/$PREFIX/features/features_${ws}s.parquet" --region "$REGION" >/dev/null 2>&1; then
        echo "  OK  features_${ws}s.parquet"
    else
        echo "  ERROR: features_${ws}s.parquet missing under s3://$BUCKET/$PREFIX/features/"
        echo "  Build and upload them first: python3.12 sagemaker/preprocessing.py --workload iot ..."
        exit 1
    fi
done

# Upload training.py + bootstrap_v3_iot.sh
echo ""
echo "Step 2: Uploading scripts to s3://$BUCKET/$PREFIX/input/..."
aws s3 cp "$SAGEMAKER_DIR/training.py" "s3://$BUCKET/$PREFIX/input/training.py" --region "$REGION"
aws s3 cp "$SAGEMAKER_DIR/bootstrap_v3_iot.sh" "s3://$BUCKET/$PREFIX/input/bootstrap_v3_iot.sh" --region "$REGION"
aws s3 ls "s3://$BUCKET/$PREFIX/input/" --region "$REGION" --human-readable

# Launch?
if [[ "${1:-}" != "--launch" ]]; then
    echo ""
    echo "Dry run complete. Run with --launch to create EC2 instances."
    exit 0
fi

# Resolve AMI + infra
echo ""
echo "Step 3: Resolving AMI + infra..."
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
    echo "ERROR: SG $SECURITY_GROUP_NAME not found. Run setup_instances.sh first."
    exit 1
fi
echo "  SG: $SG_ID"

# Launch instances
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
aws s3 cp "s3://$BUCKET/$PREFIX/input/bootstrap_v3_iot.sh" /home/ubuntu/bootstrap_v3_iot.sh --region "$REGION"
chmod +x /home/ubuntu/bootstrap_v3_iot.sh
su - ubuntu -c "BUCKET=$BUCKET INSTANCE_ROLE=$role /home/ubuntu/bootstrap_v3_iot.sh" &> /home/ubuntu/training.log &
USERDATA
)

    INSTANCE_ID=$(aws ec2 run-instances \
        --image-id "$AMI_ID" \
        --instance-type "$itype" \
        --key-name "$KEY_NAME" \
        --security-group-ids "$SG_ID" \
        --iam-instance-profile Name="$PROFILE_NAME" \
        --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":100,"VolumeType":"gp3"}}]' \
        --user-data "$USER_DATA" \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$name},{Key=Project,Value=$PROJECT_TAG}]" \
        --region "$REGION" \
        --query 'Instances[0].InstanceId' \
        --output text)

    echo "  $name ($itype): $INSTANCE_ID"
}

launch_instance "v3-iot-gpu-1" "gpu-1" "g5.2xlarge"
launch_instance "v3-iot-gpu-2" "gpu-2" "g5.2xlarge"
launch_instance "v3-iot-cpu-1" "cpu-1" "c5.4xlarge"

echo ""
echo "=========================================="
echo "All 3 IoT training instances launched."
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
echo "Terminate when done:"
echo "  aws ec2 describe-instances --region $REGION \\"
echo "    --filters 'Name=tag:Project,Values=$PROJECT_TAG' \\"
echo "    --query 'Reservations[].Instances[].InstanceId' --output text | \\"
echo "    xargs aws ec2 terminate-instances --region $REGION --instance-ids"
