#!/bin/bash
# V3 LOFO sanity-check launcher.
#
# Uploads training.py + bootstrap_v3_lofo.sh to S3, then launches
# three g5.xlarge instances, one per held-out fault (F1 broker / F4 consumer
# / F7 Flink). Each instance retrains the 8-model panel on e-commerce 30s
# features with the held-out fault dropped from train/hpo_val/cal and the
# test set restricted to NORMAL/RECOVERY plus that fault.
#
# Reuses VPC default SG / key pair / instance profile created by
# setup_instances.sh.
#
# Usage:
#   export AWS_PROFILE=<your-aws-profile>
#   ./sagemaker/setup_v3_lofo.sh           # upload only (dry run)
#   ./sagemaker/setup_v3_lofo.sh --launch  # upload + launch 3 g5.xlarge

set -euo pipefail

BUCKET="${BUCKET:?set BUCKET to your S3 bucket}"
REGION="us-east-2"
PREFIX="v3"
KEY_NAME="stream-ad-key"
SECURITY_GROUP_NAME="stream-ad-sg"
PROJECT_TAG="stream-ad-v3-lofo"
INSTANCE_TYPE="${INSTANCE_TYPE:-g5.xlarge}"

HELD_OUT_FAULTS=(F1 F4 F7)

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SAGEMAKER_DIR="$PROJECT_DIR/sagemaker"

echo "=========================================="
echo "V3 LOFO sanity-check launcher"
echo "Region:       $REGION"
echo "Bucket:       s3://$BUCKET/$PREFIX/"
echo "Faults:       ${HELD_OUT_FAULTS[*]}"
echo "Instances:    ${#HELD_OUT_FAULTS[@]} x $INSTANCE_TYPE"
echo "=========================================="

# Verify e-commerce 30s features exist
if ! aws s3 ls "s3://$BUCKET/$PREFIX/input/features_30s.parquet" --region "$REGION" >/dev/null 2>&1; then
    echo "ERROR: s3://$BUCKET/$PREFIX/input/features_30s.parquet missing."
    exit 1
fi

# Upload training.py + bootstrap_v3_lofo.sh
echo ""
echo "Step 1: Uploading scripts to s3://$BUCKET/$PREFIX/iot/input/..."
aws s3 cp "$SAGEMAKER_DIR/training.py"          "s3://$BUCKET/$PREFIX/iot/input/training.py"          --region "$REGION"
aws s3 cp "$SAGEMAKER_DIR/bootstrap_v3_lofo.sh" "s3://$BUCKET/$PREFIX/iot/input/bootstrap_v3_lofo.sh" --region "$REGION"

if [[ "${1:-}" != "--launch" ]]; then
    echo ""
    echo "Dry run complete. Re-run with --launch to spin up ${#HELD_OUT_FAULTS[@]} x $INSTANCE_TYPE."
    exit 0
fi

# Resolve AMI + infra
echo ""
echo "Step 2: Resolving AMI + infra..."
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
echo "  Instance profile: $PROFILE_NAME"

echo ""
echo "Step 3: Launching ${#HELD_OUT_FAULTS[@]} x $INSTANCE_TYPE..."

launch_lofo_instance() {
    local fault=$1
    local name="v3-lofo-${fault}"
    local role="lofo-${fault}"

    USER_DATA=$(cat <<USERDATA
#!/bin/bash
export INSTANCE_ROLE="$role"
export HELD_OUT="$fault"
export AWS_DEFAULT_REGION="$REGION"
aws s3 cp "s3://$BUCKET/$PREFIX/iot/input/bootstrap_v3_lofo.sh" /home/ubuntu/bootstrap_v3_lofo.sh --region "$REGION"
chmod +x /home/ubuntu/bootstrap_v3_lofo.sh
su - ubuntu -c "BUCKET=$BUCKET INSTANCE_ROLE=$role HELD_OUT=$fault /home/ubuntu/bootstrap_v3_lofo.sh" &> /home/ubuntu/training.log &
USERDATA
)

    INSTANCE_ID=$(aws ec2 run-instances \
        --image-id "$AMI_ID" \
        --instance-type "$INSTANCE_TYPE" \
        --key-name "$KEY_NAME" \
        --security-group-ids "$SG_ID" \
        --iam-instance-profile Name="$PROFILE_NAME" \
        --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":100,"VolumeType":"gp3"}}]' \
        --user-data "$USER_DATA" \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$name},{Key=Project,Value=$PROJECT_TAG},{Key=HeldOut,Value=$fault}]" \
        --region "$REGION" \
        --query 'Instances[0].InstanceId' \
        --output text)

    echo "  $name ($INSTANCE_TYPE, held_out=$fault): $INSTANCE_ID"
}

for FAULT in "${HELD_OUT_FAULTS[@]}"; do
    launch_lofo_instance "$FAULT"
done

echo ""
echo "=========================================="
echo "All ${#HELD_OUT_FAULTS[@]} LOFO instances launched"
echo "=========================================="
echo ""
echo "Monitor:"
echo "  aws ec2 describe-instances --region $REGION \\"
echo "    --filters 'Name=tag:Project,Values=$PROJECT_TAG' 'Name=instance-state-name,Values=running' \\"
echo "    --query 'Reservations[].Instances[].[Tags[?Key==\`Name\`].Value|[0],InstanceId,PublicIpAddress,State.Name]' \\"
echo "    --output table"
echo ""
echo "Check completion (each instance writes results/lofo-<fault>/DONE):"
echo "  aws s3 ls s3://$BUCKET/$PREFIX/results/ --region $REGION --recursive | grep DONE"
echo ""
echo "Terminate when done:"
echo "  aws ec2 describe-instances --region $REGION \\"
echo "    --filters 'Name=tag:Project,Values=$PROJECT_TAG' \\"
echo "    --query 'Reservations[].Instances[].InstanceId' --output text | \\"
echo "    xargs aws ec2 terminate-instances --region $REGION --instance-ids"
