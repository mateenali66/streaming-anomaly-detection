#!/bin/bash
# V3 ecom DSVDD/DAGMM multi-window gap-fill launcher.
# Launches a single g5.xlarge that trains 2 models x 3 windows x 10 folds x 30 trials.

set -euo pipefail

BUCKET="${BUCKET:?set BUCKET to your S3 bucket}"
REGION="us-east-2"
PREFIX="v3"
KEY_NAME="stream-ad-key"
SECURITY_GROUP_NAME="stream-ad-sg"
PROJECT_TAG="stream-ad-v3-ecom-gapfill-mw"
INSTANCE_TYPE="${INSTANCE_TYPE:-g5.xlarge}"

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SAGEMAKER_DIR="$PROJECT_DIR/sagemaker"

echo "=========================================="
echo "V3 ecom DSVDD/DAGMM multi-window gap-fill"
echo "Region:    $REGION"
echo "Bucket:    s3://$BUCKET/$PREFIX/"
echo "Instance:  1 x $INSTANCE_TYPE"
echo "=========================================="

echo ""
echo "Step 1: Uploading scripts..."
aws s3 cp "$SAGEMAKER_DIR/training.py" "s3://$BUCKET/$PREFIX/iot/input/training.py" --region "$REGION"
aws s3 cp "$SAGEMAKER_DIR/bootstrap_v3_ecom_gapfill_multiwindow.sh" \
          "s3://$BUCKET/$PREFIX/iot/input/bootstrap_v3_ecom_gapfill_multiwindow.sh" --region "$REGION"

if [[ "${1:-}" != "--launch" ]]; then
    echo ""
    echo "Dry run complete. Re-run with --launch to spin up 1 x $INSTANCE_TYPE."
    exit 0
fi

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
    echo "ERROR: SG $SECURITY_GROUP_NAME not found."
    exit 1
fi
echo "  SG: $SG_ID"

echo ""
echo "Step 3: Launching instance..."

USER_DATA=$(cat <<USERDATA
#!/bin/bash
export INSTANCE_ROLE="ecom-gapfill-mw"
export AWS_DEFAULT_REGION="$REGION"
aws s3 cp "s3://$BUCKET/$PREFIX/iot/input/bootstrap_v3_ecom_gapfill_multiwindow.sh" /home/ubuntu/bootstrap.sh --region "$REGION"
chmod +x /home/ubuntu/bootstrap.sh
su - ubuntu -c "BUCKET=$BUCKET INSTANCE_ROLE=ecom-gapfill-mw /home/ubuntu/bootstrap.sh" &> /home/ubuntu/training.log &
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
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=v3-ecom-gapfill-mw},{Key=Project,Value=$PROJECT_TAG}]" \
    --region "$REGION" \
    --query 'Instances[0].InstanceId' \
    --output text)

echo "  v3-ecom-gapfill-mw ($INSTANCE_TYPE): $INSTANCE_ID"
echo ""
echo "=========================================="
echo "Launched. Monitor:"
echo "  aws s3 ls s3://$BUCKET/$PREFIX/results/ecom-gapfill-mw/ --region $REGION --recursive"
echo "Done sentinel: ecom-gapfill-mw/DONE"
echo "Terminate:"
echo "  aws ec2 terminate-instances --region $REGION --instance-ids $INSTANCE_ID"
echo "=========================================="
