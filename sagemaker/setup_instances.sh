#!/bin/bash
# Setup script for the ML training infrastructure.
# Creates S3 bucket, uploads data/scripts, launches 3x g5.2xlarge instances.
# The key pair, security group and instance profile it creates are reused by
# the other setup_*.sh launchers.
#
# Prerequisites:
#   - AWS CLI configured with credentials for your account
#   - Feature files in data/features/
#   - training.py and bootstrap.sh in sagemaker/
#
# Usage:
#   export AWS_PROFILE=<your-aws-profile>
#   ./sagemaker/setup_instances.sh [--launch]
#
# Without --launch: creates bucket + uploads data only (dry run for instances)
# With --launch: also launches EC2 instances

set -euo pipefail

BUCKET="${BUCKET:?set BUCKET to your S3 bucket}"
REGION="us-east-2"
KEY_NAME="stream-ad-key"
SECURITY_GROUP_NAME="stream-ad-sg"
INSTANCE_TYPE="g5.2xlarge"
# Deep Learning AMI GPU PyTorch 2.x (Ubuntu 22.04) in us-east-2
AMI_ID=""  # Resolved dynamically in Step 3

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SAGEMAKER_DIR="$PROJECT_DIR/sagemaker"
DATA_DIR="$PROJECT_DIR/data/features"

echo "=========================================="
echo "Setup Training Infrastructure"
echo "Region:  $REGION"
echo "Bucket:  $BUCKET"
echo "Project: $PROJECT_DIR"
echo "=========================================="

# --- Step 1: Create S3 bucket (idempotent, NOT in Terraform) ---
echo ""
echo "Step 1: S3 bucket..."
if aws s3api head-bucket --bucket "$BUCKET" --region "$REGION" 2>/dev/null; then
    echo "  Bucket exists: s3://$BUCKET"
else
    echo "  Creating bucket: s3://$BUCKET"
    aws s3api create-bucket \
        --bucket "$BUCKET" \
        --region "$REGION" \
        --create-bucket-configuration LocationConstraint="$REGION"
    # Enable versioning for safety
    aws s3api put-bucket-versioning \
        --bucket "$BUCKET" \
        --versioning-configuration Status=Enabled
    echo "  Bucket created with versioning enabled"
fi

# --- Step 2: Upload data and scripts ---
echo ""
echo "Step 2: Uploading data and scripts..."

# Upload feature files
for ws in 15 30 60 120; do
    FEATURE_FILE="$DATA_DIR/features_${ws}s.parquet"
    if [ -f "$FEATURE_FILE" ]; then
        echo "  Uploading features_${ws}s.parquet..."
        aws s3 cp "$FEATURE_FILE" "s3://$BUCKET/input/features_${ws}s.parquet" --region "$REGION"
    else
        echo "  WARNING: $FEATURE_FILE not found, skipping"
    fi
done

# Upload training script
echo "  Uploading training.py..."
aws s3 cp "$SAGEMAKER_DIR/training.py" "s3://$BUCKET/input/training.py" --region "$REGION"

# Upload bootstrap script
echo "  Uploading bootstrap.sh..."
aws s3 cp "$SAGEMAKER_DIR/bootstrap.sh" "s3://$BUCKET/input/bootstrap.sh" --region "$REGION"

echo "  Upload complete."
aws s3 ls "s3://$BUCKET/input/" --region "$REGION" --human-readable

# --- Step 3: Launch instances (only with --launch flag) ---
if [[ "${1:-}" != "--launch" ]]; then
    echo ""
    echo "Dry run complete. S3 bucket ready with data."
    echo "Run with --launch to create EC2 instances."
    echo ""
    echo "  $0 --launch"
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
if [ "$AMI_ID" = "None" ] || [ -z "$AMI_ID" ]; then
    echo "ERROR: Could not find Deep Learning AMI. Set AMI_ID manually."
    exit 1
fi
echo "  AMI: $AMI_ID"

# --- Step 4: Create key pair (if not exists) ---
echo ""
echo "Step 4: Key pair..."
if aws ec2 describe-key-pairs --key-names "$KEY_NAME" --region "$REGION" &>/dev/null; then
    echo "  Key pair exists: $KEY_NAME"
else
    echo "  Creating key pair: $KEY_NAME"
    aws ec2 create-key-pair \
        --key-name "$KEY_NAME" \
        --region "$REGION" \
        --query 'KeyMaterial' \
        --output text > "$PROJECT_DIR/$KEY_NAME.pem"
    chmod 400 "$PROJECT_DIR/$KEY_NAME.pem"
    echo "  Key saved to: $PROJECT_DIR/$KEY_NAME.pem"
fi

# --- Step 5: Create security group (if not exists) ---
echo ""
echo "Step 5: Security group..."
VPC_ID=$(aws ec2 describe-vpcs --region "$REGION" --filters "Name=isDefault,Values=true" --query 'Vpcs[0].VpcId' --output text)
SG_ID=$(aws ec2 describe-security-groups \
    --region "$REGION" \
    --filters "Name=group-name,Values=$SECURITY_GROUP_NAME" "Name=vpc-id,Values=$VPC_ID" \
    --query 'SecurityGroups[0].GroupId' \
    --output text 2>/dev/null || echo "None")

if [ "$SG_ID" = "None" ] || [ -z "$SG_ID" ]; then
    echo "  Creating security group: $SECURITY_GROUP_NAME"
    SG_ID=$(aws ec2 create-security-group \
        --group-name "$SECURITY_GROUP_NAME" \
        --description "ML training instances" \
        --vpc-id "$VPC_ID" \
        --region "$REGION" \
        --query 'GroupId' --output text)
    # Allow SSH from your own address only
    aws ec2 authorize-security-group-ingress \
        --group-id "$SG_ID" \
        --protocol tcp --port 22 --cidr "${SSH_CIDR:?set SSH_CIDR to your address, for example 203.0.113.5/32}" \
        --region "$REGION"
    echo "  Created: $SG_ID"
else
    echo "  Exists: $SG_ID"
fi

# --- Step 6: Create IAM instance profile (if not exists) ---
echo ""
echo "Step 6: IAM role..."
ROLE_NAME="stream-ad-ec2-role"
PROFILE_NAME="stream-ad-ec2-profile"

if aws iam get-instance-profile --instance-profile-name "$PROFILE_NAME" &>/dev/null; then
    echo "  Instance profile exists: $PROFILE_NAME"
else
    echo "  Creating IAM role and instance profile..."
    # Create role
    aws iam create-role \
        --role-name "$ROLE_NAME" \
        --assume-role-policy-document '{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "ec2.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        }' || true

    # Attach S3 access policy
    aws iam put-role-policy \
        --role-name "$ROLE_NAME" \
        --policy-name "stream-ad-s3-access" \
        --policy-document "{
            \"Version\": \"2012-10-17\",
            \"Statement\": [{
                \"Effect\": \"Allow\",
                \"Action\": [\"s3:GetObject\", \"s3:PutObject\", \"s3:ListBucket\", \"s3:DeleteObject\"],
                \"Resource\": [\"arn:aws:s3:::$BUCKET\", \"arn:aws:s3:::$BUCKET/*\"]
            }]
        }"

    # Create instance profile and attach role
    aws iam create-instance-profile --instance-profile-name "$PROFILE_NAME" || true
    aws iam add-role-to-instance-profile \
        --instance-profile-name "$PROFILE_NAME" \
        --role-name "$ROLE_NAME" || true

    echo "  Waiting for profile propagation..."
    sleep 15
    echo "  Created: $PROFILE_NAME"
fi

# --- Step 7: Launch instances ---
echo ""
echo "Step 7: Launching instances..."

launch_instance() {
    local name=$1
    local role=$2

    echo "  Launching $name ($role)..."

    # Generate user-data script that sets INSTANCE_ROLE and runs bootstrap
    USER_DATA=$(cat <<USERDATA
#!/bin/bash
export INSTANCE_ROLE="$role"
export AWS_DEFAULT_REGION="$REGION"
aws s3 cp "s3://$BUCKET/input/bootstrap.sh" /home/ubuntu/bootstrap.sh --region "$REGION"
chmod +x /home/ubuntu/bootstrap.sh
su - ubuntu -c "BUCKET=$BUCKET INSTANCE_ROLE=$role /home/ubuntu/bootstrap.sh" &> /home/ubuntu/training.log &
USERDATA
)

    INSTANCE_ID=$(aws ec2 run-instances \
        --image-id "$AMI_ID" \
        --instance-type "$INSTANCE_TYPE" \
        --key-name "$KEY_NAME" \
        --security-group-ids "$SG_ID" \
        --iam-instance-profile Name="$PROFILE_NAME" \
        --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":50,"VolumeType":"gp3"}}]' \
        --user-data "$USER_DATA" \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$name},{Key=Project,Value=stream-ad}]" \
        --region "$REGION" \
        --query 'Instances[0].InstanceId' \
        --output text)

    echo "    Instance: $INSTANCE_ID ($name)"
}

launch_instance "stream-ad-gpu-1" "gpu-1"
launch_instance "stream-ad-gpu-2" "gpu-2"
launch_instance "stream-ad-gpu-3" "gpu-3"

echo ""
echo "=========================================="
echo "All 3 instances launched."
echo "=========================================="
echo ""
echo "Monitor:"
echo "  aws ec2 describe-instances --region $REGION \\"
echo "    --filters 'Name=tag:Project,Values=stream-ad' 'Name=instance-state-name,Values=running' \\"
echo "    --query 'Reservations[].Instances[].[Tags[?Key==\`Name\`].Value|[0],InstanceId,PublicIpAddress,State.Name]' \\"
echo "    --output table"
echo ""
echo "SSH:"
echo "  ssh -i $PROJECT_DIR/$KEY_NAME.pem ubuntu@<IP>"
echo ""
echo "Check completion:"
echo "  aws s3 ls s3://$BUCKET/results/ --region $REGION --recursive | grep DONE"
echo ""
echo "Terminate when done:"
echo "  aws ec2 describe-instances --region $REGION \\"
echo "    --filters 'Name=tag:Project,Values=stream-ad' \\"
echo "    --query 'Reservations[].Instances[].InstanceId' --output text | \\"
echo "    xargs aws ec2 terminate-instances --region $REGION --instance-ids"
