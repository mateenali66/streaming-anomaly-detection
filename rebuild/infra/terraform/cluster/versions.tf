# One root per campaign cluster (ec-a, iot-b, pilot-ec ...), one state per campaign.
#
# Docs relied on (context7, checked 13 Oct 2026):
#   - terraform-aws-modules/eks v21 (kubernetes_version, addons[*].pod_identity_association,
#     endpoint_public_access_cidrs, control_plane_subnet_ids, iam_role_use_name_prefix):
#     https://github.com/terraform-aws-modules/terraform-aws-eks/blob/master/docs/UPGRADE-21.0.md
#   - EKS Pod Identity trust policy (pods.eks.amazonaws.com, sts:AssumeRole + sts:TagSession),
#     minimum SDKs (boto3/botocore 1.34.41, AWS CLI v2 2.15.0, Java v1 1.12.746):
#     https://docs.aws.amazon.com/eks/latest/userguide/pod-id-role.html
#   - EBS CSI driver tagging (extraVolumeTags, ebs.csi.aws.com/cluster is shared by every cluster in
#     the account, so the IAM policy below scopes on our own Campaign tag instead):
#     https://github.com/kubernetes-sigs/aws-ebs-csi-driver/blob/master/docs/tagging.md
terraform {
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "= 6.68.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "= 2.17.0"
    }
    kubectl = {
      source  = "alekc/kubectl"
      version = "= 2.2.0"
    }
    # Pulled in by the EKS module. Pinned here too, so every provider is an exact version.
    null = {
      source  = "hashicorp/null"
      version = "= 3.3.2"
    }
    time = {
      source  = "hashicorp/time"
      version = "= 0.14.2"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "= 4.4.1"
    }
    cloudinit = {
      source  = "hashicorp/cloudinit"
      version = "= 2.4.1"
    }
  }

  backend "s3" {} # partial configuration, key = campaigns/<campaign_id>/terraform.tfstate
}
