# One GPU instance for the model training of protocol section 6, in a reserve slot subnet of the network root.
# Added on 17 October 2026, after the six campaigns, for the training only. It touches no campaign resource.
#
# Docs relied on (context7, checked 17 Oct 2026):
#   - GPU instances are best deployed from an AWS Deep Learning AMI, which carries the NVIDIA driver:
#     https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/gpu-instances-started.html
#   - Instance-initiated shutdown behavior "terminate", so the hard cap in user data stops billing:
#     https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/terminating-instances.html
terraform {
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "= 6.68.0"
    }
  }

  backend "s3" {} # partial configuration, key = training/terraform.tfstate
}
