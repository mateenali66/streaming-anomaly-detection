# Applied ONCE per account. Never destroyed by campaign teardown.
#
# Docs relied on (context7, checked 13 Oct 2026):
#   - Terraform S3 backend, partial configuration and `use_lockfile` (needs Terraform >= 1.10):
#     https://developer.hashicorp.com/terraform/language/backend/s3
#   - AWS provider resources aws_s3_bucket_lifecycle_configuration, aws_ecr_repository:
#     https://registry.terraform.io/providers/hashicorp/aws/latest/docs
terraform {
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "= 6.68.0" # exact. The EKS module 21.29.0 used by the cluster root needs >= 6.59.
    }
  }

  # Partial configuration: bucket, key, region and use_lockfile are passed at init time
  # (terraform init -backend-config=backend.hcl). See backend.hcl.example.
  # The bucket name contains the account ID, so it is never committed.
  backend "s3" {}
}
