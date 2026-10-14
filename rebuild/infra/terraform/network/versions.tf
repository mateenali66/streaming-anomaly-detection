# Applied ONCE. One new VPC, one NAT gateway, one S3 gateway endpoint, N pre-cut /22 node subnets
# (one per cluster slot) and two control-plane-only /24 subnets. It only creates new resources.
# It never reads, imports or modifies any existing VPC, so other teams' networks are never touched.
terraform {
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "= 6.68.0"
    }
  }

  backend "s3" {} # partial configuration, see backend.hcl.example
}
