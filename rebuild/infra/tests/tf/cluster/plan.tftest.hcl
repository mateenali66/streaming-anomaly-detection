# Offline plan of the cluster root with mock providers (Terraform >= 1.7). No AWS call, no cluster call: every provider is
# mocked and the two remote states are overridden. scripts/validate_local.sh copies this file into a COPY of the root
# (terraform/cluster/tests/) and runs `terraform test -verbose`, then greps the printed plan (REVIEW4_OPERATIONS B1, B2, C2).
# Only `command = plan` is used, so nothing is applied and no destroy-time provisioner can ever run.
mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = { account_id = "000000000000", arn = "arn:aws:iam::000000000000:user/op", user_id = "AIDAX" }
  }
  mock_data "aws_partition" {
    defaults = { partition = "aws", dns_suffix = "amazonaws.com" }
  }
  mock_data "aws_iam_session_context" {
    defaults = { issuer_arn = "arn:aws:iam::000000000000:user/op" }
  }
  mock_data "aws_iam_policy_document" {
    defaults = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
  }
  mock_data "aws_vpc" {
    defaults = { id = "vpc-0123456789abcdef0", tags = { Project = "streaming-anomaly-detection", Scope = "network" } }
  }
  mock_data "aws_subnet" {
    defaults = { id = "subnet-00000000000000000", vpc_id = "vpc-0123456789abcdef0" }
  }
  # A sentinel release that must NEVER appear in the plan: the node groups use the pinned release, not the SSM one (C2).
  mock_data "aws_ssm_parameter" {
    defaults = { value = "1.35.9-29991231", insecure_value = "1.35.9-29991231" }
  }
}
mock_provider "helm" {}
mock_provider "kubectl" {}
mock_provider "null" {}
mock_provider "time" {}
mock_provider "tls" {}
mock_provider "cloudinit" {}

override_data {
  target = data.terraform_remote_state.network
  values = {
    outputs = {
      vpc_id                   = "vpc-0123456789abcdef0"
      slot_subnet_by_campaign  = { "pilot-ec" = "subnet-00000000000000000", "pilot-iot" = "subnet-11111111111111111" }
      control_plane_subnet_ids = ["subnet-22222222222222222", "subnet-33333333333333333"]
    }
  }
}
override_data {
  target = data.terraform_remote_state.shared
  values = {
    outputs = {
      results_bucket        = "streaming-anomaly-detection-results-0a1b2c3d"
      alerts_topic_arn      = "arn:aws:sns:us-east-2:000000000000:streaming-anomaly-detection-alerts"
      ecr_registry          = "000000000000.dkr.ecr.us-east-2.amazonaws.com"
      health_log_group_name = "/streaming-anomaly-detection/health"
    }
  }
}

variables {
  account_id               = "000000000000"
  owner                    = "team"
  campaign_id              = "pilot-ec"
  workload                 = "ecommerce"
  state_bucket             = "state"
  api_allowed_cidrs        = ["203.0.113.7/32"]
  python_bin               = "/opt/venv/bin/python"
  node_ami_release_version = "1.35.4-20261003"
  addon_versions = {
    "coredns"                = "v1.12.4-eksbuild.1"
    "kube-proxy"             = "v1.35.0-eksbuild.2"
    "vpc-cni"                = "v1.20.4-eksbuild.1"
    "eks-pod-identity-agent" = "v1.3.9-eksbuild.3"
    "aws-ebs-csi-driver"     = "v1.50.1-eksbuild.1"
  }
  # The built images have no digest until scripts/build_images.sh has pushed them. The digest guard is tested on its own below.
  enforce_image_digests = false
}

# The first plan of a fresh campaign, with nothing in state (B1: coalesce on a null profile; B2: module depends_on).
run "first_plan" {
  command = plan

  assert {
    condition     = module.eks.cluster_name == "streaming-anomaly-detection-pilot-ec"
    error_message = "cluster name"
  }
  assert {
    condition     = terraform_data.teardown_guard.input.profile == ""
    error_message = "a null aws_profile must become an empty string in the guard input"
  }
  assert {
    condition     = terraform_data.teardown_guard.input.bucket == "streaming-anomaly-detection-results-0a1b2c3d"
    error_message = "guard reads the results bucket from the shared root"
  }
}

# The same with a named profile.
run "first_plan_with_profile" {
  command = plan
  variables {
    aws_profile = "some-profile"
  }
  assert {
    condition     = terraform_data.teardown_guard.input.profile == "some-profile"
    error_message = "a named profile reaches the guard"
  }
}

# The IoT pilot plans too.
run "first_plan_iot" {
  command = plan
  variables {
    campaign_id = "pilot-iot"
    workload    = "iot"
  }
  assert {
    condition     = module.eks.cluster_name == "streaming-anomaly-detection-pilot-iot"
    error_message = "iot cluster name"
  }
}

# A campaign id of the other workload is refused at plan time (cross-variable validation, Terraform >= 1.9).
run "campaign_must_match_workload" {
  command = plan
  variables {
    campaign_id = "pilot-iot"
    workload    = "ecommerce"
  }
  expect_failures = [var.workload]
}

# A VPC without this project's tags is refused (postcondition on data.aws_vpc.this).
run "foreign_vpc_is_refused" {
  command = plan
  override_data {
    target = data.aws_vpc.this
    values = { id = "vpc-0123456789abcdef0", tags = { Project = "someone-else", Scope = "network" } }
  }
  expect_failures = [data.aws_vpc.this]
}

# A node subnet in another VPC is refused (postcondition on data.aws_subnet.node).
run "foreign_subnet_is_refused" {
  command = plan
  override_data {
    target = data.aws_subnet.node
    values = { id = "subnet-00000000000000000", vpc_id = "vpc-ffffffffffffffff0" }
  }
  expect_failures = [data.aws_subnet.node]
}

# With the digest guard on, an image lock without digests stops the plan.
run "empty_digests_stop_the_plan" {
  command = plan
  variables {
    enforce_image_digests = true
  }
  expect_failures = [terraform_data.assert_image_digests]
}
