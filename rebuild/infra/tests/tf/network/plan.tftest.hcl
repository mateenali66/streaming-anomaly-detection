# Offline plan of the network root with a mock AWS provider. No AWS call. Only `command = plan` is used, so the destroy guard's
# provisioner never runs.
mock_provider "aws" {}

variables {
  account_id        = "000000000000"
  owner             = "team"
  vpc_cidr          = "10.123.0.0/16"
  workload_az       = "us-east-2a"
  control_plane_azs = ["us-east-2b", "us-east-2c"]
  python_bin        = "/opt/venv/bin/python"
}

run "plan" {
  command = plan

  assert {
    condition     = length(aws_subnet.slot) == 10 && aws_subnet.public.cidr_block == "10.123.252.0/28" && aws_subnet.cp[1].cidr_block == "10.123.251.0/24" && aws_subnet.slot[9].cidr_block == "10.123.36.0/22"
    error_message = "subnet arithmetic"
  }
  assert {
    condition     = alltrue([for s in aws_subnet.slot : s.availability_zone == "us-east-2a"]) && aws_subnet.public.availability_zone == "us-east-2a"
    error_message = "every node subnet and the NAT subnet sit in the workload zone (decision 16)"
  }
  assert {
    condition     = terraform_data.network_guard.input.profile == "" && endswith(terraform_data.network_guard.input.script, "scripts/network_guard.py")
    error_message = "network destroy guard (REVIEW4 M5)"
  }
}

run "workload_zone_not_in_control_plane_zones" {
  command = plan
  variables {
    control_plane_azs = ["us-east-2a", "us-east-2c"]
  }
  expect_failures = [aws_vpc.this]
}
