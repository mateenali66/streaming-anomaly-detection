# Offline plan of the shared root with a mock AWS provider. No AWS call. scripts/validate_local.sh copies this file into a
# COPY of the root and runs `terraform test`. Only `command = plan` is used.
mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = { account_id = "000000000000" }
  }
  mock_data "aws_iam_policy_document" {
    defaults = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
  }
}

variables {
  account_id       = "000000000000"
  owner            = "team"
  alert_email      = "alerts@example.org"
  bucket_suffix    = "0a1b2c3d"
  budget_limit_usd = 60
}

run "plan" {
  command = plan

  # Decision 29 and REVIEW4 C1: no account ID in the bucket name.
  assert {
    condition     = aws_s3_bucket.results.bucket == "streaming-anomaly-detection-results-0a1b2c3d"
    error_message = "bucket name"
  }
  assert {
    condition     = !strcontains(aws_s3_bucket.results.bucket, var.account_id)
    error_message = "the bucket name carries the account ID"
  }
  # REVIEW4 M12: Health events to one log group and to the alert topic.
  assert {
    condition     = aws_cloudwatch_log_group.health.name == "/streaming-anomaly-detection/health"
    error_message = "health log group name"
  }
  assert {
    condition     = jsondecode(aws_cloudwatch_event_rule.health.event_pattern).source == ["aws.health"]
    error_message = "the rule must match aws.health"
  }
  assert {
    condition     = aws_cloudwatch_event_target.health_logs.rule == aws_cloudwatch_event_rule.health.name && aws_cloudwatch_event_target.health_sns.rule == aws_cloudwatch_event_rule.health.name
    error_message = "both targets hang on the health rule"
  }
  assert {
    condition     = aws_cloudwatch_log_resource_policy.health.policy_name == "streaming-anomaly-detection-health-events"
    error_message = "log resource policy"
  }
  assert {
    condition     = output.health_log_group_name == "/streaming-anomaly-detection/health"
    error_message = "output for the cluster root"
  }
  assert {
    condition     = length(aws_budgets_budget.project) == 1
    error_message = "the budget is created when a cap and an address are given"
  }
}

# The suffix is required and is never an account ID.
run "suffix_cannot_be_an_account_id" {
  command = plan
  variables {
    bucket_suffix = "000000000000"
  }
  expect_failures = [var.bucket_suffix]
}
