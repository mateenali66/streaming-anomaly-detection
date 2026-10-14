provider "aws" {
  region              = var.region
  profile             = var.aws_profile
  allowed_account_ids = [var.account_id] # wrong credentials => hard error before anything is read or changed

  default_tags {
    tags = {
      Project   = var.project
      Scope     = "shared"
      ManagedBy = "terraform"
      Owner     = var.owner
    }
  }
}

data "aws_caller_identity" "this" {}

locals {
  images = yamldecode(file("${path.module}/../../images/images.lock.yaml"))

  built_repos  = toset([for k, v in local.images.built : v.repo])
  mirror_repos = { for m in local.images.mirrored : m.key => m.repo }

  # No account ID in the name (decision 29, REVIEW4 C1). The suffix is a fixed random string from a gitignored tfvars, chosen once
  # before the first apply, because a bucket can never be renamed. Probe records quote checkpoint paths, so the name reaches data.
  bucket_name = "${var.project}-results-${var.bucket_suffix}" # at most 63 characters, checked below

  health_log_group = "/${var.project}/health"
}

check "bucket_name_length" {
  assert {
    condition     = length(local.bucket_name) <= 63
    error_message = "The results bucket name would exceed 63 characters. Shorten var.project."
  }
}

# ------------------------------------------------------------------ results bucket
# Not owned by any cluster. Teardown never touches it. REVIEW3 A1.
resource "aws_s3_bucket" "results" {
  bucket        = local.bucket_name
  force_destroy = false

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_ownership_controls" "results" {
  bucket = aws_s3_bucket.results.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_versioning" "results" {
  bucket = aws_s3_bucket.results.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "results" {
  bucket                  = aws_s3_bucket.results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "results" {
  bucket = aws_s3_bucket.results.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Lifecycle. campaign/ is never expired. scratch/ (Flink checkpoints and savepoints) is.
# The bucket is versioned, so scratch/ needs a noncurrent-version rule and a delete-marker rule too,
# or deleted checkpoints would stay as hidden versions and keep costing money.
resource "aws_s3_bucket_lifecycle_configuration" "results" {
  bucket = aws_s3_bucket.results.id

  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 3
    }
  }

  rule {
    id     = "scratch-expire"
    status = "Enabled"
    filter {
      prefix = "scratch/"
    }
    expiration {
      days = var.scratch_expiry_days
    }
    noncurrent_version_expiration {
      noncurrent_days = 1
    }
  }

  rule {
    id     = "scratch-clean-delete-markers"
    status = "Enabled"
    filter {
      prefix = "scratch/"
    }
    expiration {
      expired_object_delete_marker = true
    }
  }

  depends_on = [aws_s3_bucket_versioning.results]
}

data "aws_iam_policy_document" "results" {
  statement {
    sid       = "TlsOnly"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.results.arn, "${aws_s3_bucket.results.arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }

  # No project-tagged principal (every cluster role carries Project=<project>) can delete results,
  # delete versions, delete the bucket or rewrite this policy. campaign/* only: scratch/ stays
  # deletable because Flink removes old checkpoints. A person deletes on purpose with an untagged
  # administrator identity after reading the runbook.
  statement {
    sid       = "NoDeleteOfResultsFromProjectPrincipals"
    effect    = "Deny"
    actions   = ["s3:DeleteObject", "s3:DeleteObjectVersion"]
    resources = ["${aws_s3_bucket.results.arn}/campaign/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "StringLike"
      variable = "aws:PrincipalTag/Project"
      values   = [var.project]
    }
  }

  statement {
    sid       = "NoBucketDeleteOrPolicyChangeFromProjectPrincipals"
    effect    = "Deny"
    actions   = ["s3:DeleteBucket", "s3:PutBucketPolicy", "s3:DeleteBucketPolicy", "s3:PutBucketVersioning", "s3:PutLifecycleConfiguration"]
    resources = [aws_s3_bucket.results.arn]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "StringLike"
      variable = "aws:PrincipalTag/Project"
      values   = [var.project]
    }
  }
}

resource "aws_s3_bucket_policy" "results" {
  bucket     = aws_s3_bucket.results.id
  policy     = data.aws_iam_policy_document.results.json
  depends_on = [aws_s3_bucket_public_access_block.results]
}

# ------------------------------------------------------------------ ECR: immutable, scanned, pulled by digest
# Built images (ours) and mirrors of every public image the clusters run (images.lock.yaml `mirrored`).
resource "aws_ecr_repository" "built" {
  for_each             = local.built_repos
  name                 = "${var.project}/${each.key}"
  image_tag_mutability = "IMMUTABLE"
  force_delete         = false

  image_scanning_configuration {
    scan_on_push = true
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_ecr_repository" "mirror" {
  for_each             = local.mirror_repos
  name                 = "${var.project}/${each.value}"
  image_tag_mutability = "IMMUTABLE"
  force_delete         = false

  image_scanning_configuration {
    scan_on_push = true
  }

  lifecycle {
    prevent_destroy = true
  }
}

# ------------------------------------------------------------------ one alert topic, one subscription
resource "aws_sns_topic" "alerts" {
  name = "${var.project}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ------------------------------------------------------------------ cost guard (optional)
# No aws_ce_cost_allocation_tag here (REVIEW4 M7). Only the management (payer) account of an organization can activate cost
# allocation tags, and in a standalone account the setting is account-wide for generic keys other teams may use. The author
# asks the payer administrator to activate Project, Campaign and Workload. Until then this tag-filtered budget tracks nothing,
# and budget data lag 8 to 12 hours anyway, so the pilot's cap is a wall-clock stop rule (PILOT_RUNBOOK.md).
resource "aws_budgets_budget" "project" {
  count        = var.budget_limit_usd > 0 && var.alert_email != "" ? 1 : 0
  name         = "${var.project}-cap"
  budget_type  = "COST"
  limit_amount = tostring(var.budget_limit_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  cost_filter {
    name   = "TagKeyValue"
    values = [format("user:Project$%s", var.project)]
  }

  dynamic "notification" {
    for_each = [50, 80, 100]
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value
      threshold_type             = "PERCENTAGE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = [var.alert_email]
    }
  }
}

# ------------------------------------------------------------------ AWS Health events of the region (protocol 1.7, REVIEW4 M12)
# The Health API needs a Business or Enterprise support plan. An EventBridge rule on aws.health events does not. A rule in
# us-east-2 receives that region's public and account-specific events (docs.aws.amazon.com/health/latest/ug/choosing-a-region.html).
# Every event goes to one log group, which the orchestrator reads with logs:FilterLogEvents (cluster root, iam.tf), and to the
# alert topic. Read only: nothing here reads or changes another team's resources.
resource "aws_cloudwatch_log_group" "health" {
  name              = local.health_log_group
  retention_in_days = 90
}

# EventBridge writes to a log group through a CloudWatch Logs resource policy, scoped to this one group
# (docs.aws.amazon.com/eventbridge/latest/userguide/eb-use-resource-based.html).
data "aws_iam_policy_document" "health_logs" {
  statement {
    sid     = "EventBridgeWritesHealthEvents"
    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com", "delivery.logs.amazonaws.com"]
    }
    resources = ["${aws_cloudwatch_log_group.health.arn}:*"]
  }
}

resource "aws_cloudwatch_log_resource_policy" "health" {
  policy_name     = "${var.project}-health-events"
  policy_document = data.aws_iam_policy_document.health_logs.json
}

resource "aws_cloudwatch_event_rule" "health" {
  name        = "${var.project}-health"
  description = "AWS Health events of this region, for the streaming anomaly-detection campaigns"
  event_pattern = jsonencode({
    source        = ["aws.health"]
    "detail-type" = ["AWS Health Event"]
  })
}

resource "aws_cloudwatch_event_target" "health_logs" {
  rule       = aws_cloudwatch_event_rule.health.name
  target_id  = "health-log-group"
  arn        = aws_cloudwatch_log_group.health.arn
  depends_on = [aws_cloudwatch_log_resource_policy.health]
}

resource "aws_cloudwatch_event_target" "health_sns" {
  rule       = aws_cloudwatch_event_rule.health.name
  target_id  = "alert-topic"
  arn        = aws_sns_topic.alerts.arn
  depends_on = [aws_sns_topic_policy.alerts]
}

# A topic policy replaces the default one, so the account-owner statement is restated. EventBridge does not support Condition
# blocks in SNS topic policies (docs.aws.amazon.com/eventbridge/latest/userguide/eb-use-resource-based.html), so its statement
# has none. Publishing from the cluster roles is still granted by their own IAM policies.
data "aws_iam_policy_document" "alerts" {
  statement {
    sid = "AccountOwner"
    actions = ["SNS:GetTopicAttributes", "SNS:SetTopicAttributes", "SNS:AddPermission", "SNS:RemovePermission", "SNS:DeleteTopic",
    "SNS:Subscribe", "SNS:ListSubscriptionsByTopic", "SNS:Publish"]
    principals {
      type        = "AWS"
      identifiers = ["*"]
    }
    resources = [aws_sns_topic.alerts.arn]
    condition {
      test     = "StringEquals"
      variable = "AWS:SourceOwner"
      values   = [var.account_id]
    }
  }
  statement {
    sid     = "EventBridgePublishesHealthEvents"
    actions = ["sns:Publish"]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
    resources = [aws_sns_topic.alerts.arn]
  }
}

resource "aws_sns_topic_policy" "alerts" {
  arn    = aws_sns_topic.alerts.arn
  policy = data.aws_iam_policy_document.alerts.json
}
