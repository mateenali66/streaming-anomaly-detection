# ------------------------------------------------ IAM (Pod Identity, no long-lived keys)
# Each role is trusted only from THIS cluster's ARN and only by the Pod Identity service.
data "aws_iam_policy_document" "pod_identity_trust" {
  statement {
    actions = ["sts:AssumeRole", "sts:TagSession"]
    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [module.eks.cluster_arn]
    }
  }
}

# ---- EBS CSI: scoped by OUR tags (Campaign=<id> AND Project=<project>, REVIEW4 M3). No snapshot permissions at all (snapshots would outlive the cluster).
# The AWS managed policy limits delete only by ebs.csi.aws.com/cluster=true, a tag that every cluster in
# the account shares, so it could delete another team's volumes. This one needs Campaign=<id>.
data "aws_iam_policy_document" "ebs_csi" {
  statement {
    sid       = "Describe"
    actions   = ["ec2:DescribeVolumes", "ec2:DescribeInstances", "ec2:DescribeAvailabilityZones", "ec2:DescribeTags", "ec2:DescribeVolumesModifications"]
    resources = ["*"]
  }
  statement {
    sid       = "CreateOnlyTaggedVolumes"
    actions   = ["ec2:CreateVolume"]
    resources = ["arn:aws:ec2:${var.region}:${local.account_id}:volume/*"]
    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/Campaign"
      values   = [var.campaign_id]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/Project"
      values   = [var.project]
    }
  }
  statement {
    sid       = "TagOnCreateOnly"
    actions   = ["ec2:CreateTags"]
    resources = ["arn:aws:ec2:${var.region}:${local.account_id}:volume/*"]
    condition {
      test     = "StringEquals"
      variable = "ec2:CreateAction"
      values   = ["CreateVolume"]
    }
  }
  statement {
    sid       = "ActOnOwnVolumes"
    actions   = ["ec2:AttachVolume", "ec2:DetachVolume", "ec2:DeleteVolume", "ec2:ModifyVolume"]
    resources = ["arn:aws:ec2:${var.region}:${local.account_id}:volume/*"]
    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/Campaign"
      values   = [var.campaign_id]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/Project"
      values   = [var.project]
    }
  }
  statement {
    sid       = "AttachToOwnInstances"
    actions   = ["ec2:AttachVolume", "ec2:DetachVolume"]
    resources = ["arn:aws:ec2:${var.region}:${local.account_id}:instance/*"]
    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/Campaign"
      values   = [var.campaign_id]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/Project"
      values   = [var.project]
    }
  }
}

resource "aws_iam_role" "ebs_csi" {
  name               = "${local.name}-ebs-csi"
  assume_role_policy = data.aws_iam_policy_document.pod_identity_trust.json
  tags               = { Role = "ebs-csi" }
}

resource "aws_iam_role_policy" "ebs_csi" {
  name   = "scoped-ebs"
  role   = aws_iam_role.ebs_csi.id
  policy = data.aws_iam_policy_document.ebs_csi.json
}

# ---- Orchestrator: reads and writes only its own prefix, is the only writer of COMPLETE, cannot delete.
# NOTE for the orchestrator code: ListBucket is limited to the prefix, so a HEAD of a MISSING key can come back
# 403, not 404. Use ListObjectsV2 with the prefix to test existence, or treat 403 and 404 alike.
data "aws_iam_policy_document" "orchestrator" {
  statement {
    sid       = "ObjectsUnderOwnPrefix"
    actions   = ["s3:PutObject", "s3:GetObject", "s3:AbortMultipartUpload"]
    resources = ["arn:aws:s3:::${local.results_bucket}/${local.results_prefix}/*"]
  }
  statement {
    sid       = "ListOwnPrefix"
    actions   = ["s3:ListBucket", "s3:ListBucketMultipartUploads"]
    resources = ["arn:aws:s3:::${local.results_bucket}"]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = [local.results_prefix, "${local.results_prefix}/*"]
    }
  }
  statement {
    sid       = "Alert"
    actions   = ["sns:Publish"]
    resources = [local.alerts_topic]
  }
  # AWS Health events for the region (protocol 1.7). The shared root's EventBridge rule writes them to this one log group.
  # FilterLogEvents only, on that group only (REVIEW4 M12, decision MIN-4).
  statement {
    sid     = "ReadHealthEvents"
    actions = ["logs:FilterLogEvents"]
    resources = [
      "arn:aws:logs:${var.region}:${local.account_id}:log-group:${local.health_log_group}",
      "arn:aws:logs:${var.region}:${local.account_id}:log-group:${local.health_log_group}:*",
    ]
  }
}

# ---- Exporter: PutObject only under baseline/ and export/. It can never write the COMPLETE marker.
data "aws_iam_policy_document" "exporter" {
  statement {
    sid       = "WriteBaselineAndExport"
    actions   = ["s3:PutObject", "s3:AbortMultipartUpload"]
    resources = ["arn:aws:s3:::${local.results_bucket}/${local.results_prefix}/baseline/*", "arn:aws:s3:::${local.results_bucket}/${local.results_prefix}/export/*"]
  }
  statement {
    sid       = "ListOwnPrefix"
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${local.results_bucket}"]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = [local.results_prefix, "${local.results_prefix}/*"]
    }
  }
}

# ---- Flink: checkpoints and savepoints under scratch/<campaign>/ only.
# Hadoop S3A (flink-s3-fs-hadoop) probes parent "directories" with HEAD and LIST before it writes. With a
# prefix-conditioned ListBucket a HEAD of a missing key returns 403 and the job fails at start. So ListBucket
# is unconditioned (key names only; object access stays scoped to scratch/<campaign>/*). The pilot checks it.
data "aws_iam_policy_document" "flink" {
  statement {
    sid       = "CheckpointObjects"
    actions   = ["s3:PutObject", "s3:GetObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = ["arn:aws:s3:::${local.results_bucket}/${local.scratch_prefix}/*"]
  }
  statement {
    sid       = "ListForS3AProbes"
    actions   = ["s3:ListBucket", "s3:ListBucketMultipartUploads"]
    resources = ["arn:aws:s3:::${local.results_bucket}"]
  }
}

resource "aws_iam_role" "orchestrator" {
  name               = "${local.name}-orchestrator"
  assume_role_policy = data.aws_iam_policy_document.pod_identity_trust.json
  tags               = { Role = "orchestrator" }
}

resource "aws_iam_role_policy" "orchestrator" {
  name   = "results-prefix"
  role   = aws_iam_role.orchestrator.id
  policy = data.aws_iam_policy_document.orchestrator.json
}

resource "aws_iam_role" "exporter" {
  name               = "${local.name}-exporter"
  assume_role_policy = data.aws_iam_policy_document.pod_identity_trust.json
  tags               = { Role = "exporter" }
}

resource "aws_iam_role_policy" "exporter" {
  name   = "results-prefix"
  role   = aws_iam_role.exporter.id
  policy = data.aws_iam_policy_document.exporter.json
}

resource "aws_iam_role" "flink" {
  name               = "${local.name}-flink"
  assume_role_policy = data.aws_iam_policy_document.pod_identity_trust.json
  tags               = { Role = "flink" }
}

resource "aws_iam_role_policy" "flink" {
  name   = "scratch-prefix"
  role   = aws_iam_role.flink.id
  policy = data.aws_iam_policy_document.flink.json
}

# Service accounts are created by the manifests (orchestrator, exporter) and by the Flink operator chart (flink).
# An association does not need the service account to exist yet, but a pod must be CREATED after the association.
resource "aws_eks_pod_identity_association" "orchestrator" {
  cluster_name    = module.eks.cluster_name
  namespace       = "streaming"
  service_account = "campaign-orchestrator"
  role_arn        = aws_iam_role.orchestrator.arn
}

resource "aws_eks_pod_identity_association" "exporter" {
  cluster_name    = module.eks.cluster_name
  namespace       = "streaming"
  service_account = "metric-exporter"
  role_arn        = aws_iam_role.exporter.arn
}

resource "aws_eks_pod_identity_association" "flink" {
  cluster_name    = module.eks.cluster_name
  namespace       = "streaming"
  service_account = "flink"
  role_arn        = aws_iam_role.flink.arn
}
