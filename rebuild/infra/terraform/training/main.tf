provider "aws" {
  region              = var.region
  profile             = var.aws_profile
  allowed_account_ids = [var.account_id] # wrong credentials => hard error, nothing is touched

  default_tags {
    tags = {
      Project   = var.project
      Scope     = "training"
      Campaign  = "training"
      ManagedBy = "terraform"
      Owner     = var.owner
    }
  }
}

# Outputs of the two roots applied before this one. Read only.
data "terraform_remote_state" "network" {
  backend = "s3"
  config = merge(
    { bucket = var.state_bucket, key = "network/terraform.tfstate", region = var.region },
    var.aws_profile == null ? {} : { profile = var.aws_profile },
  )
}

data "terraform_remote_state" "shared" {
  backend = "s3"
  config = merge(
    { bucket = var.state_bucket, key = "shared/terraform.tfstate", region = var.region },
    var.aws_profile == null ? {} : { profile = var.aws_profile },
  )
}

data "aws_ssm_parameter" "ami" {
  name = var.ami_ssm_parameter
}

locals {
  name           = "${var.project}-training"
  results_bucket = data.terraform_remote_state.shared.outputs.results_bucket
  alerts_topic   = data.terraform_remote_state.shared.outputs.alerts_topic_arn
  vpc_id         = data.terraform_remote_state.network.outputs.vpc_id
  subnet_id = (var.subnet == "slot"
    ? data.terraform_remote_state.network.outputs.slot_subnet_by_campaign[var.slot_key]
  : data.terraform_remote_state.network.outputs.control_plane_subnet_ids[tonumber(substr(var.subnet, 2, 1))])
  cap_seconds = var.cap_hours * 3600
}

# ------------------------------------------------------------------ IAM: training/ prefix only, plus SSM and the alert topic
data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "training" {
  name               = local.name
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.training.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "training" {
  statement {
    sid       = "ListTrainingPrefix"
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${local.results_bucket}"]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["training/*", "training"]
    }
  }
  statement {
    sid       = "ReadWriteTrainingPrefix"
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["arn:aws:s3:::${local.results_bucket}/training/*"]
  }
  statement {
    sid       = "Alerts"
    actions   = ["sns:Publish"]
    resources = [local.alerts_topic]
  }
}

resource "aws_iam_role_policy" "training" {
  name   = "training-s3-sns"
  role   = aws_iam_role.training.id
  policy = data.aws_iam_policy_document.training.json
}

resource "aws_iam_instance_profile" "training" {
  name = local.name
  role = aws_iam_role.training.name
}

# ------------------------------------------------------------------ network: no inbound at all, access is SSM only
resource "aws_security_group" "training" {
  name        = local.name
  description = "Training instance. No inbound. Outbound for S3, SSM and package downloads through the NAT."
  vpc_id      = local.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ------------------------------------------------------------------ the instance
resource "aws_instance" "training" {
  ami                                  = data.aws_ssm_parameter.ami.insecure_value # a public AMI id, not a secret
  instance_type                        = var.instance_type
  subnet_id                            = local.subnet_id
  vpc_security_group_ids               = [aws_security_group.training.id]
  iam_instance_profile                 = aws_iam_instance_profile.training.name
  instance_initiated_shutdown_behavior = "terminate"
  associate_public_ip_address          = false

  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  root_block_device {
    volume_type = "gp3"
    volume_size = var.root_volume_gib
    encrypted   = true
  }

  # The hard cap. A systemd unit runs at every boot and schedules a shutdown at first boot plus cap_seconds, so a
  # reboot cannot extend it. Shutdown behavior terminate turns that shutdown into a termination.
  user_data = <<-EOT
    #!/bin/bash
    set -eu
    mkdir -p /var/lib/sad-training
    [ -s /var/lib/sad-training/first_boot ] || date +%s > /var/lib/sad-training/first_boot
    cat > /usr/local/sbin/sad-training-cap <<'EOS'
    #!/bin/bash
    fb=$(cat /var/lib/sad-training/first_boot)
    left=$(( (fb + ${local.cap_seconds} - $(date +%s)) / 60 ))
    if [ "$left" -le 1 ]; then /sbin/shutdown -h now; else /sbin/shutdown -h "+$left"; fi
    EOS
    chmod 755 /usr/local/sbin/sad-training-cap
    cat > /etc/systemd/system/sad-training-cap.service <<'EOS'
    [Unit]
    Description=Hard cap on the training instance lifetime
    After=multi-user.target
    [Service]
    Type=oneshot
    ExecStart=/usr/local/sbin/sad-training-cap
    [Install]
    WantedBy=multi-user.target
    EOS
    systemctl daemon-reload
    systemctl enable --now sad-training-cap.service
  EOT

  tags = { Name = local.name }

  # Fail fast when a zone has no capacity, instead of retrying for many minutes (seen 17 Oct 2026 in us-east-2a).
  timeouts {
    create = "4m"
  }

  lifecycle {
    ignore_changes = [ami] # a newer AMI behind the parameter must not replace a running instance
  }
}
