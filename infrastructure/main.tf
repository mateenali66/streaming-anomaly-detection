provider "aws" {
  region = var.region
}

data "aws_availability_zones" "available" {
  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

locals {
  azs = slice(data.aws_availability_zones.available.names, 0, 3)
}

# -----------------------------------------------------------------------------
# VPC
# -----------------------------------------------------------------------------
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.21"

  name = "${var.cluster_name}-vpc"
  cidr = "10.0.0.0/16"

  azs             = local.azs
  private_subnets = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  public_subnets  = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]

  enable_nat_gateway   = true
  single_nat_gateway   = true # cost optimization: one NAT for research workload
  enable_dns_hostnames = true
  enable_dns_support   = true

  public_subnet_tags = {
    "kubernetes.io/role/elb" = 1
  }

  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = 1
  }

  tags = var.tags
}

# -----------------------------------------------------------------------------
# EKS Cluster
# -----------------------------------------------------------------------------
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 21.0"

  name               = var.cluster_name
  kubernetes_version = var.kubernetes_version

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  endpoint_public_access = true

  # Grant current caller admin access
  enable_cluster_creator_admin_permissions = true

  # EKS Addons
  addons = {
    coredns = {
      most_recent                 = true
      resolve_conflicts_on_create = "OVERWRITE"
      resolve_conflicts_on_update = "OVERWRITE"
      configuration_values = jsonencode({
        tolerations = [{
          key      = "workload"
          operator = "Exists"
          effect   = "NoSchedule"
        }]
      })
    }
    kube-proxy = {
      most_recent = true
    }
    vpc-cni = {
      most_recent    = true
      before_compute = true
    }
    eks-pod-identity-agent = {
      most_recent    = true
      before_compute = true
    }
    aws-ebs-csi-driver = {
      most_recent                 = true
      resolve_conflicts_on_create = "OVERWRITE"
      service_account_role_arn    = module.ebs_csi_irsa.iam_role_arn
      configuration_values = jsonencode({
        controller = {
          tolerations = [{
            key      = "workload"
            operator = "Exists"
            effect   = "NoSchedule"
          }]
          nodeSelector = {
            workload = "monitoring"
          }
        }
      })
    }
  }

  # ---------- Node Group 1: Kafka Brokers (3x r6g.xlarge) ----------
  eks_managed_node_groups = {
    kafka = {
      ami_type       = "AL2023_ARM_64_STANDARD"
      instance_types = [var.kafka_instance_type]

      min_size     = var.kafka_node_count
      max_size     = var.kafka_node_count
      desired_size = var.kafka_node_count

      labels = {
        workload = "kafka"
      }

      taints = {
        workload = {
          key    = "workload"
          value  = "kafka"
          effect = "NO_SCHEDULE"
        }
      }

      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size           = var.ebs_volume_size
            volume_type           = "gp3"
            iops                  = 3000
            throughput            = 125
            encrypted             = true
            delete_on_termination = true
          }
        }
      }
    }

    # ---------- Node Group 2: Flink Workers (2x r6g.2xlarge) ----------
    flink = {
      ami_type       = "AL2023_ARM_64_STANDARD"
      instance_types = [var.flink_instance_type]

      min_size     = var.flink_node_count
      max_size     = var.flink_node_count
      desired_size = var.flink_node_count

      labels = {
        workload = "flink"
      }

      taints = {
        workload = {
          key    = "workload"
          value  = "flink"
          effect = "NO_SCHEDULE"
        }
      }

      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size           = var.ebs_volume_size
            volume_type           = "gp3"
            iops                  = 3000
            throughput            = 125
            encrypted             = true
            delete_on_termination = true
          }
        }
      }
    }

    # ---------- Node Group 3: Monitoring (1x r6g.2xlarge) ----------
    monitoring = {
      ami_type       = "AL2023_ARM_64_STANDARD"
      instance_types = [var.monitoring_instance_type]

      min_size     = 1
      max_size     = 1
      desired_size = 1

      labels = {
        workload = "monitoring"
      }

      taints = {
        workload = {
          key    = "workload"
          value  = "monitoring"
          effect = "NO_SCHEDULE"
        }
      }

      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size           = var.ebs_volume_size
            volume_type           = "gp3"
            iops                  = 3000
            throughput            = 125
            encrypted             = true
            delete_on_termination = true
          }
        }
      }
    }
  }

  tags = var.tags
}

# -----------------------------------------------------------------------------
# EBS CSI Driver IRSA
# -----------------------------------------------------------------------------
module "ebs_csi_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.0"

  role_name             = "${var.cluster_name}-ebs-csi-driver"
  attach_ebs_csi_policy = true

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["kube-system:ebs-csi-controller-sa"]
    }
  }

  tags = var.tags
}

# -----------------------------------------------------------------------------
# Helm & Kubectl Providers
# -----------------------------------------------------------------------------
provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)

    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name, "--region", var.region]
    }
  }
}

provider "kubectl" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
  load_config_file       = false

  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name, "--region", var.region]
  }
}

# -----------------------------------------------------------------------------
# Namespaces
# -----------------------------------------------------------------------------
resource "kubectl_manifest" "ns_streaming" {
  yaml_body = <<-YAML
    apiVersion: v1
    kind: Namespace
    metadata:
      name: streaming
      labels:
        purpose: kafka-flink-pipeline
  YAML

  depends_on = [module.eks]
}

resource "kubectl_manifest" "ns_observability" {
  yaml_body = <<-YAML
    apiVersion: v1
    kind: Namespace
    metadata:
      name: observability
      labels:
        purpose: monitoring
  YAML

  depends_on = [module.eks]
}

resource "kubectl_manifest" "ns_chaos" {
  yaml_body = <<-YAML
    apiVersion: v1
    kind: Namespace
    metadata:
      name: chaos-mesh
      labels:
        purpose: fault-injection
  YAML

  depends_on = [module.eks]
}

# -----------------------------------------------------------------------------
# gp3 StorageClass (default for Kafka/Flink persistent volumes)
# -----------------------------------------------------------------------------
resource "kubectl_manifest" "gp3_storage_class" {
  yaml_body = <<-YAML
    apiVersion: storage.k8s.io/v1
    kind: StorageClass
    metadata:
      name: gp3
      annotations:
        storageclass.kubernetes.io/is-default-class: "true"
    provisioner: ebs.csi.aws.com
    parameters:
      type: gp3
      iops: "3000"
      throughput: "125"
      encrypted: "true"
    reclaimPolicy: Delete
    volumeBindingMode: WaitForFirstConsumer
    allowVolumeExpansion: true
  YAML

  depends_on = [module.eks]
}

# -----------------------------------------------------------------------------
# Helm Releases
# -----------------------------------------------------------------------------

# Strimzi Kafka Operator
resource "helm_release" "strimzi" {
  name             = "strimzi"
  repository       = "oci://quay.io/strimzi-helm"
  chart            = "strimzi-kafka-operator"
  version          = "0.45.0"
  namespace        = "streaming"
  create_namespace = false
  force_update     = true
  replace          = true
  timeout          = 600
  cleanup_on_fail  = true

  values = [file("${path.module}/helm-values/strimzi-values.yaml")]

  depends_on = [module.eks, kubectl_manifest.ns_streaming]
}

# Flink Kubernetes Operator
resource "helm_release" "flink_operator" {
  name             = "flink-operator"
  repository       = "https://archive.apache.org/dist/flink/flink-kubernetes-operator-1.10.0/"
  chart            = "flink-kubernetes-operator"
  version          = "1.10.0"
  namespace        = "streaming"
  create_namespace = false

  values = [file("${path.module}/helm-values/flink-operator-values.yaml")]

  depends_on = [module.eks, kubectl_manifest.ns_streaming]
}

# Prometheus + Grafana (kube-prometheus-stack)
resource "helm_release" "prometheus" {
  name             = "prometheus"
  repository       = "https://prometheus-community.github.io/helm-charts"
  chart            = "kube-prometheus-stack"
  version          = "69.8.0"
  namespace        = "observability"
  create_namespace = false
  timeout          = 900
  wait             = true
  wait_for_jobs    = false

  values = [file("${path.module}/helm-values/prometheus-values.yaml")]

  depends_on = [module.eks, kubectl_manifest.ns_observability]
}

# Chaos Mesh for fault injection
resource "helm_release" "chaos_mesh" {
  name             = "chaos-mesh"
  repository       = "https://charts.chaos-mesh.org"
  chart            = "chaos-mesh"
  version          = "2.7.0"
  namespace        = "chaos-mesh"
  create_namespace = false

  values = [file("${path.module}/helm-values/chaos-mesh-values.yaml")]

  depends_on = [module.eks, kubectl_manifest.ns_chaos]
}

# -----------------------------------------------------------------------------
# ECR Repositories
# -----------------------------------------------------------------------------
resource "aws_ecr_repository" "load_generator" {
  name                 = "${var.cluster_name}/load-generator"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = false
  }

  tags = var.tags
}

resource "aws_ecr_repository" "flink_jobs" {
  name                 = "${var.cluster_name}/flink-jobs"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = false
  }

  tags = var.tags
}

# -----------------------------------------------------------------------------
# S3 Bucket for campaign data (baseline, fault injection exports)
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "campaign_data" {
  bucket        = "${var.cluster_name}-data"
  force_destroy = true

  tags = var.tags
}

resource "aws_s3_bucket_public_access_block" "campaign_data" {
  bucket                  = aws_s3_bucket.campaign_data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# -----------------------------------------------------------------------------
# IRSA: Metric Exporter (S3 put/get)
# -----------------------------------------------------------------------------
data "aws_iam_policy_document" "metric_exporter_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [module.eks.oidc_provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "${replace(module.eks.cluster_oidc_issuer_url, "https://", "")}:sub"
      values   = ["system:serviceaccount:streaming:metric-exporter"]
    }
    condition {
      test     = "StringEquals"
      variable = "${replace(module.eks.cluster_oidc_issuer_url, "https://", "")}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "metric_exporter" {
  name               = "${var.cluster_name}-metric-exporter"
  assume_role_policy = data.aws_iam_policy_document.metric_exporter_assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "metric_exporter_s3" {
  name = "s3-export"
  role = aws_iam_role.metric_exporter.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"]
      Resource = [aws_s3_bucket.campaign_data.arn, "${aws_s3_bucket.campaign_data.arn}/*"]
    }]
  })
}

# -----------------------------------------------------------------------------
# IRSA: Health Monitor Alerter (SNS publish)
# -----------------------------------------------------------------------------
data "aws_iam_policy_document" "alerter_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [module.eks.oidc_provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "${replace(module.eks.cluster_oidc_issuer_url, "https://", "")}:sub"
      values   = ["system:serviceaccount:streaming:health-monitor"]
    }
    condition {
      test     = "StringEquals"
      variable = "${replace(module.eks.cluster_oidc_issuer_url, "https://", "")}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "alerter" {
  name               = "${var.cluster_name}-alerter"
  assume_role_policy = data.aws_iam_policy_document.alerter_assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "alerter_sns" {
  name = "sns-publish"
  role = aws_iam_role.alerter.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "sns:Publish"
      Resource = aws_sns_topic.alerts.arn
    }]
  })
}

# -----------------------------------------------------------------------------
# SNS Topic + Email Subscription for pipeline alerts
# -----------------------------------------------------------------------------
resource "aws_sns_topic" "alerts" {
  name = "${var.cluster_name}-alerts"
  tags = var.tags
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}
