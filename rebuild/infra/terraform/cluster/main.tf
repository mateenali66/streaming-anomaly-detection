provider "aws" {
  region              = var.region
  profile             = var.aws_profile
  allowed_account_ids = [var.account_id] # wrong credentials => hard error, nothing is touched

  default_tags {
    tags = local.tags
  }
}

data "aws_caller_identity" "this" {}

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

locals {
  # EKS name limit 100, IAM name limit 64. Longest derived IAM name is "<name>-monitoring-node": 37 + 16 = 53.
  name = "${var.project}-${var.campaign_id}"

  tags = {
    Project   = var.project
    Campaign  = var.campaign_id
    Workload  = var.workload
    Scope     = "cluster"
    ManagedBy = "terraform"
    Owner     = var.owner
  }

  results_bucket = data.terraform_remote_state.shared.outputs.results_bucket
  alerts_topic   = data.terraform_remote_state.shared.outputs.alerts_topic_arn
  ecr_registry   = data.terraform_remote_state.shared.outputs.ecr_registry
  # AWS Health events for the region, written by the shared root's EventBridge rule (protocol 1.7, REVIEW4 M12)
  health_log_group = data.terraform_remote_state.shared.outputs.health_log_group_name

  vpc_id            = data.terraform_remote_state.network.outputs.vpc_id
  node_subnet_id    = data.terraform_remote_state.network.outputs.slot_subnet_by_campaign[var.campaign_id]
  cp_subnet_ids     = data.terraform_remote_state.network.outputs.control_plane_subnet_ids
  results_prefix    = "campaign/${var.campaign_id}"
  scratch_prefix    = "scratch/${var.campaign_id}"
  account_id        = data.aws_caller_identity.this.account_id
  node_group_counts = { kafka = var.kafka_node_count, flink = var.flink_node_count, monitoring = var.monitoring_node_count }
  node_group_types  = { kafka = var.kafka_instance_type, flink = var.flink_instance_type, monitoring = var.monitoring_instance_type }
}

check "derived_names_fit_limits" {
  assert {
    # longest derived IAM role name is "<name>-monitoring-node" (IAM limit 64); the EKS cluster name limit is 100
    condition     = length(local.name) + length("-monitoring-node") <= 64
    error_message = "A derived IAM role name would exceed 64 characters. Shorten var.project or the campaign id."
  }
}

# The campaign id and the workload must agree: a cross-variable validation on var.workload (variables.tf).
#
# Refuse to build in anything that is not the VPC our network root created. The checks are postconditions on the two data
# sources, and the module takes its VPC and subnet ids FROM these data sources, so it depends on the checked values. There is
# no module-level depends_on: it would defer every data source inside the module to apply time and the first plan would
# stop with "Invalid count argument" (REVIEW4_OPERATIONS B2; developer.hashicorp.com/terraform/language/meta-arguments/depends_on).
data "aws_vpc" "this" {
  id = local.vpc_id

  lifecycle {
    postcondition {
      condition     = lookup(self.tags, "Project", "") == var.project && lookup(self.tags, "Scope", "") == "network"
      error_message = "The VPC from the network state is not tagged Project=${var.project}, Scope=network. Refusing to build in a VPC this project did not create."
    }
  }
}

data "aws_subnet" "node" {
  id = local.node_subnet_id

  lifecycle {
    postcondition {
      condition     = self.vpc_id == data.aws_vpc.this.id
      error_message = "The node subnet of ${var.campaign_id} is not in the project VPC. Refusing to build."
    }
  }
}

# ---------------------------------------------------------------- EKS
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "= 21.29.0"

  name               = local.name
  kubernetes_version = var.kubernetes_version

  vpc_id                   = data.aws_vpc.this.id      # checked by the postcondition above
  subnet_ids               = [data.aws_subnet.node.id] # ONE zone for every node (decision 16), checked above
  control_plane_subnet_ids = concat([data.aws_subnet.node.id], local.cp_subnet_ids)

  endpoint_public_access       = true
  endpoint_public_access_cidrs = var.api_allowed_cidrs
  endpoint_private_access      = true

  enable_cluster_creator_admin_permissions = true
  enable_irsa                              = false # Pod Identity: no per-cluster OIDC provider to leak

  create_kms_key                      = false
  encryption_config                   = null
  enabled_log_types                   = [] # control-plane logs cost $0.50 per GB and nothing here reads them
  create_cloudwatch_log_group         = false
  iam_role_use_name_prefix            = false # name_prefix is capped at 38 characters, our names are longer
  security_group_use_name_prefix      = false
  node_security_group_use_name_prefix = false

  # The API server's service proxy (`kubectl get --raw .../services/<svc>:<port>/proxy/...`) connects from the control plane
  # to the pod port. The module opens only the webhook and kubelet ports, so the pilot's first check_flink_jobs.py call timed
  # out. These three ports are opened from the cluster security group alone: Flink REST (check_flink_jobs.py), Prometheus and
  # the load generator status (runbook section 10). In-cluster traffic never needed them.
  node_security_group_additional_rules = {
    for name, port in { flink_rest  = 8081, prometheus = 9090, loadgen_status = 9102 } : "ingress_cluster_proxy_${name}" => {
      description                   = "Cluster API service proxy to ${name} ${port}/tcp"
      protocol                      = "tcp"
      from_port                     = port
      to_port                       = port
      type                          = "ingress"
      source_cluster_security_group = true
    }
  }

  addons = {
    coredns = {
      addon_version               = var.addon_versions["coredns"]
      resolve_conflicts_on_create = "OVERWRITE"
      resolve_conflicts_on_update = "OVERWRITE"
      configuration_values = jsonencode({
        tolerations  = [{ key = "workload", operator = "Exists", effect = "NoSchedule" }]
        nodeSelector = { workload = "monitoring" }
      })
    }
    kube-proxy = {
      addon_version = var.addon_versions["kube-proxy"] # iptables mode (IPVS is deprecated in 1.35)
    }
    vpc-cni = {
      addon_version  = var.addon_versions["vpc-cni"]
      before_compute = true
    }
    eks-pod-identity-agent = {
      addon_version  = var.addon_versions["eks-pod-identity-agent"]
      before_compute = true
    }
    aws-ebs-csi-driver = {
      addon_version = var.addon_versions["aws-ebs-csi-driver"]
      pod_identity_association = [{
        role_arn        = aws_iam_role.ebs_csi.arn
        service_account = "ebs-csi-controller-sa"
      }]
      resolve_conflicts_on_create = "OVERWRITE"
      resolve_conflicts_on_update = "OVERWRITE"
      configuration_values = jsonencode({
        controller = {
          # Every PV carries Campaign=<id> and Scope=pv. The IAM policy below and the teardown
          # guard and sweep both key on these tags.
          extraVolumeTags = merge(local.tags, { Scope = "pv" })
          tolerations     = [{ key = "workload", operator = "Exists", effect = "NoSchedule" }]
          nodeSelector    = { workload = "monitoring" }
        }
      })
    }
  }

  eks_managed_node_groups = {
    for k, count in local.node_group_counts : k => {
      ami_type            = "AL2023_ARM_64_STANDARD"
      ami_release_version = var.node_ami_release_version
      # The node group submodule defaults this to true (nullable = false) and then ignores ami_release_version, taking the
      # SSM "recommended" release instead. A later apply would then roll every node to a newer release (REVIEW4 C2).
      use_latest_ami_release_version = false
      capacity_type                  = "ON_DEMAND"
      instance_types                 = [local.node_group_types[k]]
      min_size                       = count
      max_size                       = count
      desired_size                   = count
      use_name_prefix                = false
      iam_role_use_name_prefix       = false
      iam_role_name                  = "${local.name}-${k}-node"
      labels                         = { workload = k }
      taints                         = { workload = { key = "workload", value = k, effect = "NO_SCHEDULE" } }
      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size           = var.node_root_volume_gb
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

  tags = local.tags
}
