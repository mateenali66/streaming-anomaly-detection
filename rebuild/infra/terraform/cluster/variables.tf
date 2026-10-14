variable "region" {
  type    = string
  default = "us-east-2"
}

variable "account_id" {
  type        = string
  description = "Guard. The provider refuses to run in any other account. Never commit."
  validation {
    condition     = can(regex("^[0-9]{12}$", var.account_id))
    error_message = "account_id must be 12 digits."
  }
}

variable "aws_profile" {
  type        = string
  default     = null
  description = "Optional. Prefer exporting AWS_PROFILE so no profile name is written to a file."
}

variable "project" {
  type    = string
  default = "streaming-anomaly-detection"
}

variable "owner" {
  type = string
  validation {
    condition     = !can(regex("@", var.owner))
    error_message = "Owner must not be an e-mail address."
  }
}

variable "campaign_id" {
  type        = string
  description = "Short unique id, for example ec-a, iot-b, pilot-ec. Suffix of every name, tag value and S3 prefix. At most 9 characters."
  validation {
    condition     = can(regex("^[a-z]{2,5}(-[a-z0-9]{1,3})$", var.campaign_id)) && length(var.campaign_id) <= 9
    error_message = "Like ec-a, iot-c, pilot-iot (at most 9 characters)."
  }
}

variable "workload" {
  type = string
  validation {
    condition     = contains(["ecommerce", "iot"], var.workload)
    error_message = "ecommerce or iot."
  }
  # Cross-variable validation (Terraform >= 1.9; this code needs >= 1.10). It replaces a terraform_data precondition that
  # the EKS module had to depend on, which broke the first plan (REVIEW4 B2).
  validation {
    condition     = (var.workload == "ecommerce" && can(regex("^(ec-|pilot-ec$)", var.campaign_id))) || (var.workload == "iot" && can(regex("^(iot-|pilot-iot$)", var.campaign_id)))
    error_message = "campaign_id does not belong to this workload. Use ec-* or pilot-ec for ecommerce, iot-* or pilot-iot for iot."
  }
}

variable "state_bucket" {
  type        = string
  description = "Name of the hand-made Terraform state bucket. Used to read the network and shared roots' outputs."
}

variable "api_allowed_cidrs" {
  type        = list(string)
  description = "CIDRs allowed to reach the public EKS API endpoint. Your own /32. 0.0.0.0/0 and anything wider than /16 are rejected."
  validation {
    condition     = length(var.api_allowed_cidrs) > 0 && alltrue([for c in var.api_allowed_cidrs : can(cidrhost(c, 0))])
    error_message = "Give at least one valid CIDR."
  }
  validation {
    condition     = !contains(var.api_allowed_cidrs, "0.0.0.0/0") && alltrue([for c in var.api_allowed_cidrs : can(cidrhost(c, 0)) ? tonumber(split("/", c)[1]) >= 16 : false])
    error_message = "0.0.0.0/0 and ranges wider than /16 are not allowed in a shared account."
  }
}

variable "python_bin" {
  type        = string
  description = "ABSOLUTE path of the interpreter with boto3 (the venv of scripts/requirements.txt). Used by scripts/eks_token.py for the Helm and kubectl providers and by the teardown guard. Required: the python3 on the operator's PATH has no boto3, and a wrong value fails only after the cluster exists (REVIEW4 M8)."
  validation {
    condition     = startswith(var.python_bin, "/")
    error_message = "python_bin must be an absolute path, for example /path/to/rebuild/infra/.venv/bin/python."
  }
}

variable "kubernetes_version" {
  type    = string
  default = "1.35"
  validation {
    condition     = contains(["1.34", "1.35", "1.36", "1.37"], var.kubernetes_version)
    error_message = "Use a standard-support EKS version (decision 11)."
  }
}

# ---- required, NO defaults: captured once by scripts/capture_versions.py and committed ----
variable "node_ami_release_version" {
  type        = string
  description = "EKS AL2023 ARM release, for example 1.35.0-20261003, from the SSM parameter /aws/service/eks/optimized-ami/<ver>/amazon-linux-2023/arm64/standard/recommended/release_version."
  validation {
    condition     = can(regex("^[0-9]+\\.[0-9]+\\.[0-9]+-[0-9]{8}$", var.node_ami_release_version))
    error_message = "Expected <k8s version>-<yyyymmdd>."
  }
  # The placeholder of addon-versions.auto.tfvars.example matches the pattern, so it is refused by name (REVIEW4 M9).
  validation {
    condition     = var.node_ami_release_version != "1.35.0-20260101"
    error_message = "This is the placeholder from addon-versions.auto.tfvars.example. Run scripts/capture_versions.py."
  }
}

variable "addon_versions" {
  type        = map(string)
  description = "Add-on name => version (v1.2.3-eksbuild.N) for the pinned Kubernetes version. No defaults on purpose."
  validation {
    condition     = alltrue([for k in ["coredns", "kube-proxy", "vpc-cni", "eks-pod-identity-agent", "aws-ebs-csi-driver"] : contains(keys(var.addon_versions), k)])
    error_message = "addon_versions must contain coredns, kube-proxy, vpc-cni, eks-pod-identity-agent and aws-ebs-csi-driver."
  }
  validation {
    condition     = alltrue([for k, v in var.addon_versions : can(regex("^v[0-9]+\\.[0-9]+\\.[0-9]+-eksbuild\\.[0-9]+$", v))])
    error_message = "Versions look like v1.21.1-eksbuild.7."
  }
  validation {
    condition     = !contains(values(var.addon_versions), "v1.0.0-eksbuild.1")
    error_message = "v1.0.0-eksbuild.1 is the placeholder from addon-versions.auto.tfvars.example. Run scripts/capture_versions.py."
  }
}

# ---- sizes: as in R1 (3 Kafka, 2 Flink, 1 monitoring) ----
variable "kafka_instance_type" {
  type    = string
  default = "r6g.xlarge"
}

variable "flink_instance_type" {
  type    = string
  default = "r6g.2xlarge"
}

variable "monitoring_instance_type" {
  type    = string
  default = "r6g.2xlarge"
}

variable "kafka_node_count" {
  type    = number
  default = 3
}

variable "flink_node_count" {
  type    = number
  default = 2
}

variable "monitoring_node_count" {
  type    = number
  default = 1
}

variable "node_root_volume_gb" {
  type    = number
  default = 100
}

variable "chart_versions" {
  type = object({
    strimzi               = string
    flink_operator        = string
    kube_prometheus_stack = string
    chaos_mesh            = string
  })
  default = {
    strimzi               = "0.45.2" # Kafka 3.8.0 to 3.9.2, tested on Kubernetes 1.25 to 1.35 (strimzi.io/downloads)
    flink_operator        = "1.10.0" # R1 continuity. Fallback 1.16.1: same values file works, see README.
    kube_prometheus_stack = "69.8.0"
    chaos_mesh            = "2.8.4"
  }
}

variable "enforce_image_digests" {
  type        = bool
  default     = true
  description = "Fail the plan when images.lock.yaml still has an empty digest. Set false only for terraform validate on the template."
}
