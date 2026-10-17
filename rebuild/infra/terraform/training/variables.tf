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

variable "state_bucket" {
  type        = string
  description = "Name of the hand-made Terraform state bucket. Used to read the network and shared roots' outputs."
}

variable "instance_type" {
  type    = string
  default = "g5.12xlarge"
}

variable "slot_key" {
  type        = string
  default     = "ec-d"
  description = "Key of network.slot_subnet_by_campaign whose subnet takes the instance. ec-d is a reserve campaign slot that no campaign used."
}

variable "subnet" {
  type        = string
  default     = "slot"
  description = "slot: the reserve slot subnet in the workload zone. cp0 or cp1: the network root's subnet in its first or second other zone, for when the workload zone has no capacity for the instance type. All three route through the same NAT gateway and S3 endpoint."
  validation {
    condition     = contains(["slot", "cp0", "cp1"], var.subnet)
    error_message = "subnet must be slot, cp0 or cp1."
  }
}

variable "ami_ssm_parameter" {
  type        = string
  default     = "/aws/service/deeplearning/ami/x86_64/base-oss-nvidia-driver-gpu-ubuntu-22.04/latest/ami-id"
  description = "Public SSM parameter of the Deep Learning Base AMI (NVIDIA driver, no framework). The resolved AMI id is an output and goes in the log."
}

variable "root_volume_gib" {
  type    = number
  default = 300
}

variable "cap_hours" {
  type        = number
  default     = 16
  description = "Hard cap. The instance shuts itself down this many hours after its first boot and, with shutdown behavior terminate, is terminated."
  validation {
    condition     = var.cap_hours > 0 && var.cap_hours <= 16
    error_message = "cap_hours must be between 0 and 16, the approved limit."
  }
}
