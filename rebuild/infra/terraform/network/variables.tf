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
  type    = string
  default = null
}

variable "python_bin" {
  type        = string
  description = "ABSOLUTE path of the interpreter with boto3 (rebuild/infra/.venv/bin/python). The destroy guard runs scripts/network_guard.py with it."
  validation {
    condition     = startswith(var.python_bin, "/")
    error_message = "python_bin must be an absolute path."
  }
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

variable "vpc_cidr" {
  type        = string
  description = "A /16 that overlaps no VPC in the account (check with scripts/discover.py, read only)."
  validation {
    condition     = can(cidrhost(var.vpc_cidr, 0)) && endswith(var.vpc_cidr, "/16")
    error_message = "Must be a valid /16 (the slot arithmetic assumes it)."
  }
}

variable "workload_az" {
  type        = string
  description = "ONE availability zone for every node, the NAT gateway and every EBS volume (author decision 16). Pick one where r6g.xlarge and r6g.2xlarge are both offered (scripts/discover.py)."
}

variable "control_plane_azs" {
  type        = list(string)
  description = "Two OTHER zones, used only for the EKS control-plane network interfaces (EKS needs subnets in at least two zones)."
  validation {
    condition     = length(var.control_plane_azs) == 2 && length(distinct(var.control_plane_azs)) == 2
    error_message = "Exactly two distinct zones (and neither may be workload_az: checked in main.tf)."
  }
}

variable "campaign_slots" {
  type        = map(number)
  description = "Campaign id => index of its pre-cut /22 subnet. Deterministic, so two live clusters can never share a subnet."
  default = {
    "pilot-ec"  = 0
    "pilot-iot" = 1
    "ec-a"      = 2
    "ec-b"      = 3
    "ec-c"      = 4
    "iot-a"     = 5
    "iot-b"     = 6
    "iot-c"     = 7
    "ec-d"      = 8 # reserve campaign (protocol 2.6, abandoned campaign replaced on a fresh cluster)
    "iot-d"     = 9
  }
  validation {
    condition     = length(distinct(values(var.campaign_slots))) == length(var.campaign_slots)
    error_message = "Two campaigns share a slot."
  }
  validation {
    condition     = alltrue([for k, v in var.campaign_slots : can(regex("^[a-z]{2,5}(-[a-z0-9]{1,3})$", k)) && length(k) <= 9])
    error_message = "Campaign ids look like ec-a, iot-b, pilot-ec and are at most 9 characters."
  }
}

variable "slots" {
  type        = number
  default     = 10
  description = "Number of /22 node subnets to cut. Must exceed the highest campaign_slots value."
  validation {
    condition     = var.slots >= 1 && var.slots <= 32
    error_message = "1 to 32."
  }
}

variable "interface_endpoints" {
  type        = bool
  default     = false
  description = "ECR, STS interface endpoints. Off: each costs about $0.01 per hour and saves little NAT data here."
}
