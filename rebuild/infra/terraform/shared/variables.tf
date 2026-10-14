variable "region" {
  type        = string
  default     = "us-east-2"
  description = "Target region."
}

variable "account_id" {
  type        = string
  description = "Guard. The AWS provider refuses to run in any other account. Pass it in a gitignored *.tfvars or TF_VAR_account_id. Never commit it."
  validation {
    condition     = can(regex("^[0-9]{12}$", var.account_id))
    error_message = "account_id must be 12 digits."
  }
}

variable "aws_profile" {
  type        = string
  default     = null
  description = "Optional named profile. Leave null and export AWS_PROFILE instead, so no profile name is written to a file."
}

variable "project" {
  type        = string
  default     = "streaming-anomaly-detection"
  description = "Name prefix for every resource and the value of the Project tag. No paper number in names."
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{3,30}$", var.project))
    error_message = "Lower case letters, digits and hyphens, 4 to 31 characters."
  }
}

variable "owner" {
  type        = string
  description = "Value of the Owner tag, so other teams can find who owns these resources. Use a team alias, not an e-mail address."
  validation {
    condition     = !can(regex("@", var.owner))
    error_message = "Owner must not be an e-mail address."
  }
}

variable "alert_email" {
  type        = string
  default     = ""
  description = "E-mail for the one SNS topic and the optional budget alerts. Empty means no subscription. Set it in a gitignored tfvars."
}

variable "budget_limit_usd" {
  type        = number
  default     = 0
  description = "Monthly cap for the Project-tag budget. 0 disables the budget. Needs alert_email, and tracks nothing until the payer account has activated the Project cost allocation tag."
  validation {
    condition     = var.budget_limit_usd >= 0
    error_message = "Must be zero or positive."
  }
}

variable "bucket_suffix" {
  type        = string
  description = "Fixed random suffix of the results bucket name, chosen ONCE before the first apply and kept in the gitignored terraform.tfvars (a bucket cannot be renamed). Make it with: python3 -c 'import secrets; print(secrets.token_hex(4))'. No account ID, no profile name (decision 29)."
  validation {
    condition     = can(regex("^[a-z0-9]{6,16}$", var.bucket_suffix))
    error_message = "6 to 16 lower-case letters or digits."
  }
  validation {
    condition     = !can(regex("^[0-9]{12}$", var.bucket_suffix))
    error_message = "The suffix must not be an account ID."
  }
}

variable "scratch_expiry_days" {
  type        = number
  default     = 7
  description = "Days after which objects under scratch/ (Flink checkpoints and savepoints) expire."
  validation {
    condition     = var.scratch_expiry_days >= 2
    error_message = "At least 2 days."
  }
}
