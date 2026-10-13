variable "region" {
  description = "AWS region for the EKS cluster"
  type        = string
  default     = "us-east-2"
}

variable "cluster_name" {
  description = "Name of the EKS cluster"
  type        = string
  default     = "stream-ad-bench"
}

variable "kubernetes_version" {
  description = "Kubernetes version for EKS"
  type        = string
  default     = "1.30"
}

variable "kafka_instance_type" {
  description = "EC2 instance type for Kafka broker nodes"
  type        = string
  default     = "r6g.xlarge"
}

variable "flink_instance_type" {
  description = "EC2 instance type for Flink worker nodes"
  type        = string
  default     = "r6g.2xlarge"
}

variable "monitoring_instance_type" {
  description = "EC2 instance type for monitoring node"
  type        = string
  default     = "r6g.2xlarge"
}

variable "kafka_node_count" {
  description = "Number of Kafka broker nodes (1 broker per node)"
  type        = number
  default     = 3
}

variable "flink_node_count" {
  description = "Number of Flink worker nodes"
  type        = number
  default     = 2
}

variable "ebs_volume_size" {
  description = "EBS volume size in GB per node"
  type        = number
  default     = 100
}

variable "alert_email" {
  description = "Email address for SNS pipeline degradation alerts"
  type        = string
  default     = ""
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default = {
    Project     = "stream-ad-bench"
    Environment = "research"
    Terraform   = "true"
    Author      = "mateen-ali-anjum"
  }
}
