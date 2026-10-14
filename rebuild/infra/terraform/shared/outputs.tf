output "results_bucket" {
  value       = aws_s3_bucket.results.id
  description = "Protected results bucket. Campaigns write campaign/<id>/..., Flink writes scratch/<id>/..."
}

output "alerts_topic_arn" {
  value       = aws_sns_topic.alerts.arn
  description = "The one SNS topic."
}

output "ecr_registry" {
  value       = "${data.aws_caller_identity.this.account_id}.dkr.ecr.${var.region}.amazonaws.com"
  description = "Registry host for ECR_REGISTRY in the render and build scripts."
}

output "ecr_built_repositories" {
  value       = { for k, v in aws_ecr_repository.built : k => v.repository_url }
  description = "Repositories for the images we build."
}

output "ecr_mirror_repositories" {
  value       = { for k, v in aws_ecr_repository.mirror : k => v.repository_url }
  description = "Repositories for mirrored public images, keyed by the images.lock.yaml `key`."
}

output "health_log_group_name" {
  value       = aws_cloudwatch_log_group.health.name
  description = "Log group with the region's AWS Health events (EventBridge rule). The cluster root grants the orchestrator FilterLogEvents on it."
}
