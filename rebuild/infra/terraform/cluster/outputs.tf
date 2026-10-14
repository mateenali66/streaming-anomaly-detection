output "cluster_name" {
  value = module.eks.cluster_name
}

output "campaign_id" {
  value = var.campaign_id
}

output "workload" {
  value = var.workload
}

output "configure_kubectl" {
  value       = "${var.python_bin} scripts/eks_kubeconfig.py --cluster-name ${module.eks.cluster_name} --region ${var.region} --campaign ${var.campaign_id} --output kubeconfig-${var.campaign_id}"
  description = "Writes a kubeconfig that authenticates through scripts/eks_token.py (boto3)."
}

output "results_bucket" {
  value = local.results_bucket
}

output "results_prefix" {
  value = "s3://${local.results_bucket}/${local.results_prefix}/"
}

output "scratch_prefix" {
  value = "s3://${local.results_bucket}/${local.scratch_prefix}/"
}

output "alerts_topic_arn" {
  value = local.alerts_topic
}

output "ecr_registry" {
  value = local.ecr_registry
}

output "orchestrator_role_arn" {
  value = aws_iam_role.orchestrator.arn
}

output "exporter_role_arn" {
  value = aws_iam_role.exporter.arn
}

output "flink_role_arn" {
  value = aws_iam_role.flink.arn
}

output "health_log_group_name" {
  value       = local.health_log_group
  description = "CloudWatch Logs group with the AWS Health events of the region (shared root). The orchestrator role may FilterLogEvents on it."
}
