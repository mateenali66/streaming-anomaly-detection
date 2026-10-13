region               = "us-east-2"
cluster_name         = "stream-ad-bench"
kubernetes_version   = "1.30"
kafka_instance_type  = "r6g.xlarge"
flink_instance_type  = "r6g.2xlarge"
monitoring_instance_type = "r6g.2xlarge"
kafka_node_count     = 3
flink_node_count     = 2
ebs_volume_size      = 100

tags = {
  Project     = "stream-ad-bench"
  Environment = "research"
  Terraform   = "true"
  Author      = "mateen-ali-anjum"
}
