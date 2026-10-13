# Infrastructure

Terraform, Helm values and Kubernetes manifests for the testbed. Terraform creates a VPC and an EKS 1.30 cluster in us-east-2 with three Graviton node groups, and installs Strimzi, the Flink Kubernetes Operator, kube-prometheus-stack and Chaos Mesh. Kafka 3.9 (KRaft, three brokers) and Flink 1.19 (one JobManager, three TaskManagers) run on separate nodes, with Prometheus and Grafana on a monitoring node.

```bash
export AWS_PROFILE=<your-aws-profile>
terraform init && terraform apply
aws eks update-kubeconfig --name stream-ad-bench --region us-east-2
kubectl apply -f manifests/kafka-cluster.yaml
kubectl wait kafka/streaming-kafka --for=condition=Ready --timeout=300s -n streaming
kubectl apply -f manifests/kafka-topics.yaml -f manifests/schema-registry.yaml
kubectl apply -f manifests/flink-session-cluster.yaml
```

`scripts/campaign_orchestrator.py` runs the fault-injection campaign, and `scripts/export_metrics.py` exports the Prometheus metrics to Parquet. Set the Grafana admin password at deploy time.

To remove everything, delete the manifests and run `terraform destroy`.
