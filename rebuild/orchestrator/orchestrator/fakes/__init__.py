"""Fake cluster harness: a simulated Kafka/Flink/Chaos Mesh cluster, Prometheus, S3, probe service and clock.

Used by `orchestrator run --dry-run` and by the test suite. Nothing here talks to AWS or to a cluster.
"""
