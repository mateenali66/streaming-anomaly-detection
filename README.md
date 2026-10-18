# Streaming anomaly detection on Apache Kafka and Flink

Code for the paper "ML-based anomaly detection for streaming pipelines: an empirical benchmark with cross-workload generalization on Apache Kafka and Flink" by Mateen Ali Anjum (Phono Technologies Inc.).

The paper's results come from `rebuild/`. Six campaigns, three e-commerce and three IoT, ran on fresh Amazon EKS clusters under a protocol registered with the signed tag `prereg-2026-10-14`. `rebuild/protocol/DEVIATIONS.md` lists every change made after registration.

- `rebuild/pipeline/` builds features, trains the detectors and runs the analysis (`make from-scores`, `make all`).
- `rebuild/orchestrator/` runs a campaign and the injection gate.
- `rebuild/infra/` holds the Terraform, Kubernetes manifests and container images.
- `streaming-app/` holds the load generators and Flink jobs.

Data, scores and analysis outputs are on Zenodo at https://doi.org/10.5281/zenodo.23346807 (all versions at https://doi.org/10.5281/zenodo.19475837).

The top-level `analysis/`, `infrastructure/`, `sagemaker/`, `data/` and `figures/` folders and `ARTIFACTS.md` hold the earlier version of the study (Zenodo version 8), in which four of the eight fault injections did not take effect. They are kept for the record.

The code is MIT licensed and the data CC BY 4.0.
