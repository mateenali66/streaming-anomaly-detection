# Streaming anomaly detection on Apache Kafka and Flink

Code for the paper "ML-based anomaly detection for streaming pipelines: an empirical benchmark with cross-workload generalization on Apache Kafka and Flink" by Mateen Ali Anjum (Phono Technologies Inc.).

Two streaming pipelines, e-commerce and industrial IoT, run on Amazon EKS with Kafka 3.9 and Flink 1.19. Eight faults are scripted ten times each on each workload, and ten detectors are compared over ten folds. The F1 (broker crash) and F2 (network partition) injections did not take effect, as `analysis/cross_workload/injection_check.py` shows.

## Layout

- `infrastructure/` holds the Terraform, Helm values, Kubernetes manifests and the fault-injection orchestrator.
- `streaming-app/` holds the load generators, Flink jobs and Avro schemas.
- `sagemaker/` holds feature extraction, training and the instance scripts.
- `analysis/cross_workload/` holds the analysis scripts and their outputs. `ARTIFACTS.md` maps each table and figure to its script.

## Data and checks

The datasets, per-window scores and analysis outputs are on Zenodo at https://doi.org/10.5281/zenodo.19475837. Unpack the v9 archives into one directory and run `python analysis/cross_workload/verify_claims.py` to recompute the paper's numbers.

The code is MIT licensed and the data CC BY 4.0.
