#!/usr/bin/env python3
"""
Extract simplified baseline stats for the campaign orchestrator.

The orchestrator expects a flat dict with:
- consumer_lag_mean, consumer_lag_std
- throughput_mean, throughput_std

This script extracts those from the detailed baseline_stats.json.
"""
import argparse
import json
import numpy as np
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    with open(args.input) as f:
        detailed = json.load(f)

    features = detailed["features"]

    # Consumer lag: max across all partitions (records_lag_max from Flink KafkaConsumer)
    lag_features = [k for k in features if "records_lag_max" in k]
    lag_means = [features[k]["mean"] for k in lag_features
                 if features[k]["mean"] is not None and features[k]["mean"] > -1e9]
    lag_stds = [features[k]["std"] for k in lag_features
                if features[k]["std"] is not None]

    # Throughput: sum messagesinpersec across brokers for raw-events topic
    throughput_features = [k for k in features
                           if "messagesinpersec_total" in k
                           and "topic_raw_events" in k]
    tp_means = [features[k]["mean"] for k in throughput_features
                if features[k]["mean"] is not None]
    tp_stds = [features[k]["std"] for k in throughput_features
               if features[k]["std"] is not None]

    # For consumer lag, report max observed as the "normal" threshold
    # (since it's a max per operator, we want the overall max across operators)
    lag_mean = float(np.mean(lag_means)) if lag_means else 0.0
    lag_std = float(np.mean(lag_stds)) if lag_stds else 1.0

    # For throughput, sum across brokers (each broker sees a portion)
    tp_mean = float(np.sum(tp_means)) if tp_means else 0.0
    tp_std = float(np.sqrt(np.sum([s**2 for s in tp_stds]))) if tp_stds else 1.0

    flat_stats = {
        "consumer_lag_mean": lag_mean,
        "consumer_lag_std": max(lag_std, 1.0),  # minimum std of 1 to avoid zero-variance issues
        "throughput_mean": tp_mean,
        "throughput_std": max(tp_std, 1.0),
        "_source": detailed["metadata"]["source_file"],
        "_num_timestamps": detailed["metadata"]["num_timestamps"],
        "_lag_features_used": len(lag_features),
        "_throughput_features_used": len(throughput_features),
    }

    with open(args.output, "w") as f:
        json.dump(flat_stats, f, indent=2)

    print(f"Extracted flat stats to {args.output}:")
    print(f"  consumer_lag_mean:   {lag_mean:.2f}")
    print(f"  consumer_lag_std:    {max(lag_std, 1.0):.2f}")
    print(f"  throughput_mean:     {tp_mean:.2f} msgs/sec")
    print(f"  throughput_std:      {max(tp_std, 1.0):.2f}")
    print(f"  lag features:        {len(lag_features)}")
    print(f"  throughput features: {len(throughput_features)}")


if __name__ == "__main__":
    main()
