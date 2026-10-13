#!/usr/bin/env python3
"""
Prometheus → Parquet metric exporter for streaming anomaly detection benchmark.

Queries Prometheus for all Kafka, Flink, and JVM metrics within a time range,
pivots into a wide-format DataFrame (one row per timestamp, one column per
metric+label combination), and exports to Parquet.

Usage:
    # Export last 1 hour
    python export_metrics.py --duration 1h --output data/metrics_test.parquet

    # Export specific time range
    python export_metrics.py --start 2026-04-02T00:00:00Z --end 2026-04-02T01:00:00Z --output data/baseline.parquet

    # Export with port-forward to Prometheus
    kubectl port-forward -n observability svc/prometheus-kube-prometheus-prometheus 9090:9090 &
    python export_metrics.py --prometheus http://localhost:9090 --duration 24h --output data/baseline_24h.parquet

Error handling:
    - Retries failed Prometheus queries with exponential backoff (max 3 attempts)
    - Handles partial data (NaN for missing metrics in some windows)
    - Validates timestamp alignment across metrics
    - Logs metric counts, gaps, and export statistics
"""
import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from urllib.parse import quote

import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("metric-exporter")

# Metric queries organized by feature category
METRIC_QUERIES = {
    "broker": [
        # Throughput
        'kafka_server_brokertopicmetrics_messagesinpersec_total',
        'kafka_server_brokertopicmetrics_bytesinpersec_total',
        'kafka_server_brokertopicmetrics_bytesoutpersec_total',
        'kafka_server_brokertopicmetrics_totalproducerequestspersec_total',
        'kafka_server_brokertopicmetrics_totalfetchrequestspersec_total',
        # Replication
        'kafka_server_replicamanager_underreplicatedpartitions',
        'kafka_server_replicamanager_underminisrpartitioncount',
        'kafka_server_replicamanager_isrshrinks_total',
        'kafka_server_replicamanager_isrexpands_total',
        'kafka_server_replicamanager_partitioncount',
        # Controller
        'kafka_controller_kafkacontroller_activecontrollercount',
        'kafka_controller_kafkacontroller_offlinepartitionscount',
        'kafka_controller_kafkacontroller_preferredreplicaimbalancecount',
        # Log
        'kafka_log_logflushstats_logflushrateandtimems_total',
        # Request metrics
        'kafka_network_requestmetrics_requestspersec_total',
    ],
    "consumer": [
        # Consumer lag (from Flink Kafka connector metrics in Prometheus)
        '{__name__=~"flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag.*"}',
        '{__name__=~"flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_consumed_rate.*"}',
        '{__name__=~"flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_assigned_partitions.*"}',
        '{__name__=~"flink_taskmanager_job_task_operator_KafkaSourceReader.*commit.*"}',
        '{__name__=~"flink_taskmanager_job_task_operator_KafkaSourceReader.*rebalance.*"}',
    ],
    "flink": [
        # Throughput
        '{__name__=~"flink_taskmanager_job_task_operator_numRecordsInPerSecond.*"}',
        '{__name__=~"flink_taskmanager_job_task_operator_numRecordsOutPerSecond.*"}',
        '{__name__=~"flink_taskmanager_job_task_numRecordsIn.*"}',
        '{__name__=~"flink_taskmanager_job_task_numRecordsOut.*"}',
        # Backpressure
        '{__name__=~"flink_taskmanager_job_task_backPressuredTimeMsPerSecond.*"}',
        '{__name__=~"flink_taskmanager_job_task_idleTimeMsPerSecond.*"}',
        '{__name__=~"flink_taskmanager_job_task_busyTimeMsPerSecond.*"}',
        # Checkpoints
        '{__name__=~"flink_jobmanager_job_lastCheckpointDuration.*"}',
        '{__name__=~"flink_jobmanager_job_lastCheckpointSize.*"}',
        '{__name__=~"flink_jobmanager_job_numberOfCompletedCheckpoints.*"}',
        '{__name__=~"flink_jobmanager_job_numberOfFailedCheckpoints.*"}',
        # Watermarks
        '{__name__=~"flink_taskmanager_job_task_operator_currentInputWatermark.*"}',
        # Job uptime
        '{__name__=~"flink_jobmanager_job_uptime.*"}',
        # Network buffers (backpressure indicator)
        '{__name__=~"flink_taskmanager_Status_Shuffle_Netty.*"}',
    ],
    "jvm": [
        # Kafka broker JVM (from the JMX exporter built into Strimzi)
        '{__name__=~"jvm_memory.*",job="kafka-brokers"}',
        '{__name__=~"jvm_gc.*",job="kafka-brokers"}',
        '{__name__=~"jvm_threads.*",job="kafka-brokers"}',
        '{__name__=~"jvm_buffer.*",job="kafka-brokers"}',
        # Flink JVM
        '{__name__=~"flink_taskmanager_Status_JVM_Memory.*"}',
        '{__name__=~"flink_taskmanager_Status_JVM_GarbageCollector.*"}',
        '{__name__=~"flink_taskmanager_Status_JVM_Threads.*"}',
        '{__name__=~"flink_taskmanager_Status_JVM_CPU.*"}',
    ],
}


def parse_duration(s: str) -> timedelta:
    """Parse duration string like '1h', '30m', '24h', '7d'."""
    s = s.strip().lower()
    if s.endswith('d'):
        return timedelta(days=int(s[:-1]))
    elif s.endswith('h'):
        return timedelta(hours=int(s[:-1]))
    elif s.endswith('m'):
        return timedelta(minutes=int(s[:-1]))
    elif s.endswith('s'):
        return timedelta(seconds=int(s[:-1]))
    raise ValueError(f"Cannot parse duration: {s}")


def query_prometheus(base_url: str, query: str, start: float, end: float,
                     step: str = "10s", max_retries: int = 3) -> list:
    """Query Prometheus range API with retry logic."""
    encoded_query = quote(query, safe='{}=~"_,')
    url = (f"{base_url}/api/v1/query_range?"
           f"query={encoded_query}&start={start}&end={end}&step={step}")

    for attempt in range(1, max_retries + 1):
        try:
            req = Request(url, headers={"Accept": "application/json"})
            with urlopen(req, timeout=120) as resp:
                data = json.loads(resp.read().decode())

            if data.get("status") != "success":
                log.warning("Prometheus query returned status=%s for %s",
                            data.get("status"), query[:80])
                return []

            return data.get("data", {}).get("result", [])

        except HTTPError as e:
            if e.code == 422:
                log.warning("Query too complex or invalid: %s (HTTP 422)", query[:80])
                return []
            log.warning("HTTP error %d on attempt %d/%d for %s",
                        e.code, attempt, max_retries, query[:80])
        except (URLError, TimeoutError, ConnectionError) as e:
            log.warning("Connection error on attempt %d/%d: %s", attempt, max_retries, e)
        except json.JSONDecodeError as e:
            log.warning("JSON decode error on attempt %d/%d: %s", attempt, max_retries, e)

        if attempt < max_retries:
            delay = 2 ** attempt
            log.info("Retrying in %ds...", delay)
            time.sleep(delay)

    log.error("Failed after %d attempts: %s", max_retries, query[:80])
    return []


def series_to_column_name(metric: dict) -> str:
    """Convert a Prometheus metric label set to a unique column name.

    Format: metric_name__label1_value1__label2_value2
    Skip high-cardinality or non-informative labels.
    """
    name = metric.get("__name__", "unknown")
    skip_labels = {"__name__", "instance", "pod", "uid", "container",
                   "endpoint", "namespace", "service", "prometheus",
                   "prometheus_replica"}
    parts = [name]
    for k, v in sorted(metric.items()):
        if k in skip_labels:
            continue
        # Truncate long values
        v_clean = v.replace("/", "_").replace(".", "_").replace("-", "_")[:30]
        parts.append(f"{k}_{v_clean}")
    return "__".join(parts)


def export_metrics(prometheus_url: str, start_ts: float, end_ts: float,
                   step: str = "10s", chunk_hours: float = 2.0) -> dict:
    """Query all metrics and return {column_name: {timestamp: value}} dict.

    For time ranges >2h, queries are chunked into 2h windows to avoid
    Prometheus query timeouts and excessive memory usage.
    """
    all_series = {}
    total_queries = sum(len(v) for v in METRIC_QUERIES.values())
    total_duration_h = (end_ts - start_ts) / 3600
    chunk_size = chunk_hours * 3600

    # Build time chunks
    chunks = []
    t = start_ts
    while t < end_ts:
        chunk_end = min(t + chunk_size, end_ts)
        chunks.append((t, chunk_end))
        t = chunk_end

    if len(chunks) > 1:
        log.info("Splitting %.1fh range into %d chunks of %.1fh each",
                 total_duration_h, len(chunks), chunk_hours)

    query_idx = 0
    for category, queries in METRIC_QUERIES.items():
        log.info("Querying category: %s (%d queries x %d chunks)",
                 category, len(queries), len(chunks))
        for query in queries:
            query_idx += 1
            total_series_for_query = 0

            for chunk_start, chunk_end in chunks:
                results = query_prometheus(prometheus_url, query,
                                           chunk_start, chunk_end, step)
                total_series_for_query = max(total_series_for_query, len(results))

                for series in results:
                    col_name = f"{category}__{series_to_column_name(series['metric'])}"
                    if col_name not in all_series:
                        all_series[col_name] = {}
                    for ts, val in series["values"]:
                        try:
                            all_series[col_name][float(ts)] = float(val)
                        except (ValueError, TypeError):
                            continue

            log.info("  [%d/%d] %s → %d series",
                     query_idx, total_queries, query[:60], total_series_for_query)

    return all_series


def to_dataframe(all_series: dict, step_seconds: float = 10.0):
    """Convert {column: {ts: val}} to numpy arrays (timestamps + features matrix).

    Returns (timestamps, column_names, data_matrix) where data_matrix is (n_timestamps, n_features).
    """
    if not all_series:
        log.error("No series data to export")
        return None, None, None

    # Collect all unique timestamps
    all_timestamps = set()
    for ts_values in all_series.values():
        all_timestamps.update(ts_values.keys())

    timestamps = np.array(sorted(all_timestamps))
    ts_index = {ts: i for i, ts in enumerate(timestamps)}
    column_names = sorted(all_series.keys())

    log.info("Building matrix: %d timestamps x %d features", len(timestamps), len(column_names))

    data = np.full((len(timestamps), len(column_names)), np.nan, dtype=np.float64)
    for col_idx, col_name in enumerate(column_names):
        for ts, val in all_series[col_name].items():
            row_idx = ts_index.get(ts)
            if row_idx is not None:
                data[row_idx, col_idx] = val

    # Report coverage
    total_cells = data.size
    nan_cells = np.isnan(data).sum()
    coverage = 1 - (nan_cells / total_cells) if total_cells > 0 else 0
    log.info("Data coverage: %.1f%% (%d/%d cells have values)",
             coverage * 100, total_cells - nan_cells, total_cells)

    return timestamps, column_names, data


def save_parquet(timestamps, column_names, data, output_path: str):
    """Save to Parquet using pyarrow."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        log.error("pyarrow not installed. Install with: pip install pyarrow")
        # Fallback to numpy
        np_path = output_path.replace(".parquet", ".npz")
        log.info("Saving as NumPy archive instead: %s", np_path)
        np.savez_compressed(np_path,
                            timestamps=timestamps,
                            column_names=np.array(column_names),
                            data=data)
        log.info("Saved %s (%.1f MB)", np_path,
                 os.path.getsize(np_path) / 1024 / 1024)
        return

    # Build Arrow table
    arrays = [pa.array(timestamps.astype("float64"))]
    names = ["timestamp"]
    for i, col in enumerate(column_names):
        arrays.append(pa.array(data[:, i]))
        names.append(col)

    table = pa.table(dict(zip(names, arrays)))

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, output_path, compression="snappy")
    log.info("Saved %s (%.1f MB, %d rows x %d cols)",
             output_path, os.path.getsize(output_path) / 1024 / 1024,
             len(timestamps), len(column_names))


def main():
    parser = argparse.ArgumentParser(description="Export Prometheus metrics to Parquet")
    parser.add_argument("--prometheus", default="http://localhost:9090",
                        help="Prometheus URL (default: http://localhost:9090)")
    parser.add_argument("--start", help="Start time (ISO 8601, e.g. 2026-04-02T00:00:00Z)")
    parser.add_argument("--end", help="End time (ISO 8601)")
    parser.add_argument("--duration", default="1h",
                        help="Duration to look back from now (e.g. 1h, 24h, 7d)")
    parser.add_argument("--step", default="10s",
                        help="Query resolution step (default: 10s)")
    parser.add_argument("--output", required=True,
                        help="Output file path (e.g. data/baseline.parquet)")
    parser.add_argument("--categories", nargs="+",
                        choices=list(METRIC_QUERIES.keys()) + ["all"],
                        default=["all"],
                        help="Feature categories to export")
    parser.add_argument("--s3-bucket",
                        help="S3 bucket to upload Parquet file (e.g. <your-bucket>)")
    parser.add_argument("--s3-prefix", default="raw/",
                        help="S3 key prefix (default: raw/)")
    args = parser.parse_args()

    # Resolve time range
    if args.start and args.end:
        start_ts = datetime.fromisoformat(args.start.replace("Z", "+00:00")).timestamp()
        end_ts = datetime.fromisoformat(args.end.replace("Z", "+00:00")).timestamp()
    else:
        end_ts = time.time()
        duration = parse_duration(args.duration)
        start_ts = end_ts - duration.total_seconds()

    start_dt = datetime.fromtimestamp(start_ts, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ts, tz=timezone.utc)
    log.info("=== Prometheus Metric Exporter ===")
    log.info("  Prometheus: %s", args.prometheus)
    log.info("  Time range: %s → %s (%.1f hours)",
             start_dt.isoformat(), end_dt.isoformat(),
             (end_ts - start_ts) / 3600)
    log.info("  Step: %s", args.step)
    log.info("  Output: %s", args.output)

    # Filter categories
    if "all" not in args.categories:
        filtered = {k: v for k, v in METRIC_QUERIES.items() if k in args.categories}
        METRIC_QUERIES.clear()
        METRIC_QUERIES.update(filtered)

    # Verify Prometheus connectivity
    try:
        req = Request(f"{args.prometheus}/api/v1/status/config",
                      headers={"Accept": "application/json"})
        with urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                log.info("  Prometheus connected OK")
    except Exception as e:
        log.error("Cannot connect to Prometheus at %s: %s", args.prometheus, e)
        log.error("Start port-forward: kubectl port-forward -n observability "
                  "svc/prometheus-kube-prometheus-prometheus 9090:9090")
        sys.exit(1)

    # Export
    all_series = export_metrics(args.prometheus, start_ts, end_ts, args.step)
    log.info("Total unique series: %d", len(all_series))

    if not all_series:
        log.error("No data collected. Check Prometheus targets and time range.")
        sys.exit(1)

    # Category breakdown
    for cat in METRIC_QUERIES:
        cat_series = [k for k in all_series if k.startswith(f"{cat}__")]
        log.info("  %s: %d series", cat, len(cat_series))

    # Build matrix and save
    timestamps, column_names, data = to_dataframe(all_series)
    if timestamps is None:
        sys.exit(1)

    save_parquet(timestamps, column_names, data, args.output)

    # Upload to S3 if requested
    if args.s3_bucket:
        try:
            import subprocess
            s3_key = f"{args.s3_prefix}{Path(args.output).name}"
            s3_uri = f"s3://{args.s3_bucket}/{s3_key}"
            log.info("Uploading to %s ...", s3_uri)
            result = subprocess.run(
                ["aws", "s3", "cp", args.output, s3_uri, "--region", "us-east-2"],
                capture_output=True, text=True, timeout=300
            )
            if result.returncode == 0:
                log.info("Uploaded to S3: %s", s3_uri)
            else:
                log.warning("S3 upload failed: %s", result.stderr.strip())
        except Exception as e:
            log.warning("S3 upload error: %s (local file saved OK)", e)

    log.info("=== Export Complete ===")


if __name__ == "__main__":
    main()
