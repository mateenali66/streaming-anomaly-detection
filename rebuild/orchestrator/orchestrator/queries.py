"""Every PromQL string the orchestrator issues, built in one place from the config.

Prometheus HTTP API (prometheus.io/docs/prometheus/latest/querying/api, context7 /websites/prometheus_io):
instant queries return resultType vector (or scalar for time()), range queries return matrix, and the
envelope carries status "success" or "error" with errorType and error. Every query here is registered in
run/thresholds.json, so a query cannot change silently after registration.

The recovery gate and the baseline statistics share these strings, so the lag threshold is computed with
the gate's own query (1.6 item 5, 11.12).
"""
from __future__ import annotations

from . import ids


def build(cfg: dict) -> dict[str, str]:
    j = cfg["prometheus_jobs"]
    m = cfg["metric_names"]
    ns = ids.NAMESPACE
    brokers = f'namespace="{ns}",job="{j["brokers"]}"'
    controller = f'namespace="{ns}",job="{j["controller"]}"'
    flink = f'namespace="{ns}",job="{j["flink"]}"'
    return {
        # Kafka state (1.6 item 3). Broker-side counts are summed over brokers. Controller metrics in KRaft are
        # reported by every controller quorum member, so the maximum is taken, never the sum.
        "under_replicated_sum": f'sum({m["under_replicated"]}{{{brokers}}})',
        "under_min_isr_sum": f'sum({m["under_min_isr"]}{{{brokers}}})',
        "offline_partitions_max": f'max({m["offline_partitions"]}{{{controller}}})',
        "preferred_imbalance_max": f'max({m["preferred_imbalance"]}{{{controller}}})',
        "fenced_brokers_max": f'max({m["fenced_brokers"]}{{{controller}}})',
        "active_controller_by_pod": f'{m["active_controller"]}{{{controller}}}',
        # Consumer lag (1.6 item 5, 11.12).
        "consumer_lag_max": f'max({m["consumer_lag_max"]}{{{flink}}})',
        # Topic rates over 60 s (1.6 items 6 and 7, 11.12).
        # The dead-letter rate (1.6 item 7) is not a Prometheus query any more: Kafka creates a topic's MessagesInPerSec
        # meter only on the first append and closes it when the broker stops leading the topic (REVIEW4 L3), so the
        # recovery gate reads the verification consumer's dead_letter_count instead (C34).
        "raw_events_in_per_s": f'sum(rate({m["messages_in"]}{{{brokers},topic="raw-events"}}[60s]))',
        # Clock check (1.6, 11.4 clock_skew_s).
        "time": "time()",
    }


def selector(metric_pattern: str, matchers: list[str]) -> str:
    inner = ",".join([f'__name__=~"{metric_pattern}"'] + matchers)
    return "{" + inner + "}"


def source_matchers(cfg: dict, source: str) -> list[str]:
    """Label matchers per allowlist source. Feature sources are restricted to the Kafka and Flink scrape jobs and
    always exclude the probe job, so a verification-channel series can never enter the feature file (2.3)."""
    j = cfg["prometheus_jobs"]
    ns = f'namespace="{ids.NAMESPACE}"'
    not_probes = f'job!="{j["probes"]}"'
    if source in ("broker",):
        return [ns, f'job="{j["brokers"]}"', not_probes]
    if source in ("consumer", "flink"):
        return [ns, f'job="{j["flink"]}"', not_probes]
    if source == "jvm":
        return [ns, f'job=~"{j["brokers"]}|{j["flink"]}"', not_probes]
    if source == "cadvisor":
        return [ns, f'job="{j["kubelet"]}"', 'container!=""', 'container!="POD"']
    if source == "kube_state":
        return [ns, f'job="{j["kube_state"]}"']
    if source == "kafka_controller":
        return [ns, f'job="{j["controller"]}"']
    if source == "kafka_broker_copy":
        return [ns, f'job="{j["brokers"]}"']
    if source == "scrape":
        # Prometheus' own per-target series (`up`, `scrape_samples_scraped`) of the two scrape jobs that feed the feature
        # sources (decision 21, C22). They carry the target labels only, so job and pod identify the target.
        return [ns, f'job=~"{j["brokers"]}|{j["flink"]}"']
    raise ValueError(f"unknown source {source}")
