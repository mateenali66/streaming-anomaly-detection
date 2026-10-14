#!/usr/bin/env python3
"""Offline test of the JMX exporter rules in k8s/base/kafka/kafka-metrics-configmap.yaml.

Simulates jmx_exporter's rule loop on sample MBean strings: rules in order, first match wins, unanchored regex,
name and labels from the capture groups, then lowercaseOutputName. Java and Python regex agree for the constructs used.
It checks the metric names that NAMES.md promises and that near-miss attributes (MeanRate, 999thPercentile on the wrong
bean) do not hit the wrong rule. It cannot prove the MBean names exist on a live broker: the pilot checks that.
"""
import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
cm = yaml.safe_load((ROOT / "k8s/base/kafka/kafka-metrics-configmap.yaml").read_text())
cfg = yaml.safe_load(cm["data"]["kafka-metrics-config.yml"])
assert cfg["lowercaseOutputName"] is True


def apply(sample: str):
    for r in cfg["rules"]:
        m = re.search(r["pattern"], sample)
        if not m:
            continue
        g = (None,) + m.groups()
        sub = lambda t: re.sub(r"\$(\d)", lambda x: g[int(x.group(1))], t)  # noqa: E731
        name = re.sub(r"[^a-zA-Z0-9:_]", "_", sub(r["name"])).lower()
        labels = {k.lower(): sub(v) for k, v in (r.get("labels") or {}).items()}
        return name, labels, r.get("type")
    return None


CASES = [
    # (sample string as jmx_exporter shows it, expected metric name, expected labels)
    ("kafka.network<type=RequestMetrics, name=LocalTimeMs, request=Produce><>Mean: 1.5", "kafka_network_requestmetrics_localtimems_mean", {"request": "Produce"}),
    ("kafka.network<type=RequestMetrics, name=TotalTimeMs, request=FetchFollower><>99thPercentile: 12.0", "kafka_network_requestmetrics_totaltimems_99thpercentile", {"request": "FetchFollower"}),
    ("kafka.network<type=RequestMetrics, name=ResponseSendTimeMs, request=FetchConsumer><>50thPercentile: 0.0", "kafka_network_requestmetrics_responsesendtimems_50thpercentile", {"request": "FetchConsumer"}),
    ("kafka.network<type=RequestMetrics, name=TotalTimeMs, request=Produce><>999thPercentile: 80.0", "kafka_network_requestmetrics_totaltimems_999thpercentile", {"request": "Produce"}),
    ("kafka.network<type=RequestMetrics, name=TotalTimeMs, request=Produce><>Max: 90.0", "kafka_network_requestmetrics_totaltimems_max", {"request": "Produce"}),
    # R1 Count rule still applies to the same bean
    ("kafka.network<type=RequestMetrics, name=RequestsPerSec, request=Produce, error=NONE><>Count: 100", "kafka_network_requestmetrics_requestspersec_total", {"request": "Produce", "error": "NONE"}),
    ("kafka.network<type=RequestMetrics, name=TotalTimeMs, request=Produce><>Count: 7", "kafka_network_requestmetrics_totaltimems_total", {"request": "Produce"}),
    # near misses must NOT hit the percentile rule
    ("kafka.network<type=RequestMetrics, name=RequestsPerSec, request=Produce, error=NONE><>MeanRate: 3.0", None, None),
    ("kafka.network<type=RequestMetrics, name=TotalTimeMs, request=Metadata><>Mean: 1.0", None, None),
    ("kafka.log<type=LogFlushStats, name=LogFlushRateAndTimeMs><>Mean: 2.0", "kafka_log_logflushstats_logflushrateandtimems_mean", {}),
    ("kafka.log<type=LogFlushStats, name=LogFlushRateAndTimeMs><>99thPercentile: 9.0", "kafka_log_logflushstats_logflushrateandtimems_99thpercentile", {}),
    ("kafka.log<type=LogFlushStats, name=LogFlushRateAndTimeMs><>MeanRate: 9.0", None, None),
    ("kafka.log<type=LogFlushStats, name=LogFlushRateAndTimeMs><>Count: 9", "kafka_log_logflushstats_logflushrateandtimems_total", {}),
    ("kafka.controller<type=KafkaController, name=FencedBrokerCount><>Value: 0", "kafka_controller_kafkacontroller_fencedbrokercount", {}),
    ("kafka.controller<type=KafkaController, name=ActiveControllerCount><>Value: 1", "kafka_controller_kafkacontroller_activecontrollercount", {}),
    ("kafka.controller<type=KafkaController, name=PreferredReplicaImbalanceCount><>Value: 0", "kafka_controller_kafkacontroller_preferredreplicaimbalancecount", {}),
    ("kafka.controller<type=KafkaController, name=SomethingElse><>Value: 0", "kafka_controller_kafkacontroller_somethingelse", {}),
    ("kafka.server<type=raft-metrics><>current-leader: 1", "kafka_server_raftmetrics_current_leader", {}),
    ("kafka.server<type=raft-metrics><>high-watermark: 1234", "kafka_server_raftmetrics_high_watermark", {}),
    ("kafka.server<type=KafkaServer, name=BrokerState><>Value: 3", "kafka_server_kafkaserver_brokerstate", {}),
    ("kafka.server<type=ReplicaManager, name=UnderReplicatedPartitions><>Value: 0", "kafka_server_replicamanager_underreplicatedpartitions", {}),
    ("kafka.server<type=ReplicaManager, name=IsrShrinksPerSec><>Count: 4", "kafka_server_replicamanager_isrshrinks_total", {}),
    ("kafka.server<type=ReplicaManager, name=LeaderCount><>Value: 12", "kafka_server_replicamanager_leadercount", {}),
    ("kafka.server<type=BrokerTopicMetrics, name=MessagesInPerSec, topic=raw-events><>Count: 5", "kafka_server_brokertopicmetrics_messagesinpersec_total", {"topic": "raw-events"}),
    ("kafka.server<type=BrokerTopicMetrics, name=FailedProduceRequestsPerSec><>Count: 5", "kafka_server_brokertopicmetrics_failedproducerequestspersec_total", {}),
    ("kafka.server<type=BrokerTopicMetrics, name=ReplicationBytesInPerSec><>Count: 5", "kafka_server_brokertopicmetrics_replicationbytesinpersec_total", {}),
    ("kafka.server<type=KafkaRequestHandlerPool, name=RequestHandlerAvgIdlePercent><>OneMinuteRate: 0.9", "kafka_server_requesthandleravgidlepercent_oneminuterate", {}),
    ("kafka.network<type=SocketServer, name=NetworkProcessorAvgIdlePercent><>Value: 0.9", "kafka_network_networkprocessoravgidlepercent", {}),
    ("kafka.server<type=DelayedOperationPurgatory, name=PurgatorySize, delayedOperation=Produce><>Value: 3", "kafka_server_delayedoperationpurgatory_purgatorysize", {"delayedoperation": "Produce"}),
    ("kafka.server<type=ReplicaFetcherManager, name=MaxLag, clientId=Replica><>Value: 0", "kafka_server_replicafetchermanager_maxlag", {"client_id": "Replica"}),
    ("kafka.controller<type=ControllerStats, name=LeaderElectionRateAndTimeMs><>Count: 2", "kafka_controller_controllerstats_leaderelectionrateandtimems_count", {}),
]

bad = 0
for sample, name, labels in CASES:
    got = apply(sample)
    if name is None:
        # a near miss may fall through to an R1 rule or nothing, but never to a NEW percentile rule
        if got is not None and re.search(r"_(mean|max|\d+thpercentile)$", got[0]):
            print("FAIL near miss matched a percentile rule:", sample, got)
            bad += 1
        continue
    if got is None or got[0] != name or got[1] != labels:
        print("FAIL", sample, "->", got, "expected", name, labels)
        bad += 1

names = {apply(s)[0] for s, n, _ in CASES if n and apply(s)}
print(f"{len(CASES)} cases, {bad} failed, {len(names)} distinct metric names")
sys.exit(1 if bad else 0)
