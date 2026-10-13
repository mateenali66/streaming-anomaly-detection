#!/usr/bin/env python3
"""Fig 1: E-Commerce Streaming Pipeline Architecture."""

from diagrams import Cluster, Diagram, Edge
from diagrams.onprem.queue import Kafka
from diagrams.onprem.analytics import Flink
from diagrams.onprem.monitoring import Prometheus, Grafana
from diagrams.programming.language import Python
import os

OUTPUT = os.path.join(os.path.dirname(__file__), "fig1_architecture")

graph_attr = {
    "fontsize": "12", "fontname": "Helvetica", "bgcolor": "white",
    "pad": "0.4", "ranksep": "1.0", "nodesep": "0.5", "dpi": "300",
}
node_attr = {"fontsize": "10", "fontname": "Helvetica"}
edge_attr = {"fontsize": "9", "fontname": "Helvetica"}

with Diagram(
    "", filename=OUTPUT, show=False, direction="TB",
    graph_attr=graph_attr, node_attr=node_attr, edge_attr=edge_attr,
    outformat=["pdf", "png"],
):
    loadgen = Python("Load Generator\n5K events/s")

    with Cluster("Kafka (KRaft 3.9, 3 Brokers)"):
        raw = Kafka("raw-events")
        enriched = Kafka("enriched-events")
        aggregated = Kafka("aggregated-metrics")
        fraud_topic = Kafka("fraud-alerts")

    with Cluster("Flink (1.19, 3 TaskManagers)"):
        job1 = Flink("Enrichment")
        job2 = Flink("Aggregation")
        job3 = Flink("Fraud Detection")

    with Cluster("Observability"):
        prom = Prometheus("Prometheus\n10s scrape")
        graf = Grafana("Grafana")

    loadgen >> Edge(label="  Avro/JSON  ", color="darkblue", style="bold") >> raw
    raw >> Edge(color="darkblue") >> job1
    job1 >> Edge(color="darkblue") >> enriched
    enriched >> Edge(color="darkblue") >> job2
    job2 >> Edge(color="darkblue") >> aggregated
    enriched >> Edge(color="darkblue") >> job3
    job3 >> Edge(color="darkblue") >> fraud_topic
    raw >> Edge(label="JMX", style="dashed", color="darkorange") >> prom
    job1 >> Edge(label="Metrics", style="dashed", color="darkorange") >> prom
    prom >> Edge(style="dashed", color="gray") >> graf
