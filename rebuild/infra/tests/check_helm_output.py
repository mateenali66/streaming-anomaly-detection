#!/usr/bin/env python3
"""Checks on `helm template` output for the four charts. Usage: check_helm_output.py <dir with strimzi.yaml flink.yaml prometheus.yaml chaos-mesh.yaml>
Offline. Proves what the values files are meant to achieve, on the rendered objects."""
import re
import sys
from pathlib import Path

import yaml

d = Path(sys.argv[1])
failures = 0


def check(name, cond, extra=""):
    global failures
    print(("ok   " if cond else "FAIL ") + name + ("" if cond else f"\n       {extra}"))
    failures += 0 if cond else 1


def load(n):
    return [x for x in yaml.safe_load_all((d / n).read_text()) if x]


def all_images(docs):
    out = set()
    def walk(x):
        if isinstance(x, dict):
            for k, v in x.items():
                if k == "image" and isinstance(v, str):
                    out.add(v)
                if k == "env" and isinstance(v, list):
                    for e in v:
                        if isinstance(e.get("value"), str) and re.search(r"/(operator|kafka|kafka-bridge|kaniko-executor|maven-builder)[:@]", e["value"]):
                            for line in e["value"].splitlines():
                                m = re.search(r"([a-zA-Z0-9.-]+/[^\s=]+:[^\s]+)$", line.strip())
                                if m:
                                    out.add(m.group(1))
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)
    walk(docs)
    return out


ECR = re.compile(r"^ACCOUNT\.dkr\.ecr\.REGION\.amazonaws\.com/streaming-anomaly-detection/")
prom, strimzi, flink, chaos = load("prometheus.yaml"), load("strimzi.yaml"), load("flink.yaml"), load("chaos-mesh.yaml")

# ---- Prometheus stack
kinds = [x["kind"] for x in prom]
check("prometheus: no admission webhook configurations", "ValidatingWebhookConfiguration" not in kinds and "MutatingWebhookConfiguration" not in kinds)
check("prometheus: no Job (the certgen patch job is gone)", "Job" not in kinds)
p = next(x for x in prom if x["kind"] == "Prometheus")["spec"]
check("prometheus: PriorityClass named in the Prometheus spec", p["priorityClassName"] == "monitoring-critical")
check("prometheus: 10 s scrape and 100Gi gp3", p["scrapeInterval"] == "10s" and p["storage"]["volumeClaimTemplate"]["spec"]["resources"]["requests"]["storage"] == "100Gi" if "storage" in p else True)
check("prometheus: retentionSize guards the volume", p["retentionSize"] == "85GB")
check("prometheus: scheduled on the monitoring node", p["nodeSelector"] == {"workload": "monitoring"})
check("prometheus: no Grafana, no Alertmanager", not any(x["kind"] == "Deployment" and "grafana" in x["metadata"]["name"] for x in prom) and not any(x["kind"] == "Alertmanager" for x in prom))
sm = {x["metadata"]["name"]: x for x in prom if x["kind"] == "ServiceMonitor"}
check("prometheus: no ServiceMonitor for kube-proxy, etcd, scheduler, controller-manager", not any(re.search(r"kube-proxy|etcd|scheduler|controller-manager", n) for n in sm), str(list(sm)))
kub = next(v for n, v in sm.items() if n.endswith("kubelet"))
cadv = next(e for e in kub["spec"]["endpoints"] if e.get("path", "").endswith("/cadvisor"))
regexes = " ".join(r.get("regex", "") for r in cadv["metricRelabelings"])
check("cAdvisor: container_spec_memory_limit_bytes is NOT dropped", not re.search(r"container_spec\.\*", regexes) and "memory_limit" not in regexes)
check("cAdvisor: cfs_throttled_seconds, memory working set, cpu usage, oom events are NOT dropped",
      not any(re.search(rx, m) for rx in (r"cfs_throttled_seconds", r"working_set", r"cpu_usage", r"oom") for m in [regexes]))
# ---- REVIEW4 C1 / decision 29: no ECR host (it carries the account ID) survives in a stored label
def relabel(rules, labels):
    """Prometheus `replace` semantics on one label set: fully anchored regex over the ;-joined source labels."""
    out = dict(labels)
    for r in rules:
        if r.get("action", "replace") != "replace" or "targetLabel" not in r:
            continue
        val = ";".join(out.get(x, "") for x in r["sourceLabels"])
        m = re.fullmatch(r.get("regex", "(.*)"), val)
        if m:
            out[r["targetLabel"]] = m.expand(re.sub(r"\$(\d+)", r"\\\1", r.get("replacement", "$1")))
    return out


ECR_REF = "000000000000.dkr.ecr.us-east-2.amazonaws.com/streaming-anomaly-detection/mirror/x:1@sha256:" + "a" * 64
EKS = "000000000000.dkr.ecr.us-east-2.amazonaws.com/eks/pause:3.10"
got = relabel(cadv["metricRelabelings"], {"image": ECR_REF, "pod": "p"})
check("cAdvisor: the ECR host is cut from `image`, path, tag and digest stay",
      got["image"] == "streaming-anomaly-detection/mirror/x:1@sha256:" + "a" * 64, str(got))
check("cAdvisor: the pause image loses its registry host too", relabel(cadv["metricRelabelings"], {"image": EKS})["image"] == "eks/pause:3.10")
check("cAdvisor: an empty image (pod-level series) stays empty", relabel(cadv["metricRelabelings"], {"image": ""})["image"] == "")
ksm_sm = next(v for n, v in sm.items() if n.endswith("kube-state-metrics"))
for ep in ksm_sm["spec"]["endpoints"]:
    lab = relabel(ep.get("metricRelabelings", []), {"image": ECR_REF, "image_spec": ECR_REF, "image_id": ECR_REF.replace(":1@", "@")})
    check(f"kube-state-metrics endpoint {ep.get('port')}: no ECR host in image, image_spec or image_id",
          not any(".dkr.ecr." in v for v in lab.values()), str(lab))

sec = [x for x in prom if x["kind"] == "Secret" and "additional-scrape-configs.yaml" in (x.get("data") or {})]
import base64
scrape = yaml.safe_load(base64.b64decode(sec[0]["data"]["additional-scrape-configs.yaml"])) if sec else []
jobs = {j["job_name"]: j for j in scrape}
check("scrape jobs present", {"kafka-brokers", "flink-metrics", "load-generator", "verification-consumer"} <= set(jobs), str(list(jobs)))
for name, j in jobs.items():
    rel = j["relabel_configs"]
    inst = [r for r in rel if r.get("target_label") == "instance"]
    check(f"{name}: instance is the pod NAME, not the pod IP", len(inst) == 1 and inst[0]["source_labels"] == ["__meta_kubernetes_pod_name"], str(inst))
    addr = [r for r in rel if r.get("target_label") == "__address__"]
    check(f"{name}: address is built from the pod IP only for the scrape target", addr and addr[0]["source_labels"] == ["__meta_kubernetes_pod_ip"] and addr[0]["replacement"].startswith("$1:"), str(addr))
check("kafka-brokers: only the brokers pool, only Running pods", any(r.get("regex") == "brokers" for r in jobs["kafka-brokers"]["relabel_configs"]) and any(r.get("regex") == "Running" for r in jobs["kafka-brokers"]["relabel_configs"]))
names = [x["metadata"]["name"] for x in prom if x["kind"] == "Service"]
check("prometheus service name is the one the orchestrator uses", "prometheus-kube-prometheus-prometheus" in names, str(names))
check("prometheus: node-exporter tolerates every taint", any(x["kind"] == "DaemonSet" and any(t == {"operator": "Exists"} for t in x["spec"]["template"]["spec"].get("tolerations", [])) for x in prom))
ksm = next(x for x in prom if x["kind"] == "Deployment" and x["metadata"]["name"].endswith("kube-state-metrics"))
check("kube-state-metrics: does not drop pod resource limits (no --metric-denylist, no restrictive --resources)", not any("denylist" in a or a.startswith("--resources=") and "pods" not in a for a in ksm["spec"]["template"]["spec"]["containers"][0].get("args", [])))

# ---- images everywhere come from ECR (one operator argument names a Thanos default that is never started)
for n, docs in (("strimzi", strimzi), ("flink-operator", flink), ("prometheus", prom), ("chaos-mesh", chaos)):
    imgs = all_images(docs)
    bad = sorted(i for i in imgs if not ECR.match(i))
    check(f"{n}: every container image is in ECR", not bad, str(bad))

# ---- strimzi
dep = next(x for x in strimzi if x["kind"] == "Deployment")
check("strimzi: operator image pinned by digest", "@sha256:" in dep["spec"]["template"]["spec"]["containers"][0]["image"])
env = {e["name"]: e.get("value", "") for e in dep["spec"]["template"]["spec"]["containers"][0]["env"]}
ns_env = next(e for e in dep["spec"]["template"]["spec"]["containers"][0]["env"] if e["name"] == "STRIMZI_NAMESPACE")
check("strimzi: watches only its own namespace, listed once", ns_env.get("value") == "streaming" or ns_env.get("valueFrom", {}).get("fieldRef", {}).get("fieldPath") == "metadata.namespace", str(ns_env))
check("strimzi: topic and user operator images pinned by digest", "@sha256:" in env["STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE"] and "@sha256:" in env["STRIMZI_DEFAULT_USER_OPERATOR_IMAGE"])

# ---- flink operator
fdep = next(x for x in flink if x["kind"] == "Deployment")
check("flink operator: image pinned by digest", "@sha256:" in fdep["spec"]["template"]["spec"]["containers"][0]["image"])
check("flink operator: no webhook objects", not any("Webhook" in x["kind"] for x in flink))
check("flink operator: creates the flink service account in streaming", any(x["kind"] == "ServiceAccount" and x["metadata"]["name"] == "flink" and x["metadata"].get("namespace") == "streaming" for x in flink))
check("flink operator: no ClusterRoleBinding to the cluster-wide edit role", not any(x["kind"] == "ClusterRoleBinding" and x["roleRef"]["name"] == "edit" for x in flink))

# ---- chaos mesh
daemon = next(x for x in chaos if x["kind"] == "DaemonSet")
dc = daemon["spec"]["template"]["spec"]["containers"][0]["command"]
vols = {v["name"]: v.get("hostPath", {}).get("path") for v in daemon["spec"]["template"]["spec"]["volumes"]}
check("chaos mesh: daemon runtime containerd, socket /run/containerd/containerd.sock", dc[dc.index("--runtime") + 1] == "containerd" and dc[dc.index("--runtime-socket-path") + 1].endswith("/containerd.sock") and vols.get("socket-path") == "/run/containerd", str((dc, vols)))
check("chaos mesh: daemon tolerates the workload taint on every node", any(t.get("key") == "workload" for t in daemon["spec"]["template"]["spec"].get("tolerations", [])))
check("chaos mesh: no dashboard, no DNS server", not any(x["kind"] == "Deployment" and re.search(r"dashboard|dns", x["metadata"]["name"]) for x in chaos))
cm = next(x for x in chaos if x["kind"] == "Deployment" and "controller-manager" in x["metadata"]["name"])
check("chaos mesh: controller on the monitoring node, 1 replica", cm["spec"]["replicas"] == 1 and cm["spec"]["template"]["spec"]["nodeSelector"] == {"workload": "monitoring"})
sys.exit(1 if failures else 0)
