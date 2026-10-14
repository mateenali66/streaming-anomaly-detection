#!/usr/bin/env python3
"""Static checks on k8s/ and the Terraform templates. Offline. Needs kubectl (for `kubectl kustomize`, which contacts no cluster)
and PyYAML.

1. Every ${NAME} in k8s/** and helm-values is a name that scripts/lock_env.py + render.py or terraform/cluster/platform.tf supplies.
2. Every kustomize build parses, every document has apiVersion, kind and a name, namespaced kinds name a namespace.
3. After rendering with dummy values, no ${ is left, and every container image is an ECR reference pinned by digest.
4. WORKLOAD has no default anywhere: no env default for it, the orchestrator reads it from the ConfigMap.
5. The orchestrator Job has no activeDeadlineSeconds and no ClusterRole is bound to it except the node reader.
5b. The one interface of protocol 11.11 (rebuild/CONTRACT_CHANGES.md): the orchestrator environment and mounted inputs, the
    sidecar mirroring manifest.json only, the verification consumer's group and port, the load generator's /status port, the
    probe service in the kustomization, the exporter identity without infra's own export Job, the 11.7 checkpoint paths.
6. No identifier that must never be written: account IDs, profile names, e-mail addresses, employer names, 0.0.0.0/0 for the API.
"""
from __future__ import annotations

import hashlib
import re
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import lock_env  # noqa: E402

failures = 0


def check(name: str, cond: bool, extra: str = "") -> None:
    global failures
    print(("ok   " if cond else "FAIL ") + name + ("" if cond else f"\n       {extra}"))
    failures += 0 if cond else 1


PLACEHOLDER = re.compile(r"\$\{([A-Za-z][A-Za-z0-9_]*)\}")
RENDER_EXTRA = {"CAMPAIGN_ID", "WORKLOAD", "AWS_REGION", "RESULTS_BUCKET", "ECR_REGISTRY", "ECR_PREFIX",
                "SNS_TOPIC_ARN", "CAMPAIGN_INDEX", "LOADGEN_SEED", "PLAN_SHA256"}
lock = lock_env.load(lock_env.DEFAULT_LOCK)
known = set(lock_env.build_vars(lock, ecr_registry="r", project="p", extra={}, fill_missing=True)) | RENDER_EXTRA

# ---- 1. placeholders
files = [p for p in (ROOT / "k8s").rglob("*.yaml")]
used: dict[str, set[str]] = {}
for p in files:
    for n in PLACEHOLDER.findall(p.read_text()):
        used.setdefault(n, set()).add(str(p.relative_to(ROOT)))
unknown = {n: sorted(f) for n, f in used.items() if n not in known}
check("every placeholder is supplied", not unknown, str(unknown))

# the Terraform side builds the same prefixes
tf = (ROOT / "terraform/cluster/platform.tf").read_text()
for prefix in ("TAG_", "DIGEST_", "SHA_", "PATH_", "REPO_", "REF_"):
    check(f"platform.tf supplies {prefix}<IMAGE>", f'"{prefix}$' in tf)
for base in ("ECR_REGISTRY", "ECR_PREFIX", "CAMPAIGN_ID", "WORKLOAD", "AWS_REGION"):
    check(f"platform.tf supplies {base}", base in tf)
tf_only = {n for n in used if n in {"RESULTS_BUCKET"}}
check("values files that Terraform renders do not need RESULTS_BUCKET", not any("helm-values" in f for n in tf_only for f in used[n]), str(used.get("RESULTS_BUCKET")))

# ---- 2. every file parses (placeholders neutralised) and every kustomize build is sane
def parse_all(text: str) -> list[dict]:
    return [d for d in yaml.safe_load_all(PLACEHOLDER.sub("X", text)) if d]

for p in files:
    if p.name == "kustomization.yaml" or p.parent.name == "helm-values" or p.parent.name == "locks":
        try:
            parse_all(p.read_text())
            ok = True
        except yaml.YAMLError as e:
            ok = False
            print(e)
        check(f"parses: {p.relative_to(ROOT)}", ok)
    else:
        try:
            docs = parse_all(p.read_text())
            ok = all("apiVersion" in d and "kind" in d and d.get("metadata", {}).get("name") for d in docs)
        except yaml.YAMLError as e:
            ok = False
            print(e)
        check(f"parses with apiVersion/kind/name: {p.relative_to(ROOT)}", ok)

CLUSTER_SCOPED = {"ClusterRole", "ClusterRoleBinding", "Namespace", "StorageClass", "PriorityClass"}
rendered: dict[str, list[dict]] = {}
for w in ("ecommerce", "iot"):
    out = subprocess.run(["kubectl", "kustomize", str(ROOT / "k8s" / w)], capture_output=True, text=True)
    check(f"kustomize builds k8s/{w}", out.returncode == 0, out.stderr)
    if out.returncode:
        continue
    r = subprocess.run([sys.executable, str(ROOT / "scripts/render.py"), "--validate", "--campaign-id", "ec-a" if w == "ecommerce" else "iot-a",
                        "--workload", "ECOMMERCE" if w == "ecommerce" else "IOT", "--results-bucket", "bucket", "--set", "SNS_TOPIC_ARN=arn:x",
                        "--set", "CAMPAIGN_INDEX=1", "--set", "LOADGEN_SEED=1", "--set", "PLAN_SHA256=" + "c" * 64, "-"],
                       input=out.stdout, capture_output=True, text=True)
    check(f"render.py fills every placeholder ({w})", r.returncode == 0, r.stderr)
    check(f"nothing left unrendered ({w})", "${" not in r.stdout)
    docs = list(yaml.safe_load_all(r.stdout))
    rendered[w] = [d for d in docs if d]
    bad = [f"{d['kind']}/{d['metadata']['name']}" for d in rendered[w] if d["kind"] not in CLUSTER_SCOPED and not d["metadata"].get("namespace")]
    check(f"namespaced objects name a namespace ({w})", not bad, str(bad))
    names = [(d["kind"], d["metadata"].get("namespace", ""), d["metadata"]["name"]) for d in rendered[w]]
    check(f"no duplicate objects ({w})", len(names) == len(set(names)))
    phases = {d["metadata"].get("labels", {}).get("infra.phase") for d in rendered[w]}
    check(f"every object has a known phase ({w})", phases <= {"rbac", "kafka", "flink", "submit", "load", "workload", "run"}, str(phases))

# ---- 3. images
IMG = re.compile(r"^[a-z0-9.-]+\.[a-z]+/")

def images(doc: dict) -> list[str]:
    out = []
    def walk(x):
        if isinstance(x, dict):
            for k, v in x.items():
                if k == "image" and isinstance(v, str):
                    out.append(v)
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)
    walk(doc)
    # Strimzi puts the image in a few more fields
    spec = doc.get("spec", {})
    for path in (("kafka", "image"),):
        node = spec
        for k in path:
            node = node.get(k, {}) if isinstance(node, dict) else {}
        if isinstance(node, str):
            out.append(node)
    return out

for w, docs in rendered.items():
    imgs = sorted({i for d in docs for i in images(d)})
    check(f"{w}: images found", len(imgs) >= 6, str(imgs))
    notpinned = [i for i in imgs if not re.search(r"^ACCOUNT\.dkr\.ecr\.REGION\.amazonaws\.com/streaming-anomaly-detection/.+@sha256:[0-9a-f]{64}$", i)]
    check(f"{w}: every image is an ECR reference pinned by digest", not notpinned, str(notpinned))

# ---- 4/5. orchestrator Job
for w, docs in rendered.items():
    job = next(d for d in docs if d["kind"] == "Job" and d["metadata"]["name"] == "campaign-orchestrator")
    check(f"{w}: no activeDeadlineSeconds on the orchestrator Job", "activeDeadlineSeconds" not in job["spec"])
    check(f"{w}: backoffLimit 0", job["spec"]["backoffLimit"] == 0)
    c = job["spec"]["template"]["spec"]["containers"][0]
    wl = next(e for e in c["env"] if e["name"] == "WORKLOAD")
    check(f"{w}: WORKLOAD comes from the ConfigMap, no default", "value" not in wl and wl["valueFrom"]["configMapKeyRef"]["key"] == "WORKLOAD" and not wl["valueFrom"]["configMapKeyRef"].get("optional"))
    side = job["spec"]["template"]["spec"]["initContainers"][0]
    check(f"{w}: S3 sync sidecar is a native sidecar, no --delete", side.get("restartPolicy") == "Always" and "--delete" not in " ".join(side["args"]))
    bindings = [d for d in docs if d["kind"] in ("ClusterRoleBinding", "RoleBinding")]
    crb = [b for b in bindings if b["kind"] == "ClusterRoleBinding"]
    check(f"{w}: the only cluster-wide binding is the node reader", [b["roleRef"]["name"] for b in crb] == ["campaign-orchestrator-nodes-read"], str(crb))
    cr = next(d for d in docs if d["kind"] == "ClusterRole")
    check(f"{w}: the ClusterRole only reads nodes", cr["rules"] == [{"apiGroups": [""], "resources": ["nodes"], "verbs": ["get", "list"]}])
    role = next(d for d in docs if d["kind"] == "Role" and d["metadata"]["name"] == "campaign-orchestrator")
    all_verbs = {v for r in role["rules"] for v in r["verbs"]}
    check(f"{w}: no wildcard verbs or resources in the orchestrator Role", "*" not in all_verbs and all("*" not in r["resources"] for r in role["rules"]))
    kafka_pvc = next(d for d in docs if d["kind"] == "KafkaNodePool")["spec"]["storage"]["size"]
    check(f"{w}: Kafka volumes are at least 300Gi", kafka_pvc == "300Gi", kafka_pvc)
    topics = {d["metadata"]["name"]: d["spec"]["config"] for d in docs if d["kind"] == "KafkaTopic"}
    check(f"{w}: raw and enriched retention fixed at 2 h, dead-letter exists", topics["raw-events"]["retention.ms"] == "7200000" and topics["enriched-events"]["retention.ms"] == "7200000" and "dead-letter" in topics)
    fd = next(d for d in docs if d["kind"] == "FlinkDeployment")["spec"]
    fc = fd["flinkConfiguration"]
    cid = "ec-a" if w == "ecommerce" else "iot-a"
    check(f"{w}: Flink checkpoints and savepoints on the 11.7 scratch prefixes", fc["state.checkpoints.dir"] == f"s3://bucket/scratch/{cid}/flink-checkpoints"
          and fc["state.savepoints.dir"] == f"s3://bucket/scratch/{cid}/flink-savepoints", str(fc["state.checkpoints.dir"]))
    check(f"{w}: failure-rate restart strategy", fc["restart-strategy.type"] == "failure-rate")
    check(f"{w}: 40 failures in 10 min, as run-job.sh passes (decision 38)", fc["restart-strategy.failure-rate.max-failures-per-interval"] == "40"
          and "-Drestart-strategy.failure-rate.max-failures-per-interval=40" in (ROOT / "images" / "flink-jobs" / "run-job.sh").read_text())
    # REVIEW1 A1 forbade a HEALING watchdog. REVIEW4 C6 adds one ALERT-ONLY CronJob, and nothing else of that kind.
    crons = [d for d in docs if d["kind"] == "CronJob"]
    check(f"{w}: exactly one CronJob, the alert-only campaign-watchdog", [c["metadata"]["name"] for c in crons] == ["campaign-watchdog"], str(crons)[:200])
    if crons:
        cj = crons[0]["spec"]
        tpl = cj["jobTemplate"]["spec"]["template"]
        wc = tpl["spec"]["containers"][0]
        orch_img = job["spec"]["template"]["spec"]["containers"][0]["image"]
        labels = {**(cj["jobTemplate"].get("metadata", {}).get("labels") or {}), **(tpl.get("metadata", {}).get("labels") or {})}
        check(f"{w}: watchdog runs the mounted script with the orchestrator image and service account, one at a time",
              wc["command"] == ["python", "-u", "/watchdog/watchdog.py"] and wc["image"] == orch_img
              and tpl["spec"]["serviceAccountName"] == "campaign-orchestrator" and cj["concurrencyPolicy"] == "Forbid"
              and tpl["spec"]["nodeSelector"] == {"workload": "monitoring"}, str(wc)[:300])
        check(f"{w}: watchdog pods carry no rebuild/campaign or rebuild/attempt label (the orchestrator counts and deletes those)",
              not any(k.startswith("rebuild/") for k in labels), str(labels))
        wcm = next((d for d in docs if d["kind"] == "ConfigMap" and d["metadata"]["name"] == "campaign-watchdog"), None)
        check(f"{w}: ConfigMap campaign-watchdog holds watchdog.py byte for byte",
              wcm is not None and wcm["data"]["watchdog.py"] == (ROOT / "k8s/base/watchdog/watchdog.py").read_text())
    for rname in ("campaign-orchestrator", "verification-probes"):
        r = next(d for d in docs if d["kind"] == "Role" and d["metadata"]["name"] == rname)
        ex = [x for x in r["rules"] if "pods/exec" in x["resources"]]
        check(f"{w}: Role {rname} grants get and create on pods/exec (kubectl WebSocket exec, REVIEW4_LIVE_CONTRACT L9)",
              len(ex) == 1 and set(ex[0]["verbs"]) == {"get", "create"}, str(ex))
    cmw = [x for x in role["rules"] if x.get("resourceNames")]
    check(f"{w}: the only name-scoped rule lets the watchdog update its own state ConfigMap",
          cmw == [{"apiGroups": [""], "resources": ["configmaps"], "resourceNames": ["campaign-watchdog-state"], "verbs": ["update", "patch"]}], str(cmw))

# ---- 5a. every env value is a string after rendering (kustomize drops quotes; a bare number would be a YAML integer)
for w, docs in rendered.items():
    bad_env = []
    for d in docs:
        tmpl = (d.get("spec", {}).get("template") or {}).get("spec") or {}
        for c in (tmpl.get("containers") or []) + (tmpl.get("initContainers") or []):
            for e in c.get("env") or []:
                if "value" in e and not isinstance(e["value"], str):
                    bad_env.append(f"{d['metadata']['name']}/{c['name']}/{e['name']}={e['value']!r}")
    check(f"{w}: every env value is a string after rendering", not bad_env, str(bad_env))

# ---- 5b. the one interface of protocol 11.11 (rebuild/CONTRACT_CHANGES.md)
CONTRACT_ENV = ("WORKLOAD", "CAMPAIGN_ID", "CAMPAIGN_INDEX", "PLAN_SHA256", "RESULTS_BUCKET", "RESULTS_PREFIX", "PLAN_PATH",
                "THRESHOLDS_PATH", "VERSIONS_FILE", "IMAGES_LOCK_FILE", "F6_INJECTOR_IMAGE", "LOADGEN_STATUS_URL", "PROBES_URL")
RETIRED_ENV = ("SCHEDULE_PATH", "SCHEDULE_SHA256", "CAMPAIGN_SEED", "LOADGEN_METRICS_URL")
for w, docs in rendered.items():
    cid = "ec-a" if w == "ecommerce" else "iot-a"
    job = next(d for d in docs if d["kind"] == "Job" and d["metadata"]["name"] == "campaign-orchestrator")
    pod = job["spec"]["template"]["spec"]
    c = next(x for x in pod["containers"] if x["name"] == "orchestrator")
    env = {e["name"]: e.get("value") for e in c["env"]}
    check(f"{w}: orchestrator environment is the 11.11 interface", all(k in env for k in CONTRACT_ENV) and not set(env) & set(RETIRED_ENV),
          str(sorted(env)))
    check(f"{w}: RESULTS_PREFIX is campaign/<id>, CAMPAIGN_INDEX and PLAN_SHA256 rendered",
          env["RESULTS_PREFIX"] == f"campaign/{cid}" and env["CAMPAIGN_INDEX"] == "1" and env["PLAN_SHA256"] == "c" * 64)
    mounts = {m["mountPath"]: m["name"] for m in c["volumeMounts"]}
    vols = {v["name"]: v for v in pod["volumes"]}
    check(f"{w}: the four inputs are mounted from ConfigMap campaign-config at /config",
          vols[mounts["/config"]]["configMap"]["name"] == "campaign-config"
          and all(env[k].startswith("/config/") for k in ("PLAN_PATH", "THRESHOLDS_PATH", "VERSIONS_FILE", "IMAGES_LOCK_FILE")))
    side = pod["initContainers"][0]
    script = " ".join(side["args"])
    sync_lines = [ln for ln in script.split("\n") if "aws s3 sync" in ln] or [x for x in [script] if "aws s3 sync" in x]
    check(f"{w}: the sidecar mirrors manifest.json only (state/ and temporary files never reach S3)",
          bool(sync_lines) and all('--exclude "*" --include "manifest.json"' in ln and "--delete" not in ln for ln in sync_lines),
          script[:300])
    cons = next(d for d in docs if d["kind"] == "Deployment" and d["metadata"]["name"] == "verification-consumer")
    cc = cons["spec"]["template"]["spec"]["containers"][0]
    cenv = {e["name"]: e.get("value") for e in cc["env"]}
    check(f"{w}: verification consumer: command consumer, group rebuild-verification, /status on 9103",
          cc["args"] == ["consumer"] and cenv.get("KAFKA_GROUP_ID") == "rebuild-verification" and cenv.get("METRICS_PORT") == "9103"
          and cc["readinessProbe"]["httpGet"]["path"] == "/status")
    lg_svc = next(d for d in docs if d["kind"] == "Service" and d["metadata"]["name"] == "load-generator")
    check(f"{w}: load generator Service serves /status and /metrics on 9102", [p["port"] for p in lg_svc["spec"]["ports"]] == [9102])
    probes = {d["kind"] for d in docs if d["metadata"]["name"] == "verification-probes"}
    check(f"{w}: verification probes are part of the kustomization", probes >= {"Deployment", "Service", "PodMonitor", "ServiceAccount", "Role", "RoleBinding"}, str(probes))
    pdep = next(d for d in docs if d["kind"] == "Deployment" and d["metadata"]["name"] == "verification-probes")
    penv = {e["name"]: e.get("value") for e in pdep["spec"]["template"]["spec"]["containers"][0]["env"]}
    check(f"{w}: probes read the load generator's /status", penv.get("LOADGEN_STATUS_URL", "").endswith(":9102/status") and "LOADGEN_METRICS_URL" not in penv)
    check(f"{w}: no infra export Job or scratch volume (the orchestrator creates export-<key> Jobs)",
          not any(d["metadata"]["name"] in ("export-baseline", "export-scratch") for d in docs))
    sa = next(d for d in docs if d["kind"] == "ServiceAccount" and d["metadata"]["name"] == "metric-exporter")
    check(f"{w}: metric-exporter ServiceAccount is created in phase rbac", sa["metadata"]["labels"]["infra.phase"] == "rbac")

# ---- 6. forbidden identifiers anywhere under rebuild/infra
# REVIEW4 M1: the words are never committed. tests/forbidden_words.sha256 holds "<length> <sha256>" of each lower-case word,
# and every window of each length in every file is hashed and compared. The plain list lives in the gitignored
# tests/forbidden_words.local; when it is present it must hash to exactly the committed list, so the two never drift.
HASHES = ROOT / "tests" / "forbidden_words.sha256"
LOCAL = ROOT / "tests" / "forbidden_words.local"
ALLOWED_CONTEXTS = ["com.phonotech."]  # Java package of the application classes: the paper's affiliation, not an account identifier


def word_hash(w: str) -> str:
    return hashlib.sha256(w.lower().encode("utf-8")).hexdigest()


def load_forbidden() -> dict[str, int]:
    out = {}
    for line in HASHES.read_text().splitlines():
        if line.strip() and not line.startswith("#"):
            n, h = line.split()
            out[h] = int(n)
    return out


def forbidden_in(text: str, hashes: dict[str, int]) -> set[str]:
    low = text.lower()
    for a in ALLOWED_CONTEXTS:
        low = low.replace(a, "")
    found = set()
    for n in sorted(set(hashes.values())):
        for i in range(len(low) - n + 1):
            h = hashlib.sha256(low[i:i + n].encode("utf-8")).hexdigest()
            if h in hashes:
                found.add(h)
    return found


if "--write-forbidden-hashes" in sys.argv:
    words = sorted({w.strip().lower() for w in LOCAL.read_text().splitlines() if w.strip() and not w.startswith("#")})
    head = [ln for ln in HASHES.read_text().splitlines() if ln.startswith("#")]
    HASHES.write_text("\n".join(head + [f"{len(w)} {word_hash(w)}" for w in words]) + "\n")
    print(f"wrote {len(words)} hashes to {HASHES.relative_to(ROOT)}")
    sys.exit(0)

forbidden = load_forbidden()
names = {}
if LOCAL.exists():
    local_words = {w.strip().lower() for w in LOCAL.read_text().splitlines() if w.strip() and not w.startswith("#")}
    names = {word_hash(w): w for w in local_words}
    check("the local forbidden-word list hashes to exactly the committed list", set(names) == set(forbidden)
          and all(forbidden[word_hash(w)] == len(w) for w in local_words),
          "run: python3 tests/check_manifests.py --write-forbidden-hashes")
if names:
    probe = sorted(names.values())[0]
    check("the hashed scan finds a forbidden word inside a longer token", bool(forbidden_in("x" + probe.upper() + "66", forbidden)))
check("the committed forbidden-word file holds hashes only", all(re.fullmatch(r"[0-9]+ [0-9a-f]{64}", ln) for ln in HASHES.read_text().splitlines()
      if ln.strip() and not ln.startswith("#")) and len(forbidden) >= 20)
skip_dirs = {".terraform", "__pycache__", ".git", ".venv"}
candidates = [p for p in ROOT.rglob("*") if p.is_file() and not set(p.parts) & skip_dirs and p != LOCAL and p.suffix not in {".jar"}]
# The operator's own files (kubeconfigs, *.tfvars, backend.hcl) hold the account by necessity and .gitignore keeps them out of
# every commit, so the scan skips exactly what git ignores. Outside a git checkout nothing is skipped (pilot, 14 October 2026).
try:
    gi = subprocess.run(["git", "-C", str(ROOT), "check-ignore", "--stdin"], input="\n".join(str(p) for p in candidates),
                        capture_output=True, text=True, timeout=120)
    ignored = {Path(x) for x in gi.stdout.splitlines()} if gi.returncode in (0, 1) else set()
except (OSError, subprocess.SubprocessError):
    ignored = set()
bad = []
for p in candidates:
    if p in ignored:
        continue
    if "locks" in p.parts and p.suffix == ".lockfile":
        continue
    try:
        t = p.read_text()
    except UnicodeDecodeError:
        continue
    for h in sorted(forbidden_in(t, forbidden)):
        bad.append(f"{p.relative_to(ROOT)}: forbidden word '{names.get(h, 'hash ' + h[:12])}'")
    for m in re.finditer(r"(?<![0-9A-Za-z.:/_-])[0-9]{12}(?![0-9])", t):
        if m.group(0) != "000000000000":
            ctx = t[max(0, m.start() - 30):m.end() + 10].replace("\n", " ")
            if not re.search(r"sha256|--hash|@|digest|[0-9a-f]{20}", ctx):
                bad.append(f"{p.relative_to(ROOT)}: 12-digit number near '{ctx}'")
    for m in re.finditer(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", t):
        if not m.group(0).endswith(("@example.org", "@example.com", "@sha256")):
            bad.append(f"{p.relative_to(ROOT)}: e-mail-like '{m.group(0)}'")
check("no account ID, profile name, e-mail or employer name in any file", not bad, "\n       ".join(bad[:20]))
api = [p for p in (ROOT / "terraform").rglob("*.tf") if "0.0.0.0/0" in p.read_text()]
check("0.0.0.0/0 appears only as a rejected value or a route", all(("contains" in p.read_text() or "route" in p.read_text() or "cidr_block" in p.read_text()) for p in api), str(api))

sys.exit(1 if failures else 0)
