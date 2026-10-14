"""Single entrypoint: `orchestrator <subcommand>` (also `python -m orchestrator`). The image runs this one binary;
the Job, the exporter Jobs, the probe service and the consumer differ only in their arguments.

  plan            generate plan/plan.json and print its SHA-256 (commit it, pass it as PLAN_SHA256)
  verify-plan     regenerate a stored plan and compare bytes and hash
  run             the campaign (in-cluster Job). --dry-run runs against the fake cluster harness.
                  --resume confirms that the previous stop was transient (2.6.6); without it a restart after a halt,
                  SIGTERM or crash is refused. --block N / --fault F run one block or one named fault (pilot,
                  campaign_index 0, only). They combine: `run --resume --fault F4` reruns F4 after a halt (REVIEW4 B6).
                  Exit codes: 0 finished (or a pilot subset finished), 1 defect, 2 refused to start, 3 halted (2.6),
                  4 resume refused, 143 SIGTERM.
  export          the metric exporter (Job export-<attempt_key>, export-baseline-<t>, export-trailing-<t>)
  probes          the verification probe service (Deployment verification-probes)
  consumer        the verification consumer (Deployment verification-consumer)
  f6-inject       the F6 injector, the same file infra's f6-injector image runs as Job poison-<attempt_key>
  gate-offline    `make gate`: re-evaluate every attempt from a local copy of the campaign prefix, compare
  verify-complete write SHA256SUMS and COMPLETE for a finished or abandoned campaign
  abandon         record an abandonment the author has confirmed, then write COMPLETE with status ABANDONED
"""
from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import re
import signal
import sys
import time
from pathlib import Path

from . import ids, thresholds
from .canonical import canonical_bytes, sha256_hex
from .config import environment_from, load_allowlist, load_config, real_run_preconditions
from .errors import ConfigError, OrchestratorError, PlanMismatch

log = logging.getLogger("orchestrator")


def _setup_logging(path: Path | None = None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if path is not None:
        path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(path))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s", handlers=handlers,
                        force=True)


# ============================================================================================ plan
def cmd_plan(a) -> int:
    """Writes plan.json and plan.json.sha256. Infra deploys plan.json with `deploy_workload.sh --plan-file`, which mounts
    it as PLAN_PATH, passes its hash as PLAN_SHA256 and reads the load generator seed from the plan itself. The plan records
    numpy_version and every start compares bytes, so this refuses any NumPy but the image's (plan_tool.sh, C10)."""
    import numpy as np

    from .plan import generate_plan, require_image_numpy, write_plan_files
    from .schema import validate_plan
    require_image_numpy()
    cfg = load_config(a.config)
    plan = generate_plan(a.workload, a.index, cfg)
    validate_plan(plan)
    sha = write_plan_files(plan, a.out)
    print(sha)
    print(json.dumps({"campaign_id": plan["campaign_id"], "campaign_seed": plan["seed"],
                      "loadgen_seed": plan["load_generator_seed"], "numpy_version": np.__version__}), file=sys.stderr)
    return 0


def cmd_verify_plan(a) -> int:
    from .plan import require_image_numpy, verify_plan
    require_image_numpy()
    cfg = load_config(a.config)
    try:
        verify_plan(a.workload, a.index, cfg, a.sha, Path(a.plan).read_bytes())
    except PlanMismatch as e:
        print(f"MISMATCH: {e}")
        return 1
    print("OK")
    return 0


# ============================================================================================ run
def code_identity() -> str:
    """The git commit if the build provided one (ORCHESTRATOR_GIT_COMMIT or a BUILD_COMMIT file next to the package),
    otherwise the SHA-256 of the package source and its config files, so the manifest always names the exact code.
    Infra's Dockerfile copies rebuild/orchestrator without .git and sets no commit."""
    env = os.environ.get("ORCHESTRATOR_GIT_COMMIT")
    if env:
        return env
    root = Path(__file__).resolve().parent.parent
    bc = root / "BUILD_COMMIT"
    if bc.exists():
        return bc.read_text().strip()
    import hashlib
    h = hashlib.sha256()
    for p in sorted(list((root / "orchestrator").rglob("*.py")) + list((root / "config").glob("*.yaml"))):
        h.update(p.relative_to(root).as_posix().encode() + b"\0" + p.read_bytes() + b"\0")
    return f"sha256-tree:{h.hexdigest()}"


def own_image(kube, pod_name: str | None) -> str:
    """This pod's orchestrator image pinned by digest (spec image, or the runtime imageID's digest)."""
    if not pod_name:
        return ""
    pod = kube.get("Pod", pod_name)
    if pod is None:
        return ""
    spec = next((c for c in pod["spec"]["containers"] if c["name"] == "orchestrator"), pod["spec"]["containers"][0])
    image = spec["image"]
    if "@sha256:" in image:
        return image
    for cs in (pod.get("status") or {}).get("containerStatuses") or []:
        if cs.get("name") == spec["name"] and "@sha256:" in (cs.get("imageID") or ""):
            return image.split("@")[0].rsplit(":", 1)[0] + "@" + cs["imageID"].split("@", 1)[1]
    return image


def build_real_runtime(env, cfg, allow, allow_bytes, th, th_bytes):
    import boto3

    from .clock import RealClock
    from .flinkrest import FlinkRest
    from .httpjson import HttpJson
    from .kube import KubectlKube
    from .plan import generate_plan, plan_bytes
    from .probe_service import ProbeClient, loadgen_status
    from .prom import HttpProm
    from .runtime import Runtime
    from .store import S3Store

    from .config import MOUNTED_FILES
    absent = [a for a in MOUNTED_FILES if not getattr(env, a) or not Path(getattr(env, a)).is_file()]
    if absent:
        raise ConfigError(f"mounted inputs missing: {absent} (PLAN_PATH, THRESHOLDS_PATH, VERSIONS_FILE, IMAGES_LOCK_FILE)")
    clock = RealClock()
    signal.signal(signal.SIGTERM, lambda s, f: clock.shutdown.request("SIGTERM"))
    signal.signal(signal.SIGINT, lambda s, f: clock.shutdown.request("SIGINT"))
    kube = KubectlKube(kubectl=env.kubectl_bin)
    plan = generate_plan(env.workload, env.campaign_index, cfg)
    # The four mounted inputs (ConfigMap campaign-config at /config, CONTRACT_CHANGES C5 and C8). real_run_preconditions has
    # already refused a start without them.
    if Path(env.thresholds_path).read_bytes() != th_bytes:
        raise ConfigError("THRESHOLDS_PATH differs from the thresholds this code registers (run `orchestrator thresholds` "
                          "with the pinned tool environment, rebuild/integration/plan_tool.sh)")
    plan_file = Path(env.plan_path).read_bytes()
    if not env.orchestrator_image:
        env.orchestrator_image = own_image(kube, env.pod_name)
    env.git_commit = env.git_commit or code_identity()
    versions = json.loads(Path(env.versions_file).read_text())
    images_lock = Path(env.images_lock_file).read_bytes()
    sns = boto3.client("sns") if env.alerts_topic_arn else None

    def alert(subject: str, message: str) -> None:
        if sns is not None:
            sns.publish(TopicArn=env.alerts_topic_arn, Subject=subject[:99], Message=message)

    def sentinel() -> None:
        from confluent_kafka import Producer
        p = Producer({"bootstrap.servers": env.kafka_bootstrap, "acks": "all", "client.id": "rebuild-sentinel"})
        res: dict = {}
        p.produce("dead-letter", value=b'{"rebuild_sentinel":true}',
                  headers=[(ids.SENTINEL_HEADER, env.campaign_id.encode())], callback=lambda e, m: res.setdefault("err", e))
        left = p.flush(30)
        if left or res.get("err") is not None:
            raise OrchestratorError(f"sentinel record not acknowledged: left={left} err={res.get('err')}")

    def loadgen_seed():
        try:
            return loadgen_status(HttpJson().get(env.loadgen_status_url, timeout_s=5.0))["seed"]
        except OrchestratorError:
            return None

    http = HttpJson()
    from .health import CloudWatchHealthEvents
    try:
        health = CloudWatchHealthEvents()
    except Exception as e:  # noqa: BLE001 - the Health record is never fatal (C30); every poll then logs the gap
        log.error("AWS Health source unavailable: %s", e)
        health = None
    return Runtime(env=env, cfg=cfg, allow=allow, allow_bytes=allow_bytes, thresholds=th, thresholds_bytes=th_bytes,
                   plan=plan, plan_bytes=plan_bytes(plan), clock=clock, kube=kube, prom=HttpProm(env.prometheus_url),
                   store=S3Store(env.results_prefix), flink=FlinkRest(env.flink_rest_url, http),
                   probes=ProbeClient(env.probes_url, http=http), http=http, sentinel=sentinel, alert=alert,
                   versions=versions, images_lock_bytes=images_lock, volume=Path(env.manifest_dir),
                   plan_file_bytes=plan_file, loadgen_seed=loadgen_seed, health=health)


# Twelve digits not inside a longer word: an account ID in an ARN, a host or a message. A run of digits inside a hex digest
# touches a letter or more digits, and is left alone.
ACCOUNT_RE = re.compile(rb"(?<![0-9A-Za-z])[0-9]{12}(?![0-9A-Za-z])")
ARN_RE = re.compile(rb"arn:aws[a-z-]*:[A-Za-z0-9-]*:[a-z0-9-]*:[0-9]*:[^\s\"',;)\]}]+")


def redact_log(data: bytes, bucket: str) -> bytes:
    """The uploaded log is part of the deposit (7.5), and botocore and kubectl errors quote ARNs, image references and the
    bucket name (REVIEW4 C1). Every ARN, every 12-digit number (an account ID) and the bucket name are replaced before
    upload (C24)."""
    if bucket:
        data = data.replace(bucket.encode("utf-8"), b"<results-bucket>")
    data = ARN_RE.sub(b"<arn>", data)
    return ACCOUNT_RE.sub(b"<account-id>", data)


def cmd_thresholds(a) -> int:
    """Writes the registered thresholds.json (what infra mounts as /config/thresholds.json) and prints its SHA-256."""
    cfg = load_config(a.config)
    data = thresholds.thresholds_bytes(thresholds.build(cfg))
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    Path(a.out).write_bytes(data)
    print(sha256_hex(data))
    return 0


def cmd_run(a) -> int:
    if a.dry_run:
        from .fakes.harness import Harness
        h = Harness(a.workload or "ecommerce", a.index if a.index is not None else 0, root=a.dry_run_root,
                    fast=not a.full_length, overrides=None)
        _setup_logging()
        rc = h.run(resume=a.resume, only_block=a.block, only_fault=a.fault)
        m = h.manifest()
        summary = {"exit_code": rc, "status": m["status"], "simulated_hours": round((h.clock.t - h.world.t0) / 3600, 2),
                   "attempts": [(x["attempt_key"], x["run_type"], x["outcome"]) for x in m["attempts"]],
                   "halts": m["halts"], "complete_written": "COMPLETE" in h.s3.objects, "volume": str(h.volume)}
        print(json.dumps(summary, indent=1))
        return rc
    env = environment_from(dict(os.environ))
    cfg = load_config(a.config)
    allow, allow_bytes = load_allowlist(os.environ.get("FEATURE_ALLOWLIST_FILE"))
    th = thresholds.build(cfg)
    problems = [p for p in real_run_preconditions(env, cfg) if "own image" not in p and "code identity" not in p]
    if problems:
        print("refusing to start: " + "; ".join(problems), file=sys.stderr)
        return 2
    start = int(time.time())
    from .manifest import state_dir
    log_path = state_dir(env.manifest_dir) / "campaign.log"  # never synced; copies go to logs/ (CONTRACT_CHANGES C1)
    _setup_logging(log_path)
    rt = build_real_runtime(env, cfg, allow, allow_bytes, th, thresholds.thresholds_bytes(th))
    problems = real_run_preconditions(env, cfg)  # again: the image reference and code identity are known only now
    if problems:
        log.error("refusing to start: %s", "; ".join(problems))
        return 2
    rt.log_uploader = lambda: rt.uploader.put_deferred(
        f"logs/orchestrator-{start}.log.gz", gzip.compress(redact_log(log_path.read_bytes(), env.results_bucket)),
        "application/gzip")

    from .campaign import Campaign
    return Campaign(rt).execute(resume=a.resume, only_block=a.block, only_fault=a.fault)


# ============================================================================================ offline gate
def cmd_gate_offline(a) -> int:
    """`make gate`: recompute every FINISHED attempt's gate from the stored inputs and compare with the online result."""
    from . import gate
    from .schema import validate_verification
    from .store import LocalStore

    store = LocalStore(a.store)
    manifest = json.loads(store.get("manifest/manifest.json"))
    plan = json.loads(store.get("plan/plan.json"))
    th_bytes = store.get("run/thresholds.json")
    th = json.loads(th_bytes)
    out_dir = Path(a.out) if a.out else None
    diffs_total = 0
    for att in manifest["attempts"]:
        if att["status"] != "FINISHED" or (a.attempt and att["attempt_key"] != a.attempt):
            continue
        online = json.loads(store.get(att["verification_key"]))
        gi = gate.load_inputs(store, manifest, plan, att["attempt_key"], th, facts=online)
        gr = gate.evaluate(gi)
        offline = gate.verification_record(gi, gr, "OFFLINE", online["gate_git_commit"], sha256_hex(th_bytes), time.time())
        validate_verification(offline)
        diffs = gate.compare(online, offline)
        diffs_total += len(diffs)
        print(f"{att['attempt_key']} {att['outcome']} {'MATCH' if not diffs else 'DIFFER'}")
        for d in diffs:
            print(f"   {d}")
        if out_dir is not None:
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / f"{att['attempt_key']}.offline.json").write_bytes(canonical_bytes(offline))
    return 1 if diffs_total else 0


def cmd_verify_complete(a, abandon: bool = False) -> int:
    env = environment_from(dict(os.environ))
    cfg = load_config(a.config)
    allow, allow_bytes = load_allowlist(os.environ.get("FEATURE_ALLOWLIST_FILE"))
    th = thresholds.build(cfg)
    _setup_logging()
    rt = build_real_runtime(env, cfg, allow, allow_bytes, th, thresholds.thresholds_bytes(th))
    from .campaign import Campaign
    camp = Campaign(rt)
    camp.m = rt.manifest_store.load()
    if camp.m is None:
        print("no manifest on the volume", file=sys.stderr)
        return 2
    if abandon:
        if not a.confirm:
            print("abandon needs --confirm (the author's decision, protocol 2.6.7)", file=sys.stderr)
            return 2
        return camp.abandon(a.reason)
    from .complete import verify_complete
    marker = verify_complete(rt, camp.m, abandoned=camp.m["status"] == "ABANDONED")
    print(json.dumps(marker, indent=1) if marker else "verify_complete failed; COMPLETE not written")
    return 0 if marker else 1


# ============================================================================================ main
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="orchestrator")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--workload", required=True, choices=ids.WORKLOADS)
    p.add_argument("--index", required=True, type=int, choices=range(0, 6))
    p.add_argument("--config")
    p.add_argument("--out", required=True)
    p = sub.add_parser("verify-plan")
    p.add_argument("--workload", required=True, choices=ids.WORKLOADS)
    p.add_argument("--index", required=True, type=int, choices=range(0, 6))
    p.add_argument("--config")
    p.add_argument("--plan", required=True)
    p.add_argument("--sha", required=True)
    p = sub.add_parser("run")
    p.add_argument("--resume", action="store_true", help="confirm that the previous stop was transient (2.6.6)")
    p.add_argument("--block", type=int, help="pilot only: run one block")
    p.add_argument("--fault", choices=ids.RUN_TYPES, help="pilot only: run one named fault")
    p.add_argument("--config")
    p.add_argument("--dry-run", action="store_true", help="run against the fake cluster harness")
    p.add_argument("--workload", choices=ids.WORKLOADS, help="dry run only (a real run takes WORKLOAD from the environment)")
    p.add_argument("--index", type=int, choices=range(0, 6), help="dry run only")
    p.add_argument("--dry-run-root", help="dry run only: keep the simulated volume here")
    p.add_argument("--full-length", action="store_true", help="dry run with the protocol's real phase lengths")
    sub.add_parser("export", add_help=False)
    sub.add_parser("probes")
    sub.add_parser("consumer")
    sub.add_parser("f6-inject", add_help=False)
    p = sub.add_parser("thresholds")
    p.add_argument("--config")
    p.add_argument("--out", required=True)
    p = sub.add_parser("gate-offline")
    p.add_argument("--store", required=True, help="local copy of campaign/<campaign_id>/")
    p.add_argument("--attempt")
    p.add_argument("--out")
    p = sub.add_parser("verify-complete")
    p.add_argument("--config")
    p = sub.add_parser("abandon")
    p.add_argument("--config")
    p.add_argument("--confirm", action="store_true")
    p.add_argument("--reason", default="abandoned by the author")
    args, rest = ap.parse_known_args(argv)
    try:
        if args.cmd == "plan":
            return cmd_plan(args)
        if args.cmd == "verify-plan":
            return cmd_verify_plan(args)
        if args.cmd == "run":
            if args.dry_run is False and (args.workload or args.index is not None):
                print("--workload and --index are for --dry-run; a real run reads WORKLOAD and CAMPAIGN_INDEX", file=sys.stderr)
                return 2
            return cmd_run(args)
        if args.cmd == "export":
            from .exporter import main as export_main
            return export_main(rest)
        if args.cmd == "thresholds":
            return cmd_thresholds(args)
        if args.cmd == "probes":
            from .probe_service import serve
            serve()
            return 0
        if args.cmd == "consumer":
            from .consumer_service import serve
            serve()
            return 0
        if args.cmd == "f6-inject":
            from .f6_injector import main as f6_main
            return f6_main(rest)
        if args.cmd == "gate-offline":
            return cmd_gate_offline(args)
        if args.cmd == "verify-complete":
            return cmd_verify_complete(args)
        if args.cmd == "abandon":
            return cmd_verify_complete(args, abandon=True)
    except (ConfigError, PlanMismatch) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    return 2


if __name__ == "__main__":
    sys.exit(main())
