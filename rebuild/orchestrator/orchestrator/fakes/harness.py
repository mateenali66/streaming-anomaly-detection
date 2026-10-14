"""Builds an orchestrator Runtime over the fakes. One Harness is one simulated cluster; each call to campaign()
is a fresh orchestrator process that shares the cluster, the S3 prefix and the manifest volume with the previous one.
"""
from __future__ import annotations

import logging
import tempfile
from pathlib import Path

from .. import ids, thresholds
from ..campaign import Campaign
from ..config import Environment, load_allowlist, load_config
from ..exporter import run_export
from ..httpjson import HttpJson
from ..plan import generate_plan, plan_bytes
from ..runtime import Runtime
from ..canonical import sha256_hex
from .clock import FakeClock
from .kube import FakeKube
from .prom import FakeProm
from .services import FakeFlink, FakeHealthLogs, FakeProbes
from .store import ExporterView, FakeS3
from .world import FakeWorld

FAST = {"fixed": {"warmup_s": 600, "pilot_baseline_s": 1200, "baseline_s": 1800, "trailing_s": 600, "chunk_s": 600}}
VERSIONS = {"kubernetes_version": "1.35", "node_ami_release": "1.35.0-20261003", "availability_zone": "us-east-2b",
            "charts": {"strimzi": "0.45.2", "flink_operator": "1.10.0", "chaos_mesh": "2.8.4",
                       "kube_prometheus_stack": "69.8.0"}}
IMAGES_LOCK_STUB = b"schema: 1\nplatform: linux/arm64\n# dry-run stub; the real file is rebuild/infra/images/images.lock.yaml\n"


class Harness:
    def __init__(self, workload: str = "ecommerce", index: int = 0, root: str | Path | None = None,
                 overrides: dict | None = None, fast: bool = True, start: float = 1_800_000_000.0) -> None:
        self.root = Path(root) if root else Path(tempfile.mkdtemp(prefix="orch-dry-"))
        ov = {}
        if fast:
            ov = {"fixed": dict(FAST["fixed"])}
        if overrides:
            for k, v in overrides.items():
                if k == "fixed":
                    ov.setdefault("fixed", {}).update(v)
                else:
                    ov[k] = v
        self.cfg = load_config(overrides=ov or None)
        self.allow, self.allow_bytes = load_allowlist()
        self.th = thresholds.build(self.cfg)
        self.th_bytes = thresholds.thresholds_bytes(self.th)
        self.workload, self.index = workload, index
        self.campaign_id = ids.campaign_id_for(workload, index)
        self.plan = generate_plan(workload, index, self.cfg)
        self.plan_bytes = plan_bytes(self.plan)
        self.plan_sha = sha256_hex(self.plan_bytes)
        self.plan_file_bytes = self.plan_bytes  # what infra mounts as /config/plan.json (PLAN_PATH)
        self.clock = FakeClock(start=start)
        self.world = FakeWorld(self.clock, workload, self.campaign_id)
        self.s3 = FakeS3(self.clock, self.world)
        self.prom = FakeProm(self.world, self.th)
        self.world.exporter_hook = lambda kind, key, s, e: run_export(self.prom, ExporterView(self.s3), self.cfg, self.allow,
                                                                      kind, key, s, e)
        self.kube = FakeKube(self.world)
        self.flink = FakeFlink(self.world)
        self.probes = FakeProbes(self.world)
        self.health = FakeHealthLogs(self.world)
        self.world.results_bucket = "dry-run-results"
        self.s3.on_put = self._on_put
        self.alerts: list = []
        self.volume = self.root / "volume"
        self.volume.mkdir(parents=True, exist_ok=True)
        self.env = Environment(workload=workload, campaign_id=self.campaign_id, campaign_index=index,
                               plan_sha256=self.plan_sha, results_bucket="dry-run-results",
                               results_key_prefix=f"campaign/{self.campaign_id}",
                               manifest_dir=str(self.volume), orchestrator_image="", f6_injector_image="",
                               git_commit="0" * 40)

    def _on_put(self, key: str, data: bytes) -> None:
        if key == "manifest/manifest.json":
            import json
            self.world.on_manifest(json.loads(data))

    def runtime(self) -> Runtime:
        return Runtime(env=self.env, cfg=self.cfg, allow=self.allow, allow_bytes=self.allow_bytes, thresholds=self.th,
                       thresholds_bytes=self.th_bytes, plan=self.plan, plan_bytes=self.plan_bytes, clock=self.clock,
                       kube=self.kube, prom=self.prom, store=self.s3, flink=self.flink, probes=self.probes, http=HttpJson(),
                       sentinel=lambda: self.world.sentinels.append(self.clock.t),
                       alert=lambda subject, msg: self.alerts.append((self.clock.t, subject, msg)),
                       versions=dict(VERSIONS), images_lock_bytes=IMAGES_LOCK_STUB, volume=self.volume,
                       plan_file_bytes=self.plan_file_bytes, health=self.health,
                       loadgen_seed=lambda: self.world.global_knobs.get("loadgen_seed", self.plan["load_generator_seed"]))

    def campaign(self) -> Campaign:
        return Campaign(self.runtime())

    def run(self, resume: bool = False, only_block: int | None = None, only_fault: str | None = None) -> int:
        return self.campaign().execute(resume=resume, only_block=only_block, only_fault=only_fault)

    def manifest(self) -> dict:
        import json
        return json.loads((self.volume / "manifest.json").read_text())


def quiet() -> None:
    logging.getLogger("orchestrator").setLevel(logging.ERROR)
