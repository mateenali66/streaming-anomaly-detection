"""Plan generation and verification (protocol 1.4 and 11.3).

The draw order is exactly the seven steps of 11.3. Every cell gets every field, whatever its run type, so the
random stream never depends on run type. The plan contains no floats, so canonical bytes are identical on
every machine that runs the same NumPy version (the version is recorded in the plan and checked on verify).
"""
from __future__ import annotations

import hashlib
from pathlib import Path

import numpy as np

from . import SCHEMA_VERSION, ids
from .canonical import canonical_bytes, sha256_hex
from .errors import ConfigError, PlanMismatch

# The NumPy of the orchestrator image (rebuild/infra/images/orchestrator/requirements.in). A committed plan must be drawn with
# exactly this version, because the plan records numpy_version and the image compares bytes at every start. The supported
# way to draw one is rebuild/integration/plan_tool.sh (CONTRACT_CHANGES C14); tests check the three pins agree.
IMAGE_NUMPY_VERSION = "2.3.5"
SEED_RULE = "int(sha256('rebuild/<workload>/campaign-<i>').hexdigest()[:16],16)"
GENERATOR = "numpy.random.Generator(numpy.random.PCG64(seed))"
RUN_TYPE_ORDER = ["F0", "F1", "F2", "F4", "F5", "F6", "F7", "F8"]
SHAM_CHOICES = [180, 300]
RESERVE_GAPS = 40

FAULT_TABLE = {
    # run_type: (name in the paper, kind, duration_s). Protocol 1.2.
    "F0": ("Sham", "SHAM", 0),
    "F1": ("Broker pod kill", "PodChaos", 300),
    "F2": ("Broker network partition", "NetworkChaos", 180),
    "F4": ("TaskManager CPU stress", "StressChaos", 300),
    "F5": ("TaskManager loss", "PodChaos", 300),
    "F6": ("Poison pill", "InjectorJob", 180),
    "F7": ("TaskManager packet loss", "NetworkChaos", 300),
    "F8": ("TaskManager memory pressure", "StressChaos", 180),
}


def campaign_seed(workload: str, index: int) -> int:
    """1.4.1: the integer value of the first 16 hex digits of SHA-256 of 'rebuild/<workload>/campaign-<i>'."""
    if workload not in ids.WORKLOADS:
        raise ConfigError(f"bad workload {workload!r}")
    return int(hashlib.sha256(f"rebuild/{workload}/campaign-{index}".encode("utf-8")).hexdigest()[:16], 16)


def blocks_for(index: int) -> int:
    """The pilot (index 0) uses block b01 only (11.2). Campaigns have ten blocks (1.1)."""
    return 1 if index == 0 else 10


def run_type_params(cfg: dict) -> dict:
    urp = "ON" if cfg["f1_f2_urp_presence_check"] else "OFF"
    return {
        "F0": {"sham_duration_choices_s": "180,300", "pseudo_targets": "FOLLOWER_RANK,ALL_TM,TM_KILL_RANKS,F6_PLANNED_COUNT"},
        "F1": {"action": "pod-kill", "grace_period_s": 0, "target": "FOLLOWER_RANK", "labelled_span": cfg["f1_labelled_span"],
               "urp_presence_check": urp},
        "F2": {"action": "partition", "direction": "both", "target": "FOLLOWER_RANK", "urp_presence_check": urp},
        "F4": {"stressor": "cpu", "workers": 4, "load_percent": 90, "container": ids.FLINK_CONTAINER, "target": "ALL_TM"},
        "F5": {"action": "pod-kill", "grace_period_s": 0, "kills": 5, "interval_s": 60, "target": "TM_KILL_RANKS",
               "job_recovery_limit_s": cfg["f5_job_recovery_limit_s"]},
        "F6": {"topic": "raw-events", "rate_permille_of_baseline_mean": 50, "header": ids.F6_HEADER,
               "payload": "JSON_WITHOUT_CLOSING_BRACE", "apply_max_s": 5, "ready_lead_s": 30},
        "F7": {"action": "loss", "loss_percent": 50, "direction": "to", "target": "ALL_TM"},
        "F8": {"stressor": "memory", "workers": 1, "target_limit_permille": cfg["f8_target_limit_permille"],
               "min_stressor_mib": cfg["f8_min_stressor_mib"], "container": ids.FLINK_CONTAINER, "target": "ALL_TM"},
    }


def phase_s(cfg: dict) -> dict:
    f = cfg["fixed"]
    return {
        "settle_max": cfg["settle_max_s"], "pre_fault": cfg["pre_fault_s"], "recovery": f["recovery_s"],
        "cooldown": f["cooldown_s"], "gate_hold": f["gate_hold_s"], "gate_timeout": f["gate_timeout_s"],
        "remediation_timeout": f["remediation_timeout_s"], "apply_max": f["apply_max_s"],
        "redeploy_extra_settle": f["redeploy_extra_settle_s"],
    }


def generate_plan(workload: str, index: int, cfg: dict) -> dict:
    campaign_id = ids.campaign_id_for(workload, index)
    seed = campaign_seed(workload, index)
    rng = np.random.Generator(np.random.PCG64(seed))
    phases = phase_s(cfg)
    settle_max = phases["settle_max"]
    nblocks = blocks_for(index)

    # Step 1: one permutation per block.
    perms = [[str(x) for x in rng.permutation(RUN_TYPE_ORDER)] for _ in range(nblocks)]
    cells_order = [(b, p) for b in range(1, nblocks + 1) for p in range(1, 9)]
    # Steps 2 to 5: one field at a time, each in block then position order.
    settle = {bp: int(rng.integers(0, settle_max + 1)) for bp in cells_order}
    follower = {bp: int(rng.integers(0, 2)) for bp in cells_order}
    sham = {bp: int(rng.choice(SHAM_CHOICES)) for bp in cells_order}
    kills = {bp: [int(x) for x in rng.integers(0, 3, size=5)] for bp in cells_order}
    # Step 6.
    load_generator_seed = int(rng.integers(0, 2147483647))
    f6_payload_seed = int(rng.integers(0, 2147483647))
    # Step 7.
    reserve = [int(x) for x in rng.integers(0, settle_max + 1, size=RESERVE_GAPS)]

    params = run_type_params(cfg)
    blocks = []
    for b in range(1, nblocks + 1):
        cells = []
        for p in range(1, 9):
            cells.append({
                "position": p,
                "cell_key": ids.cell_key(workload, index, b, p),
                "run_type": perms[b - 1][p - 1],
                "settle_gap_s": settle[(b, p)],
                "follower_rank": follower[(b, p)],
                "sham_duration_s": sham[(b, p)],
                "tm_kill_ranks": kills[(b, p)],
            })
        blocks.append({"block": b, "cells": cells})
    return {
        "schema_version": SCHEMA_VERSION,
        "workload": workload,
        "campaign_index": index,
        "campaign_id": campaign_id,
        "seed": str(seed),
        "seed_rule": SEED_RULE,
        "generator": GENERATOR,
        "numpy_version": np.__version__,
        "run_types": [
            {"run_type": rt, "name": FAULT_TABLE[rt][0], "kind": FAULT_TABLE[rt][1], "duration_s": FAULT_TABLE[rt][2],
             "params": params[rt]}
            for rt in RUN_TYPE_ORDER
        ],
        "phase_s": phases,
        "blocks": blocks,
        "reserve_settle_gap_s": reserve,
        "load_generator_seed": load_generator_seed,
        "f6_payload_seed": f6_payload_seed,
    }


def plan_bytes(plan: dict) -> bytes:
    return canonical_bytes(plan)


def require_image_numpy() -> None:
    """Refuse to write or verify a committed plan with a NumPy other than the image's."""
    if np.__version__ != IMAGE_NUMPY_VERSION:
        raise ConfigError(f"this process has numpy {np.__version__}, the orchestrator image has {IMAGE_NUMPY_VERSION}; a plan "
                          "drawn here would be refused at every start. Use rebuild/integration/plan_tool.sh")


def plan_sha_file_bytes(sha: str) -> bytes:
    """plan/plan.json.sha256 holds the 64-character hash and a newline (11.3)."""
    return (sha + "\n").encode("ascii")


def write_plan_files(plan: dict, out_dir: str | Path) -> str:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    data = plan_bytes(plan)
    sha = sha256_hex(data)
    (out / "plan.json").write_bytes(data)
    (out / "plan.json.sha256").write_bytes(plan_sha_file_bytes(sha))
    return sha


def verify_plan(workload: str, index: int, cfg: dict, expected_sha: str, stored: bytes | None) -> tuple[dict, bytes, str]:
    """1.4.4: regenerate from the seed and compare byte for byte with the stored plan and the configured hash."""
    regenerated = generate_plan(workload, index, cfg)
    data = plan_bytes(regenerated)
    sha = sha256_hex(data)
    if stored is not None:
        if stored != data:
            stored_np = _numpy_version_of(stored)
            hint = ""
            if stored_np and stored_np != np.__version__:
                hint = f" (plan drawn with numpy {stored_np}, this process has {np.__version__})"
            raise PlanMismatch("stored plan differs from the plan regenerated from the seed" + hint)
    if sha != expected_sha:
        raise PlanMismatch(f"regenerated plan hash {sha} differs from PLAN_SHA256 {expected_sha}")
    return regenerated, data, sha


def _numpy_version_of(data: bytes) -> str | None:
    import json
    try:
        return json.loads(data.decode("utf-8")).get("numpy_version")
    except Exception:  # the plan is corrupt; the byte mismatch is already the error
        return None


def cells_in_order(plan: dict) -> list[dict]:
    out = []
    for blk in plan["blocks"]:
        for c in blk["cells"]:
            out.append({**c, "block": blk["block"]})
    return out


def run_type_entry(plan: dict, run_type: str) -> dict:
    for rt in plan["run_types"]:
        if rt["run_type"] == run_type:
            return rt
    raise KeyError(run_type)
