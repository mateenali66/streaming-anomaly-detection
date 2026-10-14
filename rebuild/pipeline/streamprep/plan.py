"""Plan generation and plan checks (protocol 1.4 and contract 11.3)."""
from __future__ import annotations

import numpy as np

from .contract import (
    GENERATOR_STRING, PHASE_KEYS, RUN_TYPES, Errors, campaign_id_for, canonical_json_bytes,
    cell_key_for, seed_for, seed_rule_strings,
)


def n_blocks_for(campaign_index: int) -> int:
    """The pilot holds one block (1.8, 11.2). Every other campaign holds ten (1.9)."""
    return 1 if campaign_index == 0 else 10


def draw_cells(seed: int, n_blocks: int, settle_max: int) -> dict:
    """Draw every random plan value in the fixed order of 11.3."""
    rng = np.random.Generator(np.random.PCG64(seed))
    perms = [[str(x) for x in rng.permutation(list(RUN_TYPES))] for _ in range(n_blocks)]
    settle = [[int(rng.integers(0, settle_max + 1)) for _ in range(8)] for _ in range(n_blocks)]
    follower = [[int(rng.integers(0, 2)) for _ in range(8)] for _ in range(n_blocks)]
    sham = [[int(rng.choice([180, 300])) for _ in range(8)] for _ in range(n_blocks)]
    kills = [[[int(x) for x in rng.integers(0, 3, size=5)] for _ in range(8)] for _ in range(n_blocks)]
    load_generator_seed = int(rng.integers(0, 2147483647))
    f6_payload_seed = int(rng.integers(0, 2147483647))
    reserve = [int(x) for x in rng.integers(0, settle_max + 1, size=40)]
    return {
        "perms": perms, "settle": settle, "follower": follower, "sham": sham, "kills": kills,
        "load_generator_seed": load_generator_seed, "f6_payload_seed": f6_payload_seed,
        "reserve": reserve,
    }


def generate_plan(workload: str, campaign_index: int, run_types: list[dict], phase_s: dict,
                  seed_rule_form: str = "concrete") -> dict:
    """Build a plan object. ``run_types`` and ``phase_s`` are protocol inputs, not random draws."""
    seed = seed_for(workload, campaign_index)
    n_blocks = n_blocks_for(campaign_index)
    draws = draw_cells(seed, n_blocks, int(phase_s["settle_max"]))
    template, concrete = seed_rule_strings(workload, campaign_index)
    blocks = []
    for b in range(n_blocks):
        cells = []
        for p in range(8):
            cells.append({
                "position": p + 1,
                "cell_key": cell_key_for(workload, campaign_index, b + 1, p + 1),
                "run_type": draws["perms"][b][p],
                "settle_gap_s": draws["settle"][b][p],
                "follower_rank": draws["follower"][b][p],
                "sham_duration_s": draws["sham"][b][p],
                "tm_kill_ranks": draws["kills"][b][p],
            })
        blocks.append({"block": b + 1, "cells": cells})
    return {
        "schema_version": "1",
        "workload": workload,
        "campaign_index": campaign_index,
        "campaign_id": campaign_id_for(workload, campaign_index),
        "seed": str(seed),
        "seed_rule": concrete if seed_rule_form == "concrete" else template,
        "generator": GENERATOR_STRING,
        "numpy_version": np.__version__,
        "run_types": run_types,
        "phase_s": {k: int(phase_s[k]) for k in PHASE_KEYS},
        "blocks": blocks,
        "reserve_settle_gap_s": draws["reserve"],
        "load_generator_seed": draws["load_generator_seed"],
        "f6_payload_seed": draws["f6_payload_seed"],
    }


def check_plan(plan: dict, raw_bytes: bytes, errs: Errors, warnings: list[str]) -> None:
    """Content checks beyond the schema: identity, seed rule, canonical form and the draws."""
    workload, idx = plan["workload"], plan["campaign_index"]
    if raw_bytes != canonical_json_bytes(plan):
        errs.add("plan/plan.json: bytes are not canonical JSON (11.1.5)")
    try:
        if plan["campaign_id"] != campaign_id_for(workload, idx):
            errs.add(f"plan.campaign_id {plan['campaign_id']!r} does not match workload and index")
    except Exception as exc:  # campaign_id_for raises on a bad index
        errs.add(f"plan identity: {exc}")
        return
    seed = seed_for(workload, idx)
    if plan["seed"] != str(seed):
        errs.add(f"plan.seed {plan['seed']} is not the 1.4 seed {seed} for {workload} campaign {idx}")
    if plan["seed_rule"] not in seed_rule_strings(workload, idx):
        errs.add(f"plan.seed_rule {plan['seed_rule']!r} is neither the template nor its substitution")
    order = [rt["run_type"] for rt in plan["run_types"]]
    if order != list(RUN_TYPES):
        errs.add(f"plan.run_types order {order} is not {list(RUN_TYPES)}")
    for rt in plan["run_types"]:
        if rt["run_type"] == "F0" and rt["duration_s"] != 0:
            errs.add("plan.run_types F0 duration_s must be 0")
    n_blocks = len(plan["blocks"])
    if n_blocks != n_blocks_for(idx):
        errs.add(f"plan.blocks has {n_blocks} blocks, expected {n_blocks_for(idx)}")
    settle_max = plan["phase_s"]["settle_max"]
    for bi, block in enumerate(plan["blocks"]):
        if block["block"] != bi + 1:
            errs.add(f"plan.blocks[{bi}].block is {block['block']}, expected {bi + 1}")
        types_in_block = sorted(c["run_type"] for c in block["cells"])
        if types_in_block != sorted(RUN_TYPES):
            errs.add(f"plan block {block['block']} does not hold one run of each type: {types_in_block}")
        for pi, cell in enumerate(block["cells"]):
            if cell["position"] != pi + 1:
                errs.add(f"plan block {block['block']} cell {pi} position is {cell['position']}")
            expect_key = cell_key_for(workload, idx, block["block"], cell["position"])
            if cell["cell_key"] != expect_key:
                errs.add(f"plan cell_key {cell['cell_key']!r}, expected {expect_key!r}")
            if not 0 <= cell["settle_gap_s"] <= settle_max:
                errs.add(f"plan {cell['cell_key']} settle_gap_s outside 0 to settle_max")
            if cell["sham_duration_s"] not in (180, 300):
                errs.add(f"plan {cell['cell_key']} sham_duration_s must be 180 or 300")
    for g in plan["reserve_settle_gap_s"]:
        if not 0 <= g <= settle_max:
            errs.add("plan.reserve_settle_gap_s value outside 0 to settle_max")
            break
    if errs:
        return
    # Regenerate the draws from the seed and compare them value by value.
    draws = draw_cells(seed, n_blocks, settle_max)
    mismatches = []
    for bi, block in enumerate(plan["blocks"]):
        for pi, cell in enumerate(block["cells"]):
            got = (cell["run_type"], cell["settle_gap_s"], cell["follower_rank"],
                   cell["sham_duration_s"], cell["tm_kill_ranks"])
            exp = (draws["perms"][bi][pi], draws["settle"][bi][pi], draws["follower"][bi][pi],
                   draws["sham"][bi][pi], draws["kills"][bi][pi])
            if got != exp:
                mismatches.append(cell["cell_key"])
    if plan["reserve_settle_gap_s"] != draws["reserve"]:
        mismatches.append("reserve_settle_gap_s")
    if plan["load_generator_seed"] != draws["load_generator_seed"]:
        mismatches.append("load_generator_seed")
    if plan["f6_payload_seed"] != draws["f6_payload_seed"]:
        mismatches.append("f6_payload_seed")
    if mismatches:
        msg = (f"plan draws do not regenerate from the seed with numpy {np.__version__}: "
               f"{mismatches[:5]}{' ...' if len(mismatches) > 5 else ''}")
        if plan["numpy_version"] == np.__version__:
            errs.add(msg)
        else:
            warnings.append(msg + f" (plan drawn with numpy {plan['numpy_version']}, so this is a warning)")


def cell_index(plan: dict) -> dict[str, dict]:
    """cell_key to {block, position, run_type, cell} for quick lookups."""
    out = {}
    for block in plan["blocks"]:
        for cell in block["cells"]:
            out[cell["cell_key"]] = {"block": block["block"], **cell}
    return out


def run_type_index(plan: dict) -> dict[str, dict]:
    return {rt["run_type"]: rt for rt in plan["run_types"]}
