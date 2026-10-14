"""Protocol 1.4 and contract 11.3: seeds, draw order, canonical bytes."""
from __future__ import annotations

import hashlib

from streamprep.contract import Errors, canonical_json_bytes, seed_for
from streamprep.plan import check_plan, generate_plan
from streamprep.synthetic import PHASE_S, RUN_TYPE_SPECS


def test_seed_rule():
    for wl in ("ecommerce", "iot"):
        for i in range(6):
            want = int(hashlib.sha256(f"rebuild/{wl}/campaign-{i}".encode()).hexdigest()[:16], 16)
            assert seed_for(wl, i) == want
    assert seed_for("ecommerce", 1) != seed_for("iot", 1)


def test_plan_regenerates_byte_for_byte():
    a = canonical_json_bytes(generate_plan("iot", 2, RUN_TYPE_SPECS, PHASE_S))
    b = canonical_json_bytes(generate_plan("iot", 2, RUN_TYPE_SPECS, PHASE_S))
    assert a == b
    plan = generate_plan("iot", 2, RUN_TYPE_SPECS, PHASE_S)
    errs, warn = Errors(), []
    check_plan(plan, a, errs, warn)
    assert not errs and not warn


def test_every_block_holds_each_run_type_once_and_the_pilot_one_block():
    plan = generate_plan("ecommerce", 1, RUN_TYPE_SPECS, PHASE_S)
    assert len(plan["blocks"]) == 10
    for b in plan["blocks"]:
        assert sorted(c["run_type"] for c in b["cells"]) == sorted(["F0", "F1", "F2", "F4", "F5", "F6", "F7", "F8"])
        for c in b["cells"]:
            assert set(c) == {"position", "cell_key", "run_type", "settle_gap_s", "follower_rank",
                              "sham_duration_s", "tm_kill_ranks"}
    pilot = generate_plan("iot", 0, RUN_TYPE_SPECS, PHASE_S)
    assert len(pilot["blocks"]) == 1 and pilot["campaign_id"] == "pilot-iot"
    assert pilot["blocks"][0]["cells"][0]["cell_key"].startswith("iot-c0-b01-p1")
    assert len(pilot["reserve_settle_gap_s"]) == 40


def test_orders_differ_between_blocks():
    plan = generate_plan("ecommerce", 1, RUN_TYPE_SPECS, PHASE_S)
    orders = {tuple(c["run_type"] for c in b["cells"]) for b in plan["blocks"]}
    assert len(orders) > 5
