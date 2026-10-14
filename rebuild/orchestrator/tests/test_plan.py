"""Plan determinism, the 11.3 draw order, hash checks, and storage before any preflight (1.4, 11.3)."""
import hashlib
import json

import numpy as np
import pytest

from orchestrator import ids
from orchestrator.canonical import canonical_bytes, sha256_hex
from orchestrator.config import load_config
from orchestrator.errors import PlanMismatch
from orchestrator.plan import campaign_seed, generate_plan, plan_bytes, verify_plan, write_plan_files
from orchestrator.schema import validate_plan

CFG = load_config()


def literal_draws(workload, index, settle_max=300):
    """The seven steps of 11.3, written out again directly from the contract text."""
    seed = int(hashlib.sha256(f"rebuild/{workload}/campaign-{index}".encode()).hexdigest()[:16], 16)
    rng = np.random.Generator(np.random.PCG64(seed))
    nb = 1 if index == 0 else 10
    perms = [list(rng.permutation(["F0", "F1", "F2", "F4", "F5", "F6", "F7", "F8"])) for _ in range(nb)]
    n = nb * 8
    settle = [int(rng.integers(0, settle_max + 1)) for _ in range(n)]
    follower = [int(rng.integers(0, 2)) for _ in range(n)]
    sham = [int(rng.choice([180, 300])) for _ in range(n)]
    kills = [[int(x) for x in rng.integers(0, 3, size=5)] for _ in range(n)]
    lg = int(rng.integers(0, 2147483647))
    f6 = int(rng.integers(0, 2147483647))
    reserve = [int(x) for x in rng.integers(0, settle_max + 1, size=40)]
    return seed, perms, settle, follower, sham, kills, lg, f6, reserve


@pytest.mark.parametrize("workload,index", [("ecommerce", 0), ("ecommerce", 2), ("iot", 0), ("iot", 5)])
def test_plan_follows_the_contract_draw_order(workload, index):
    plan = generate_plan(workload, index, CFG)
    validate_plan(plan)
    seed, perms, settle, follower, sham, kills, lg, f6, reserve = literal_draws(workload, index)
    assert plan["seed"] == str(seed)
    cells = [c for b in plan["blocks"] for c in b["cells"]]
    assert [c["run_type"] for c in cells] == [str(x) for p in perms for x in p]
    assert [c["settle_gap_s"] for c in cells] == settle
    assert [c["follower_rank"] for c in cells] == follower
    assert [c["sham_duration_s"] for c in cells] == sham
    assert [c["tm_kill_ranks"] for c in cells] == kills
    assert (plan["load_generator_seed"], plan["f6_payload_seed"], plan["reserve_settle_gap_s"]) == (lg, f6, reserve)
    assert len(plan["blocks"]) == (1 if index == 0 else 10)
    assert plan["campaign_id"] == ids.campaign_id_for(workload, index)


def test_every_cell_gets_every_field_whatever_its_run_type():
    plan = generate_plan("iot", 3, CFG)
    for b in plan["blocks"]:
        for c in b["cells"]:
            assert set(c) == {"position", "cell_key", "run_type", "settle_gap_s", "follower_rank", "sham_duration_s",
                              "tm_kill_ranks"}


def test_plan_is_deterministic_and_has_no_floats():
    a = plan_bytes(generate_plan("ecommerce", 1, CFG))
    b = plan_bytes(generate_plan("ecommerce", 1, CFG))
    assert a == b
    assert b"." not in json.dumps([c for blk in json.loads(a)["blocks"] for c in blk["cells"]]).encode()
    assert canonical_bytes(json.loads(a)) == a  # canonical form (11.1.5)


def test_workloads_and_indices_get_different_seeds():
    seeds = {campaign_seed(w, i) for w in ids.WORKLOADS for i in range(6)}
    assert len(seeds) == 12


def test_verify_plan_accepts_identical_and_rejects_changed_bytes_or_hash(tmp_path):
    plan = generate_plan("ecommerce", 1, CFG)
    sha = write_plan_files(plan, tmp_path)
    data = (tmp_path / "plan.json").read_bytes()
    assert (tmp_path / "plan.json.sha256").read_bytes() == (sha + "\n").encode()
    verify_plan("ecommerce", 1, CFG, sha, data)
    with pytest.raises(PlanMismatch):
        verify_plan("ecommerce", 1, CFG, "0" * 64, data)
    tampered = data.replace(b'"settle_gap_s":', b'"settle_gap_s": ', 1)
    with pytest.raises(PlanMismatch):
        verify_plan("ecommerce", 1, CFG, sha, tampered)


def test_numpy_version_mismatch_is_reported():
    plan = generate_plan("ecommerce", 1, CFG)
    plan["numpy_version"] = "0.0.1"
    with pytest.raises(PlanMismatch, match="numpy"):
        verify_plan("ecommerce", 1, CFG, sha256_hex(plan_bytes(plan)), plan_bytes(plan))


def test_cost_option_changes_the_plan_hash():
    base = sha256_hex(plan_bytes(generate_plan("iot", 1, CFG)))
    opt_c = load_config(overrides={"pre_fault_s": 300, "settle_max_s": 120})
    assert sha256_hex(plan_bytes(generate_plan("iot", 1, opt_c))) != base


def test_orchestrator_refuses_to_start_on_a_wrong_plan_hash(make):
    h = make()
    h.env.plan_sha256 = "a" * 64
    assert h.run() == 2
    assert not (h.volume / "manifest.json").exists()
    assert not any(k.startswith("manifest/") for k in h.s3.objects)


def test_plan_is_the_first_object_written_and_a_restart_compares_it(make):
    h = make()
    h.world.global_knobs["prom_clock_offset"] = 5.0  # make preflight fail after the plan is stored
    assert h.run() != 0
    assert h.s3.put_log[:2] == ["plan/plan.json", "plan/plan.json.sha256"]
    stored = h.s3.get("plan/plan.json")
    assert stored == h.plan_bytes
    # a stored plan that differs is refused on the next start
    h.s3.objects["plan/plan.json"] = (stored.replace(b'"seed":"', b'"seed":"9', 1), h.clock.t)
    h.world.global_knobs["prom_clock_offset"] = 0.0
    assert h.run() == 2


def test_pilot_fault_filter_keeps_plan_positions(make):
    """Review 2 item 5 and adversarial test C: a fault filter must not remap run numbers or gaps."""
    h = make()
    assert h.run(only_fault="F4") == 0
    full = {c["cell_key"]: c for b in h.plan["blocks"] for c in b["cells"]}
    a = [x for x in h.manifest()["attempts"]]
    assert len(a) == 1 and a[0]["run_type"] == "F4"
    cell = full[a[0]["cell_key"]]
    assert cell["run_type"] == "F4"
    assert a[0]["segments"][0]["end_at"] - a[0]["segments"][0]["start_at"] == cell["settle_gap_s"]


def test_block_and_fault_filters_are_pilot_only(make):
    h = make(index=1)
    assert h.run(only_fault="F4") == 2


def _pin(path, name):
    import re
    from pathlib import Path
    m = re.search(rf"^{re.escape(name)}==([^\s\\]+)", Path(path).read_text(), re.M)
    return m.group(1) if m else None


def test_image_numpy_pin_agrees_across_the_three_places():
    """CONTRACT_CHANGES C14: the orchestrator's IMAGE_NUMPY_VERSION, the image's requirements.in and the pinned plan tool's
    lock name one NumPy (and the tool's PyYAML is the image's)."""
    from pathlib import Path
    from orchestrator.plan import IMAGE_NUMPY_VERSION
    rebuild = Path(__file__).resolve().parents[2]
    req = rebuild / "infra" / "images" / "orchestrator" / "requirements.in"
    lock = rebuild / "integration" / "plan-tool.lock"
    assert _pin(req, "numpy") == _pin(lock, "numpy") == IMAGE_NUMPY_VERSION
    assert _pin(req, "PyYAML") == _pin(lock, "PyYAML")
    assert _pin(rebuild / "infra" / "images" / "orchestrator" / "requirements.lock", "numpy") == IMAGE_NUMPY_VERSION


def test_plan_command_refuses_any_numpy_but_the_images(tmp_path, monkeypatch, capsys):
    from orchestrator.cli import main
    from orchestrator.plan import IMAGE_NUMPY_VERSION
    monkeypatch.setattr(np, "__version__", "9.9.9")
    assert main(["plan", "--workload", "ecommerce", "--index", "0", "--out", str(tmp_path / "x")]) == 2
    assert "plan_tool.sh" in capsys.readouterr().err and not (tmp_path / "x").exists()
    assert main(["verify-plan", "--workload", "ecommerce", "--index", "0", "--plan", "p", "--sha", "0" * 64]) == 2
    monkeypatch.setattr(np, "__version__", IMAGE_NUMPY_VERSION)
    assert main(["plan", "--workload", "ecommerce", "--index", "0", "--out", str(tmp_path / "y")]) == 0
    plan = json.loads((tmp_path / "y" / "plan.json").read_bytes())
    assert plan["numpy_version"] == IMAGE_NUMPY_VERSION
