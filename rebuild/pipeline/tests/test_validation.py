"""Contract checks of section 11. Each test breaks one rule in a copy of the full campaign."""
from __future__ import annotations

import json

import pytest

from conftest import attempt_by, edit_json, edit_manifest, resync_verification_hash
from streamprep.build import build_campaign
from streamprep.config import load_config
from streamprep.contract import BuildError, ContractError, canonical_json_bytes
from streamprep.loader import load_campaign
from streamprep.synthetic import rehash_campaign


def _load(path):
    return load_campaign(path, load_config())


def test_full_campaign_is_valid(full):
    c = _load(full.campaign)
    assert c.campaign_id == "ec-a" and len(c.attempts) == 85


def test_unknown_manifest_field_is_an_error(mutable):
    edit_manifest(mutable, lambda m: m.update({"extra_field": 1}))
    with pytest.raises(ContractError, match="unknown field 'extra_field'"):
        _load(mutable)


def test_missing_attempt_field_is_an_error(mutable):
    edit_manifest(mutable, lambda m: m["attempts"][0].pop("onset_source"))
    with pytest.raises(ContractError, match="missing field 'onset_source'"):
        _load(mutable)


def test_checksum_mismatch_is_an_error(mutable):
    key = json.loads((mutable / "manifest" / "manifest.json").read_text())["attempts"][0]["attempt_key"]
    p = mutable / "attempts" / key / "probes.parquet"
    p.write_bytes(p.read_bytes() + b"x")
    with pytest.raises(ContractError, match="sha256 mismatch"):
        _load(mutable)


def test_unlisted_local_file_is_an_error(mutable):
    (mutable / "export" / "stray.parquet").write_bytes(b"x")
    with pytest.raises(ContractError, match="not listed"):
        _load(mutable)


def test_abandoned_campaign_is_refused(mutable):
    p = mutable / "COMPLETE"
    c = json.loads(p.read_text())
    c["status"] = "ABANDONED"
    p.write_bytes(canonical_json_bytes(c))
    with pytest.raises(ContractError, match="never analysed"):
        _load(mutable)


def test_plan_must_be_canonical(mutable):
    p = mutable / "plan" / "plan.json"
    p.write_text(json.dumps(json.loads(p.read_text()), indent=2))
    rehash_campaign(mutable)
    with pytest.raises(ContractError):
        _load(mutable)


def test_plan_draws_must_regenerate(mutable):
    p = mutable / "plan" / "plan.json"
    plan = json.loads(p.read_text())
    plan["blocks"][0]["cells"][0]["settle_gap_s"] = (plan["blocks"][0]["cells"][0]["settle_gap_s"] + 1) % 300
    raw = canonical_json_bytes(plan)
    p.write_bytes(raw)
    from streamprep.contract import sha256_bytes
    (mutable / "plan" / "plan.json.sha256").write_text(sha256_bytes(raw) + "\n")
    edit_manifest(mutable, lambda m: m.update({"plan_sha256": sha256_bytes(raw)}))
    with pytest.raises(ContractError, match="regenerate|SETTLE"):
        _load(mutable)


def test_duplicate_run_id_is_an_error(mutable):
    def dup(m):
        m["attempts"][1]["run_id"] = m["attempts"][0]["run_id"]
    edit_manifest(mutable, dup)
    with pytest.raises(ContractError, match="run_id .* appears twice"):
        _load(mutable)


def test_attempt_key_must_name_this_campaign(mutable):
    def wrong(m):
        a = m["attempts"][0]
        a["attempt_key"] = a["attempt_key"].replace("-c1-", "-c2-")
    edit_manifest(mutable, wrong)
    with pytest.raises(ContractError):
        _load(mutable)


def test_repeat_uses_the_reserve_settle_gap(mutable):
    def shift(m):
        a = attempt_by(m, lambda x: x["attempt"] == 2)
        a["segments"][0]["end_at"] = round(a["segments"][0]["end_at"] - 1.0, 3)
        a["segments"][1]["start_at"] = a["segments"][0]["end_at"]
    edit_manifest(mutable, shift)
    with pytest.raises(ContractError, match="SETTLE lasts"):
        _load(mutable)


def test_repeats_consume_reserve_gaps_in_order(full):
    plan = json.loads((full.campaign / "plan" / "plan.json").read_text())
    reserve = iter(plan["reserve_settle_gap_s"])
    for a in full.manifest["attempts"]:
        if a["attempt"] > 1:
            s = a["segments"][0]
            extra = 600 if "AFTER_REDEPLOY" in a["flags"] else 0
            assert round(s["end_at"] - s["start_at"], 3) == next(reserve) + extra


def test_two_valid_attempts_for_one_cell_is_an_error(mutable):
    def two(m):
        a = attempt_by(m, lambda x: x["outcome"] == "GATE_FAILED" and x["applied"])
        a["outcome"] = "VALID"
        a["failed_checks"] = []
    edit_manifest(mutable, two)
    with pytest.raises(ContractError):
        _load(mutable)


def test_valid_outcome_with_failing_scrape_record_is_an_error(mutable):
    """Decision 21 (C22): the gate's own scrape rows show an untargeted target missing 9 scrapes, yet the attempt is VALID."""
    m = json.loads((mutable / "manifest" / "manifest.json").read_text())
    key = attempt_by(m, lambda x: x["run_type"] == "F4" and x["outcome"] == "VALID" and x["attempt"] == 1)["attempt_key"]

    def miss(rec):
        r = [x for x in rec["scrapes"] if not x["targeted"] and x["segment"] == "PRE_FAULT"][0]
        r["missed_count"] = 9
    edit_json(mutable, f"attempts/{key}/verification.json", miss, rehash=False)
    resync_verification_hash(mutable, key)
    with pytest.raises(ContractError, match="decision 21"):
        _load(mutable)


def test_series_gap_flag_must_match_the_presence_rows(mutable):
    """Series presence is a value and the SERIES_GAP flag, never a verdict (C22). A presence gap without the flag is an
    error; the attempt itself stays valid."""
    m = json.loads((mutable / "manifest" / "manifest.json").read_text())
    key = attempt_by(m, lambda x: x["run_type"] == "F4" and x["outcome"] == "VALID" and x["attempt"] == 1)["attempt_key"]

    def low(rec):
        t = [x for x in rec["telemetry"] if not x["targeted"] and x["segment"] == "INJECTION"][0]
        t["present_count"] = int(t["expected_count"] * 0.9)
        t["coverage_frac"] = t["present_count"] / t["expected_count"]
    edit_json(mutable, f"attempts/{key}/verification.json", low, rehash=False)
    resync_verification_hash(mutable, key)
    with pytest.raises(ContractError, match="SERIES_GAP"):
        _load(mutable)
    edit_manifest(mutable, lambda mm: attempt_by(mm, lambda x: x["attempt_key"] == key)["flags"].append("SERIES_GAP"))
    _load(mutable)


def test_telemetry_failed_outcome_needs_failing_telemetry(mutable):
    m = json.loads((mutable / "manifest" / "manifest.json").read_text())
    key = attempt_by(m, lambda x: x["outcome"] == "TELEMETRY_FAILED")["attempt_key"]

    def fix(rec):
        for r in rec["scrapes"]:
            r["missed_count"] = 0
    edit_json(mutable, f"attempts/{key}/verification.json", fix, rehash=False)
    resync_verification_hash(mutable, key)
    with pytest.raises(ContractError, match="result.telemetry|TELEMETRY_FAILED"):
        _load(mutable)


def test_valid_attempt_with_untargeted_scrape_misses_in_the_export_fails(mutable, tmp_path):
    """The gate record says every target was scraped, but the exported verification file has broker 2's `up` at 0 for
    60 s of the injection (an untargeted target for F4). The independent recomputation stops the build (decision 21)."""
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq
    from streamprep.contract import sha256_file
    m = json.loads((mutable / "manifest" / "manifest.json").read_text())
    a = attempt_by(m, lambda x: x["run_type"] == "F4" and x["outcome"] == "VALID" and x["attempt"] == 1)
    inj = [s for s in a["segments"] if s["segment_type"] == "INJECTION"][0]
    path = mutable / "export" / "attempts" / a["attempt_key"] / "verification.parquet"
    t = pq.read_table(path)
    lo, hi = int(inj["start_at"]) + 60, int(inj["start_at"]) + 120
    ts = t.column("ts").to_numpy()
    pod = np.asarray(t.column("pod").to_pylist(), dtype=object)
    met = np.asarray(t.column("metric").to_pylist(), dtype=object)
    hit = (pod == "streaming-kafka-brokers-2") & (met == "up") & (ts >= lo) & (ts < hi)
    t = t.set_column(t.schema.get_field_index("value"), "value", pa.array(np.where(hit, 0.0, t.column("value").to_numpy())))
    pq.write_table(t, path, compression="zstd")
    sha = sha256_file(path)
    edit_manifest(mutable, lambda mm: attempt_by(mm, lambda x: x["attempt_key"] == a["attempt_key"])["export"]
                  .update({"verification_metrics_sha256": sha}))
    with pytest.raises(BuildError, match="telemetry rule"):
        build_campaign(mutable, tmp_path / "out", load_config(), stages=("features",))


def test_thresholds_without_the_gate_rule_parameters_are_refused(mutable):
    """The verification-channel rules take their numbers from the registered thresholds (C22, C26), so a thresholds file
    without them is refused."""
    from streamprep.contract import sha256_file
    edit_json(mutable, "run/thresholds.json", lambda th: th["gate"].pop("telemetry"), rehash=False)
    th_sha = sha256_file(mutable / "run" / "thresholds.json")
    for rec in mutable.glob("attempts/*/verification.json"):
        edit_json(mutable, f"attempts/{rec.parent.name}/verification.json", lambda r: r.update(thresholds_sha256=th_sha),
                  rehash=False)

    def fix(m):
        m["gate"]["thresholds_sha256"] = th_sha
        for a in m["attempts"]:
            a["verification_sha256"] = sha256_file(mutable / "attempts" / a["attempt_key"] / "verification.json")
    edit_manifest(mutable, fix)
    with pytest.raises(ContractError, match="gate.telemetry"):
        _load(mutable)


def test_valid_sham_with_a_passing_signature_is_an_error(mutable):
    """2.4 with decision 22 (C23): a sham whose signature passes fails the gate, so it can never be valid."""
    m = json.loads((mutable / "manifest" / "manifest.json").read_text())
    key = attempt_by(m, lambda x: x["run_type"] == "F0" and x["outcome"] == "VALID")["attempt_key"]

    def fp(rec):
        next(x for x in rec["signatures_all_faults"] if x["run_type"] == "F7")["pass"] = True
    edit_json(mutable, f"attempts/{key}/verification.json", fp, rehash=False)
    resync_verification_hash(mutable, key)
    with pytest.raises(ContractError, match="valid sham"):
        _load(mutable)


def test_signature_false_pass_is_never_a_manifest_flag(mutable):
    edit_manifest(mutable, lambda m: attempt_by(m, lambda x: x["run_type"] == "F7" and x["outcome"] == "VALID")["flags"]
                  .append("SIGNATURE_FALSE_PASS"))
    with pytest.raises(ContractError, match="SIGNATURE_FALSE_PASS"):
        _load(mutable)


def test_health_event_flags_follow_the_event_interval(mutable):
    """C30: an event overlaps an attempt when [start_at, end_at) meets the attempt's span; end_at null means still open."""
    def widen(m):
        ev = m["health_events"][0]
        ev["end_at"] = None  # still open: it overlaps every later attempt, which carry no flag
    edit_manifest(mutable, widen)
    with pytest.raises(ContractError, match="no HEALTH_EVENT flag"):
        _load(mutable)


def test_health_record_must_have_been_read(mutable):
    edit_manifest(mutable, lambda m: m.update(health_events_polled_until=None))
    with pytest.raises(ContractError, match="health_events_polled_until"):
        _load(mutable)


def test_after_resubmit_adds_the_extra_settle(mutable):
    """C28: an attempt flagged AFTER_RESUBMIT has 600 s more settle, like AFTER_REDEPLOY."""
    edit_manifest(mutable, lambda m: attempt_by(m, lambda x: x["outcome"] == "VALID" and x["attempt"] == 1
                                                and not x["flags"])["flags"].append("AFTER_RESUBMIT"))
    with pytest.raises(ContractError, match="SETTLE lasts"):
        _load(mutable)


def test_stuck_resource_into_next_attempt_is_an_error(mutable):
    def stuck(m):
        atts = m["attempts"]
        for i, a in enumerate(atts):
            if a["outcome"] == "VALID" and a["resources"] and a["run_type"] == "F4":
                r = a["resources"][0]
                r["not_found_at"] = round(atts[i + 1]["started_at"] + 5.0, 3)
                return
    edit_manifest(mutable, stuck)
    with pytest.raises(ContractError, match="still present"):
        _load(mutable)


def test_sham_with_resource_is_an_error(mutable):
    def bad(m):
        a = attempt_by(m, lambda x: x["run_type"] == "F0")
        a["resources"] = [{"kind": "PodChaos", "name": "pk-x", "created_at": a["planned_onset_at"],
                           "delete_called_at": a["planned_onset_at"], "not_found_at": a["planned_onset_at"]}]
    edit_manifest(mutable, bad)
    with pytest.raises(ContractError, match="sham"):
        _load(mutable)


def test_overrun_flag_must_match_the_span(mutable):
    edit_manifest(mutable, lambda m: attempt_by(m, lambda x: "OVERRUN" in x["flags"])["flags"].remove("OVERRUN"))
    with pytest.raises(ContractError, match="OVERRUN"):
        _load(mutable)


def test_segment_gap_inside_an_attempt_is_an_error(mutable):
    def gap(m):
        a = m["attempts"][3]
        a["segments"][1]["start_at"] = round(a["segments"][1]["start_at"] + 1.0, 3)
    edit_manifest(mutable, gap)
    with pytest.raises(ContractError, match="gap or overlap|PRE_FAULT"):
        _load(mutable)


def test_sub_millisecond_instant_is_an_error(mutable):
    def sub(m):
        m["attempts"][0]["planned_onset_at"] = m["attempts"][0]["planned_onset_at"] + 0.0004
    edit_manifest(mutable, sub)
    with pytest.raises(ContractError, match="millisecond"):
        _load(mutable)


def test_node_replacement_is_an_error(mutable):
    edit_manifest(mutable, lambda m: m["node_inventory"][1]["instance_ids"].__setitem__(0, "i-0ffffffffffffffff"))
    with pytest.raises(ContractError, match="node"):
        _load(mutable)


def test_attempt_keys_and_window_ids_unique_across_campaigns(full, iot2, pilot):
    keys, runs, wids = set(), set(), set()
    for c in (full, iot2, pilot):
        k = {a["attempt_key"] for a in c.manifest["attempts"]}
        r = {a["run_id"] for a in c.manifest["attempts"]}
        w = set(c.windows[30]["window_id"])
        assert not (k & keys) and not (r & runs) and not (w & wids)
        keys |= k
        runs |= r
        wids |= w
    assert all(key.split("-")[1] in ("c0", "c1", "c2") for key in keys)


def test_object_outside_the_11_7_layout_is_an_error(mutable):
    (mutable / "run" / "notes.txt").write_text("x")
    rehash_campaign(mutable)
    with pytest.raises(ContractError, match="11.7 layout"):
        _load(mutable)


def test_resource_names_follow_11_2(mutable):
    def bad(m):
        a = attempt_by(m, lambda x: x["run_type"] == "F4" and x["outcome"] == "VALID")
        a["resources"][0]["name"] = "stress-for-cpu"
    edit_manifest(mutable, bad)
    with pytest.raises(ContractError, match="resource name"):
        _load(mutable)


def test_valid_attempt_must_end_with_the_expected_jobs_running(mutable):
    def bad(m):
        a = attempt_by(m, lambda x: x["outcome"] == "VALID")
        a["flink_jobs_at_end"][0]["state"] = "RESTARTING"
    edit_manifest(mutable, bad)
    with pytest.raises(ContractError, match="expected jobs RUNNING"):
        _load(mutable)


def test_injector_log_only_for_f6(mutable):
    m = json.loads((mutable / "manifest" / "manifest.json").read_text())
    key = attempt_by(m, lambda x: x["run_type"] == "F4")["attempt_key"]
    (mutable / "attempts" / key / "injector.log").write_text("x")
    rehash_campaign(mutable)
    with pytest.raises(ContractError, match="F6 attempts only"):
        _load(mutable)


# ---- section 11 clarifications agreed at integration (rebuild/CONTRACT_CHANGES.md) ----------------------------------

def test_manifest_volume_state_is_never_part_of_the_record(mutable):
    """C1: only manifest/manifest.json is an object of 11.7 under manifest/. The orchestrator's resume state
    (facts.json, plan copy, pending.json, campaign.log) stays on the volume and must never be synced or checksummed."""
    for rel in ("manifest/state/attempts/ecommerce-c1-b01-p1-a1/facts.json", "manifest/attempts/x/facts.json",
                "manifest/plan.json", "manifest/campaign.log"):
        p = mutable / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("{}")
    rehash_campaign(mutable)
    with pytest.raises(ContractError, match="11.7 layout") as e:
        _load(mutable)
    assert str(e.value).count("is not an object of the 11.7 layout") == 4


def test_recovery_gate_start_may_be_null_only_for_aborted_attempts(mutable):
    """C3: recovery_gate.started_at is null only when the attempt ended before its GATE segment (ABORTED)."""
    def null_on_valid(m):
        attempt_by(m, lambda x: x["outcome"] == "VALID")["recovery_gate"]["started_at"] = None
    edit_manifest(mutable, null_on_valid)
    with pytest.raises(ContractError, match="recovery_gate.started_at is null"):
        _load(mutable)


def test_recovery_gate_start_null_is_accepted_for_an_aborted_attempt(mutable):
    def null_on_aborted(m):
        attempt_by(m, lambda x: x["outcome"] == "ABORTED")["recovery_gate"]["started_at"] = None
    edit_manifest(mutable, null_on_aborted)
    _load(mutable)


def test_valid_attempt_with_an_unobserved_value_is_an_error(mutable):
    """C4: a value the orchestrator could not observe is null, and a check that needs it fails as MISSING, so a valid
    attempt can never carry one."""
    m = json.loads((mutable / "manifest" / "manifest.json").read_text())
    key = attempt_by(m, lambda x: x["run_type"] == "F6" and x["outcome"] == "VALID")["attempt_key"]
    edit_json(mutable, f"attempts/{key}/verification.json", lambda rec: rec["f6"].update(first_produce_at=None), rehash=False)
    resync_verification_hash(mutable, key)
    with pytest.raises(ContractError, match="unobserved values"):
        _load(mutable)


def test_unobserved_values_are_accepted_on_a_failed_attempt(mutable):
    m = json.loads((mutable / "manifest" / "manifest.json").read_text())
    key = attempt_by(m, lambda x: x["outcome"] == "GATE_FAILED" and x["run_type"] in ("F1", "F2", "F0"))["attempt_key"]

    def unobserved(rec):
        if rec["controller"] is not None:
            rec["controller"]["node_id_after_injection"] = None
    edit_json(mutable, f"attempts/{key}/verification.json", unobserved, rehash=False)
    resync_verification_hash(mutable, key)
    _load(mutable)


def test_telemetry_verdict_needs_at_least_one_row(mutable):
    """C2 and C22: result.telemetry is true only when no untargeted scrape target misses too many scrapes AND at least one
    scrape row exists."""
    m = json.loads((mutable / "manifest" / "manifest.json").read_text())
    key = attempt_by(m, lambda x: x["outcome"] == "ABORTED")["attempt_key"]

    def vacuous(rec):
        rec["scrapes"] = []
        rec["result"]["telemetry"] = True
    edit_json(mutable, f"attempts/{key}/verification.json", vacuous, rehash=False)
    resync_verification_hash(mutable, key)
    with pytest.raises(ContractError, match="result.telemetry"):
        _load(mutable)


def test_terminal_jobs_in_flink_history_are_not_extra_jobs(mutable):
    """C19: flink_jobs_at_end lists every job Flink reports, failed ones kept in its history too. Only jobs that are not
    terminal count for "no extra jobs" (1.6.4)."""
    def history(m):
        a = attempt_by(m, lambda x: x["outcome"] == "VALID")
        a["flink_jobs_at_end"].append({"name": a["flink_jobs_at_end"][0]["name"], "job_id": "f" * 32, "state": "FAILED"})
    edit_manifest(mutable, history)
    _load(mutable)


def test_an_extra_live_job_is_still_an_error(mutable):
    def extra(m):
        a = attempt_by(m, lambda x: x["outcome"] == "VALID")
        a["flink_jobs_at_end"].append({"name": "Some Other Job", "job_id": "e" * 32, "state": "RUNNING"})
    edit_manifest(mutable, extra)
    with pytest.raises(ContractError, match="expected jobs RUNNING"):
        _load(mutable)
