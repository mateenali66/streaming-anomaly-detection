"""Load and validate one campaign directory against section 11.

The directory mirrors ``campaign/<campaign_id>/`` of the results bucket (11.7). Nothing here
talks to S3. Every check raises ContractError with all the problems found in that phase.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow.parquet as pq

from . import schema as S
from .contract import (
    CHAOS_RUN_TYPES, CHUNK_RE, EXTRA_SETTLE_FLAGS, FIXED_SPAN_RUN_TYPES, FLINK_JOBS, RECOVER_EVENT_RUN_TYPES,
    SHA256_RE, VALID_OUTCOMES, ContractError, Errors, campaign_id_for, canonical_json_bytes,
    cluster_name_for, floor10, ceil10, load_json_bytes, parse_attempt_key, sha256_bytes,
    sha256_file, to_ms,
)
from .health import scrape_verdict
from .plan import cell_index, check_plan, run_type_index

FULL_ORDER = ("SETTLE", "PRE_FAULT", "APPLY", "INJECTION", "RECOVERY", "COOLDOWN", "GATE")
TERMINAL_JOB_STATES = ("FAILED", "CANCELED", "FINISHED")  # Flink's globally terminal job states
NOT_APPLIED_ORDER = ("SETTLE", "PRE_FAULT", "APPLY", "GATE")
TOL_MS = 1  # instants are compared in integer milliseconds; 1 ms absorbs float rounding


@dataclass
class MetricFile:
    path: Path
    key: str
    kind: str  # "baseline", "attempt" or "trailing"
    span_lo_ms: int  # first allowed ts (ms), inclusive
    span_hi_ms: int  # last allowed ts (ms), inclusive
    order: int  # read order, used to pick the later export when duplicates are allowed
    attempt_key: str | None = None


@dataclass
class Campaign:
    root: Path
    campaign_id: str
    workload: str
    campaign_index: int
    plan: dict
    manifest: dict
    complete: dict
    baseline_stats: dict
    verifications: dict[str, dict]
    checksums: dict[str, str]
    feature_files: list[MetricFile]
    verification_files: list[MetricFile]
    probe_files: list[Path]
    input_hashes: dict[str, str]
    thresholds: dict = field(default_factory=dict)  # run/thresholds.json, the gate's registered rule parameters
    warnings: list[str] = field(default_factory=list)

    @property
    def attempts(self) -> list[dict]:
        return self.manifest["attempts"]

    def attempt(self, key: str) -> dict:
        for a in self.manifest["attempts"]:
            if a["attempt_key"] == key:
                return a
        raise KeyError(key)


def _read(path: Path, what: str) -> bytes:
    if not path.is_file():
        raise ContractError(f"{what}: missing file {path}")
    return path.read_bytes()


def _resolve_key(key: str, campaign_id: str, what: str, errs: Errors) -> str:
    """Keys are relative to the campaign prefix. A key that carries the prefix is accepted too,
    because 11.4 to 11.6 never say which form the manifest stores (README, A7)."""
    prefix = f"campaign/{campaign_id}/"
    rel = key[len(prefix):] if key.startswith(prefix) else key
    if rel.startswith("/") or ".." in rel.split("/") or rel == "":
        errs.add(f"{what}: key {key!r} is not a relative object key")
    return rel


# ----------------------------------------------------------------------------------------------
# Phase 1: completeness marker and checksums
# ----------------------------------------------------------------------------------------------

def _list_local_files(root: Path, ignore: set[str]) -> set[str]:
    out = set()
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            if name in ignore:
                continue
            rel = Path(dirpath, name).relative_to(root).as_posix()
            out.add(rel)
    return out


_KEY = r"(ecommerce|iot)-c[0-5]-b(0[1-9]|10)-p[1-8]-a[1-9]"
LAYOUT = [re.compile(p) for p in (
    r"^plan/plan\.json$", r"^plan/plan\.json\.sha256$", r"^run/versions\.json$", r"^run/images\.lock\.yaml$",
    r"^run/feature_allowlist\.yaml$", r"^run/thresholds\.json$", r"^manifest/manifest\.json$",
    r"^stats/baseline_stats\.json$", r"^baseline/(features|verification)/chunk-\d+\.parquet$",
    rf"^export/attempts/{_KEY}/(features|verification)\.parquet$",
    r"^export/trailing/(features|verification)/chunk-\d+\.parquet$", r"^probes/(baseline|trailing)/chunk-\d+\.parquet$",
    rf"^attempts/{_KEY}/(probes\.parquet|verification\.json|injector\.log)$",
    rf"^attempts/{_KEY}/chaos/(APPLY_PLUS_10S|MID|BEFORE_DELETE|AFTER_DELETE)-[^/]+\.yaml$",
    r"^logs/orchestrator-\d+\.log\.gz$",
)]


def _check_complete_and_checksums(root: Path, cfg: dict) -> tuple[dict, dict[str, str], dict[str, int]]:
    errs = Errors()
    raw = _read(root / "COMPLETE", "COMPLETE")
    complete = load_json_bytes(raw, "COMPLETE")
    S.validate(S.COMPLETE, complete, "COMPLETE").raise_if_any()
    if raw != canonical_json_bytes(complete):
        errs.add("COMPLETE: bytes are not canonical JSON (11.10)")
    if complete["status"] != "COMPLETE":
        raise ContractError(f"COMPLETE.status is {complete['status']}. Abandoned campaigns are never analysed (2.6.8)")
    for chk in complete["checks"]:
        if not chk["pass"]:
            errs.add(f"COMPLETE check {chk['name']!r} did not pass")
    if complete["written_at"] < complete["newest_object_at"]:
        errs.add("COMPLETE.written_at is earlier than newest_object_at (11.10)")

    sums_path = root / "checksums" / "SHA256SUMS"
    sums_raw = _read(sums_path, "checksums/SHA256SUMS")
    if sha256_bytes(sums_raw) != complete["checksums_sha256"]:
        errs.add("COMPLETE.checksums_sha256 does not match checksums/SHA256SUMS")
    line_re = re.compile(r"^([0-9a-f]{64})  (\S.*)$")
    listed: dict[str, str] = {}
    keys_in_order: list[str] = []
    text = sums_raw.decode("utf-8")
    if text and not text.endswith("\n"):
        errs.add("checksums/SHA256SUMS: last line has no newline")
    for i, line in enumerate(text.splitlines()):
        m = line_re.match(line)
        if not m:
            errs.add(f"checksums/SHA256SUMS line {i + 1}: not '<sha256>  <key>'")
            continue
        key = m.group(2)
        if key in listed:
            errs.add(f"checksums/SHA256SUMS: key {key!r} listed twice")
        listed[key] = m.group(1)
        keys_in_order.append(key)
    if keys_in_order != sorted(keys_in_order):
        errs.add("checksums/SHA256SUMS: lines are not sorted by key (11.10)")
    if "COMPLETE" in listed:
        errs.add("checksums/SHA256SUMS lists COMPLETE, which 11.10 excludes")
    # A file cannot list its own hash, so SHA256SUMS is read as excluding itself (README, A6).
    if "checksums/SHA256SUMS" in listed:
        errs.add("checksums/SHA256SUMS lists itself, which no hash can satisfy")
    for key in listed:
        if not any(p.match(key) for p in LAYOUT):
            errs.add(f"{key!r} is not an object of the 11.7 layout")
    ignore = set(cfg["checksums"]["ignore_local_files"])
    local = _list_local_files(root, ignore) - {"COMPLETE", "checksums/SHA256SUMS"}
    for extra in sorted(local - set(listed)):
        errs.add(f"local file {extra!r} is not listed in checksums/SHA256SUMS")
    sizes: dict[str, int] = {}
    for key in sorted(set(listed) - local):
        errs.add(f"checksums/SHA256SUMS lists {key!r}, which is missing")
    for key in sorted(set(listed) & local):
        p = root / key
        sizes[key] = p.stat().st_size
        if sha256_file(p) != listed[key]:
            errs.add(f"sha256 mismatch for {key!r}")
    if complete["object_count"] != len(listed):
        errs.add(f"COMPLETE.object_count {complete['object_count']} != {len(listed)} listed objects")
    if complete["total_bytes"] != sum(sizes.values()):
        errs.add(f"COMPLETE.total_bytes {complete['total_bytes']} != {sum(sizes.values())}")
    for key, field_name in (("plan/plan.json", "plan_sha256"), ("manifest/manifest.json", "manifest_sha256")):
        if key not in listed:
            errs.add(f"{key} is not listed in checksums/SHA256SUMS")
        elif listed[key] != complete[field_name]:
            errs.add(f"COMPLETE.{field_name} does not match {key}")
    errs.raise_if_any()
    return complete, listed, sizes


# ----------------------------------------------------------------------------------------------
# Phase 2: plan, manifest, run files, baseline statistics
# ----------------------------------------------------------------------------------------------

def _check_plan_files(root: Path, warnings: list[str]) -> tuple[dict, str]:
    errs = Errors()
    raw = _read(root / "plan" / "plan.json", "plan/plan.json")
    plan = load_json_bytes(raw, "plan/plan.json")
    S.validate(S.PLAN, plan, "plan").raise_if_any()
    sha = sha256_bytes(raw)
    sha_file = _read(root / "plan" / "plan.json.sha256", "plan/plan.json.sha256")
    if sha_file != (sha + "\n").encode("ascii"):
        errs.add("plan/plan.json.sha256 is not the 64-character hash of plan.json plus a newline")
    check_plan(plan, raw, errs, warnings)
    errs.raise_if_any()
    return plan, sha


def _check_manifest(root: Path, plan: dict, plan_sha: str, complete: dict, listed: dict[str, str]) -> dict:
    errs = Errors()
    raw = _read(root / "manifest" / "manifest.json", "manifest/manifest.json")
    manifest = load_json_bytes(raw, "manifest/manifest.json")
    S.validate(S.MANIFEST, manifest, "manifest").raise_if_any()
    wl, idx, cid = manifest["workload"], manifest["campaign_index"], manifest["campaign_id"]
    if (wl, idx, cid) != (plan["workload"], plan["campaign_index"], plan["campaign_id"]):
        errs.add("manifest workload, campaign_index or campaign_id differ from the plan")
    if cid != complete["campaign_id"]:
        errs.add("manifest.campaign_id differs from COMPLETE.campaign_id")
    try:
        if cid != campaign_id_for(wl, idx):
            errs.add(f"manifest.campaign_id {cid!r} does not follow 11.2 for {wl} index {idx}")
    except ContractError as exc:
        errs.add(str(exc))
    if manifest["cluster_name"] != cluster_name_for(cid):
        errs.add(f"manifest.cluster_name {manifest['cluster_name']!r} is not {cluster_name_for(cid)!r}")
    if manifest["plan_sha256"] != plan_sha:
        errs.add("manifest.plan_sha256 does not match plan/plan.json")
    if manifest["seed"] != plan["seed"]:
        errs.add("manifest.seed differs from plan.seed")
    if sorted(manifest["flink_jobs_expected"]) != sorted(FLINK_JOBS[wl]):
        errs.add(f"manifest.flink_jobs_expected {manifest['flink_jobs_expected']} are not the {wl} jobs of 11.11")
    if manifest["status"] != "COMPLETE":
        errs.add(f"manifest.status is {manifest['status']}, expected COMPLETE")
    if abs(manifest["clock_skew_s"]) >= 1.0:
        errs.add(f"manifest.clock_skew_s is {manifest['clock_skew_s']}, the health gate requires under 1 s (1.6)")
    if manifest["timeline_end_at"] is None:
        errs.add("manifest.timeline_end_at is null in a COMPLETE campaign")
    for key, field_name in (("run/images.lock.yaml", "images_lock_sha256"),
                            ("run/feature_allowlist.yaml", "feature_allowlist_sha256")):
        if key not in listed:
            errs.add(f"{key} missing from the campaign")
        elif listed[key] != manifest[field_name]:
            errs.add(f"manifest.{field_name} does not match {key}")
    # Read as the hash of run/thresholds.json, the thresholds as registered (README, A8).
    if "run/thresholds.json" not in listed:
        errs.add("run/thresholds.json missing from the campaign")
    elif listed["run/thresholds.json"] != manifest["gate"]["thresholds_sha256"]:
        errs.add("manifest.gate.thresholds_sha256 does not match run/thresholds.json")
    for key in ("run/versions.json",):
        if key not in listed:
            errs.add(f"{key} missing from the campaign")
    errs.raise_if_any()
    return manifest


def _check_baseline_stats(root: Path, manifest: dict, listed: dict[str, str]) -> dict:
    errs = Errors()
    key = manifest["baseline_stats_key"]
    sha = manifest["baseline_stats_sha256"]
    if key is None or sha is None:
        raise ContractError("manifest.baseline_stats_key/sha256 are null in a COMPLETE campaign")
    rel = _resolve_key(key, manifest["campaign_id"], "baseline_stats_key", errs)
    if rel != "stats/baseline_stats.json":
        errs.add(f"baseline_stats_key {key!r} is not stats/baseline_stats.json (11.7)")
    errs.raise_if_any()
    if listed.get(rel) != sha:
        errs.add("manifest.baseline_stats_sha256 does not match stats/baseline_stats.json")
    stats = load_json_bytes(_read(root / rel, rel), rel)
    S.validate(S.BASELINE_STATS, stats, "baseline_stats").raise_if_any()
    if stats["campaign_id"] != manifest["campaign_id"]:
        errs.add("baseline_stats.campaign_id differs from the manifest")
    base = [s for s in manifest["campaign_segments"] if s["segment_type"] == "BASELINE"]
    if base:
        lo = min(s["start_at"] for s in base)
        hi = max(s["end_at"] for s in base)
        if abs(stats["window"]["start_at"] - lo) > 0.001 or abs(stats["window"]["end_at"] - hi) > 0.001:
            errs.add("baseline_stats.window does not match the BASELINE segment")
    exp_rate = 0.05 * stats["raw_events_in_per_s"]["mean"]
    if abs(stats["f6_rate_per_s"] - exp_rate) > 1e-6 * max(1.0, abs(exp_rate)):
        errs.add("baseline_stats.f6_rate_per_s is not 5% of raw_events_in_per_s.mean (11.12)")
    if stats["f6_planned_count"] != round(stats["f6_rate_per_s"] * 180):
        errs.add("baseline_stats.f6_planned_count is not round(f6_rate_per_s * 180) (11.12)")
    errs.raise_if_any()
    return stats


# ----------------------------------------------------------------------------------------------
# Phase 3: attempts, segments, outcomes
# ----------------------------------------------------------------------------------------------

def _seg_ms(segs: list[dict], what: str, errs: Errors) -> list[tuple[str, int, int]]:
    out = []
    for i, s in enumerate(segs):
        a = to_ms(s["start_at"], f"{what}.segments[{i}].start_at", errs)
        b = to_ms(s["end_at"], f"{what}.segments[{i}].end_at", errs)
        if b < a:
            errs.add(f"{what}: segment {s['segment_type']} ends before it starts")
        out.append((s["segment_type"], a, b))
    return out


def _near(a_ms: int, b_ms: int) -> bool:
    return abs(a_ms - b_ms) <= TOL_MS


def _check_attempt_segments(att: dict, phase: dict, settle_expected_s: int | None, cfg: dict,
                            errs: Errors) -> None:
    key = att["attempt_key"]
    segs = _seg_ms(att["segments"], key, errs)
    if not segs:
        errs.add(f"{key}: no segments")
        return
    types = tuple(t for t, _, _ in segs)
    aborted = att["outcome"] == "ABORTED"
    applied_or_sham = att["applied"] or att["run_type"] == "F0"
    if aborted:
        if not (types == FULL_ORDER[:len(types)] or types == NOT_APPLIED_ORDER[:len(types)]):
            errs.add(f"{key}: ABORTED segment sequence {types} is not a prefix of 11.5")
    elif applied_or_sham:
        if types != FULL_ORDER:
            errs.add(f"{key}: segments {types} are not {FULL_ORDER} (applied or sham)")
    else:
        if types != NOT_APPLIED_ORDER:
            errs.add(f"{key}: segments {types} are not {NOT_APPLIED_ORDER} (nothing applied)")
    started = to_ms(att["started_at"], f"{key}.started_at", errs)
    ended = to_ms(att["ended_at"], f"{key}.ended_at", errs)
    if segs[0][1] != started:
        errs.add(f"{key}: first segment does not start at started_at")
    if segs[-1][2] != ended:
        errs.add(f"{key}: last segment does not end at ended_at")
    for (t1, _a1, b1), (t2, a2, _b2) in zip(segs, segs[1:]):
        if a2 != b1:
            errs.add(f"{key}: gap or overlap between {t1} and {t2}")
    by = {t: (a, b) for t, a, b in segs}
    last_type = types[-1]

    def complete(t: str) -> bool:  # the running segment of an ABORTED attempt is cut short
        return t in by and not (aborted and t == last_type)

    planned = to_ms(att["planned_onset_at"], f"{key}.planned_onset_at", errs)
    if complete("SETTLE") and settle_expected_s is not None:
        # 1.3 item 1 and C28: 600 s more after a full redeploy or a resubmission
        extra = phase["redeploy_extra_settle"] if any(f in att["flags"] for f in EXTRA_SETTLE_FLAGS) else 0
        if not _near(by["SETTLE"][1] - by["SETTLE"][0], (settle_expected_s + extra) * 1000):
            errs.add(f"{key}: SETTLE lasts {(by['SETTLE'][1] - by['SETTLE'][0]) / 1000} s, "
                     f"expected {settle_expected_s + extra} s from the plan")
    if complete("PRE_FAULT"):
        a, b = by["PRE_FAULT"]
        if not _near(b - a, phase["pre_fault"] * 1000):
            errs.add(f"{key}: PRE_FAULT lasts {(b - a) / 1000} s, expected {phase['pre_fault']} s")
        if not _near(b, planned):
            errs.add(f"{key}: PRE_FAULT does not end at planned_onset_at")
    if "APPLY" in by and not _near(by["APPLY"][0], planned):
        errs.add(f"{key}: APPLY does not start at planned_onset_at")
    onset = att["onset_verified_at"]
    if onset is not None:
        onset_ms = to_ms(onset, f"{key}.onset_verified_at", errs)
        if complete("APPLY") and not _near(by["APPLY"][1], onset_ms):
            errs.add(f"{key}: APPLY does not end at onset_verified_at")
        if "INJECTION" in by and not _near(by["INJECTION"][0], onset_ms):
            errs.add(f"{key}: INJECTION does not start at onset_verified_at")
    if complete("INJECTION"):
        end_src = att["labelled_end_at"] if att["labelled_end_at"] is not None else att["aborted_at"]
        if end_src is None:
            errs.add(f"{key}: INJECTION has neither labelled_end_at nor aborted_at as its end")
        elif not _near(by["INJECTION"][1], to_ms(end_src, f"{key}.injection end", errs)):
            errs.add(f"{key}: INJECTION does not end at labelled_end_at or aborted_at")
    for t, length in (("RECOVERY", phase["recovery"]), ("COOLDOWN", phase["cooldown"])):
        if complete(t) and not _near(by[t][1] - by[t][0], length * 1000):
            errs.add(f"{key}: {t} lasts {(by[t][1] - by[t][0]) / 1000} s, expected {length} s")

    if att["outcome"] in VALID_OUTCOMES:
        _check_valid_attempt_timing(att, by, phase, cfg, errs)


def _check_valid_attempt_timing(att: dict, by: dict, phase: dict, cfg: dict, errs: Errors) -> None:
    key, rt = att["attempt_key"], att["run_type"]
    onset, end = att["onset_verified_at"], att["labelled_end_at"]
    if onset is None or end is None:
        errs.add(f"{key}: valid attempt without onset_verified_at or labelled_end_at")
        return
    onset_ms, end_ms = to_ms(onset, key, errs), to_ms(end, key, errs)
    apply_len_s = (by["APPLY"][1] - by["APPLY"][0]) / 1000
    limit = cfg["f6_apply_max_s"] if rt == "F6" else phase["apply_max"]
    if apply_len_s > limit + 0.001:
        errs.add(f"{key}: valid attempt with APPLY of {apply_len_s} s, above {limit} s")
    dur = att["configured_duration_s"]
    if rt == "F0":
        expect = ("PLANNED", "PLANNED")
        if not _near(onset_ms, to_ms(att["planned_onset_at"], key, errs)):
            errs.add(f"{key}: sham onset is not the planned onset")
        if not _near(end_ms - onset_ms, dur * 1000):
            errs.add(f"{key}: sham span is not the sham length")
    elif rt in FIXED_SPAN_RUN_TYPES:
        expect = ("CHAOS_APPLY_EVENT", "FIXED_SPAN")
        if not _near(end_ms - onset_ms, dur * 1000):
            errs.add(f"{key}: {rt} labelled span is not onset plus {dur} s")
    elif rt == "F6":
        expect = ("FIRST_PRODUCE_ACK", "LAST_PRODUCE_ACK")
        if end_ms <= onset_ms:
            errs.add(f"{key}: F6 last produce ack is not after the first")
    else:
        expect = ("CHAOS_APPLY_EVENT", "CHAOS_RECOVER_EVENT")
    if (att["onset_source"], att["end_source"]) != expect:
        errs.add(f"{key}: onset/end sources {(att['onset_source'], att['end_source'])}, expected {expect}")
    if rt in RECOVER_EVENT_RUN_TYPES:
        overrun = (end_ms - onset_ms) / 1000 - dur
        if att["overrun_s"] is None or abs(att["overrun_s"] - overrun) > 0.002:
            errs.add(f"{key}: overrun_s {att['overrun_s']} does not equal labelled end minus onset minus duration ({overrun:.3f})")
        if overrun > 120 or overrun < -60:
            errs.add(f"{key}: valid attempt with overrun {overrun:.3f} s, which fails timing (2.4)")
        if (overrun > 60) != ("OVERRUN" in att["flags"]):
            errs.add(f"{key}: OVERRUN flag does not match an overrun of {overrun:.3f} s")
    elif att["overrun_s"] is not None:
        errs.add(f"{key}: overrun_s is set for {rt}, which 11.5 limits to F2, F4, F7 and F8")
    if rt == "F0":
        if att["applied"] or att["resources"] or att["apply_called_at"] is not None:
            errs.add(f"{key}: sham with an applied resource or apply call")
    else:
        if not att["applied"] or not att["resources"] or att["apply_called_at"] is None:
            errs.add(f"{key}: valid fault attempt without applied resources and an apply call")
    if rt in ("F0", "F1", "F2") and att["controller_at_onset"] is None:
        errs.add(f"{key}: controller_at_onset is null for {rt}")
    if att["failed_checks"]:
        errs.add(f"{key}: valid attempt with failed_checks {att['failed_checks']}")
    if att["aborted_at"] is not None:
        errs.add(f"{key}: valid attempt with aborted_at set")
    rg = att["recovery_gate"]
    if rg["passed_at"] is None:
        errs.add(f"{key}: valid attempt whose recovery gate never passed")
    performed = rg["remediation"]["performed"]
    if (att["outcome"] == "VALID_REMEDIATED") != performed:
        errs.add(f"{key}: outcome {att['outcome']} but remediation.performed is {performed}")
    if (att["outcome"] == "VALID_REMEDIATED") != ("REMEDIATED" in att["flags"]):
        errs.add(f"{key}: REMEDIATED flag does not match outcome {att['outcome']}")


def _check_attempts(plan: dict, manifest: dict, cfg: dict, errs: Errors) -> None:
    phase = plan["phase_s"]
    cells = cell_index(plan)
    rts = run_type_index(plan)
    attempts = manifest["attempts"]
    wl, idx = manifest["workload"], manifest["campaign_index"]
    seen_keys, seen_runs = set(), set()
    reserve_used = 0
    last_start = None
    last_cell_order = (0, 0)
    per_cell: dict[str, list[dict]] = {}
    for i, att in enumerate(attempts):
        key = att["attempt_key"]
        if key in seen_keys:
            errs.add(f"attempt_key {key} appears twice")
        seen_keys.add(key)
        if att["run_id"] in seen_runs:
            errs.add(f"run_id {att['run_id']} appears twice")
        seen_runs.add(att["run_id"])
        try:
            k_wl, k_idx, k_b, k_p, k_a = parse_attempt_key(key)
        except ContractError as exc:
            errs.add(str(exc))
            continue
        if (k_wl, k_idx) != (wl, idx):
            errs.add(f"{key}: key names workload {k_wl} campaign {k_idx}, not {wl} campaign {idx}")
        if (att["block"], att["position"], att["attempt"]) != (k_b, k_p, k_a):
            errs.add(f"{key}: block, position or attempt differ from the key")
        if att["cell_key"] != key.rsplit("-a", 1)[0]:
            errs.add(f"{key}: cell_key {att['cell_key']} is not the key without its attempt part")
        cell = cells.get(att["cell_key"])
        if cell is None:
            errs.add(f"{key}: cell {att['cell_key']} is not in the plan")
            continue
        if att["run_type"] != cell["run_type"]:
            errs.add(f"{key}: run_type {att['run_type']} but the plan cell holds {cell['run_type']}")
        if att["fault_name"] != rts[att["run_type"]]["name"]:
            errs.add(f"{key}: fault_name {att['fault_name']!r} differs from the plan")
        exp_dur = cell["sham_duration_s"] if att["run_type"] == "F0" else rts[att["run_type"]]["duration_s"]
        if att["configured_duration_s"] != exp_dur:
            errs.add(f"{key}: configured_duration_s {att['configured_duration_s']} != {exp_dur}")
        if att["status"] != "FINISHED" or att["outcome"] is None or att["ended_at"] is None:
            errs.add(f"{key}: not FINISHED with an outcome and ended_at in a COMPLETE campaign")
            continue
        if att["verification_key"] is None or att["verification_sha256"] is None:
            errs.add(f"{key}: FINISHED without verification_key and verification_sha256")
        if att["outcome"] != "ABORTED" and att["recovery_gate"]["started_at"] is None:
            errs.add(f"{key}: recovery_gate.started_at is null, which only an ABORTED attempt may leave (CONTRACT_CHANGES C3)")
        if last_start is not None and att["started_at"] <= last_start:
            errs.add(f"{key}: attempts are not in start order")
        last_start = att["started_at"]
        order = (att["block"], att["position"])
        if order < last_cell_order:
            errs.add(f"{key}: runs out of plan order (after block {last_cell_order[0]} position {last_cell_order[1]})")
        last_cell_order = max(last_cell_order, order)
        if att["attempt"] == 1:
            settle = cell["settle_gap_s"]
        else:
            if reserve_used >= len(plan["reserve_settle_gap_s"]):
                errs.add(f"{key}: more repeats than reserve settle gaps")
                settle = None
            else:
                settle = plan["reserve_settle_gap_s"][reserve_used]
            reserve_used += 1
        per_cell.setdefault(att["cell_key"], []).append(att)
        _check_attempt_segments(att, phase, settle, cfg, errs)
        for r in att["resources"]:
            if len(r["name"]) > 63 or not re.match(rf"^[a-z0-9]+-{re.escape(key)}(-[0-9]+)?$", r["name"]):
                errs.add(f"{key}: resource name {r['name']!r} is not <prefix>-<attempt_key>[-<n>] within 63 characters (11.2)")
        if att["outcome"] in VALID_OUTCOMES:
            # flink_jobs_at_end lists every job the Flink REST API reports, terminal jobs kept in Flink's history too;
            # "no extra jobs" (1.6.4) counts the jobs that are not terminal (CONTRACT_CHANGES C19).
            jobs = [j for j in att["flink_jobs_at_end"] if j["state"] not in TERMINAL_JOB_STATES]
            if sorted(j["name"] for j in jobs) != sorted(manifest["flink_jobs_expected"]) or \
                    any(j["state"] != "RUNNING" for j in jobs):
                errs.add(f"{key}: valid attempt does not end with exactly the expected jobs RUNNING (1.6.4)")
        if att["run_type"] == "F0" and (att["applied"] or att["resources"]):
            errs.add(f"{key}: a sham shows a campaign resource or injector, which halts the campaign (2.6.5)")
        if att["run_type"] != "F0" and att["applied"] != bool(att["resources"]):
            errs.add(f"{key}: applied is {att['applied']} but {len(att['resources'])} resources are recorded")
        if att["export"]["status"] != "DONE":
            errs.add(f"{key}: export status {att['export']['status']}, expected DONE")
    for cell_key, atts in per_cell.items():
        nums = [a["attempt"] for a in atts]
        if nums != list(range(1, len(nums) + 1)):
            errs.add(f"{cell_key}: attempt numbers {nums} are not 1..n in start order")
        valid = [a for a in atts if a["outcome"] in VALID_OUTCOMES]
        if len(valid) > 1:
            errs.add(f"{cell_key}: {len(valid)} valid attempts, at most one is allowed")
        if valid and valid[0] is not atts[-1]:
            errs.add(f"{cell_key}: an attempt follows the valid attempt {valid[0]['attempt_key']}")
        if cfg["require_all_cells_valid"] and not valid:
            errs.add(f"{cell_key}: no valid attempt in a COMPLETE campaign")
    if cfg["require_all_cells_valid"]:
        for cell_key in cells:
            if cell_key not in per_cell:
                errs.add(f"{cell_key}: plan cell has no attempt")
    _check_resources(manifest, errs)


def _check_resources(manifest: dict, errs: Errors) -> None:
    """A resource that outlives its attempt can carry a live fault into later windows (Review 2, item 4)."""
    attempts = manifest["attempts"]
    t_end = manifest["timeline_end_at"]
    for i, att in enumerate(attempts):
        nxt = attempts[i + 1]["started_at"] if i + 1 < len(attempts) else t_end
        for r in att["resources"]:
            what = f"{att['attempt_key']} resource {r['kind']}/{r['name']}"
            if r["created_at"] < att["started_at"]:
                errs.add(f"{what}: created before the attempt started")
            if r["delete_called_at"] is None or r["not_found_at"] is None:
                errs.add(f"{what}: never confirmed NotFound")
                continue
            if r["not_found_at"] < r["created_at"]:
                errs.add(f"{what}: NotFound before creation")
            if nxt is not None and r["not_found_at"] > nxt:
                errs.add(f"{what}: still present when the next attempt or TRAILING began")
            if att["outcome"] != "ABORTED" and r["not_found_at"] > att["ended_at"]:
                errs.add(f"{what}: still present after the attempt ended")
            if att["outcome"] in VALID_OUTCOMES and r["not_found_at"] - r["delete_called_at"] > 60.0:
                errs.add(f"{what}: NotFound more than 60 s after delete in a valid attempt (2.4)")


def _check_campaign_records(manifest: dict, errs: Errors) -> None:
    for h in manifest["halts"]:
        if h["resolution"] != "RESUMED" or h["resumed_at"] is None:
            errs.add(f"halt at {h['at']} is not RESUMED in a COMPLETE campaign")
    inv = manifest["node_inventory"]
    for e in inv:
        if e["instance_ids"] != sorted(e["instance_ids"]):
            errs.add(f"node_inventory at {e['at']}: instance_ids are not sorted")
    if inv and any(set(e["instance_ids"]) != set(inv[0]["instance_ids"]) for e in inv):
        errs.add("node_inventory changes during the campaign; a node replacement halts it (2.6.5)")
    attempts = manifest["attempts"]
    hit = set()
    keys = [ev["event_key"] for ev in manifest["health_events"]]
    if len(set(keys)) != len(keys):
        errs.add("health_events: an event_key appears twice (one entry per AWS Health event, C30)")
    for ev in manifest["health_events"]:
        if ev["end_at"] is not None and ev["end_at"] < ev["start_at"]:
            errs.add(f"health event {ev['event_key']}: end_at before start_at")
        for att in attempts:
            # C30: an event overlaps an attempt when [start_at, end_at) meets [started_at, ended_at); end_at null is open
            if att["ended_at"] is not None and ev["start_at"] < att["ended_at"] and (
                    ev["end_at"] is None or ev["end_at"] > att["started_at"]):
                hit.add(att["attempt_key"])
                if "HEALTH_EVENT" not in att["flags"]:
                    errs.add(f"{att['attempt_key']}: health event {ev['event_key']} overlaps it but no HEALTH_EVENT flag")
    for att in attempts:
        if "HEALTH_EVENT" in att["flags"] and att["attempt_key"] not in hit:
            errs.add(f"{att['attempt_key']}: HEALTH_EVENT flag without an overlapping health event")
    if manifest["health_events_polled_until"] is None:
        errs.add("manifest.health_events_polled_until is null: the AWS Health record was never read (C30)")


# ----------------------------------------------------------------------------------------------
# Phase 4: verification records
# ----------------------------------------------------------------------------------------------

def _check_verifications(root: Path, manifest: dict, listed: dict[str, str], cfg: dict, thresholds: dict,
                         errs: Errors) -> dict[str, dict]:
    out: dict[str, dict] = {}
    cid = manifest["campaign_id"]
    th_tel = thresholds["gate"]["telemetry"]
    miss_max = th_tel["scrape_miss_max"]
    gap_below = th_tel["series_gap_flag_below"]
    for att in manifest["attempts"]:
        key = att["attempt_key"]
        if att["verification_key"] is None:
            continue
        rel = _resolve_key(att["verification_key"], cid, f"{key}.verification_key", errs)
        if rel != f"attempts/{key}/verification.json":
            errs.add(f"{key}: verification_key {att['verification_key']!r} is not attempts/{key}/verification.json")
        if rel not in listed:
            errs.add(f"{key}: verification record {rel} is missing")
            continue
        if listed[rel] != att["verification_sha256"]:
            errs.add(f"{key}: verification_sha256 does not match {rel}")
        rec = load_json_bytes((root / rel).read_bytes(), rel)
        sub = S.validate(S.VERIFICATION, rec, rel)
        if sub:
            errs.items.extend(sub.items)
            continue
        out[key] = rec
        if (rec["attempt_key"], rec["run_id"], rec["run_type"]) != (key, att["run_id"], att["run_type"]):
            errs.add(f"{rel}: attempt_key, run_id or run_type differ from the manifest")
        if rec["mode"] != "ONLINE":
            errs.add(f"{rel}: mode {rec['mode']}, the stored record must be ONLINE")
        if rec["gate_git_commit"] != manifest["gate"]["git_commit"]:
            errs.add(f"{rel}: gate_git_commit differs from the manifest")
        if rec["thresholds_sha256"] != manifest["gate"]["thresholds_sha256"]:
            errs.add(f"{rel}: thresholds_sha256 differs from the manifest")
        rt = att["run_type"]
        if (rec["pseudo_targets"] is not None) != (rt == "F0"):
            errs.add(f"{rel}: pseudo_targets must be set for F0 only")
        valid = att["outcome"] in VALID_OUTCOMES
        for blk, owner in (("f5", "F5"), ("f6", "F6"), ("f8", "F8")):
            if rec[blk] is not None and rt != owner:
                errs.add(f"{rel}: {blk} is set for {rt}")
            if valid and rt == owner and rec[blk] is None:
                errs.add(f"{rel}: valid {rt} attempt without its {blk} block")
        if valid:
            _check_valid_observations(rel, rt, rec, errs)
        for snap in rec["chaos_snapshots"]:
            yrel = _resolve_key(snap["yaml_key"], cid, f"{rel} yaml_key", errs)
            if yrel not in listed:
                errs.add(f"{rel}: chaos snapshot {snap['yaml_key']!r} is not in the campaign")
        # Series presence (3.4.4): recorded values and the SERIES_GAP flag, never a verdict (decision 21, C22).
        series_gap = False
        for t in rec["telemetry"]:
            if t["expected_count"] <= 0:
                errs.add(f"{rel}: telemetry entry {t['source']}/{t['segment']} has expected_count {t['expected_count']}")
                continue
            frac = t["present_count"] / t["expected_count"]
            if abs(frac - t["coverage_frac"]) > 1e-9:
                errs.add(f"{rel}: telemetry {t['source']}/{t['segment']} coverage_frac is not present/expected")
            if (not t["targeted"]) and frac < gap_below:
                series_gap = True
        if series_gap != ("SERIES_GAP" in att["flags"]):
            errs.add(f"{key}: SERIES_GAP flag is {'SERIES_GAP' in att['flags']} but the telemetry rows give {series_gap} (C22)")
        # The telemetry verdict is scrape health (decision 21, C22): no untargeted target misses more than
        # scrape_miss_max scrapes in a segment, and at least one scrape row exists.
        for r in rec["scrapes"]:
            if r["expected_count"] <= 0 or r["missed_count"] > r["expected_count"]:
                errs.add(f"{rel}: scrape row {r['target']}/{r['segment']} has counts {r['missed_count']} of {r['expected_count']}")
        verdict = scrape_verdict(rec["scrapes"], miss_max)
        res = rec["result"]
        if res["telemetry"] != verdict:
            errs.add(f"{rel}: result.telemetry is {res['telemetry']} but the scrape rows give {verdict} (decision 21)")
        layers_ok = res["mechanism"] and res["signature"] and res["envelope"] and res["timing"]
        if res["gate"] != (layers_ok and res["telemetry"]):
            errs.add(f"{rel}: result.gate is not the conjunction of the five layers")
        outcome = att["outcome"]
        if outcome in VALID_OUTCOMES and not res["gate"]:
            errs.add(f"{key}: outcome {outcome} but the verification record fails the gate")
        if outcome in VALID_OUTCOMES and not verdict:
            errs.add(f"{key}: outcome {outcome} but an untargeted scrape target missed more than {miss_max} scrapes")
        if outcome == "TELEMETRY_FAILED" and res["telemetry"]:
            errs.add(f"{key}: TELEMETRY_FAILED but result.telemetry is true")
        if outcome == "GATE_FAILED" and layers_ok:
            errs.add(f"{key}: GATE_FAILED but mechanism, signature, envelope and timing all pass")
        if outcome != "ABORTED":
            failed_names = {c["name"] for c in rec["checks"] if not c["pass"]}
            if set(att["failed_checks"]) != failed_names:
                errs.add(f"{key}: failed_checks {sorted(att['failed_checks'])} differ from the record's failing checks {sorted(failed_names)}")
        if valid and rt == "F1":
            brokers = [t for t in rec["targets_selected"] if t["broker_id"] is not None]
            if len(brokers) != 1:
                errs.add(f"{rel}: valid F1 attempt must select exactly one broker target")
    _check_sham_false_passes(manifest, out, errs)
    return out


def _check_valid_observations(rel: str, rt: str, rec: dict, errs: Errors) -> None:
    """11.6 marks a value null when the orchestrator could not observe it (CONTRACT_CHANGES C4). A valid attempt passed
    every check, and every check that needs a value fails as MISSING without it, so the values its run type relies on
    must all be present."""
    missing: list[str] = []
    if rt in ("F1", "F2") and rec["controller"] is not None:
        missing += [f"controller.{k}" for k in ("node_id_before_apply", "node_id_after_injection") if rec["controller"][k] is None]
    if rt == "F5" and rec["f5"] is not None:
        missing += [f"f5.kills[{i}].apply_at" for i, k in enumerate(rec["f5"]["kills"]) if k["apply_at"] is None]
        if rec["f5"]["new_tm_uid_count"] is None:
            missing.append("f5.new_tm_uid_count")
    if rt == "F6" and rec["f6"] is not None:
        missing += [f"f6.{k}" for k in ("pod_ready_at", "sent_count", "acked_count", "first_produce_at", "last_produce_at",
                                        "consumer_count") if rec["f6"][k] is None]
    if rt == "F8" and rec["f8"] is not None:
        for i, p in enumerate(rec["f8"]["per_pod"]):
            # export_stressor_bytes is recorded only (C36) and may be null on a valid attempt
            missing += [f"f8.per_pod[{i}].{k}" for k, v in p.items() if v is None and k != "export_stressor_bytes"]
            if not p["ws_samples"] or not p["limit_samples"]:
                missing.append(f"f8.per_pod[{i}] sizing samples")
    if missing:
        errs.add(f"{rel}: valid {rt} attempt with unobserved values {missing}")


def _check_sham_false_passes(manifest: dict, recs: dict[str, dict], errs: Errors) -> None:
    """2.4 with decision 22 (C23): a sham whose signature passes fails the gate, so a valid sham has no passing signature.
    The fault-run flag of 2.5 is the analysis's (timeline.py), never a manifest flag, which the schema already refuses."""
    for att in manifest["attempts"]:
        if att["run_type"] != "F0" or att["outcome"] not in VALID_OUTCOMES or att["attempt_key"] not in recs:
            continue
        passed = [s["run_type"] for s in recs[att["attempt_key"]]["signatures_all_faults"] if s["pass"]]
        if passed:
            errs.add(f"{att['attempt_key']}: valid sham on which the {passed} signature passed (2.4, C23)")


# ----------------------------------------------------------------------------------------------
# Phase 5: exported files
# ----------------------------------------------------------------------------------------------

def _expected_chunks(segments: list[dict], errs: Errors) -> list[tuple[int, int, int]]:
    """(name_epoch, chunk_start_ms, chunk_end_ms) per 11.7: 3,600 s or the remainder of the segment."""
    out = []
    for s in segments:
        a = to_ms(s["start_at"], "chunk segment", errs)
        b = to_ms(s["end_at"], "chunk segment", errs)
        c = a
        while c < b:
            e = min(c + 3_600_000, b)
            out.append((c // 1000, c, e))
            c = e
    return out


def _chunk_files(root: Path, prefix: str, expected: list[tuple[int, int, int]], listed: dict[str, str],
                 errs: Errors) -> list[tuple[str, int, int]]:
    present = {}
    for key in listed:
        if key.startswith(prefix + "/") and "/" not in key[len(prefix) + 1:]:
            name = key[len(prefix) + 1:]
            m = CHUNK_RE.match(name)
            if not m:
                errs.add(f"{key}: not a chunk-<start_epoch>.parquet name")
                continue
            present[int(m.group(1))] = key
    names = [e[0] for e in expected]
    if len(set(names)) != len(names):
        errs.add(f"{prefix}: two chunks would share one start epoch")
    if set(present) != set(names):
        missing = sorted(set(names) - set(present))
        extra = sorted(set(present) - set(names))
        errs.add(f"{prefix}: chunk files do not match the segments (missing {missing[:3]}, extra {extra[:3]})")
    return [(present[n], a, b) for n, a, b in expected if n in present]


def _check_exports(root: Path, manifest: dict, listed: dict[str, str], errs: Errors
                   ) -> tuple[list[MetricFile], list[MetricFile], list[Path]]:
    cid = manifest["campaign_id"]
    by_key = {a["attempt_key"]: a for a in manifest["attempts"]}
    for key in listed:
        parts = key.split("/")
        k = None
        if parts[0] == "attempts" and len(parts) >= 3:
            k = parts[1]
        elif parts[:2] == ["export", "attempts"] and len(parts) >= 4:
            k = parts[2]
        if k is not None and k not in by_key:
            errs.add(f"{key}: object of an attempt the manifest does not list")
        if key.endswith("/injector.log"):
            a = by_key.get(key.split("/")[1])
            if a is not None and a["run_type"] != "F6":
                errs.add(f"{key}: injector.log belongs to F6 attempts only (11.7)")
    for a in manifest["attempts"]:
        if a["run_type"] == "F6" and a["applied"] and f"attempts/{a['attempt_key']}/injector.log" not in listed:
            errs.add(f"{a['attempt_key']}: applied F6 attempt without attempts/<key>/injector.log (11.7)")
    feature_files: list[MetricFile] = []
    verif_files: list[MetricFile] = []
    probe_files: list[Path] = []
    segs = manifest["campaign_segments"]
    lead = sorted([s for s in segs if s["segment_type"] in ("WARMUP", "BASELINE")], key=lambda s: s["start_at"])
    trail = sorted([s for s in segs if s["segment_type"] == "TRAILING"], key=lambda s: s["start_at"])
    order = 0
    lead_chunks = _expected_chunks(lead, errs)
    trail_chunks = _expected_chunks(trail, errs)
    for key, a, b in _chunk_files(root, "baseline/features", lead_chunks, listed, errs):
        feature_files.append(MetricFile(root / key, key, "baseline", floor10(a), ceil10(b), order)); order += 1
    for key, a, b in _chunk_files(root, "baseline/verification", lead_chunks, listed, errs):
        verif_files.append(MetricFile(root / key, key, "baseline", floor10(a), ceil10(b), order))
    for key, _a, _b in _chunk_files(root, "probes/baseline", lead_chunks, listed, errs):
        probe_files.append(root / key)
    for att in manifest["attempts"]:
        k = att["attempt_key"]
        lo = floor10(to_ms(att["started_at"], k, errs))
        hi = ceil10(to_ms(att["ended_at"], k, errs)) if att["ended_at"] is not None else lo
        exp = att["export"]
        for fld, sha_fld, want, sink, is_metric in (
            ("features_key", "features_sha256", f"export/attempts/{k}/features.parquet", feature_files, True),
            ("verification_metrics_key", "verification_metrics_sha256", f"export/attempts/{k}/verification.parquet", verif_files, True),
            ("probes_key", "probes_sha256", f"attempts/{k}/probes.parquet", probe_files, False),
        ):
            rel = _resolve_key(exp[fld], cid, f"{k}.export.{fld}", errs)
            if rel != want:
                errs.add(f"{k}: export.{fld} {exp[fld]!r} is not {want} (11.7)")
            if rel not in listed:
                errs.add(f"{k}: {rel} is missing")
                continue
            if listed[rel] != exp[sha_fld]:
                errs.add(f"{k}: export.{sha_fld} does not match {rel}")
            if is_metric:
                sink.append(MetricFile(root / rel, rel, "attempt", lo, hi, order, k))
                order += 1
            else:
                sink.append(root / rel)
    for key, a, b in _chunk_files(root, "export/trailing/features", trail_chunks, listed, errs):
        feature_files.append(MetricFile(root / key, key, "trailing", floor10(a), ceil10(b), order)); order += 1
    for key, a, b in _chunk_files(root, "export/trailing/verification", trail_chunks, listed, errs):
        verif_files.append(MetricFile(root / key, key, "trailing", floor10(a), ceil10(b), order))
    for key, _a, _b in _chunk_files(root, "probes/trailing", trail_chunks, listed, errs):
        probe_files.append(root / key)
    return feature_files, verif_files, probe_files


# ----------------------------------------------------------------------------------------------
# Probe and verification-metric file schemas (11.8). These files never feed a feature.
# ----------------------------------------------------------------------------------------------

METRIC_COLUMNS = {
    "ts": "int64", "series_id": "string", "metric": "string", "metric_type": "string",
    "source": "string", "role": "string", "pod": "string", "pod_uid": "string",
    "broker_id": "int32", "container": "string", "labels": "string", "value": "double",
}
METRIC_NULLABLE = {"pod_uid", "broker_id", "container"}
PROBE_COLUMNS = {
    "ts": "double", "probe": "string", "src_pod": "string", "dst": "string", "tried_count": "int32",
    "ok_count": "int32", "timeout_s": "double", "latency_s": "double", "value": "double",
    "detail": "string", "error": "string",
}
PROBE_NULLABLE = {"src_pod", "dst", "latency_s", "value", "detail", "error"}


def check_parquet_schema(path: Path, columns: dict[str, str], nullable: set[str], what: str,
                         errs: Errors) -> bool:
    try:
        sch = pq.read_schema(path)
    except Exception as exc:  # unreadable parquet
        errs.add(f"{what}: unreadable parquet ({exc})")
        return False
    names = list(sch.names)
    if set(names) != set(columns):
        errs.add(f"{what}: columns {sorted(names)} differ from 11.8 {sorted(columns)}")
        return False
    ok = True
    for name, want in columns.items():
        got = str(sch.field(name).type)
        if want == "string" and got in ("string", "large_string"):
            continue
        if got != want:
            errs.add(f"{what}: column {name} has type {got}, expected {want}")
            ok = False
    if sch.metadata and b"pandas" in sch.metadata:
        errs.add(f"{what}: parquet carries pandas metadata (11.1.7 says no pandas index)")
    return ok


def check_probe_file(path: Path, errs: Errors) -> None:
    import numpy as np
    from .contract import PROBE_KINDS
    what = str(path)
    if not check_parquet_schema(path, PROBE_COLUMNS, PROBE_NULLABLE, what, errs):
        return
    t = pq.read_table(path)
    for name in PROBE_COLUMNS:
        if name not in PROBE_NULLABLE and t.column(name).null_count:
            errs.add(f"{what}: column {name} has nulls")
    kinds = set(t.column("probe").to_pylist())
    if not kinds <= set(PROBE_KINDS):
        errs.add(f"{what}: unknown probe kinds {sorted(kinds - set(PROBE_KINDS))}")
    for name in ("ts", "timeout_s", "latency_s", "value"):
        if np.isnan(np.asarray(t.column(name).drop_null().to_numpy(), dtype=float)).any():
            errs.add(f"{what}: column {name} holds NaN (11.1.7)")
    tried = np.asarray(t.column("tried_count").to_numpy())
    okc = np.asarray(t.column("ok_count").to_numpy())
    probe = np.asarray(t.column("probe").to_pylist(), dtype=object)
    want = np.where(probe == "tcp_burst", 10, 1)
    if (tried != want).any():
        errs.add(f"{what}: tried_count is not 1 (10 for tcp_burst)")
    if ((okc < 0) | (okc > tried)).any():
        errs.add(f"{what}: ok_count outside 0 to tried_count")


def check_verification_metric_file(mf: MetricFile, errs: Errors) -> None:
    """Schema and source check only. The verification channel never becomes a feature (4.1.6)."""
    import numpy as np
    from .contract import VERIFICATION_SOURCES
    what = mf.key
    if not check_parquet_schema(mf.path, METRIC_COLUMNS, METRIC_NULLABLE, what, errs):
        return
    t = pq.read_table(mf.path, columns=["ts", "source", "value"])
    srcs = set(t.column("source").unique().to_pylist())
    if not srcs <= set(VERIFICATION_SOURCES):
        errs.add(f"{what}: sources {sorted(srcs - set(VERIFICATION_SOURCES))} are not verification sources")
    ts = t.column("ts").to_numpy()
    if (ts % 10 != 0).any():
        errs.add(f"{what}: ts not a multiple of 10")
    if len(ts) and (ts.min() * 1000 < mf.span_lo_ms or ts.max() * 1000 > mf.span_hi_ms):
        errs.add(f"{what}: rows outside the export span")
    if np.isnan(t.column("value").to_numpy()).any():
        errs.add(f"{what}: value holds NaN (11.1.7)")


# ----------------------------------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------------------------------

def load_campaign(root: Path, cfg: dict) -> Campaign:
    root = Path(root)
    warnings: list[str] = []
    complete, listed, _sizes = _check_complete_and_checksums(root, cfg)
    plan, plan_sha = _check_plan_files(root, warnings)
    manifest = _check_manifest(root, plan, plan_sha, complete, listed)
    stats = _check_baseline_stats(root, manifest, listed)
    errs = Errors()
    _check_attempts(plan, manifest, cfg, errs)
    _check_campaign_records(manifest, errs)
    errs.raise_if_any()
    thresholds = _load_thresholds(root)
    errs = Errors()
    verifications = _check_verifications(root, manifest, listed, cfg, thresholds, errs)
    errs.raise_if_any()
    errs = Errors()
    feature_files, verif_files, probe_files = _check_exports(root, manifest, listed, errs)
    errs.raise_if_any()
    errs = Errors()
    for mf in verif_files:
        check_verification_metric_file(mf, errs)
    for p in probe_files:
        check_probe_file(p, errs)
    errs.raise_if_any()
    input_hashes = {
        "checksums/SHA256SUMS": complete["checksums_sha256"],
        "plan/plan.json": complete["plan_sha256"],
        "manifest/manifest.json": complete["manifest_sha256"],
    }
    return Campaign(
        root=root, campaign_id=manifest["campaign_id"], workload=manifest["workload"],
        campaign_index=manifest["campaign_index"], plan=plan, manifest=manifest, complete=complete,
        baseline_stats=stats, verifications=verifications, checksums=listed,
        feature_files=feature_files, verification_files=verif_files, probe_files=probe_files,
        input_hashes=input_hashes, thresholds=thresholds, warnings=warnings,
    )


THRESHOLD_KEYS = {
    "telemetry": ("segments", "scrape_miss_max", "scrape_roles", "exempt_segments", "targeted", "series_gap_flag_below"),
    "pre_fault": ("taskmanagers_registered", "tcp_ok_frac_min", "packet_loss_frac_max", "expected_pods"),
}


def _load_thresholds(root: Path) -> dict:
    """run/thresholds.json as registered. The verification-channel rules of the preprocessing (streamprep.health) take
    their numbers from it, so they are the gate's numbers by construction (C22, C26)."""
    th = load_json_bytes(_read(root / "run" / "thresholds.json", "run/thresholds.json"), "run/thresholds.json")
    errs = Errors()
    gate = th.get("gate") if isinstance(th, dict) else None
    for section, keys in THRESHOLD_KEYS.items():
        sec = (gate or {}).get(section)
        if not isinstance(sec, dict):
            errs.add(f"run/thresholds.json has no gate.{section} section")
            continue
        for k in keys:
            if k not in sec:
                errs.add(f"run/thresholds.json gate.{section} has no {k}")
    errs.raise_if_any()
    return th
