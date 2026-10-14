"""verify_complete: checksums/SHA256SUMS and the COMPLETE marker (protocol 11.10, 6.5.5, 7.4.1).

COMPLETE is written once, last, and only if every check passes. SHA256SUMS lists every object under the campaign
prefix except COMPLETE and except SHA256SUMS itself (it cannot contain its own hash), sorted by key, as
"<sha256>  <key>". The teardown guard (infra) refuses to destroy a cluster unless COMPLETE exists, is newer than
every object under the prefix, and has status COMPLETE or an author-confirmed ABANDONED.
"""
from __future__ import annotations

import json
import logging

from . import SCHEMA_VERSION
from .canonical import canonical_bytes, sha256_hex
from .exporter import export_span, ts_range
from .probefile import read_parquet as read_probes
from .schema import timeline_gaps, validate_complete, validate_manifest, validate_verification

log = logging.getLogger("orchestrator.complete")
CHECKSUMS_KEY = "checksums/SHA256SUMS"


def _ts(store, key: str, probes: bool) -> list | None:
    """[min ts, max ts] of a stored parquet file ([] when it has no rows), or None if it cannot be read (a damaged file
    fails its check). Only the ts column is read."""
    try:
        data = store.get(key)
        if probes:
            read_probes(data)  # the whole file must parse
        mm = ts_range(data)
    except Exception as e:  # noqa: BLE001 - any unreadable object is a failed check, never a crash
        log.error("cannot read %s: %s", key, e)
        return None
    return [] if mm is None else list(mm)


def _chunk_keys(m: dict, seg_types: tuple[str, ...]) -> list[tuple[int, float, float]]:
    return [(int(s["start_at"]), s["start_at"], s["end_at"]) for s in m["campaign_segments"] if s["segment_type"] in seg_types]


def run_checks(store, m: dict, plan: dict, abandoned: bool) -> list[dict]:
    checks: list[dict] = []
    keys = {o.key for o in store.list("")}

    def add(name: str, ok: bool, why: str = "") -> None:
        checks.append({"name": name, "pass": bool(ok)})
        if not ok:
            log.error("completeness check failed: %s %s", name, why)

    try:
        validate_manifest(m)
        add("manifest_schema_valid", True)
    except Exception as e:
        add("manifest_schema_valid", False, str(e))
    add("manifest_status", m["status"] == ("ABANDONED" if abandoned else "COMPLETE"), m["status"])
    add("no_running_attempt", all(a["status"] == "FINISHED" for a in m["attempts"]))
    add("no_unresolved_halt", abandoned or all(h["resolution"] is not None for h in m["halts"]))
    add("plan_object_present", "plan/plan.json" in keys and "plan/plan.json.sha256" in keys)
    for k in ("run/versions.json", "run/images.lock.yaml", "run/feature_allowlist.yaml", "run/thresholds.json"):
        add(f"present:{k}", k in keys)
    if not abandoned:
        valid_cells = {a["cell_key"] for a in m["attempts"] if a["outcome"] in ("VALID", "VALID_REMEDIATED")}
        add("every_cell_valid", all(c["cell_key"] in valid_cells for b in plan["blocks"] for c in b["cells"]))
        add("baseline_stats_present", m["baseline_stats_key"] is not None and m["baseline_stats_key"] in keys)

    # Every FINISHED attempt has its verification record, probe file and both export files, with matching hashes,
    # and every export's rows lie inside its span.
    files_ok, rows_ok = True, True
    for a in m["attempts"]:
        if a["status"] != "FINISHED":
            continue
        e = a["export"]
        pairs = [(a["verification_key"], a["verification_sha256"]), (e["features_key"], e["features_sha256"]),
                 (e["verification_metrics_key"], e["verification_metrics_sha256"]), (e["probes_key"], e["probes_sha256"])]
        for key, sha in pairs:
            if key not in keys or e["status"] != "DONE":
                files_ok = False
                log.error("missing %s for %s", key, a["attempt_key"])
                continue
            try:
                data = store.get(key)
            except Exception as ex:  # noqa: BLE001
                files_ok = False
                log.error("cannot read %s: %s", key, ex)
                continue
            if sha256_hex(data) != sha:
                files_ok = False
                log.error("hash mismatch for %s", key)
            if key == a["verification_key"]:
                try:
                    validate_verification(json.loads(data.decode("utf-8")))
                except Exception as ex:
                    files_ok = False
                    log.error("verification record %s invalid: %s", key, ex)
        lo, hi = export_span("attempt", a["started_at"], a["ended_at"])
        for key in (e["features_key"], e["verification_metrics_key"]):
            if key in keys:
                ts = _ts(store, key, False)
                if ts is None or (ts and (min(ts) < lo or max(ts) > hi)):
                    rows_ok = False
        if e["probes_key"] in keys:
            ts = _ts(store, e["probes_key"], True)
            if ts is None or (ts and (min(ts) < lo or max(ts) > hi + 10)):
                rows_ok = False
    add("attempt_files_present_and_hashed", files_ok)
    add("attempt_export_rows_inside_span", rows_ok)

    # Baseline and trailing chunks cover their segments.
    cover_ok = True
    for seg_types, fk, vk, pk in ((("WARMUP", "BASELINE"), "baseline/features", "baseline/verification", "probes/baseline"),
                                  (("TRAILING",), "export/trailing/features", "export/trailing/verification", "probes/trailing")):
        for start, s_at, e_at in _chunk_keys(m, seg_types):
            for prefix in (fk, vk, pk):
                key = f"{prefix}/chunk-{start}.parquet"
                if key not in keys:
                    cover_ok = False
                    log.error("missing chunk %s", key)
                    continue
                ts = _ts(store, key, prefix.startswith("probes"))
                lo, hi = export_span("chunk", s_at, e_at)
                if not ts or min(ts) < (s_at if prefix.startswith("probes") else lo) or max(ts) > (
                        e_at if prefix.startswith("probes") else hi):
                    cover_ok = False
                    log.error("chunk %s does not cover or overruns its segment", key)
    add("chunks_cover_segments", cover_ok)
    gaps = timeline_gaps(m) if m["timeline_end_at"] is not None else ["timeline_end_at is null"]
    add("timeline_covers_every_second", not gaps, "; ".join(gaps[:3]))
    return checks


def verify_complete(rt, m: dict, abandoned: bool = False) -> dict | None:
    """Writes SHA256SUMS and then COMPLETE. Returns the marker, or None (and writes nothing) if a check fails."""
    store = rt.store
    if any(o.key == "COMPLETE" for o in store.list("COMPLETE")):
        log.error("COMPLETE already exists; it is written once")
        return None
    checks = run_checks(store, m, rt.plan, abandoned)
    if not all(c["pass"] for c in checks):
        return None
    objects = sorted((o for o in store.list("") if o.key not in ("COMPLETE", CHECKSUMS_KEY)), key=lambda o: o.key)
    lines, total, newest = [], 0, 0.0
    for o in objects:
        data = store.get(o.key)
        lines.append(f"{sha256_hex(data)}  {o.key}")
        total += len(data)
        newest = max(newest, o.last_modified)
    sums = ("\n".join(lines) + "\n").encode("utf-8")
    rt.uploader.put_blocking(CHECKSUMS_KEY, sums, "text/plain")
    listed_newest = max(o.last_modified for o in store.list("") if o.key != "COMPLETE")
    # written_at must be later than every object under the prefix, SHA256SUMS included (teardown guard).
    while rt.clock.now() <= listed_newest + 1.0:
        rt.clock.sleep(1)
    manifest_bytes = store.get("manifest/manifest.json")
    marker = {
        "schema_version": SCHEMA_VERSION, "campaign_id": m["campaign_id"],
        "status": "ABANDONED" if abandoned else "COMPLETE", "written_at": round(rt.clock.now(), 3),
        "plan_sha256": sha256_hex(store.get("plan/plan.json")), "manifest_sha256": sha256_hex(manifest_bytes),
        "checksums_key": CHECKSUMS_KEY, "checksums_sha256": sha256_hex(sums),
        "object_count": len(objects), "total_bytes": total, "newest_object_at": round(newest, 3), "checks": checks,
    }
    validate_complete(marker)
    rt.uploader.put_blocking("COMPLETE", canonical_bytes(marker), "application/json")
    return marker
