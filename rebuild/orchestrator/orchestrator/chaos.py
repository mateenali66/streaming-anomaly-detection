"""Chaos Mesh resources (2.8.x) and their status.

Docs relied on (context7 /websites/chaos-mesh and /chaos-mesh/chaos-mesh):
  * Pod list selector `selector.pods: {<namespace>: [<pod>...]}`; when it is used, other selectors are ignored
    ("Define the scope of chaos experiments"). Every resource here names its exact target pods this way.
  * PodChaos `action: pod-kill`, `gracePeriod` (simulate-pod-chaos-on-kubernetes).
  * NetworkChaos `action: partition` with `direction` and `target`; `action: loss` with `loss.loss` and
    `loss.correlation` (simulate-network-chaos-on-kubernetes).
  * StressChaos `stressors.cpu.{workers,load}`, `stressors.memory.{workers,size}`, `containerNames`
    (simulate-heavy-stress-on-kubernetes; memory size units B, KB/KiB, MB/MiB, GB/GiB).
  * Status (controllers/common/condition/README.md, controllers/common/records/README.md): conditions
    Selected, AllInjected (every record phase Injected), AllRecovered (every record phase "Not Injected"),
    Paused; per-target records in `.status.experiment.containerRecords` with id, selectorKey, phase and
    bounded per-record success/failure events. Each event carries type (Succeeded or Failed), operation
    (Apply or Recover) and a timestamp. The README notes that a non-nil EMPTY record list satisfies both
    "all" conditions, so this code never trusts a condition alone: it always checks the records themselves.
  * Deletion: the records step recovers every target before the finalizer is removed (pipeline README:
    "cleanup must run after records so a deleting object is not released before recovery has completed").

Every resource sets `duration` where the action supports it, so a fault ends by itself even if the
orchestrator is killed in the middle of an injection.
"""
from __future__ import annotations

from typing import Any

from . import ids
from .timeutil import parse_rfc3339

API_VERSION = "chaos-mesh.org/v1alpha1"
SELECTOR_MAIN = "."  # Chaos Mesh selector key of spec.selector (the target selector is ".Target")


def _meta(name: str, campaign_id: str, akey: str, run_type: str) -> dict:
    return {"name": name, "namespace": ids.NAMESPACE, "labels": ids.campaign_labels(campaign_id, akey, run_type)}


def _pods(pods: list[str]) -> dict:
    return {"pods": {ids.NAMESPACE: sorted(pods)}}


def pod_kill(name: str, campaign_id: str, akey: str, run_type: str, pod: str) -> dict:
    return {"apiVersion": API_VERSION, "kind": "PodChaos", "metadata": _meta(name, campaign_id, akey, run_type),
            "spec": {"action": "pod-kill", "mode": "all", "gracePeriod": 0, "selector": _pods([pod])}}


def partition(name: str, campaign_id: str, akey: str, follower: str, others: list[str], duration_s: int) -> dict:
    return {"apiVersion": API_VERSION, "kind": "NetworkChaos", "metadata": _meta(name, campaign_id, akey, "F2"),
            "spec": {"action": "partition", "mode": "all", "selector": _pods([follower]), "direction": "both",
                     "target": {"mode": "all", "selector": _pods(others)}, "duration": f"{duration_s}s"}}


def loss(name: str, campaign_id: str, akey: str, tms: list[str], percent: int, duration_s: int) -> dict:
    # No target: direction "to" applies the loss to every packet leaving the selected pods (outbound, 1.2 F7).
    return {"apiVersion": API_VERSION, "kind": "NetworkChaos", "metadata": _meta(name, campaign_id, akey, "F7"),
            "spec": {"action": "loss", "mode": "all", "selector": _pods(tms), "direction": "to",
                     "loss": {"loss": str(percent), "correlation": "0"}, "duration": f"{duration_s}s"}}


def cpu_stress(name: str, campaign_id: str, akey: str, tms: list[str], workers: int, load: int, duration_s: int) -> dict:
    return {"apiVersion": API_VERSION, "kind": "StressChaos", "metadata": _meta(name, campaign_id, akey, "F4"),
            "spec": {"mode": "all", "selector": _pods(tms), "containerNames": [ids.FLINK_CONTAINER],
                     "stressors": {"cpu": {"workers": workers, "load": load}}, "duration": f"{duration_s}s"}}


def memory_stress(name: str, campaign_id: str, akey: str, tm: str, size_mib: int, duration_s: int) -> dict:
    return {"apiVersion": API_VERSION, "kind": "StressChaos", "metadata": _meta(name, campaign_id, akey, "F8"),
            "spec": {"mode": "all", "selector": _pods([tm]), "containerNames": [ids.FLINK_CONTAINER],
                     "stressors": {"memory": {"workers": 1, "size": f"{size_mib}MiB"}}, "duration": f"{duration_s}s"}}


# --------------------------------------------------------------------------------------------- status
def record_pod(record_id: str) -> str:
    """Record ids are <namespace>/<pod> or <namespace>/<pod>/<container>. Returns <namespace>/<pod>."""
    parts = record_id.split("/")
    if len(parts) < 2 or not parts[0] or not parts[1]:
        raise ValueError(f"unexpected record id {record_id!r}")
    return f"{parts[0]}/{parts[1]}"


def records(obj: dict) -> list[dict] | None:
    """Parsed records, or None when selection has not completed (records field nil)."""
    exp = ((obj.get("status") or {}).get("experiment") or {})
    raw = exp.get("containerRecords")
    if raw is None:
        return None
    out = []
    for r in raw:
        applies, recovers = [], []
        for ev in r.get("events") or []:
            if ev.get("type") != "Succeeded" or not ev.get("timestamp"):
                continue
            t = parse_rfc3339(ev["timestamp"])
            if ev.get("operation") == "Apply":
                applies.append(t)
            elif ev.get("operation") == "Recover":
                recovers.append(t)
        out.append({
            "id": record_pod(r["id"]),
            "selector_key": r.get("selectorKey", SELECTOR_MAIN),
            "phase": r.get("phase", ""),
            "first_apply_at": min(applies) if applies else None,
            "last_recover_at": max(recovers) if recovers else None,
            "failed_events": sum(1 for ev in (r.get("events") or []) if ev.get("type") == "Failed"),
        })
    return sorted(out, key=lambda x: (x["selector_key"], x["id"]))


def conditions(obj: dict) -> dict[str, str]:
    out = {}
    for c in (obj.get("status") or {}).get("conditions") or []:
        out[c.get("type")] = str(c.get("status"))
    return out


def all_applied(recs: list[dict] | None, expected_ids: set[str]) -> bool:
    if not recs:
        return False
    got = {r["id"] for r in recs}
    return got == expected_ids and all(r["first_apply_at"] is not None for r in recs)


def all_recovered(recs: list[dict] | None) -> bool:
    if not recs:
        return False
    return all(r["phase"] == "Not Injected" and r["last_recover_at"] is not None for r in recs)


def snapshot_entry(tag: str, taken_at: float, obj: dict, yaml_key: str, first_seen: dict[str, tuple[str, float]]) -> dict:
    """One element of chaos_snapshots (11.6).

    Chaos Mesh conditions carry type, status and reason, with no transition time. last_transition_at is
    therefore the first time this orchestrator observed the condition with its current status (first_seen).
    """
    conds = conditions(obj)
    cond_out: dict[str, Any] = {}
    for ctype in ("Selected", "AllInjected", "AllRecovered", "Paused"):
        status = conds.get(ctype, "Unknown")
        seen = first_seen.get(ctype)
        if seen is None or seen[0] != status:
            first_seen[ctype] = (status, taken_at)
        cond_out[ctype] = {"status": status, "last_transition_at": first_seen[ctype][1]}
    recs = records(obj) or []
    return {
        "tag": tag, "taken_at": taken_at, "kind": obj.get("kind"), "name": obj["metadata"]["name"], "yaml_key": yaml_key,
        "conditions": cond_out,
        "records": [{"id": r["id"], "phase": r["phase"], "first_apply_at": r["first_apply_at"],
                     "last_recover_at": r["last_recover_at"]} for r in recs],
    }
