"""The campaign state machine (protocol 1.3 to 1.6, 2.6, 2.7, 3.1, 11.4, 11.5).

Everything the machine needs to continue is in the manifest on the campaign-manifest volume (plus per-attempt facts
next to it), so a restart after SIGTERM, SIGKILL or a node loss continues at the same plan position:

  * the current cell is the first cell of the plan without a VALID or VALID_REMEDIATED attempt;
  * the next attempt number is one more than the highest number already recorded for that cell, so a number is
    never reused, including after a resume (2.6.3);
  * a repeat takes the next reserve settle gap: the n-th repeat of the campaign uses reserve_settle_gap_s[n-1];
  * an attempt left RUNNING is finished as ABORTED with ended_at = restart time (11.5, 2.7.5), its resources are
    deleted with NotFound confirmed, then 300 s pass and the recovery gate runs; that time is IDLE;
  * an attempt's validity is set only when it becomes FINISHED, and only from gate.py's result (2.7.1, 2.7.5);
  * the failed-attempt streak and the total are recomputed from the manifest, across faults (2.6.5).

Every second from the start of WARMUP belongs to exactly one segment: attempt segments, or WARMUP, BASELINE,
TRAILING and IDLE campaign segments. Any time between recorded segments (exports, gate evaluation, health gates,
pauses) is recorded as IDLE when the next segment starts (3.1).
"""
from __future__ import annotations

import json
import logging
import math

from . import SCHEMA_VERSION, baseline, gate, ids, schema
from .canonical import canonical_bytes, canonical_str, sha256_hex
from .clock import ceil10, floor10
from .complete import verify_complete
from .errors import (CampaignStop, ConfigError, HaltRequested, OrchestratorError, PlanMismatch,
                     QueryError, ShutdownRequested)
from .injectors import INJECTORS, AttemptCtx, empty_facts
from .manifest import MANIFEST_KEY, atomic_write, state_dir
from .plan import cells_in_order, plan_sha_file_bytes, verify_plan
from .probefile import to_parquet as probes_to_parquet
from .recovery import RecoveryGate, instance_ids
from .runtime import JobRunner, Runtime
from .kube import pod_ready
from .health import HealthPoller

log = logging.getLogger("orchestrator.campaign")
FAILED = ("GATE_FAILED", "TELEMETRY_FAILED")
VALID = ("VALID", "VALID_REMEDIATED")


class ResumeRefused(OrchestratorError):
    """A restart that the protocol does not allow without an explicit author action (2.6.6)."""


class Campaign:
    def __init__(self, rt: Runtime) -> None:
        self.rt = rt
        self.ms = rt.manifest_store
        self.m: dict | None = None
        self.jobs = JobRunner(rt)
        self.gate = RecoveryGate(rt)
        self.active: AttemptCtx | None = None
        self._stats: dict | None = None
        self.pending_path = state_dir(rt.volume) / "pending.json"
        self.health = HealthPoller(self)
        rt.clock.add_hook(self.health.tick)

    # ================================================================================== small helpers
    def now(self) -> float:
        return self.rt.clock.now()

    def save(self) -> None:
        assert self.m is not None
        self.ms.save(self.m)

    def baseline_stats(self) -> dict | None:
        if self._stats is None and self.m and self.m.get("baseline_stats_key"):
            self._stats = json.loads(self.rt.store.get(self.m["baseline_stats_key"]).decode("utf-8"))
        return self._stats

    def pending(self) -> dict:
        try:
            return json.loads(self.pending_path.read_text())
        except FileNotFoundError:
            return {}

    def set_pending(self, **kw) -> None:
        p = self.pending()
        p.update(kw)
        atomic_write(self.pending_path, json.dumps(p, sort_keys=True).encode())

    def note_remediation(self, out) -> None:
        """A remediation that restarted a job flags the next attempt and gives it the 600 s extra settle: AFTER_REDEPLOY
        after a full redeploy (1.6), AFTER_RESUBMIT after a resubmission alone (REVIEW4 MIN-2, C28)."""
        if out.redeployed:
            self.set_pending(after_redeploy=True, after_resubmit=False)
        elif getattr(out, "resubmitted", False):
            self.set_pending(after_resubmit=True)

    def pending_flags(self) -> list[str]:
        p = self.pending()
        if p.get("after_redeploy"):
            return ["AFTER_REDEPLOY"]
        if p.get("after_resubmit"):
            return ["AFTER_RESUBMIT"]
        return []

    def last_boundary(self) -> float:
        assert self.m is not None
        ends = [s["end_at"] for s in self.m["campaign_segments"]]
        for a in self.m["attempts"]:
            if a["segments"]:
                ends.append(a["segments"][-1]["end_at"])
        return max(ends) if ends else self.m["timeline_start_at"]

    def fill_idle(self, until: float) -> None:
        """Record IDLE from the last segment boundary to `until` (3.1: every second in exactly one segment)."""
        start = self.last_boundary()
        if until > start + 1e-6:
            self.m["campaign_segments"].append({"segment_type": "IDLE", "start_at": round(start, 3), "end_at": round(until, 3)})
            self.save()

    def align(self) -> float:
        """Segment starts are whole seconds, so Chaos Mesh's second-resolution events never precede them."""
        t = math.ceil(self.now())
        self.rt.clock.sleep_until(t)
        return float(t)

    # ================================================================================== startup
    def store_plan(self) -> None:
        """1.4.4: the plan is stored before any preflight, and regenerated and compared on every start."""
        rt = self.rt
        existing = {o.key for o in rt.store.list("plan/")}
        stored = rt.store.get("plan/plan.json") if "plan/plan.json" in existing else None
        local = state_dir(rt.volume) / "plan.json"
        if stored is None and local.exists():
            stored = local.read_bytes()
        plan, data, sha = verify_plan(rt.env.workload, rt.env.campaign_index, rt.cfg, rt.env.plan_sha256, stored)
        if data != rt.plan_bytes:
            raise PlanMismatch("runtime plan bytes differ from the regenerated plan")
        if rt.plan_file_bytes is not None and rt.plan_file_bytes != data:
            # infra mounts the committed plan as /config/plan.json (NAMES.md section 4)
            raise PlanMismatch("the mounted plan (PLAN_PATH) differs from the plan regenerated from the seed")
        if "plan/plan.json" not in existing:
            rt.uploader.put_blocking("plan/plan.json", data, "application/json")
        if "plan/plan.json.sha256" not in existing:
            rt.uploader.put_blocking("plan/plan.json.sha256", plan_sha_file_bytes(sha), "text/plain")
        elif rt.store.get("plan/plan.json.sha256") != plan_sha_file_bytes(sha):
            raise PlanMismatch("plan/plan.json.sha256 in the store differs")
        if not local.exists():
            atomic_write(local, data)

    def write_run_files(self) -> None:
        """run/ objects are written once. A restart with different bytes refuses to continue (2.6.6)."""
        rt = self.rt
        files = {"run/versions.json": canonical_bytes(rt.versions), "run/images.lock.yaml": rt.images_lock_bytes,
                 "run/feature_allowlist.yaml": rt.allow_bytes, "run/thresholds.json": rt.thresholds_bytes}
        existing = {o.key for o in rt.store.list("run/")}
        for key, data in files.items():
            if key in existing:
                if rt.store.get(key) != data:
                    if rt.env.campaign_index == 0:
                        log.warning("pilot: %s changed; the new version is stored (bucket versioning keeps the old)", key)
                        rt.uploader.put_blocking(key, data)
                    else:
                        raise ResumeRefused(f"{key} differs from the stored copy: configuration or thresholds changed")
            else:
                rt.uploader.put_blocking(key, data)

    def preflight(self) -> dict:
        """Facts recorded in the manifest at warm-up start."""
        rt = self.rt
        problems = []
        server = rt.kube.server_minor_version()
        if server != rt.cfg["expected_kubernetes_version"]:
            problems.append(f"Kubernetes server is {server}, expected {rt.cfg['expected_kubernetes_version']}")
        for k in ("kubernetes_version", "node_ami_release"):
            if not isinstance(rt.versions.get(k), str) or not rt.versions[k]:
                problems.append(f"versions file has no {k}")
        nodes = rt.kube.list_nodes()
        zones = sorted({(n["metadata"].get("labels") or {}).get("topology.kubernetes.io/zone", "") for n in nodes})
        if len(zones) != 1 or not zones[0]:
            problems.append(f"nodes must all sit in one availability zone (1.7), found {zones}")
        if rt.versions.get("availability_zone") not in (None, zones[0] if zones else None):
            problems.append("versions file availability_zone disagrees with the node labels")
        skew = rt.clock_skew()
        if skew is None or abs(skew) >= rt.thresholds["health_gate"]["clock_skew_max_s"]:
            problems.append(f"clock skew to Prometheus is {skew}")
        try:
            rt.probes.status()
        except OrchestratorError as e:
            problems.append(f"verification-probes service not answering: {e}")
        consumers = [p for p in rt.kube.list_pods(ids.CONSUMER_SELECTOR) if pod_ready(p)]
        if not consumers:
            problems.append("verification-consumer is not Ready")
        if rt.loadgen_seed is not None:
            seed = rt.loadgen_seed()
            if seed is None or int(seed) != rt.plan["load_generator_seed"]:
                # the plan draws the load generator's own seed (11.3 step 6); deploy_workload.sh reads it from the plan (C12)
                problems.append(f"load generator seed is {seed}, the plan says {rt.plan['load_generator_seed']} "
                                "(redeploy the load phase with deploy_workload.sh --plan-file <this campaign's plan.json>)")
        if problems:
            raise ConfigError("preflight failed: " + "; ".join(problems))
        ids_now = sorted(instance_ids(nodes))
        rt.instance_ids_at_start = ids_now
        return {"zone": zones[0], "skew": skew, "instance_ids": ids_now}

    def new_manifest(self, pre: dict, t: float) -> dict:
        rt = self.rt
        return {
            "schema_version": SCHEMA_VERSION, "campaign_id": rt.env.campaign_id, "workload": rt.env.workload,
            "campaign_index": rt.env.campaign_index, "cluster_name": ids.cluster_name(rt.env.campaign_id),
            "region": "us-east-2", "availability_zone": pre["zone"], "kubernetes_version": rt.versions["kubernetes_version"],
            "node_ami_release": rt.versions["node_ami_release"], "plan_sha256": rt.plan_sha, "seed": rt.plan["seed"],
            "orchestrator": {"git_commit": rt.env.git_commit, "image_digest": rt.env.image_digest},
            "gate": {"git_commit": rt.env.git_commit, "thresholds_sha256": rt.thresholds_sha},
            "images_lock_sha256": sha256_hex(rt.images_lock_bytes), "feature_allowlist_sha256": rt.allow_sha,
            "flink_jobs_expected": [j[0] for j in ids.FLINK_JOBS[rt.env.workload]],
            "created_at": round(t, 3), "updated_at": round(t, 3), "timeline_start_at": round(t, 3), "timeline_end_at": None,
            "clock_skew_s": pre["skew"], "campaign_segments": [], "baseline_stats_key": None, "baseline_stats_sha256": None,
            "attempts": [], "halts": [], "node_inventory": [{"at": round(t, 3), "instance_ids": pre["instance_ids"]}],
            "health_events": [], "health_events_polled_until": None, "status": "RUNNING",
        }

    # ================================================================================== top level
    def execute(self, resume: bool = False, only_block: int | None = None, only_fault: str | None = None) -> int:
        try:
            self._execute(resume, only_block, only_fault)
            self.ms.release_lock()
            return 0
        except ResumeRefused as e:
            log.error("resume refused: %s", e)
            return 4
        except (PlanMismatch, ConfigError) as e:
            log.error("refusing to start: %s", e)
            self.ms.release_lock()
            return 2
        except HaltRequested as h:
            self.stop(h.reason, h.attempt_key)
            return 3
        except ShutdownRequested as s:
            self.stop(f"ORCHESTRATOR_STOPPED: {s}", None)
            return 143
        except Exception as e:  # a defect or an unexpected failure: stop loudly, never continue
            log.exception("unexpected error")
            self.stop(f"ORCHESTRATOR_ERROR: {type(e).__name__}: {e}"[:300], None)
            return 1

    def stop(self, reason: str, akey: str | None) -> None:
        """Halt bookkeeping. Deletes the active attempt's resources (confirmed), records the halt and exits.
        The attempt stays RUNNING; the resumed orchestrator finishes it as ABORTED (2.7.5)."""
        flag = getattr(self.rt.clock, "shutdown", None)
        if flag is not None:
            flag.suppress = True  # the cleanup below must be able to sleep while it polls for NotFound
        self.rt.uploader.stopping = True  # an S3 outage must not cut the chaos cleanup short
        if self.active is not None:
            try:
                errs = self.active.cleanup_all(snapshots=False)
                if errs:
                    reason += f" | cleanup: {errs}"[:200]
            except (Exception, CampaignStop) as e:  # shutdown path; the resumed process repeats the cleanup
                reason += f" | cleanup raised {type(e).__name__}"
        if self.m is not None:
            running = next((a["attempt_key"] for a in self.m["attempts"] if a["status"] == "RUNNING"), None)
            self.m["halts"].append({"at": round(self.now(), 3), "reason": reason[:500], "attempt_key": akey or running,
                                    "resumed_at": None, "resolution": None})
            try:
                self.save()
            except (Exception, CampaignStop):  # the volume write is the source of truth; upload retried on resume
                try:
                    self.ms.save(self.m, validate=False)
                except (Exception, CampaignStop):
                    pass
        try:
            self.rt.alert(f"campaign {self.rt.env.campaign_id} halted", reason)
        except Exception:  # alerts never block a halt
            pass
        self.upload_log()
        self.ms.release_lock()

    def abandon(self, reason: str) -> int:
        """The author confirmed an abandonment (2.6.7). Running attempts are finished as ABORTED, the campaign is
        marked ABANDONED, and COMPLETE is written with status ABANDONED so the teardown guard can proceed."""
        m, now = self.m, self.now()
        for a in [a for a in m["attempts"] if a["status"] == "RUNNING"]:
            self.abort_attempt(a, now)
        self.health.final_poll()
        unresolved = [h for h in m["halts"] if h["resolution"] is None]
        if not unresolved:
            m["halts"].append({"at": round(now, 3), "reason": reason[:500], "attempt_key": None, "resumed_at": None,
                               "resolution": "ABANDONED"})
        for h in unresolved:
            h["resolution"] = "ABANDONED"
        m["timeline_end_at"] = round(self.last_boundary(), 3)
        m["status"] = "ABANDONED"
        self.save()
        self.ms.final_upload()
        self.upload_log()
        marker = verify_complete(self.rt, m, abandoned=True)
        self.ms.release_lock()
        return 0 if marker else 1

    def _execute(self, resume: bool, only_block: int | None, only_fault: str | None) -> None:
        rt = self.rt
        if (only_block is not None or only_fault is not None) and rt.env.campaign_index != 0:
            raise ConfigError("--block and --fault are for the pilot only (campaign_index 0)")
        prev_lock = self.ms.take_lock({"started_at": self.now(), "mode": "run"})
        self.store_plan()
        self.write_run_files()
        self.m = self.ms.load()
        if self.m is None:
            if prev_lock is not None:
                log.warning("previous process died before warm-up; nothing to resume")
            pre = self.preflight()
            # 2.3.10 (REVIEW4 L3, C34): the sentinel goes out before the startup gate, so nothing that reads the dead-letter
            # topic waits for the first F6. The gate's dead-letter rate comes from the verification consumer.
            rt.sentinel()
            self.startup_gate()
            self.check_job_settings("STARTUP")
            self.health.check_source()  # C30: a campaign never completes with the AWS Health record unread
            t = self.align()
            self.m = self.new_manifest(pre, t)
            self.save()
        else:
            self.resume_existing(prev_lock is not None, resume)
        self.ms.flush()
        self.export_missing_chunks()
        self.run_phases(only_block, only_fault)

    # ================================================================================== resume
    def resume_existing(self, crashed: bool, resume_flag: bool) -> None:
        rt, m = self.rt, self.m
        if m["status"] != "RUNNING":
            raise ResumeRefused(f"campaign status is {m['status']}")
        if m["plan_sha256"] != rt.plan_sha:
            raise PlanMismatch("manifest plan_sha256 differs from this plan")
        rt.instance_ids_at_start = list(m["node_inventory"][0]["instance_ids"])
        unresolved = [h for h in m["halts"] if h["resolution"] is None]
        running = [a for a in m["attempts"] if a["status"] == "RUNNING"]
        needs_confirm = crashed or unresolved or running
        if needs_confirm and not resume_flag:
            raise ResumeRefused("the previous run stopped (halt, signal or crash). Confirm the cause was transient and "
                                "nothing changed, then start with --resume (2.6.6)")
        now = self.now()
        if rt.env.campaign_index != 0:
            changed = []
            if m["orchestrator"]["git_commit"] != rt.env.git_commit or m["orchestrator"]["image_digest"] != rt.env.image_digest:
                changed.append("orchestrator image or commit")
            if m["gate"]["thresholds_sha256"] != rt.thresholds_sha:
                changed.append("thresholds")
            if m["feature_allowlist_sha256"] != rt.allow_sha:
                changed.append("feature allowlist")
            if changed:
                raise ResumeRefused(f"{', '.join(changed)} changed since the campaign started: abandon it (2.6.7)")
            paused_since = min([h["at"] for h in unresolved] + [m["updated_at"]])
            if needs_confirm and now - paused_since >= rt.cfg["fixed"]["pause_abandon_s"]:
                raise ResumeRefused(f"the pause lasted {now - paused_since:.0f} s, not under two hours: abandon (2.6.6)")
        else:
            m["orchestrator"] = {"git_commit": rt.env.git_commit, "image_digest": rt.env.image_digest}
            m["gate"] = {"git_commit": rt.env.git_commit, "thresholds_sha256": rt.thresholds_sha}
        if crashed and not unresolved:
            m["halts"].append({"at": m["updated_at"], "reason": "ORCHESTRATOR_STOPPED: process ended without a clean shutdown",
                               "attempt_key": running[0]["attempt_key"] if running else None,
                               "resumed_at": None, "resolution": None})
        for h in m["halts"]:
            if h["resolution"] is None:
                h["resumed_at"] = round(now, 3)
                h["resolution"] = "RESUMED"
        self.save()
        for a in running:
            self.abort_attempt(a, now)
        if needs_confirm:
            # 11.5: delete anything left, confirm NotFound, wait 300 s and run the recovery gate. That time is IDLE.
            self.sweep_campaign_objects()
            rt.clock.sleep(rt.plan["phase_s"]["cooldown"])
            out = self.gate.run(now, None, self.baseline_stats())
            if not out.passed:
                raise HaltRequested("RECOVERY_GATE_FAILED_AFTER_RESUME")
            self.note_remediation(out)
        self.check_job_settings("RESUME")
        if m["health_events_polled_until"] is None:
            self.health.check_source()

    def abort_attempt(self, a: dict, now: float) -> None:
        """Finish a RUNNING attempt as ABORTED: the time since its last boundary belongs to the running segment."""
        order = ("SETTLE", "PRE_FAULT", "APPLY", "INJECTION", "RECOVERY", "COOLDOWN", "GATE")
        if a["segments"]:
            last = a["segments"][-1]
            nxt = None
            idx = order.index(last["segment_type"])
            if last["segment_type"] == "GATE":
                last["end_at"] = round(now, 3)  # crashed after the gate, in post-processing: GATE runs to the restart
            else:
                nxt = order[idx + 1]
                if last["segment_type"] == "APPLY" and not (a["applied"] or a["run_type"] == "F0"):
                    nxt = "GATE"
                a["segments"].append({"segment_type": nxt, "start_at": last["end_at"], "end_at": round(now, 3)})
        else:
            a["segments"].append({"segment_type": "SETTLE", "start_at": a["started_at"], "end_at": round(now, 3)})
        a["ended_at"] = round(now, 3)
        if a["aborted_at"] is None and a["applied"] and a["labelled_end_at"] is None:
            a["aborted_at"] = round(now, 3)
        self.save()
        facts = self.ms.load_facts(a["attempt_key"]) or empty_facts(a["run_type"])
        ctx = AttemptCtx(self, a, self.cell_of(a["cell_key"]), facts)
        self.active = ctx
        errs = ctx.cleanup_all()
        if errs:
            raise HaltRequested(f"STUCK_RESOURCE_ON_RESUME {errs}"[:300], a["attempt_key"])
        if not facts.get("pods_after"):
            facts["pods_after"] = ctx.pod_summaries()
        self.finalize(ctx, aborted=True)
        self.active = None

    def sweep_campaign_objects(self) -> None:
        """Delete every chaos resource and injector or exporter Job of this campaign that is still present."""
        rt = self.rt
        sel = f"{ids.LABEL_CAMPAIGN}={rt.env.campaign_id}"
        for kind in ids.CHAOS_KINDS + ("Job",):
            for obj in rt.kube.list(kind, sel):
                name = obj["metadata"]["name"]
                if kind == "Job" and name == ids.ORCHESTRATOR_JOB:
                    continue
                self.jobs.delete_confirmed(kind, name, timeout_s=300)

    # ================================================================================== phases
    def seg_total(self, seg_type: str) -> float:
        return sum(s["end_at"] - s["start_at"] for s in self.m["campaign_segments"] if s["segment_type"] == seg_type)

    def run_phases(self, only_block: int | None, only_fault: str | None) -> None:
        rt, f = self.rt, self.rt.cfg["fixed"]
        warmup_s = f["warmup_s"]
        baseline_s = f["pilot_baseline_s"] if rt.env.campaign_index == 0 else f["baseline_s"]
        if self.seg_total("WARMUP") < warmup_s - 1e-6:
            self.chunked("WARMUP", warmup_s)
        if self.seg_total("BASELINE") < baseline_s - 1e-6:
            self.chunked("BASELINE", baseline_s)
        if self.m["baseline_stats_key"] is None:
            self.write_baseline_stats()
        for cell in cells_in_order(rt.plan):
            if only_block is not None and cell["block"] != only_block:
                continue
            if only_fault is not None and cell["run_type"] != only_fault:
                continue
            self.run_cell(cell)
        if only_block is not None or only_fault is not None:
            log.info("pilot subset finished; the campaign stays RUNNING")
            return
        if rt.thresholds["campaign_rules"]["trailing_baseline"] and self.seg_total("TRAILING") < f["trailing_s"] - 1e-6:
            if self.seg_total("TRAILING") == 0:
                self.before_trailing()
            self.chunked("TRAILING", f["trailing_s"])
        self.finish_campaign()

    def before_trailing(self) -> None:
        """REVIEW4 MIN-3 (C29): TRAILING never starts right after the last recovery gate. The health gate runs, then a fixed
        settle of settle_max seconds, plus 600 s when the last remediation redeployed Flink or resubmitted a job. That time
        is IDLE."""
        self.health_gate()
        extra = self.rt.plan["phase_s"]["redeploy_extra_settle"] if self.pending_flags() else 0
        self.rt.clock.sleep(self.rt.thresholds["campaign_rules"]["trailing_settle_s"] + extra)
        if extra:
            self.set_pending(after_redeploy=False, after_resubmit=False)

    def chunked(self, seg_type: str, total_s: int) -> None:
        """WARMUP, BASELINE and TRAILING in chunks of at most chunk_s, each recorded as its own contiguous segment
        and exported once it ends (11.7). The next chunk runs while the previous one is exported. A crash loses at
        most the chunk in progress, which becomes IDLE on resume."""
        rt = self.rt
        chunk_s = rt.cfg["fixed"]["chunk_s"]
        start = self.align()
        self.fill_idle(start)
        while self.seg_total(seg_type) < total_s - 1e-6:
            length = min(chunk_s, total_s - self.seg_total(seg_type))
            end = start + length
            rt.clock.sleep_until(end)
            self.m["campaign_segments"].append({"segment_type": seg_type, "start_at": round(start, 3), "end_at": round(end, 3)})
            self.save()
            self.export_chunk(seg_type, start, end)
            start = end

    def check_job_settings(self, where: str) -> None:
        """REVIEW4 L1 (C40): at every start, every expected job runs with the registered job settings (parallelism 4,
        exactly-once checkpoints every 60 s, retained on cancel), read from the Flink REST API. A mismatch halts."""
        from .flinkrest import job_settings_problems
        probs = job_settings_problems(self.rt.flink, [j[0] for j in ids.FLINK_JOBS[self.rt.env.workload]],
                                      self.rt.thresholds["flink_job_settings"])
        if probs:
            raise HaltRequested(f"FLINK_JOB_SETTINGS_WRONG at {where}: {probs}"[:300])

    def startup_gate(self) -> None:
        """Before warm-up: the pipeline must first pass the health gate (1.5.1). There are no baseline statistics yet,
        so the lag condition is not evaluated; every other condition, the instance set and the clock are."""
        out = self.gate.run(self.now(), None, None)
        if not out.passed:
            raise HaltRequested("STARTUP_HEALTH_GATE_FAILED")
        conds = self.gate.evaluate(self.now() - 600, None, None, health=True)
        if not all(c["pass"] for c in conds):
            raise HaltRequested(f"STARTUP_HEALTH_GATE_FAILED {[c['name'] for c in conds if not c['pass']]}")

    def export_chunk(self, seg_type: str, start: float, end: float) -> None:
        rt = self.rt
        key = str(int(start))
        kind = "trailing" if seg_type == "TRAILING" else "baseline"
        rt.clock.sleep_until(end + 30)
        self.jobs.export(kind, key, start, end)
        rows = scrub_probe_rows(self.fetch_probes(start, end), rt.env.results_bucket, rt.env.campaign_id)
        prefix = "probes/trailing" if seg_type == "TRAILING" else "probes/baseline"
        rt.uploader.put_blocking(f"{prefix}/chunk-{key}.parquet", probes_to_parquet(rows), "application/vnd.apache.parquet")
        self.ms.flush()

    def chunk_keys(self, seg: dict) -> list[str]:
        key = str(int(seg["start_at"]))
        if seg["segment_type"] == "TRAILING":
            return [f"export/trailing/features/chunk-{key}.parquet", f"export/trailing/verification/chunk-{key}.parquet",
                    f"probes/trailing/chunk-{key}.parquet"]
        return [f"baseline/features/chunk-{key}.parquet", f"baseline/verification/chunk-{key}.parquet",
                f"probes/baseline/chunk-{key}.parquet"]

    def export_missing_chunks(self) -> None:
        """A chunk is recorded when it ends and exported right after. A crash in between leaves a recorded chunk
        without files; every start exports such chunks again (the samples are still in Prometheus, 30 d retention)."""
        have = {o.key for o in self.rt.store.list("")}
        for seg in self.m["campaign_segments"]:
            if seg["segment_type"] in ("WARMUP", "BASELINE", "TRAILING"):
                if not all(k in have for k in self.chunk_keys(seg)):
                    log.warning("re-exporting chunk %s %s", seg["segment_type"], seg["start_at"])
                    self.export_chunk(seg["segment_type"], seg["start_at"], seg["end_at"])

    def fetch_probes(self, start: float, end: float) -> list[dict]:
        rt = self.rt
        first = None
        while True:
            try:
                _, rows = rt.probes.samples(start, end)
                return [r for r in rows if start <= r["ts"] < end]
            except QueryError as e:
                now = self.now()
                first = first or now
                if now - first >= rt.cfg["fixed"]["s3_failure_halt_s"]:
                    raise HaltRequested("PROBE_SERVICE_UNREACHABLE_15_MIN")
                log.warning("probe fetch failed: %s", e)
                rt.clock.sleep(30)

    def write_baseline_stats(self) -> None:
        rt = self.rt
        first = None
        while True:
            try:
                stats = baseline.compute(rt.prom, rt.cfg, rt.thresholds, rt.env.campaign_id, self.m["campaign_segments"],
                                         self.now())
                break
            except QueryError as e:
                first = first or self.now()
                if self.now() - first >= rt.cfg["fixed"]["s3_failure_halt_s"]:
                    raise HaltRequested("BASELINE_STATS_QUERIES_FAILING")
                log.warning("baseline statistics failed: %s", e)
                rt.clock.sleep(30)
        schema.validate_baseline_stats(stats)
        data = canonical_bytes(stats)
        sha = rt.uploader.put_blocking("stats/baseline_stats.json", data, "application/json")
        self.m["baseline_stats_key"] = "stats/baseline_stats.json"
        self.m["baseline_stats_sha256"] = sha
        self._stats = stats
        self.save()

    def upload_log(self) -> None:
        if self.rt.log_uploader is not None:
            try:
                self.rt.log_uploader()
            except Exception as e:  # the log is a convenience copy; never block on it
                log.warning("log upload failed: %s", e)

    def finish_campaign(self) -> None:
        rt, m = self.rt, self.m
        self.health.final_poll()  # C30: the last AWS Health poll, before the manifest's last write
        m["timeline_end_at"] = round(self.last_boundary(), 3)
        m["status"] = "COMPLETE"
        self.save()
        self.ms.final_upload()  # the last write of manifest.json: the sidecar finds nothing newer after this
        self.upload_log()  # before SHA256SUMS: nothing may be written after COMPLETE (11.10)
        if sha256_hex(rt.store.get(MANIFEST_KEY)) != sha256_hex(self.ms.path.read_bytes()):
            raise HaltRequested("MANIFEST_UPLOAD_MISMATCH")
        marker = verify_complete(rt, m)
        if marker is None:
            raise HaltRequested("VERIFY_COMPLETE_FAILED")

    # ================================================================================== cells and attempts
    def cell_of(self, cell_key: str) -> dict:
        return next(c for c in cells_in_order(self.rt.plan) if c["cell_key"] == cell_key)

    def attempts_of(self, cell_key: str) -> list[dict]:
        return [a for a in self.m["attempts"] if a["cell_key"] == cell_key]

    def run_cell(self, cell: dict) -> None:
        rules = self.rt.thresholds["campaign_rules"]
        while True:
            mine = self.attempts_of(cell["cell_key"])
            if any(a["outcome"] in VALID for a in mine):
                return
            n = max((a["attempt"] for a in mine), default=0) + 1
            if n > rules["max_attempt_number"]:
                raise HaltRequested(f"ATTEMPT_NUMBERS_EXHAUSTED {cell['cell_key']}")
            if n == 1:
                gap = cell["settle_gap_s"]
            else:
                repeats = sum(1 for a in self.m["attempts"] if a["attempt"] >= 2)
                if repeats >= len(self.rt.plan["reserve_settle_gap_s"]):
                    raise HaltRequested("RESERVE_SETTLE_GAPS_EXHAUSTED")
                gap = self.rt.plan["reserve_settle_gap_s"][repeats]
            self.health_gate()
            self.run_attempt(cell, n, gap)
            self.check_halt_rules()

    def check_halt_rules(self) -> None:
        """2.6.5: two consecutive failed attempts counted across faults, or more than 8 in total."""
        rules = self.rt.thresholds["campaign_rules"]
        resumed = [h["resumed_at"] for h in self.m["halts"] if h["resolution"] == "RESUMED" and h["resumed_at"]]
        cut = max(resumed) if resumed else -math.inf
        finished = [a for a in self.m["attempts"] if a["status"] == "FINISHED"]
        streak = 0
        for a in reversed(finished):
            if a["started_at"] < cut:
                break
            if a["outcome"] in FAILED:
                streak += 1
            elif a["outcome"] in VALID:
                break
        total = sum(1 for a in finished if a["outcome"] in FAILED)
        if streak >= rules["halt_consecutive_failed"]:
            raise HaltRequested(f"CONSECUTIVE_FAILED_ATTEMPTS {streak}", finished[-1]["attempt_key"])
        if total > rules["halt_total_failed_gt"]:
            raise HaltRequested(f"TOTAL_FAILED_ATTEMPTS {total}", finished[-1]["attempt_key"])

    def reference_at(self) -> float:
        done = [a for a in self.m["attempts"] if a["status"] == "FINISHED"]
        if done:
            a = done[-1]
            return a["labelled_end_at"] or a["aborted_at"] or a["ended_at"]
        base = [s for s in self.m["campaign_segments"] if s["segment_type"] == "BASELINE"]
        return base[-1]["end_at"] if base else self.m["timeline_start_at"]

    def health_gate(self) -> None:
        """1.6: the recovery-gate conditions plus the EC2 instance set and the clock check, before every settle gap."""
        stats = self.baseline_stats()
        # Checkpoints are counted since the last labelled end, or over the last 180 s when that is more recent, so
        # the first health gate after the baseline does not wait for two fresh checkpoints.
        ref = min(self.reference_at(), self.now() - 180)
        conds = self.gate.evaluate(ref, None, stats, health=True)
        by = {c["name"]: c for c in conds}
        if not by["ec2_instance_ids_match"]["pass"]:
            try:
                self.m["node_inventory"].append({"at": round(self.now(), 3),
                                                 "instance_ids": sorted(instance_ids(self.rt.kube.list_nodes()))})
                self.save()
            except OrchestratorError:
                pass
            raise HaltRequested("NODE_REPLACED")
        if not by["clock_skew_s"]["pass"]:
            raise HaltRequested("CLOCK_SKEW")
        if all(c["pass"] for c in conds):
            return
        log.warning("SKIPPED_UNHEALTHY: health gate failed before the settle gap: %s",
                    [c["name"] for c in conds if not c["pass"]])
        out = self.gate.run(ref, None, stats)
        self.note_remediation(out)
        if not out.passed:
            raise HaltRequested("HEALTH_GATE_FAILED")
        conds = self.gate.evaluate(min(self.reference_at(), self.now() - 180), None, stats, health=True)
        if not all(c["pass"] for c in conds):
            raise HaltRequested("HEALTH_GATE_FAILED")

    def new_attempt(self, cell: dict, n: int, t0: float, gap: int, flags: list[str]) -> dict:
        rt = self.rt
        entry = next(r for r in rt.plan["run_types"] if r["run_type"] == cell["run_type"])
        akey = ids.attempt_key(cell["cell_key"], n)
        extra = rt.plan["phase_s"]["redeploy_extra_settle"] if ("AFTER_REDEPLOY" in flags or "AFTER_RESUBMIT" in flags) else 0
        planned = t0 + gap + extra + rt.plan["phase_s"]["pre_fault"]
        duration = cell["sham_duration_s"] if cell["run_type"] == "F0" else entry["duration_s"]
        return {
            "attempt_key": akey, "run_id": ids.new_run_id(), "cell_key": cell["cell_key"], "block": cell["block"],
            "position": cell["position"], "attempt": n, "run_type": cell["run_type"], "fault_name": entry["name"],
            "status": "RUNNING", "outcome": None, "failed_checks": [], "flags": list(flags),
            "started_at": t0, "ended_at": None, "planned_onset_at": planned, "apply_called_at": None,
            "onset_verified_at": None, "onset_source": None, "configured_duration_s": duration,
            "labelled_end_at": None, "end_source": None, "overrun_s": None, "applied": False, "aborted_at": None,
            "resources": [], "controller_at_onset": None, "segments": [],
            "recovery_gate": {"started_at": None, "passed_at": None,
                              "remediation": {"performed": False, "actions": [], "started_at": None, "ended_at": None}},
            "flink_jobs_at_end": [], "verification_key": None, "verification_sha256": None,
            "export": {"features_key": f"export/attempts/{akey}/features.parquet", "features_sha256": "",
                       "verification_metrics_key": f"export/attempts/{akey}/verification.parquet",
                       "verification_metrics_sha256": "", "probes_key": f"attempts/{akey}/probes.parquet",
                       "probes_sha256": "", "status": "PENDING"},
        }

    def add_segment(self, rec: dict, seg_type: str, start: float, end: float) -> None:
        rec["segments"].append({"segment_type": seg_type, "start_at": round(start, 3), "end_at": round(end, 3)})
        self.active.save()

    def run_attempt(self, cell: dict, n: int, gap: int) -> None:
        rt = self.rt
        clock = rt.clock
        flags = self.pending_flags()
        t0 = self.align()
        self.fill_idle(t0)
        rec = self.new_attempt(cell, n, t0, gap, flags)
        self.m["attempts"].append(rec)
        facts = empty_facts(rec["run_type"])
        ctx = AttemptCtx(self, rec, cell, facts)
        self.active = ctx
        ctx.save()  # the record exists before anything happens, so the attempt number is taken (2.6.3)
        if flags:
            self.set_pending(after_redeploy=False, after_resubmit=False)
        ph = rt.plan["phase_s"]
        inj = INJECTORS[rec["run_type"]](ctx)

        # SETTLE (excluded)
        settle_end = rec["planned_onset_at"] - ph["pre_fault"]
        clock.sleep_until(settle_end)
        self.add_segment(rec, "SETTLE", t0, settle_end)

        # PRE_FAULT (NORMAL if valid). F6 creates its injector inside it, at least 30 s before onset.
        failure: Exception | None = None
        prep_at = inj.prepare_at()
        if prep_at is not None:
            clock.sleep_until(prep_at)
            try:
                inj.prepare()
            except OrchestratorError as e:
                failure = e
        clock.sleep_until(rec["planned_onset_at"])
        self.add_segment(rec, "PRE_FAULT", settle_end, rec["planned_onset_at"])

        # APPLY: from the planned onset to the verified onset (excluded)
        facts["pods_before"] = ctx.pod_summaries()
        ctx.save()
        if failure is None:
            try:
                inj.apply()
            except OrchestratorError as e:
                failure = e
        apply_end = rec["onset_verified_at"] if failure is None else self.now()
        if apply_end < rec["planned_onset_at"]:
            apply_end = rec["planned_onset_at"]
        self.add_segment(rec, "APPLY", rec["planned_onset_at"], apply_end)

        # INJECTION
        if failure is None:
            try:
                inj.hold()
            except OrchestratorError as e:
                failure = e
        if failure is not None:
            log.warning("attempt %s: injection failed: %s", rec["attempt_key"], failure)
            if rec["applied"] and rec["aborted_at"] is None:
                rec["aborted_at"] = round(self.now(), 3)
            errs = ctx.cleanup_all()
            if errs:
                log.error("cleanup errors: %s", errs)  # the recovery gate will block on the leftover resource
            ctx.save()

        if rec["applied"] or rec["run_type"] == "F0":
            inj_end = rec["labelled_end_at"] if failure is None else (rec["aborted_at"] or self.now())
            inj_end = max(inj_end, apply_end)
            self.add_segment(rec, "INJECTION", apply_end, inj_end)
            # RECOVERY and COOLDOWN always run once anything was applied, even after a failure (1.3, 2.6.2).
            rec_end = inj_end + ph["recovery"]
            clock.sleep_until(rec_end)
            facts["pods_after"] = ctx.pod_summaries()
            self.add_segment(rec, "RECOVERY", inj_end, rec_end)
            cool_end = rec_end + ph["cooldown"]
            clock.sleep_until(cool_end)
            self.add_segment(rec, "COOLDOWN", rec_end, cool_end)

        # GATE: the recovery gate (excluded)
        gate_start = rec["segments"][-1]["end_at"]
        rec["recovery_gate"]["started_at"] = gate_start
        ctx.save()
        ref = rec["labelled_end_at"] or rec["aborted_at"] or gate_start

        def remediation_started(rem: dict) -> None:
            rec["recovery_gate"]["remediation"] = rem
            ctx.save()

        out = self.gate.run(ref, rec["resources"], self.baseline_stats(), on_remediation_start=remediation_started)
        facts["recovery_gate_conditions"] = out.conditions
        rec["recovery_gate"]["remediation"] = out.remediation
        self.note_remediation(out)
        if not out.passed:
            ctx.save()
            raise HaltRequested("RECOVERY_GATE_FAILED_AFTER_REMEDIATION", rec["attempt_key"])
        rec["recovery_gate"]["passed_at"] = round(out.passed_at, 3)
        self.add_segment(rec, "GATE", gate_start, out.passed_at)
        rec["ended_at"] = rec["segments"][-1]["end_at"]
        if not facts["pods_after"]:
            facts["pods_after"] = ctx.pod_summaries()
        rec["flink_jobs_at_end"] = self.flink_jobs_at_end()
        ctx.save()
        self.finalize(ctx, aborted=False)
        self.active = None

    def flink_jobs_at_end(self) -> list[dict]:
        """REVIEW4 CAM-2 item 2 (C25): the job list at the end of the attempt, retried until it succeeds, so a valid attempt
        never ends with an empty list. Fifteen minutes without an answer halt the campaign."""
        first = None
        while True:
            try:
                return [{"name": j["name"], "job_id": j["job_id"], "state": j["state"]} for j in self.rt.flink.snapshot()]
            except OrchestratorError as e:
                now = self.now()
                first = first or now
                if now - first >= self.rt.cfg["fixed"]["s3_failure_halt_s"]:
                    raise HaltRequested("FLINK_REST_UNREACHABLE_15_MIN")
                log.warning("Flink job list at end unavailable, retrying: %s", e)
                self.rt.clock.sleep(10)

    def finalize(self, ctx: AttemptCtx, aborted: bool) -> None:
        """Probes, export, gate, verification record, then the outcome. Validity is set only here (2.7.5)."""
        rt, rec, facts = self.rt, ctx.rec, ctx.facts
        akey = rec["attempt_key"]
        # C25: an applied F6 attempt always stores its injector log, on abort the orchestrator_observed line alone
        ctx.ensure_injector_log("ABORTED" if aborted else "finished")
        lo, hi = floor10(rec["started_at"]), ceil10(rec["ended_at"])
        rt.clock.sleep_until(rec["ended_at"] + 30)  # every scrape of the span is in Prometheus before the export
        rows = scrub_probe_rows(self.fetch_probes(lo, hi + 1e-6), rt.env.results_bucket, rt.env.campaign_id)
        rec["export"]["probes_sha256"] = rt.uploader.put_blocking(rec["export"]["probes_key"], probes_to_parquet(rows),
                                                                  "application/vnd.apache.parquet")
        ctx.save()
        try:
            res = self.jobs.export("attempt", akey, rec["started_at"], rec["ended_at"], akey)
        except HaltRequested:
            rec["export"]["status"] = "FAILED"
            ctx.save()
            raise
        rec["export"]["features_sha256"] = res["features_sha256"]
        rec["export"]["verification_metrics_sha256"] = res["verification_sha256"]
        rec["export"]["status"] = "DONE"
        ctx.save()
        gi = gate.load_inputs(rt.store, self.m, rt.plan, akey, rt.thresholds, facts=facts)
        gr = gate.evaluate(gi)
        merge_derived(facts, gr.derived)
        vrec = gate.verification_record(gi, gr, "ONLINE", rt.env.git_commit or "dry-run", rt.thresholds_sha,
                                        round(self.now(), 3))
        schema.validate_verification(vrec)
        vkey = f"attempts/{akey}/verification.json"
        vsha = rt.uploader.put_blocking(vkey, canonical_bytes(vrec), "application/json")
        rec["verification_key"], rec["verification_sha256"] = vkey, vsha
        flags = list(rec["flags"])
        for f in gr.flags:
            if f not in flags:
                flags.append(f)
        remediated = rec["recovery_gate"]["remediation"]["performed"]
        if remediated and "REMEDIATED" not in flags:
            flags.append("REMEDIATED")
        if aborted:
            outcome = "ABORTED"
        elif gr.result["gate"]:
            outcome = "VALID_REMEDIATED" if remediated else "VALID"
        else:
            outcome = gate.classify_failure(gr, rec["run_type"])
        rec["flags"] = flags
        rec["failed_checks"] = gr.failed_checks if outcome not in VALID else []
        rec["outcome"] = outcome
        rec["status"] = "FINISHED"
        self.health.flag_attempts()
        ctx.save()
        log.info("attempt %s finished: %s %s", akey, outcome, rec["failed_checks"])


def scrub_probe_rows(rows: list[dict], bucket: str, campaign_id: str) -> list[dict]:
    """C24 (decision 29, REVIEW4 C1): the flink_jobs probe reports Flink's external_path, a full s3://<bucket>/... URI. The
    stored probe records hold it relative to this campaign's scratch prefix, so the bucket name never enters the
    deposited data. Remediation reads the live samples, which keep the full URI."""
    from .gate import scrub_checkpoint_path
    out = []
    for r in rows:
        if r["probe"] == "flink_jobs" and r.get("detail"):
            try:
                jobs = json.loads(r["detail"])
            except json.JSONDecodeError:
                out.append(r)
                continue
            if isinstance(jobs, list):
                for j in jobs:
                    if isinstance(j, dict) and j.get("latest_completed_path"):
                        j["latest_completed_path"] = scrub_checkpoint_path(j["latest_completed_path"], bucket, campaign_id)
                r = dict(r, detail=canonical_str(jobs))
        out.append(r)
    return out


def merge_derived(facts: dict, derived: dict) -> None:
    if facts.get("f5") is not None:
        facts["f5"]["all_jobs_running_at"] = derived.get("f5_all_jobs_running_at")
        facts["f5"]["new_tm_uid_count"] = derived.get("f5_new_tm_uid_count")
    if facts.get("f6") is not None:
        facts["f6"]["consumer_count"] = derived.get("f6_consumer_count")
    if facts.get("f8") is not None:
        during = derived.get("f8_during") or {}
        export_size = derived.get("f8_export_size") or {}
        for p in facts["f8"]["per_pod"]:
            d = during.get(p["pod"])
            if d:
                p["ws_during_bytes"], p["increase_bytes"] = d
            p["export_stressor_bytes"] = export_size.get(p["pod"])
