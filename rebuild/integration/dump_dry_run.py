"""Run an orchestrator dry run against the simulated cluster and write its S3 objects to disk.

The orchestrator's fake S3 keeps objects in memory. This writes them under OUT/s3 exactly as their keys read, so the
preprocessing can build the campaign from a local mirror, as it would from a real one (11.7). It also writes
OUT/summary.json: the exit codes, the attempts with their outcomes, the halts, and the scenario that was run, which
run_e2e.sh compares with the pipeline's outputs.

Scenarios:
  --knob F4.fail_first_n=1        a fault knob of orchestrator/fakes/world.py DEFAULT_KNOBS (repeatable; JSON values)
  --world f6_exit_code=1          a global knob of FakeWorld.global_knobs (repeatable; JSON values)
  --kill INJECTION:F5:20          SIGKILL the orchestrator 20 s into the first F5 attempt's INJECTION segment (times come
                                  from an uninterrupted reference run of the same scenario), restart the process after
                                  --restart-after seconds and resume with --resume, as the operator would (2.6.6)
  --term INJECTION:F6:60          the same with SIGTERM: the orchestrator's halt path runs (cleanup, halt record)
  --resume-halts N                after a halt (exit 3) restart and resume with --resume, up to N times (2.6.6)

OUT/summary.json also holds ground_truth: every interval in which the simulated cluster was disturbed, from the fake world's
own state (fakes/world.py ground_truth), which check_e2e.py compares with the labelled windows (REVIEW4 MIN-1).

Run (Python 3.12, from rebuild/orchestrator):
  python3.12 ../integration/dump_dry_run.py --workload ecommerce --index 0 --out /tmp/e2e [--full-length]
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "orchestrator"))
from orchestrator.fakes.clock import ProcessDead  # noqa: E402
from orchestrator.fakes.harness import Harness, quiet  # noqa: E402


def _value(raw: str):
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def build(args, root: Path) -> Harness:
    h = Harness(args.workload, args.index, root=str(root), fast=not args.full_length, overrides=None)
    for kv in args.knob:
        name, _, raw = kv.partition("=")
        rt, _, key = name.partition(".")
        h.world.knobs.setdefault(rt, {})[key] = _value(raw)
    for kv in args.world:
        key, _, raw = kv.partition("=")
        if key not in h.world.global_knobs:
            raise SystemExit(f"unknown world knob {key}")
        h.world.global_knobs[key] = _value(raw)
    return h


def kill_time(args, out: Path, spec: str | None = None) -> float:
    """The instant to kill at, from a reference run of the same scenario (the simulation is deterministic in time)."""
    seg, _, rest = (spec or args.kill).partition(":")
    run_type, _, offset = rest.partition(":")
    ref = build(args, out / "reference-root")
    if ref.run() != 0:
        raise SystemExit("the reference run did not finish")
    m = ref.manifest()
    if seg in ("WARMUP", "BASELINE", "TRAILING", "IDLE"):
        s = next(x for x in m["campaign_segments"] if x["segment_type"] == seg)
    else:
        a = next(x for x in m["attempts"] if x["run_type"] == run_type)
        s = next(x for x in a["segments"] if x["segment_type"] == seg)
    shutil.rmtree(out / "reference-root", ignore_errors=True)
    return s["start_at"] + float(offset or 1)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workload", required=True, choices=["ecommerce", "iot"])
    ap.add_argument("--index", type=int, required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--full-length", action="store_true")
    ap.add_argument("--knob", action="append", default=[])
    ap.add_argument("--world", action="append", default=[])
    ap.add_argument("--kill", help="SEGMENT:RUN_TYPE:OFFSET_S, then resume")
    ap.add_argument("--term", help="SEGMENT:RUN_TYPE:OFFSET_S, SIGTERM, then resume")
    ap.add_argument("--resume-halts", type=int, default=0)
    ap.add_argument("--restart-after", type=float, default=45.0)
    ap.add_argument("--verbose", action="store_true")
    a = ap.parse_args()
    if not a.verbose:
        quiet()
    out = Path(a.out)
    if (out / "s3").exists():
        raise SystemExit(f"{out / 's3'} exists; use a fresh --out")
    runs = []
    killed_at = None
    if a.kill or a.term:
        killed_at = kill_time(a, out, a.kill or a.term)
        h = build(a, out / "volume-root")
        if a.kill:
            h.clock.kill_at = killed_at
        else:
            h.clock.term_at = killed_at
        try:
            rc = h.run()
            runs.append({"mode": "run", "exit_code": rc, "killed": False})
        except ProcessDead:
            runs.append({"mode": "run", "exit_code": None, "killed": True})
        h.clock.restart(a.restart_after)
        rc = h.run()
        runs.append({"mode": "run (no --resume)", "exit_code": rc})
        rc = h.run(resume=True)
        runs.append({"mode": "run --resume", "exit_code": rc})
    else:
        h = build(a, out / "volume-root")
        rc = h.run()
        runs.append({"mode": "run", "exit_code": rc})
    for _ in range(a.resume_halts):
        if rc != 3:
            break
        h.clock.restart(a.restart_after)
        rc = h.run(resume=True)
        runs.append({"mode": "run --resume (after a halt)", "exit_code": rc})
    for key, (data, _) in h.s3.objects.items():
        p = out / "s3" / key
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
    m = h.manifest()
    summary = {
        "workload": a.workload, "index": a.index, "campaign_id": h.campaign_id, "full_length": a.full_length,
        "knobs": a.knob, "world": a.world, "kill": a.kill, "killed_at": killed_at, "runs": runs, "exit_code": rc,
        "status": m["status"], "objects": len(h.s3.objects), "complete": "COMPLETE" in h.s3.objects,
        "attempts": [{"attempt_key": x["attempt_key"], "run_type": x["run_type"], "outcome": x["outcome"],
                      "flags": x["flags"], "failed_checks": x["failed_checks"]} for x in m["attempts"]],
        "halts": m["halts"], "redeploys": list(h.world.redeploys),
        "ground_truth": h.world.ground_truth(m["timeline_end_at"] or h.clock.t),
        "volume_files": sorted(p.relative_to(h.volume).as_posix() for p in h.volume.rglob("*") if p.is_file()),
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=1, sort_keys=True))
    print(json.dumps({k: summary[k] for k in ("campaign_id", "exit_code", "status", "objects", "complete")}))
    return rc


if __name__ == "__main__":
    sys.exit(main())
