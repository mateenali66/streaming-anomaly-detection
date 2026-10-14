"""Command line: python3 -m streamprep {build,validate,synth} ..."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _validate(argv: list[str]) -> int:
    from .config import load_config
    from .contract import BuildError, ContractError
    from .loader import load_campaign
    from .timeline import build_timeline
    p = argparse.ArgumentParser(prog="streamprep validate",
                                description="Check one campaign against section 11 without writing outputs.")
    p.add_argument("--campaign-dir", type=Path, required=True)
    p.add_argument("--config", type=Path, default=None)
    a = p.parse_args(argv)
    try:
        c = load_campaign(a.campaign_dir, load_config(a.config))
        tl = build_timeline(c)
    except (ContractError, BuildError) as exc:
        print(f"INVALID ({type(exc).__name__})", file=sys.stderr)
        for m in exc.messages:
            print(f"  {m}", file=sys.stderr)
        return 2
    print(f"{c.campaign_id}: valid. {len(c.attempts)} attempts, {len(tl.segments)} segments, "
          f"timeline {tl.start_ms / 1000:.3f} to {tl.end_ms / 1000:.3f}")
    for w in c.warnings:
        print(f"  warning: {w}")
    return 0


def _synth(argv: list[str]) -> int:
    from .synthetic import SynthConfig, generate_campaign
    p = argparse.ArgumentParser(prog="streamprep synth", description="Write a synthetic campaign.")
    p.add_argument("--out", type=Path, required=True, help="campaign directory to create")
    p.add_argument("--truth", type=Path, default=None, help="where to write generator truth (outside the campaign)")
    p.add_argument("--workload", choices=["ecommerce", "iot"], default="ecommerce")
    p.add_argument("--campaign-index", type=int, default=1)
    p.add_argument("--scenario", choices=["full", "clean"], default="full")
    p.add_argument("--baseline-s", type=int, default=7200)
    p.add_argument("--seed", type=int, default=7)
    a = p.parse_args(argv)
    if a.out.exists() and any(a.out.iterdir()):
        print(f"{a.out} is not empty", file=sys.stderr)
        return 2
    cfg = SynthConfig(workload=a.workload, campaign_index=a.campaign_index, scenario=a.scenario,
                      baseline_s=a.baseline_s, seed=a.seed)
    truth = generate_campaign(a.out, cfg, a.truth)
    print(f"wrote {truth['campaign_id']} to {a.out}")
    return 0


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] in ("-h", "--help"):
        print("usage: python3 -m streamprep {build,validate,synth} [options]")
        return 0 if argv else 2
    cmd, rest = argv[0], argv[1:]
    if cmd == "build":
        from .build import build_cli
        return build_cli(rest)
    if cmd == "validate":
        return _validate(rest)
    if cmd == "synth":
        return _synth(rest)
    print(f"unknown command {cmd!r}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
