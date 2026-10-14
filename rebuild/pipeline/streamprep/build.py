"""Build one campaign end to end: labels, features and folds, written under derived/<campaign_id>/."""
from __future__ import annotations

import platform
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa

from . import __version__
from .config import config_sha256, window_sizes_for
from .contract import FEATURE_SOURCES, BuildError, ceil10, floor10, sha256_file
from .features import compute_features, prepare_features
from .folds import assign_folds, folds_table
from .labels import apply_normal_time_health
from .loader import Campaign, load_campaign
from .metrics_io import read_feature_files
from .outputs import float_column, write_json, write_parquet
from .telemetry import baseline_outage, recompute_attempt_telemetry
from .timeline import Timeline, build_timeline, timeline_table
from .windows import build_windows, labels_table

STAGES = ("labels", "features", "folds")
META_ORDER = ["meta__tm_reporting_min", "meta__tm_reporting_mean"] + \
    [f"meta__samples__{s}" for s in FEATURE_SOURCES] + ["meta__telemetry_ok"]


def _freeze_ok(c: Campaign, cfg: dict, freeze_marker: Path | None) -> str | None:
    """6.1 and 7.1: features, folds and scores of a replicate campaign wait for the freeze tag."""
    if c.campaign_index in (0, int(cfg["search_campaign_index"])):
        return None
    if freeze_marker is None or not Path(freeze_marker).is_file() or Path(freeze_marker).stat().st_size == 0:
        raise BuildError(f"campaign {c.campaign_id} (index {c.campaign_index}) is a replicate. Its features "
                         "and folds are not read until the freeze tag exists (6.1, 7.1). Pass --freeze-marker.")
    return sha256_file(Path(freeze_marker))


def windows_table(win: pd.DataFrame, cols: dict[str, np.ndarray]) -> pa.Table:
    base = labels_table(win)
    arrays = list(base.columns)
    names = list(base.column_names)
    for m in META_ORDER:
        v = cols[m]
        if m == "meta__telemetry_ok":
            arrays.append(pa.array(v.astype(bool), pa.bool_()))
        elif m.startswith("meta__samples__"):
            arrays.append(pa.array(v.astype(np.int32), pa.int32()))
        else:
            arrays.append(float_column(v))
        names.append(m)
    for f in sorted(k for k in cols if k.startswith("f__")):
        arrays.append(float_column(cols[f]))
        names.append(f)
    return pa.Table.from_arrays(arrays, names=names)


def _label_counts(df: pd.DataFrame) -> dict:
    out = {}
    for (seg, lab), n in df.groupby(["segment_type", "label"]).size().items():
        out[f"{seg}/{lab}"] = int(n)
    return out


def build_campaign(campaign_dir: Path, out_root: Path, cfg: dict, stages: tuple[str, ...] = STAGES,
                   window_sizes: list[int] | None = None, freeze_marker: Path | None = None,
                   check_against: Path | None = None, keep_step_columns: bool = False) -> dict:
    for s in stages:
        if s not in STAGES:
            raise BuildError(f"unknown stage {s!r}")
    c = load_campaign(Path(campaign_dir), cfg)
    out = Path(out_root) / "derived" / c.campaign_id
    sizes = sorted(window_sizes or window_sizes_for(cfg, c.workload, c.campaign_index))
    tl = build_timeline(c)
    windows = {w: build_windows(tl, c.campaign_id, c.workload, w, cfg) for w in sizes}
    # Decision 24 (C26): BASELINE and TRAILING windows that fail the NORMAL-time health rule are EXCLUDED.
    normal_time_health = apply_normal_time_health(c, windows)
    outputs: dict[str, str] = {}
    report: dict = {
        "pipeline_version": __version__, "contract_schema_version": "1",
        "config_sha256": config_sha256(cfg), "campaign_id": c.campaign_id, "workload": c.workload,
        "campaign_index": c.campaign_index, "window_sizes": sizes, "stages": list(stages),
        "inputs": c.input_hashes, "warnings": list(c.warnings),
        "environment": {"python": platform.python_version(), "numpy": np.__version__,
                        "pandas": pd.__version__, "pyarrow": pa.__version__},
        "attempts": {
            "count": len(c.attempts),
            "by_outcome": pd.Series([a["outcome"] for a in c.attempts]).value_counts().sort_index().to_dict(),
        },
        "segments": tl.segments.groupby("segment_type").size().sort_index().astype(int).to_dict(),
        "windows": {str(w): _label_counts(df) for w, df in windows.items()},
        "normal_time_health": normal_time_health,
    }
    if "labels" in stages:
        outputs["timeline.parquet"] = write_parquet(timeline_table(tl), out / "timeline.parquet")
        for w, df in windows.items():
            outputs[f"labels_{w}s.parquet"] = write_parquet(labels_table(df), out / f"labels_{w}s.parquet")
    result = None
    if "features" in stages or "folds" in stages:
        fz = _freeze_ok(c, cfg, freeze_marker)
        if fz:
            report["freeze_marker_sha256"] = fz
    if "features" in stages:
        t0 = floor10(tl.start_ms)
        n_steps = (ceil10(tl.end_ms) - t0) // 10_000 + 1
        lm = read_feature_files(c, t0, n_steps, cfg)
        ctx = prepare_features(lm, tl, cfg)
        fr = ctx.report
        violations, ref_units = recompute_attempt_telemetry(c, tl, lm, ctx, cfg)
        fr.telemetry_violations, fr.reference_units = violations, ref_units
        if violations and cfg["telemetry"]["feature_recompute"] == "error":
            raise BuildError(["valid attempts fail the scrape-health telemetry rule (decision 21) on the exported "
                              "verification files:"] + violations[:20])
        w_ref = 30 if 30 in windows else sizes[0]
        n_cols, not_ok = {}, {}
        step_columns = None
        # One window size at a time, so only one feature table is in memory.
        for w in [w_ref] + [x for x in sizes if x != w_ref]:
            res = compute_features(lm, tl, {w: windows[w]}, cfg, keep_step_columns=keep_step_columns and w == w_ref,
                                   ctx=ctx)
            cols = res.per_w[w]
            if w == w_ref:
                fr.baseline_outage_windows, fr.baseline_not_ok_windows = baseline_outage(
                    cols, windows[w]["segment_type"].to_numpy(), windows[w]["label"].to_numpy())
                if fr.baseline_outage_windows and cfg["telemetry"]["baseline_outage"] == "error":
                    raise BuildError(f"{fr.baseline_outage_windows} BASELINE windows at {w} s hold no Flink data. "
                                     "They would be NORMAL training windows (3.3). Decide before building.")
                step_columns = res.step_columns
                result = res
            n_cols[str(w)] = sum(1 for k in cols if k.startswith("f__"))
            not_ok[str(w)] = int((~cols["meta__telemetry_ok"]).sum())
            outputs[f"windows_{w}s.parquet"] = write_parquet(windows_table(windows[w], cols), out / f"windows_{w}s.parquet")
            del res, cols
        result.step_columns = step_columns
        report["features"] = {
            "n_series": fr.n_series, "n_counter_series": fr.n_counter_series,
            "n_columns_direct": fr.n_columns_direct, "n_tm_groups": fr.n_tm_groups,
            "n_feature_columns": n_cols,
            "collisions_outside_windows": fr.collisions_outside_windows,
            "monotone_levels": fr.monotone_levels, "reference_units": fr.reference_units,
            "telemetry_violations": fr.telemetry_violations,
            "baseline_outage_windows": fr.baseline_outage_windows,
            "baseline_not_ok_windows": fr.baseline_not_ok_windows,
            "export_duplicates_dropped": lm.duplicates_dropped, "series_id_form": lm.series_id_form,
            "telemetry_not_ok_windows": not_ok,
        }
    if "folds" in stages:
        allw = pd.concat([df for df in windows.values()], ignore_index=True)
        fdf = assign_folds(allw, len(c.plan["blocks"]), int(cfg["folds"]["n_folds"]) if len(c.plan["blocks"]) > 1 else 1)
        outputs["folds.parquet"] = write_parquet(folds_table(fdf), out / "folds.parquet")
    report["outputs"] = outputs
    if check_against is not None:
        bad = []
        for name, sha in outputs.items():
            ref = Path(check_against) / (name + ".sha256")
            if ref.is_file() and ref.read_text().strip() != sha:
                bad.append(f"{name}: {sha} differs from the committed {ref.read_text().strip()}")
        if bad:
            raise BuildError(["outputs differ from the committed hashes (3.4.5, 7.1):"] + bad)
    write_json(report, out / "build_report.json")
    report["_result"] = result
    report["_timeline"] = tl
    report["_windows"] = windows
    report["_campaign"] = c
    report["_out"] = out
    return report


def build_cli(argv: list[str]) -> int:
    import argparse
    from .config import load_config
    from .contract import ContractError
    p = argparse.ArgumentParser(prog="streamprep build", description="Build one campaign end to end.")
    p.add_argument("--campaign-dir", type=Path, required=True,
                   help="local mirror of campaign/<campaign_id>/")
    p.add_argument("--out", type=Path, required=True, help="root; files go to <out>/derived/<campaign_id>/")
    p.add_argument("--config", type=Path, default=None, help="JSON overrides for config/pipeline.json")
    p.add_argument("--stage", choices=STAGES + ("all",), default="all")
    p.add_argument("--window-sizes", type=int, nargs="+", default=None)
    p.add_argument("--freeze-marker", type=Path, default=None)
    p.add_argument("--check-against", type=Path, default=None,
                   help="directory of committed .sha256 files to compare outputs with")
    a = p.parse_args(argv)
    cfg = load_config(a.config)
    stages = STAGES if a.stage == "all" else (a.stage,)
    try:
        rep = build_campaign(a.campaign_dir, a.out, cfg, stages, a.window_sizes, a.freeze_marker, a.check_against)
    except (ContractError, BuildError) as exc:
        print(f"BUILD FAILED ({type(exc).__name__})", file=sys.stderr)
        for m in exc.messages:
            print(f"  {m}", file=sys.stderr)
        return 2
    print(f"built {rep['campaign_id']} into {rep['_out']}")
    for name, sha in sorted(rep["outputs"].items()):
        print(f"  {sha}  {name}")
    for wmsg in rep["warnings"]:
        print(f"  warning: {wmsg}")
    return 0
