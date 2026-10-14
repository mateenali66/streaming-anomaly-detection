"""Per-segment window grids and window labels (protocol 3.2, 3.3 and contract 11.9).

Each windowed segment has its own grid. PRE_FAULT runs backwards from its end (the planned
onset), INJECTION forwards from the verified onset, RECOVERY forwards from the labelled end,
BASELINE and TRAILING forwards from their own start. A window that would cross the segment
boundary is dropped. Grids are computed in integer milliseconds, so no float drift.

within_run (Review 4 MIN-9, decision 27) tags the within-run negatives that every model shares: the
last n_late + n_early PRE_FAULT windows of every valid attempt (label NORMAL), counted back from the
planned onset. The last n_late are "late", the n_early before them "early". Every other window is
null. The numbers come from config/pipeline.json evaluation.within_run.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pyarrow as pa

from .contract import WINDOWED_SEGMENT_TYPES, BuildError, canonical_json_bytes
from .timeline import Timeline

KEY_COLUMNS = (
    "window_id", "campaign_id", "workload", "window_s", "start_at", "end_at", "segment_type",
    "label", "attempt_key", "run_id", "cell_key", "block", "position", "run_type", "outcome",
    "flags", "seq_group", "seq_index", "within_run",
)


def within_run_tags(df: pd.DataFrame, cfg: dict) -> np.ndarray:
    """The within_run value of every row of a windows table: "late", "early" or None.

    Rows are a PRE_FAULT window of a valid attempt (label NORMAL). Rank 1 is the window that ends at
    the planned onset. The rule needs only the table itself, so train.py can check a windows file
    against the configuration it uses.
    """
    wr = cfg["evaluation"]["within_run"]
    n_late, n_early = int(wr["n_late"]), int(wr["n_early"])
    out = np.full(len(df), None, dtype=object)
    pre = (df["segment_type"].to_numpy() == "PRE_FAULT") & (df["label"].to_numpy() == "NORMAL")
    if not pre.any():
        return out
    sub = df.loc[pre, ["attempt_key", "start_at"]]
    rank = sub.groupby("attempt_key", sort=False)["start_at"].rank(method="first", ascending=False).to_numpy()
    tags = np.where(rank <= n_late, "late", np.where(rank <= n_late + n_early, "early", None))
    out[np.flatnonzero(pre)] = tags
    return out


def grid_for_segment(segment_type: str, start_ms: int, end_ms: int, w_s: int) -> list[tuple[int, int]]:
    w = w_s * 1000
    n = (end_ms - start_ms) // w
    if n <= 0:
        return []
    if segment_type == "PRE_FAULT":
        return [(end_ms - (k + 1) * w, end_ms - k * w) for k in range(n)][::-1]
    return [(start_ms + k * w, start_ms + (k + 1) * w) for k in range(n)]


def build_windows(tl: Timeline, campaign_id: str, workload: str, w_s: int, cfg: dict | None = None) -> pd.DataFrame:
    if cfg is None:
        from .config import load_config  # build.py does not pass its config yet (B_pipeline.md, interface changes)
        cfg = load_config()
    rows = []
    stretch = {"BASELINE": 0, "TRAILING": 0}
    for seg in tl.segments.itertuples(index=False):
        if seg.segment_type not in WINDOWED_SEGMENT_TYPES:
            continue
        grid = grid_for_segment(seg.segment_type, seg.start_ms, seg.end_ms, w_s)
        if seg.segment_type in ("BASELINE", "TRAILING"):
            group = f"{seg.segment_type.lower()}-{stretch[seg.segment_type]}"
            stretch[seg.segment_type] += 1
            run_type, attempt_key, run_id, cell_key = seg.segment_type, None, None, None
            block = position = 0
            outcome, flags = None, "[]"
        else:
            group = seg.attempt_key
            run_type, attempt_key, run_id, cell_key = seg.run_type, seg.attempt_key, seg.run_id, seg.cell_key
            block, position, outcome = int(seg.block), int(seg.position), seg.outcome
            flags = canonical_json_bytes(list(seg.flags)).decode("utf-8")
        for a, b in grid:
            if a < seg.start_ms or b > seg.end_ms:  # cannot happen by construction; kept as a guard
                raise BuildError(f"window {a}-{b} crosses segment {seg.segment_type} of {seg.attempt_key}")
            rows.append({
                "window_id": f"{campaign_id}-{w_s}-{a}",
                "campaign_id": campaign_id, "workload": workload, "window_s": w_s,
                "start_ms": a, "end_ms": b, "segment_type": seg.segment_type, "label": seg.label,
                "attempt_key": attempt_key, "run_id": run_id, "cell_key": cell_key,
                "block": block, "position": position, "run_type": run_type, "outcome": outcome,
                "flags": flags, "seq_group": group, "segment_index": int(seg.segment_index),
            })
    df = pd.DataFrame(rows)
    if df.empty:
        raise BuildError(f"no windows at {w_s} s")
    df = df.sort_values("start_ms", kind="stable").reset_index(drop=True)
    if df["window_id"].duplicated().any():
        raise BuildError(f"duplicate window_id at {w_s} s")
    if (df["start_ms"].to_numpy()[1:] < df["end_ms"].to_numpy()[:-1]).any():
        raise BuildError(f"overlapping windows at {w_s} s")
    df["seq_index"] = df.groupby("seq_group", sort=False).cumcount().astype(np.int32)
    df["start_at"] = df["start_ms"] / 1000.0
    df["end_at"] = df["end_ms"] / 1000.0
    df["within_run"] = within_run_tags(df, cfg)
    return df


def labels_table(df: pd.DataFrame) -> pa.Table:
    """The key and meta columns of 11.9, in contract order and types."""
    return pa.table({
        "window_id": pa.array(df["window_id"].tolist(), pa.string()),
        "campaign_id": pa.array(df["campaign_id"].tolist(), pa.string()),
        "workload": pa.array(df["workload"].tolist(), pa.string()),
        "window_s": pa.array(df["window_s"].to_numpy(), pa.int32()),
        "start_at": pa.array(df["start_at"].to_numpy(), pa.float64()),
        "end_at": pa.array(df["end_at"].to_numpy(), pa.float64()),
        "segment_type": pa.array(df["segment_type"].tolist(), pa.string()),
        "label": pa.array(df["label"].tolist(), pa.string()),
        "attempt_key": pa.array(df["attempt_key"].tolist(), pa.string()),
        "run_id": pa.array(df["run_id"].tolist(), pa.string()),
        "cell_key": pa.array(df["cell_key"].tolist(), pa.string()),
        "block": pa.array(df["block"].to_numpy(), pa.int32()),
        "position": pa.array(df["position"].to_numpy(), pa.int32()),
        "run_type": pa.array(df["run_type"].tolist(), pa.string()),
        "outcome": pa.array(df["outcome"].tolist(), pa.string()),
        "flags": pa.array(df["flags"].tolist(), pa.string()),
        "seq_group": pa.array(df["seq_group"].tolist(), pa.string()),
        "seq_index": pa.array(df["seq_index"].to_numpy(), pa.int32()),
        "within_run": pa.array(df["within_run"].tolist(), pa.string()),
    })
