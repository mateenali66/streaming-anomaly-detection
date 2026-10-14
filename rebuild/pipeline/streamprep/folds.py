"""Fold roles (protocol 4.2, contract 11.9).

Leave one block out, with the R1 rotation (Table 5 of the current draft, R1 assign_folds):
fold i tests block i, calibrates on block (i mod B) + 1, tunes on block ((i + 1) mod B) + 1 and
trains on the rest. BASELINE windows are train in every fold, TRAILING windows are eval_only in
every fold. Roles are set per block, so every window of an attempt takes its block's role.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pyarrow as pa

from .contract import BuildError


def block_roles(n_blocks: int, n_folds: int) -> dict[int, dict[int, str]]:
    """fold -> block -> role."""
    if n_blocks == 1:
        # Pilot (one block). The rotation needs three blocks, so the pilot gets one fold that
        # tests its only block and trains on the baseline alone (README, A15).
        return {1: {1: "test"}}
    if n_blocks < 3:
        raise BuildError(f"{n_blocks} blocks cannot fill the test, cal and hpo_val roles")
    if n_folds != n_blocks:
        raise BuildError(f"leave-one-block-out needs n_folds == n_blocks, got {n_folds} and {n_blocks}")
    out = {}
    blocks = list(range(1, n_blocks + 1))
    for i in range(1, n_folds + 1):
        test = blocks[i - 1]
        cal = blocks[i % n_blocks]
        hpo = blocks[(i + 1) % n_blocks]
        roles = {b: "train" for b in blocks}
        roles[test], roles[cal], roles[hpo] = "test", "cal", "hpo_val"
        out[i] = roles
    return out


def assign_folds(windows: pd.DataFrame, n_blocks: int, n_folds: int) -> pd.DataFrame:
    """windows: every window of every size, with window_id, window_s, start_ms, block, segment_type."""
    roles = block_roles(n_blocks, n_folds)
    parts = []
    seg = windows["segment_type"].to_numpy()
    blk = windows["block"].to_numpy()
    for fold, by_block in roles.items():
        role = np.empty(len(windows), dtype=object)
        role[seg == "BASELINE"] = "train"
        role[seg == "TRAILING"] = "eval_only"
        att = (seg != "BASELINE") & (seg != "TRAILING")
        for b in np.unique(blk[att]):
            if int(b) not in by_block:
                raise BuildError(f"block {b} has no role in fold {fold}")
            role[att & (blk == b)] = by_block[int(b)]
        if any(r is None for r in role):
            raise BuildError(f"fold {fold}: a window received no role")
        parts.append(pd.DataFrame({
            "window_id": windows["window_id"].to_numpy(), "fold": np.int32(fold), "role": role,
            "_w": windows["window_s"].to_numpy(), "_t": windows["start_ms"].to_numpy(),
        }))
    df = pd.concat(parts, ignore_index=True)
    df = df.sort_values(["fold", "_w", "_t"], kind="stable").drop(columns=["_w", "_t"]).reset_index(drop=True)
    return df


def folds_table(df: pd.DataFrame) -> pa.Table:
    return pa.table({
        "window_id": pa.array(df["window_id"].tolist(), pa.string()),
        "fold": pa.array(df["fold"].to_numpy(np.int32), pa.int32()),
        "role": pa.array(df["role"].tolist(), pa.string()),
    })
