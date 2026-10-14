"""Contiguous sequences (protocol 4.1.7). This is the only sequence mode.

A sequence is L consecutive windows of one seq_group: one attempt's PRE_FAULT, INJECTION and
RECOVERY windows (which may cross that attempt's APPLY gap), or one baseline or trailing
stretch. It never crosses into another group, so it never crosses any other excluded segment.
Every member must also pass the caller's member mask, for example role train and label NORMAL.
"""
from __future__ import annotations

import numpy as np


def sequence_index(groups: np.ndarray, seq_index: np.ndarray, member: np.ndarray, end: np.ndarray,
                   length: int) -> tuple[np.ndarray, np.ndarray]:
    """Return (members, ends).

    members is (n_seq, length) of row positions into the input arrays, in time order. ends[k] is
    the row of the last window of sequence k, which carries the sequence's label.
    """
    n = len(groups)
    if length < 1:
        raise ValueError("sequence length must be at least 1")
    if not (len(seq_index) == len(member) == len(end) == n):
        raise ValueError("input arrays differ in length")
    if n == 0:
        return np.zeros((0, length), dtype=np.int64), np.zeros(0, dtype=np.int64)
    g = np.asarray(groups, dtype=object)
    order = np.lexsort((np.asarray(seq_index), g.astype(str)))
    gs, si, ms = g[order].astype(str), np.asarray(seq_index)[order], np.asarray(member, bool)[order]
    link = np.zeros(n, dtype=bool)
    link[1:] = ms[1:] & ms[:-1] & (gs[1:] == gs[:-1]) & (si[1:] == si[:-1] + 1)
    start = ms & ~link
    last_start = np.maximum.accumulate(np.where(start, np.arange(n), -1))
    run = np.where(ms, np.arange(n) - last_start + 1, 0)
    ok = (run >= length) & np.asarray(end, bool)[order]
    pos = np.flatnonzero(ok)
    members_sorted = pos[:, None] + np.arange(-length + 1, 1)[None, :]
    return order[members_sorted], order[pos]


def check_contiguous(groups: np.ndarray, seq_index: np.ndarray, members: np.ndarray) -> None:
    """Raise if any sequence spans two groups or skips a seq_index."""
    if len(members) == 0:
        return
    g = np.asarray(groups, dtype=object)[members]
    s = np.asarray(seq_index)[members]
    if (g != g[:, :1]).any():
        raise AssertionError("a sequence spans two seq_groups")
    if (np.diff(s, axis=1) != 1).any():
        raise AssertionError("a sequence skips a window")
