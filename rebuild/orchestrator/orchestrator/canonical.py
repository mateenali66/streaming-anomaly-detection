"""Canonical JSON and hashing (protocol 11.1 items 4 and 5)."""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any


def canonical_bytes(obj: Any) -> bytes:
    """UTF-8 bytes of json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).

    This is the exact definition of canonical JSON in protocol 11.1.5. allow_nan=False makes a NaN or an
    infinity raise ValueError, so a non-finite number can never be written into a record.
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def canonical_str(obj: Any) -> str:
    return canonical_bytes(obj).decode("utf-8")


def sha256_hex(data: bytes) -> str:
    """Lower-case hex SHA-256, 64 characters (11.1.4)."""
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def is_sha256_hex(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def ms(value: float) -> float:
    """Round an instant to millisecond precision (11.1.1)."""
    if not math.isfinite(value):
        raise ValueError(f"non-finite instant {value!r}")
    return round(float(value), 3)
