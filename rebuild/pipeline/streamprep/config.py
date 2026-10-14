"""Pipeline configuration. Defaults live in config/pipeline.json next to this package."""
from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from .contract import canonical_json_bytes, sha256_bytes

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "pipeline.json"


def _merge(base: dict, override: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def load_config(path: Path | None = None, overrides: dict | None = None) -> dict:
    cfg = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
    if path is not None:
        cfg = _merge(cfg, json.loads(Path(path).read_text(encoding="utf-8")))
    if overrides:
        cfg = _merge(cfg, overrides)
    return cfg


def config_sha256(cfg: dict) -> str:
    return sha256_bytes(canonical_json_bytes(cfg))


def window_sizes_for(cfg: dict, workload: str, campaign_index: int) -> list[int]:
    """11.9: w = 30 on every campaign, plus 15, 60, 120 (and 300 on IoT) on campaign 1."""
    sizes = set(int(w) for w in cfg["window_sizes"]["every_campaign"])
    if campaign_index == int(cfg["search_campaign_index"]):
        sizes |= set(int(w) for w in cfg["window_sizes"]["search_campaign"][workload])
    return sorted(sizes)


def get(cfg: dict, dotted: str) -> Any:
    cur: Any = cfg
    for part in dotted.split("."):
        cur = cur[part]
    return cur
