"""Prometheus HTTP API client.

Docs relied on (prometheus.io/docs/prometheus/latest/querying/api, context7 /websites/prometheus_io):
  GET /api/v1/query (instant, resultType vector or scalar), GET /api/v1/query_range (resultType matrix,
  "values": [[<unix_time>, "<value>"], ...], start and end inclusive, step in seconds), GET /api/v1/metadata
  (data is metric name -> list of {type, help, unit}). Every response has status "success" or "error";
  400, 422 and 503 carry a JSON error object. Sample values are strings and may be "NaN" or "+Inf".

Every failure raises QueryError. An empty result is returned as an empty list and never as zero; the caller
decides what missing means, and for the gate it always means fail (2.4).
"""
from __future__ import annotations

import json
import math
import time
import urllib.error
import urllib.parse
import urllib.request

from .errors import QueryError

Sample = tuple[dict, float]
Series = tuple[dict, list[tuple[float, float]]]


class Prom:
    def instant(self, query: str, at: float | None = None) -> list[Sample]: ...
    def scalar(self, query: str, at: float | None = None) -> float: ...
    def range(self, query: str, start: float, end: float, step: int) -> list[Series]: ...
    def metadata(self) -> dict[str, list[dict]]: ...


def _value(v: str) -> float:
    return float(v)  # float("NaN") and float("+Inf") parse; callers drop NaN where the contract says so


class HttpProm(Prom):
    def __init__(self, base_url: str, timeout_s: float = 60.0, retries: int = 3) -> None:
        self.base = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self.retries = retries

    def _get(self, path: str, params: dict) -> dict:
        url = f"{self.base}{path}?{urllib.parse.urlencode(params)}"
        last: Exception | None = None
        for i in range(self.retries):
            try:
                with urllib.request.urlopen(urllib.request.Request(url, headers={"Accept": "application/json"}),
                                            timeout=self.timeout_s) as resp:
                    body = json.loads(resp.read().decode("utf-8"))
                break
            except urllib.error.HTTPError as e:
                try:
                    body = json.loads(e.read().decode("utf-8"))
                except Exception:
                    body = None
                if e.code in (400, 422) and body:
                    raise QueryError(f"Prometheus rejected query ({e.code} {body.get('errorType')}): {body.get('error')}")
                last = e
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, ConnectionError) as e:
                last = e
            time.sleep(min(2 ** i, 5))
        else:
            raise QueryError(f"Prometheus {path} failed after {self.retries} tries: {last}")
        if body.get("status") != "success":
            raise QueryError(f"Prometheus {path} status={body.get('status')} {body.get('errorType')}: {body.get('error')}")
        return body["data"]

    def instant(self, query: str, at: float | None = None) -> list[Sample]:
        params = {"query": query}
        if at is not None:
            params["time"] = f"{at:.3f}"
        data = self._get("/api/v1/query", params)
        if data.get("resultType") != "vector":
            raise QueryError(f"expected a vector for {query!r}, got {data.get('resultType')}")
        return [(r["metric"], _value(r["value"][1])) for r in data["result"]]

    def scalar(self, query: str, at: float | None = None) -> float:
        params = {"query": query}
        if at is not None:
            params["time"] = f"{at:.3f}"
        data = self._get("/api/v1/query", params)
        if data.get("resultType") != "scalar":
            raise QueryError(f"expected a scalar for {query!r}, got {data.get('resultType')}")
        v = _value(data["result"][1])
        if not math.isfinite(v):
            raise QueryError(f"non-finite scalar for {query!r}")
        return v

    def range(self, query: str, start: float, end: float, step: int) -> list[Series]:
        if (end - start) / step > 11000:
            raise QueryError("range query would exceed 11,000 points per series")
        data = self._get("/api/v1/query_range", {"query": query, "start": f"{start:.3f}", "end": f"{end:.3f}",
                                                  "step": str(step)})
        if data.get("resultType") != "matrix":
            raise QueryError(f"expected a matrix for {query!r}, got {data.get('resultType')}")
        out = []
        for r in data["result"]:
            if r.get("histograms"):
                raise QueryError(f"native histograms are not supported in the export ({query!r})")
            out.append((r["metric"], [(float(t), _value(v)) for t, v in r.get("values", [])]))
        return out

    def metadata(self) -> dict[str, list[dict]]:
        return self._get("/api/v1/metadata", {})
