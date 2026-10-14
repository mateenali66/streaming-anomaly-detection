"""Small JSON-over-HTTP client for Flink REST, the load generator, the verification consumer and the probe service."""
from __future__ import annotations

import json
import urllib.error
import urllib.request

from .errors import QueryError


class HttpJson:
    def get(self, url: str, timeout_s: float = 5.0):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={"Accept": "application/json"}),
                                        timeout=timeout_s) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, ConnectionError, json.JSONDecodeError, ValueError) as e:
            raise QueryError(f"GET {url} failed: {e}") from e

    def patch(self, url: str, timeout_s: float = 10.0):
        req = urllib.request.Request(url, method="PATCH", headers={"Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                body = resp.read().decode("utf-8")
                return json.loads(body) if body.strip() else {}
        except (urllib.error.URLError, TimeoutError, ConnectionError, json.JSONDecodeError) as e:
            raise QueryError(f"PATCH {url} failed: {e}") from e
