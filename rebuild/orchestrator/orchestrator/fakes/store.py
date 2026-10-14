"""In-memory S3 stand-in for one campaign prefix: versioned, with LastModified, write-role restrictions (11.7) and
injectable upload failures."""
from __future__ import annotations

from ..errors import StoreError
from ..store import ObjectInfo, ObjectStore


class FakeS3(ObjectStore):
    def __init__(self, clock, world=None) -> None:
        self.clock = clock
        self.world = world
        self.objects: dict[str, tuple[bytes, float]] = {}
        self.versions: dict[str, list[bytes]] = {}
        self.fail_put_until: float | None = None
        self.puts = 0
        self.put_log: list[str] = []
        self.kill_after_put = None  # predicate(key) -> bool: simulate SIGKILL right after this object is stored
        self.on_put = None  # callable(key, data): the harness lets the world react to the orchestrator's own records

    def _alive(self) -> None:
        if self.world is not None and self.world.dead:
            from .clock import ProcessDead
            raise ProcessDead("process is dead")

    def put(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
        self._alive()
        if self.fail_put_until is not None and self.clock.t < self.fail_put_until:
            raise StoreError(f"put {key}: simulated S3 outage")
        if key.startswith("/") or ".." in key:
            raise StoreError(f"bad key {key}")
        self.puts += 1
        self.objects[key] = (bytes(data), self.clock.t)
        self.versions.setdefault(key, []).append(bytes(data))
        self.put_log.append(key)
        if self.on_put is not None:
            self.on_put(key, data)
        if self.kill_after_put is not None and self.kill_after_put(key):
            self.kill_after_put = None
            from .clock import ProcessDead
            if self.world is not None:
                self.world.dead = True
            raise ProcessDead(f"SIGKILL right after storing {key}")

    def get(self, key: str) -> bytes:
        self._alive()
        if key not in self.objects:
            raise StoreError(f"get {key}: NoSuchKey")
        return self.objects[key][0]

    def list(self, prefix: str = "") -> list[ObjectInfo]:
        self._alive()
        return sorted((ObjectInfo(k, len(v[0]), v[1]) for k, v in self.objects.items() if k.startswith(prefix)),
                      key=lambda o: o.key)


class ExporterView(ObjectStore):
    """The exporter's IAM role may write only baseline/ and export/ (11.7, rebuild/infra/terraform/cluster/iam.tf)."""

    def __init__(self, s3: FakeS3) -> None:
        self.s3 = s3

    def put(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
        if not (key.startswith("baseline/") or key.startswith("export/")):
            raise StoreError(f"exporter role may not write {key}")
        self.s3.put(key, data, content_type)

    def get(self, key: str) -> bytes:
        raise StoreError("exporter role has no GetObject")

    def list(self, prefix: str = ""):
        return self.s3.list(prefix)
