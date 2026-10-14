"""Campaign manifest persistence (protocol 11.4).

Written after every state change to a temporary file on the campaign-manifest volume, fsynced, renamed into place,
then uploaded. The volume copy is the source of truth during the campaign. Every write is validated against the
section 11 schema first, so a malformed manifest can never reach S3.

Per-attempt facts (the parts of the verification record that only the online orchestrator can observe: pods,
targets, controller, chaos snapshots, F5/F6/F8 details) are also kept on the volume as they are collected, so a
SIGKILL never loses them and a resumed orchestrator can still write the verification record of an ABORTED attempt.

Volume layout (protocol 11.4 and 11.7 as clarified in rebuild/CONTRACT_CHANGES.md, item C1):

  /manifest/manifest.json        the campaign manifest; the only file that is part of the record. The orchestrator
                                 uploads it to manifest/manifest.json after every write, and infra's s3-sync sidecar
                                 mirrors this one file and nothing else.
  /manifest/state/               internal resume state, never uploaded, never listed in SHA256SUMS:
      plan.json                  local copy of the plan (a start before the plan reached S3)
      pending.json               the AFTER_REDEPLOY carry-over to the next attempt
      run.lock                   the previous process's lock (crash detection)
      campaign.log               the log file; its copies go to logs/orchestrator-<start_epoch>.log.gz
      attempts/<key>/facts.json  per-attempt facts; every field reaches S3 inside attempts/<key>/verification.json
  /manifest/.s3sync.status       written by the sidecar
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path

from .canonical import canonical_bytes, sha256_hex
from .errors import HaltRequested, OrchestratorError
from .schema import validate_manifest

log = logging.getLogger("orchestrator.manifest")
MANIFEST_KEY = "manifest/manifest.json"
MANIFEST_FILE = "manifest.json"
STATE_DIR = "state"


def state_dir(volume: str | os.PathLike) -> Path:
    """Internal resume state on the volume. Never synced to S3 and never part of the record (CONTRACT_CHANGES C1)."""
    return Path(volume) / STATE_DIR


def atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    try:
        dfd = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    except OSError:
        pass


class Uploader:
    """S3 uploads with the halt rule of 2.6.5: an upload that keeps failing for 15 minutes halts the campaign."""

    def __init__(self, store, clock, halt_after_s: float) -> None:
        self.store, self.clock, self.halt_after_s = store, clock, halt_after_s
        self.failing_since: float | None = None
        self.stopping = False  # set by the halt path: deferred uploads never raise again while it cleans up

    def put_blocking(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
        """Retries until the object is stored or 15 minutes of failure have passed (then halts)."""
        delay = 2.0
        while True:
            try:
                self.store.put(key, data, content_type)
                self.failing_since = None
                return sha256_hex(data)
            except OrchestratorError as e:
                now = self.clock.now()
                if self.failing_since is None:
                    self.failing_since = now
                log.warning("upload of %s failed: %s", key, e)
                if now - self.failing_since >= self.halt_after_s:
                    raise HaltRequested("S3_UPLOAD_FAILING_15_MIN")
                self.clock.sleep(delay)
                delay = min(delay * 2, 60.0)

    def put_deferred(self, key: str, data: bytes, content_type: str = "application/json") -> bool:
        """One try. A failure is remembered; failures that span 15 minutes halt at the next call."""
        try:
            self.store.put(key, data, content_type)
            self.failing_since = None
            return True
        except OrchestratorError as e:
            now = self.clock.now()
            if self.failing_since is None:
                self.failing_since = now
            log.warning("deferred upload of %s failed: %s", key, e)
            if now - self.failing_since >= self.halt_after_s and not self.stopping:
                raise HaltRequested("S3_UPLOAD_FAILING_15_MIN")
            return False


class ManifestStore:
    def __init__(self, volume: str | os.PathLike, uploader: Uploader, clock) -> None:
        self.dir = Path(volume)
        self.path = self.dir / MANIFEST_FILE
        self.state = state_dir(volume)
        self.uploader = uploader
        self.clock = clock
        self.dirty = False

    def load(self) -> dict | None:
        if not self.path.exists():
            return None
        m = json.loads(self.path.read_text(encoding="utf-8"))
        validate_manifest(m)
        return m

    def save(self, m: dict, validate: bool = True) -> None:
        m["updated_at"] = round(self.clock.now(), 3)
        if validate:
            validate_manifest(m)
        data = canonical_bytes(m)
        atomic_write(self.path, data)
        self.dirty = not self.uploader.put_deferred(MANIFEST_KEY, data)

    def flush(self) -> None:
        """Called at safe points: re-upload the volume copy if the last upload failed."""
        if self.dirty and self.path.exists():
            self.dirty = not self.uploader.put_deferred(MANIFEST_KEY, self.path.read_bytes())

    def final_upload(self) -> str:
        data = self.path.read_bytes()
        return self.uploader.put_blocking(MANIFEST_KEY, data, "application/json")

    # ------------------------------------------------------------------------------- per-attempt facts
    def facts_path(self, akey: str) -> Path:
        return self.state / "attempts" / akey / "facts.json"

    def save_facts(self, akey: str, facts: dict) -> None:
        atomic_write(self.facts_path(akey), json.dumps(facts, sort_keys=True, allow_nan=False).encode("utf-8"))

    def load_facts(self, akey: str) -> dict:
        p = self.facts_path(akey)
        return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}

    # ------------------------------------------------------------------------------- run lock
    @property
    def lock_path(self) -> Path:
        return self.state / "run.lock"

    def take_lock(self, info: dict) -> dict | None:
        """Returns the previous lock if a process ended without a clean shutdown (SIGKILL, node loss)."""
        prev = None
        if self.lock_path.exists():
            try:
                prev = json.loads(self.lock_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                prev = {"corrupt": True}
        atomic_write(self.lock_path, json.dumps(info, sort_keys=True).encode("utf-8"))
        return prev

    def release_lock(self) -> None:
        try:
            self.lock_path.unlink()
        except FileNotFoundError:
            pass
