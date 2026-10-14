"""Object store for the campaign prefix (11.7). Keys are always relative to campaign/<campaign_id>/.

S3 notes (boto3 docs, context7 /boto/boto3): errors surface as botocore.exceptions.ClientError with
e.response["Error"]["Code"]; list_objects_v2 returns at most 1,000 keys per page, so the paginator is used, and
each entry carries Key, Size and LastModified. The infra IAM policy limits ListBucket to the campaign prefix,
so a HEAD or GET of a missing key can return 403 instead of 404 (rebuild/infra/terraform/cluster/iam.tf note).
Existence is therefore always tested with a prefix listing, never with HEAD.

Uploads send ChecksumSHA256 so S3 verifies the bytes it stores against the hash the orchestrator records.
"""
from __future__ import annotations

import base64
import hashlib
import os
from dataclasses import dataclass
from pathlib import Path

from .errors import StoreError


@dataclass(frozen=True)
class ObjectInfo:
    key: str
    size: int
    last_modified: float


class ObjectStore:
    def put(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> None: ...
    def get(self, key: str) -> bytes: ...
    def list(self, prefix: str = "") -> list[ObjectInfo]: ...

    def exists(self, key: str) -> bool:
        return any(o.key == key for o in self.list(key))


def parse_s3_prefix(url: str) -> tuple[str, str]:
    if not url.startswith("s3://"):
        raise StoreError(f"RESULTS_PREFIX must be an s3:// URL, got {url!r}")
    rest = url[5:]
    bucket, _, prefix = rest.partition("/")
    prefix = prefix.strip("/")
    if not bucket or not prefix:
        raise StoreError(f"RESULTS_PREFIX needs a bucket and a prefix: {url!r}")
    return bucket, prefix + "/"


class S3Store(ObjectStore):
    def __init__(self, results_prefix: str, client=None) -> None:
        import boto3  # baked into the image; imported lazily so the offline tests need no AWS libraries
        self.bucket, self.prefix = parse_s3_prefix(results_prefix)
        self.s3 = client or boto3.client("s3")

    def put(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
        from botocore.exceptions import BotoCoreError, ClientError
        checksum = base64.b64encode(hashlib.sha256(data).digest()).decode("ascii")
        try:
            self.s3.put_object(Bucket=self.bucket, Key=self.prefix + key, Body=data, ContentType=content_type,
                               ChecksumSHA256=checksum)
        except (ClientError, BotoCoreError) as e:
            raise StoreError(f"put {key} failed: {e}") from e

    def get(self, key: str) -> bytes:
        from botocore.exceptions import BotoCoreError, ClientError
        try:
            return self.s3.get_object(Bucket=self.bucket, Key=self.prefix + key)["Body"].read()
        except (ClientError, BotoCoreError) as e:
            raise StoreError(f"get {key} failed: {e}") from e

    def list(self, prefix: str = "") -> list[ObjectInfo]:
        from botocore.exceptions import BotoCoreError, ClientError
        out = []
        try:
            pages = self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=self.prefix + prefix)
            for page in pages:
                for o in page.get("Contents", []):
                    out.append(ObjectInfo(o["Key"][len(self.prefix):], int(o["Size"]), o["LastModified"].timestamp()))
        except (ClientError, BotoCoreError) as e:
            raise StoreError(f"list {prefix} failed: {e}") from e
        return out


class LocalStore(ObjectStore):
    """A directory standing in for the campaign prefix. Used by the offline gate and the dry run."""

    def __init__(self, root: str | os.PathLike) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def put(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
        p = self.root / key
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_bytes(data)
        os.replace(tmp, p)

    def get(self, key: str) -> bytes:
        try:
            return (self.root / key).read_bytes()
        except FileNotFoundError as e:
            raise StoreError(f"get {key}: not found") from e

    def list(self, prefix: str = "") -> list[ObjectInfo]:
        out = []
        for p in self.root.rglob("*"):
            if p.is_file() and not p.name.endswith(".tmp"):
                key = p.relative_to(self.root).as_posix()
                if key.startswith(prefix):
                    st = p.stat()
                    out.append(ObjectInfo(key, st.st_size, st.st_mtime))
        return sorted(out, key=lambda o: o.key)
