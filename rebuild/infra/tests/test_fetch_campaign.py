#!/usr/bin/env python3
"""Offline test of scripts/fetch_campaign.py (REVIEW4 B7) with an in-memory S3 stand-in. No AWS call."""
import hashlib
import io
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import fetch_campaign as fc  # noqa: E402

fails = 0


def check(name, cond, extra=""):
    global fails
    print(("ok   " if cond else "FAIL ") + name + ("" if cond else f"  {extra}"))
    fails += 0 if cond else 1


class S3:
    """list_objects_v2 paginator and get_object over a dict key -> bytes."""
    def __init__(self, objects): self.objects = objects
    def get_paginator(self, op):
        outer = self
        class P:
            def paginate(self, Bucket, Prefix):
                keys = sorted(k for k in outer.objects if k.startswith(Prefix))
                yield {"Contents": [{"Key": k, "Size": len(outer.objects[k])} for k in keys[:2]]}
                yield {"Contents": [{"Key": k, "Size": len(outer.objects[k])} for k in keys[2:]]}
        return P()
    def get_object(self, Bucket, Key): return {"Body": io.BytesIO(self.objects[Key])}


def sha(b): return hashlib.sha256(b).hexdigest()


def campaign(cid="ec-a", marker=True, tamper=None, extra=None, marker_override=None):
    p = f"campaign/{cid}/"
    files = {"plan/plan.json": b'{"plan":1}', "manifest/manifest.json": b'{"status":"COMPLETE"}',
             "attempts/a1/verification.json": b'{"ok":true}', "export/attempts/a1/features.parquet": b"PAR1" + b"x" * 100}
    sums = "".join(f"{sha(v)}  {k}\n" for k, v in sorted(files.items())).encode()
    objs = {p + k: v for k, v in files.items()}
    objs[p + "checksums/SHA256SUMS"] = sums
    if marker:
        m = {"schema_version": "1", "campaign_id": cid, "status": "COMPLETE", "plan_sha256": sha(files["plan/plan.json"]),
             "manifest_sha256": sha(files["manifest/manifest.json"]), "checksums_key": "checksums/SHA256SUMS",
             "checksums_sha256": sha(sums), "object_count": len(files), "total_bytes": sum(len(v) for v in files.values())}
        m.update(marker_override or {})
        objs[p + "COMPLETE"] = json.dumps(m).encode()
    if tamper:
        objs[p + tamper] = b"changed"
    if extra:
        objs[p + extra] = b"surprise"
    return objs


def run(objs, cid="ec-a"):
    out = Path(tempfile.mkdtemp()) / "copy"
    import contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = fc.fetch(S3(objs), "bucket", cid, out)
    return rc, buf.getvalue(), out


rc, txt, out = run(campaign())
check("finished campaign: every check passes", rc == 0 and "WARNING" not in txt, txt)
check("the copy holds the S3 layout and nothing else", sorted(str(p.relative_to(out)) for p in out.rglob("*") if p.is_file()) ==
      sorted(["COMPLETE", "attempts/a1/verification.json", "checksums/SHA256SUMS", "export/attempts/a1/features.parquet",
              "manifest/manifest.json", "plan/plan.json"]))
check("the local hash list sits NEXT TO the copy", fc.local_sums(out).exists() and len(fc.local_sums(out).read_text().splitlines()) == 6)
rc, txt, _ = run(campaign(tamper="attempts/a1/verification.json"))
check("a changed object fails its SHA-256", rc == 1 and "SHA-256 differs: attempts/a1/verification.json" in txt, txt)
rc, txt, _ = run(campaign(extra="attempts/a2/verification.json"))
check("an object missing from SHA256SUMS fails", rc == 1 and "not in SHA256SUMS" in txt, txt)
rc, txt, _ = run(campaign(marker_override={"checksums_sha256": "0" * 64}))
check("COMPLETE checksums_sha256 that does not match fails", rc == 1 and "checksums_sha256" in txt, txt)
rc, txt, _ = run(campaign(marker_override={"manifest_sha256": "0" * 64}))
check("COMPLETE manifest_sha256 that does not match fails", rc == 1 and "manifest_sha256" in txt, txt)
rc, txt, _ = run(campaign(marker_override={"object_count": 99}))
check("COMPLETE object_count that does not match fails", rc == 1 and "object_count" in txt, txt)
rc, txt, _ = run(campaign(marker_override={"campaign_id": "ec-b"}))
check("COMPLETE of another campaign fails", rc == 1 and "names campaign" in txt, txt)
pilot = {k.replace("ec-a", "pilot-ec"): v for k, v in campaign(marker=False).items() if "SHA256SUMS" not in k}
rc, txt, _ = run(pilot, "pilot-ec")
check("pilot without COMPLETE or SHA256SUMS: exit 0 with a clear warning", rc == 0 and "WARNING no COMPLETE marker" in txt and "NOT verified" in txt, txt)
manual = campaign(marker=False)
manual["campaign/ec-a/COMPLETE"] = json.dumps({"campaign_id": "ec-a", "kind": "manual-marker", "status": "ABANDONED", "author_confirmed": True}).encode()
rc, txt, _ = run(manual)
check("manual marker: warning, the SHA256SUMS check still runs", rc == 0 and "only a manual marker" in txt, txt)
manual["campaign/ec-a/plan/plan.json"] = b"tampered"
rc, txt, _ = run(manual)
check("manual marker: a changed object still fails", rc == 1 and "SHA-256 differs: plan/plan.json" in txt, txt)
try:
    run({"campaign/ec-a/../escape": b"x"})
    check("a key that leaves the directory is refused", False)
except SystemExit as e:
    check("a key that leaves the directory is refused", "leave the output directory" in str(e))
rc, txt, _ = run({})
check("an empty prefix fails", rc == 1 and "nothing under" in txt, txt)
src = Path(fc.__file__).read_text()
check("fetch_campaign calls no mutating S3 API", not any(w in src for w in (".put_object", ".delete_object", ".copy_object", "upload_")))
sys.exit(1 if fails else 0)
