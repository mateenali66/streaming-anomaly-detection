"""series_id is a file-integrity key, never a feature identity (contract 11.8 and 11.9, rebuild/CONTRACT_CHANGES.md C17).

11.8 hashes the metric, the pod and EVERY remaining label into series_id, including the volatile ones: Flink's job_id
(new on every resubmission), task_attempt_id and task_attempt_num (new on every job restart) and the node label the scrape
config adds. So one logical series gets a new series_id each time Flink restarts a job. The preprocessing reads series_id
only to check the files (the hash is recomputed), to drop duplicate samples of one series at export boundaries and to fix
a deterministic series order; feature columns are named and matched from the source, the metric, the scope and the
retained labels (A1 drops job_id, task_attempt_id, task_attempt_num and node before naming).

The test rewrites every volatile label value of every exported feature series, which gives every such series a new
series_id, and checks that the build produces the same feature columns with the same values."""
from __future__ import annotations

import hashlib
import inspect
import json
import shutil

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from streamprep import features
from streamprep.build import build_campaign
from streamprep.config import load_config
from streamprep.contract import canonical_json_bytes, series_id_for, sha256_file
from streamprep.synthetic import rehash_campaign

VOLATILE = ("job_id", "task_attempt_id", "task_attempt_num", "node")


def _new_value(key: str, old: str) -> str:
    return "v" + hashlib.sha256(f"{key}={old}".encode()).hexdigest()[:20]


def _rewrite_feature_file(path) -> int:
    t = pq.read_table(path)
    df = t.to_pandas()
    changed = 0
    new_ids, new_labels = {}, {}
    for sid, lab_str in df[["series_id", "labels"]].drop_duplicates().itertuples(index=False):
        lab = json.loads(lab_str)
        if any(k in lab for k in VOLATILE):
            lab = {k: (_new_value(k, v) if k in VOLATILE else v) for k, v in lab.items()}
            changed += 1
        new_labels[sid] = canonical_json_bytes(lab).decode()
    for sid, metric, pod in df[["series_id", "metric", "pod"]].drop_duplicates().itertuples(index=False):
        new_ids[sid] = series_id_for(metric, pod, json.loads(new_labels[sid]))
    df["labels"] = df["series_id"].map(new_labels)
    df["series_id"] = df["series_id"].map(new_ids)
    df = df.sort_values(["ts", "series_id"], kind="stable").reset_index(drop=True)
    out = pa.Table.from_pandas(df, schema=t.schema, preserve_index=False).replace_schema_metadata(None)
    pq.write_table(out, path, compression="zstd")
    return changed


def test_feature_columns_and_values_never_depend_on_series_id(full, tmp_path):
    camp = tmp_path / "campaign"
    shutil.copytree(full.campaign, camp)
    changed = 0
    files = sorted(p for p in camp.rglob("*.parquet") if "features" in p.relative_to(camp).as_posix())
    for p in files:
        changed += _rewrite_feature_file(p)
    assert changed > 100, "the synthetic campaign must carry volatile labels for this test to mean anything"
    m_path = camp / "manifest" / "manifest.json"
    m = json.loads(m_path.read_text())
    for a in m["attempts"]:
        a["export"]["features_sha256"] = sha256_file(camp / a["export"]["features_key"])
    m_path.write_text(json.dumps(m, indent=1, sort_keys=True) + "\n")
    rehash_campaign(camp)

    rep = build_campaign(camp, tmp_path / "out", load_config(), window_sizes=[30, 60])
    for w in (30, 60):
        before = full.windows[w]
        after = pd.read_parquet(rep["_out"] / f"windows_{w}s.parquet")
        assert list(after.columns) == list(before.columns)
        fcols = [c for c in before.columns if c.startswith("f__")]
        a, b = after[fcols].to_numpy(float), before[fcols].to_numpy(float)
        assert np.array_equal(np.isnan(a), np.isnan(b))
        # Only the floating order of sums over series may change (series are ordered by series_id).
        assert np.allclose(a[~np.isnan(a)], b[~np.isnan(b)], rtol=1e-12, atol=1e-9)
        meta = [c for c in before.columns if not c.startswith("f__")]
        pd.testing.assert_frame_equal(after[meta], before[meta])


def test_column_mapping_has_no_series_id_in_its_keys():
    """A guard on the code itself: the functions that name and group feature columns never read series_id."""
    for fn in (features.map_columns, features.counter_mask, features.rate_values, features.derived_step_columns,
               features.prepare_features):
        assert "series_id" not in inspect.getsource(fn), fn.__name__
