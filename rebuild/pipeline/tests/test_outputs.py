"""Contract 11.9 outputs: names, columns, types, no NaN, deterministic bytes and .sha256 sidecars."""
from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from streamprep.build import build_campaign
from streamprep.config import load_config
from streamprep.contract import BuildError, sha256_file

KEY_TYPES = [
    ("window_id", pa.string()), ("campaign_id", pa.string()), ("workload", pa.string()),
    ("window_s", pa.int32()), ("start_at", pa.float64()), ("end_at", pa.float64()),
    ("segment_type", pa.string()), ("label", pa.string()), ("attempt_key", pa.string()),
    ("run_id", pa.string()), ("cell_key", pa.string()), ("block", pa.int32()), ("position", pa.int32()),
    ("run_type", pa.string()), ("outcome", pa.string()), ("flags", pa.string()), ("seq_group", pa.string()),
    ("seq_index", pa.int32()), ("within_run", pa.string()),
]
META_TYPES = [("meta__tm_reporting_min", pa.float64()), ("meta__tm_reporting_mean", pa.float64()),
              ("meta__samples__broker", pa.int32()), ("meta__samples__consumer", pa.int32()),
              ("meta__samples__flink", pa.int32()), ("meta__samples__jvm", pa.int32()),
              ("meta__telemetry_ok", pa.bool_())]


def test_output_files_are_named_as_in_11_9(full):
    names = sorted(p.name for p in full.out.iterdir())
    expected = {"timeline.parquet", "folds.parquet"} | {f"windows_{w}s.parquet" for w in (15, 30, 60, 120)} \
        | {f"labels_{w}s.parquet" for w in (15, 30, 60, 120)}
    for n in expected:
        assert n in names and n + ".sha256" in names
    assert full.out.parent.name == "derived" and full.out.name == "ec-a"


def test_iot_campaign_one_would_add_300s(iot2):
    from streamprep.config import window_sizes_for
    cfg = load_config()
    assert window_sizes_for(cfg, "iot", 1) == [15, 30, 60, 120, 300]
    assert window_sizes_for(cfg, "ecommerce", 1) == [15, 30, 60, 120]
    assert window_sizes_for(cfg, "iot", 2) == [30]
    assert sorted(p.name for p in iot2.out.glob("windows_*s.parquet")) == ["windows_30s.parquet"]


def test_sidecars_hold_the_file_hash(full):
    for p in full.out.glob("*.parquet"):
        side = (p.parent / (p.name + ".sha256")).read_bytes()
        assert side == (sha256_file(p) + "\n").encode()
        assert full.rep["outputs"][p.name] == sha256_file(p)


@pytest.mark.parametrize("w", [15, 30, 60, 120])
def test_windows_schema_and_no_nan(full, w):
    t = pq.read_table(full.out / f"windows_{w}s.parquet")
    sch = t.schema
    assert [(f.name, f.type) for f in sch][: len(KEY_TYPES) + len(META_TYPES)] == KEY_TYPES + META_TYPES
    assert not sch.metadata or b"pandas" not in sch.metadata
    for name, col in zip(t.column_names, t.columns):
        if pa.types.is_floating(col.type):
            assert not pc.any(pc.is_nan(col)).as_py(), name
    labels = pq.read_table(full.out / f"labels_{w}s.parquet")
    assert labels.equals(t.select([n for n, _ in KEY_TYPES]))
    assert all(v == w for v in t.column("window_s").to_pylist()[:50])


def test_timeline_and_folds_schema(full):
    t = pq.read_table(full.out / "timeline.parquet")
    assert [(f.name, f.type) for f in t.schema] == [
        ("segment_index", pa.int32()), ("segment_type", pa.string()), ("start_at", pa.float64()),
        ("end_at", pa.float64()), ("attempt_key", pa.string()), ("run_id", pa.string()), ("run_type", pa.string()),
        ("outcome", pa.string()), ("label", pa.string())]
    f = pq.read_table(full.out / "folds.parquet")
    assert [(x.name, x.type) for x in f.schema] == [("window_id", pa.string()), ("fold", pa.int32()), ("role", pa.string())]


def test_outputs_are_deterministic(full, tmp_path):
    rep = build_campaign(full.campaign, tmp_path / "again", load_config())
    assert rep["outputs"] == full.rep["outputs"]
    for name in rep["outputs"]:
        assert (tmp_path / "again" / "derived" / "ec-a" / name).read_bytes() == (full.out / name).read_bytes()
    assert (tmp_path / "again" / "derived" / "ec-a" / "build_report.json").read_bytes() == \
        (full.out / "build_report.json").read_bytes()


def test_committed_hashes_are_checked(full, tmp_path):
    committed = tmp_path / "committed"
    committed.mkdir()
    (committed / "labels_30s.parquet.sha256").write_text("0" * 64 + "\n")
    with pytest.raises(BuildError, match="committed"):
        build_campaign(full.campaign, tmp_path / "x", load_config(), stages=("labels",), check_against=committed)
    (committed / "labels_30s.parquet.sha256").write_text(full.rep["outputs"]["labels_30s.parquet"] + "\n")
    build_campaign(full.campaign, tmp_path / "y", load_config(), stages=("labels",), check_against=committed)


def test_outputs_do_not_depend_on_input_parquet_encoding(full, mutable, tmp_path):
    """Re-encode every feature file (codec, row groups, dictionary pages). Outputs must not change."""
    from conftest import edit_manifest
    for f in mutable.rglob("features.parquet"):
        t = pq.read_table(f)
        pq.write_table(t, f, compression="snappy", row_group_size=1000, use_dictionary=False)
    for f in (mutable / "baseline" / "features").glob("*.parquet"):
        pq.write_table(pq.read_table(f), f, compression="gzip", row_group_size=777)

    def fix(m):
        for a in m["attempts"]:
            a["export"]["features_sha256"] = sha256_file(mutable / "export" / "attempts" / a["attempt_key"] / "features.parquet")
    edit_manifest(mutable, fix)
    rep = build_campaign(mutable, tmp_path / "re", load_config(), window_sizes=[15, 30, 60, 120])
    assert rep["outputs"] == full.rep["outputs"]
