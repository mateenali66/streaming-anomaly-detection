"""analysis/gate_summary.py on the synthetic e-commerce campaign 1 of conftest (every scenario of synthetic.full_scenario:
failed, telemetry-failed and aborted attempts with repeats, a remediated run, flags), against what that scenario
defines, and the 2.5 false-pass rule on edited records."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis import gate_summary as G  # noqa: E402


@pytest.fixture(scope="module")
def tables(full):
    return G.gate_tables(full.campaign)


def test_outcomes_repeats_and_flags_follow_the_full_scenario(full, tables):
    att = tables["attempts"]
    assert len(att) == 85 and att["cell_key"].nunique() == 80
    assert att["outcome"].value_counts().to_dict() == {"VALID": 79, "GATE_FAILED": 3, "TELEMETRY_FAILED": 1,
                                                        "ABORTED": 1, "VALID_REMEDIATED": 1}
    ft = G.fault_table(att, tables["cells"])
    assert ft["repeats"].sum() == 5 and ft["labelled_runs"].sum() == 80
    assert (ft["cells_planned"] == 10).all() and (ft["labelled_runs"] == 10).all()
    flags = {f: int(ft[f"flag_{f}"].sum()) for f in G.FLAGS}
    for f in ("SERIES_GAP", "OVERRUN", "REMEDIATED", "HEALTH_EVENT", "F7_TM_LOSS"):
        assert flags[f] >= 1, f
    assert ft.loc[ft["run_type"] == "F7", "remediated"].item() == 1
    assert ft.loc[ft["run_type"] == "F1", "GATE_FAILED"].item() == 1


def test_failed_attempts_are_never_gate_true_and_labelled_ones_always_are(tables):
    att = tables["attempts"]
    lab = att[att["labelled"]]
    assert lab["result_gate"].all() and (lab["n_checks_failed"] == 0).all()
    failed = att[att["outcome"].isin(["GATE_FAILED", "TELEMETRY_FAILED"])]
    assert not failed["result_gate"].any() and (failed["n_checks_failed"] > 0).all()
    assert att["failed_checks_match_record"].dropna().all()
    tel = att[att["outcome"] == "TELEMETRY_FAILED"]
    assert all("telemetry_scrape_misses" in json.loads(x) for x in tel["failed_checks"])


def test_every_cross_check_passes_on_the_synthetic_campaign(full, tables):
    cid = full.rep["campaign_id"]
    rec, per = G.label_tables(full.out.parent, cid)
    xc = G.cross_checks(cid, tables, rec, per, tables["meta"]["phase_s"])
    bad = [c for c in xc if not c["pass"]]
    assert not bad, bad
    assert len(xc) >= 19
    assert rec["excluded_windows_failed_attempts"] > 0
    assert rec["anomalous_prevalence_attempt_windows"] == pytest.approx(
        rec["anomalous_windows"] / rec["attempt_eval_windows"])


def test_a_sham_false_pass_is_counted_and_two_set_the_flag(tables):
    sig = tables["signatures"].copy()
    sh = sig.index[(sig["run_type"] == "F0") & (sig["signature"] == "F4")]
    assert len(sh) >= 2
    clean = G.sham_false_passes(sig, "x")
    assert clean["false_passes"].sum() == 0 and not clean["signature_false_pass_flag"].any()
    sig.loc[sh[:1], "pass"] = True
    one = G.sham_false_passes(sig, "x").set_index("signature")
    assert one.loc["F4", "false_passes"] == 1 and not one.loc["F4", "signature_false_pass_flag"]
    assert one.loc["F4", "false_passes_from_checks"] == 0      # the record's own check still says it passed cleanly
    sig.loc[sh[:2], "pass"] = True
    two = G.sham_false_passes(sig, "x").set_index("signature")
    assert two.loc["F4", "false_passes"] == 2 and two.loc["F4", "signature_false_pass_flag"]


def test_an_edited_verification_record_fails_the_hash_check(full, mutable):
    m = json.loads((mutable / "manifest" / "manifest.json").read_text())
    key = next(a["verification_key"] for a in m["attempts"] if a.get("verification_key"))
    p = mutable / key
    p.write_text(p.read_text() + " ")
    t = G.gate_tables(mutable)
    assert not t["attempts"]["verification_sha_ok"].dropna().all()


def test_parse_offline_reads_match_and_differ_lines():
    out = ("ec-a-k1 VALID MATCH\n"
           "ec-a-k2 GATE_FAILED DIFFER\n"
           "   check signature/f4_cpu_ratio/tm-0: online=True offline=False\n"
           "ec-a-k3 VALID MATCH\n")
    df = G.parse_offline(out)
    assert df["result"].tolist() == ["MATCH", "DIFFER", "MATCH"]
    assert json.loads(df.loc[1, "diffs"]) == ["check signature/f4_cpu_ratio/tm-0: online=True offline=False"]


def test_summarise_writes_every_output(full, tmp_path):
    cid = full.rep["campaign_id"]
    copies, derived = tmp_path / "copies", tmp_path / "derived"
    copies.mkdir()
    derived.mkdir()
    (copies / f"{cid}-final").symlink_to(full.campaign, target_is_directory=True)
    (derived / cid).symlink_to(full.out, target_is_directory=True)
    s = G.summarise(copies, derived, tmp_path / "out", [cid])
    for f in ("attempts.parquet", "checks.parquet", "signatures.parquet", "faults.csv", "sham_false_passes.csv",
              "cross_signatures.csv", "labels.csv", "cross_checks.csv", "gate_summary.json"):
        assert (tmp_path / "out" / f).is_file(), f
    t = s["total"]
    assert t["attempts"] == 85 and t["labelled_runs"] == 80 and t["repeats"] == 5
    assert t["cross_checks_passed"] == t["cross_checks"]
    lab = pd.read_csv(tmp_path / "out" / "labels.csv")
    assert lab.loc[0, "windows_total"] == len(full.windows[30])
