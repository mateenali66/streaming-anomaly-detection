#!/usr/bin/env python3
"""Gate, verification and label summary per campaign (protocol 8.4, first two rows), with the sham false passes of 2.5
and the label accounting of 3.3 to 3.6, and the offline gate of 7.1 step 2 (make gate).

Added on 18 October 2026, after registration. It reads the verified campaign copies and the preprocessing outputs only,
and decides nothing: every outcome, flag and check comes from the orchestrator's records. The choices the protocol
leaves open are recorded in protocol/DEVIATIONS.md.

    python3 -m analysis.gate_summary --copies DIR --derived DIR --out DIR [--offline-gate]

--copies holds <cid><suffix>/ (--copy-suffix, default -final), a verified copy of campaign/<cid>/ (manifest, plan, attempts, export). --derived holds
<cid>/ with build_report.json, labels_30s.parquet, windows_30s.parquet and timeline.parquet. --offline-gate runs the
registered `orchestrator gate-offline` (rebuild/orchestrator) on every attempt of every campaign, writes the offline
records under OUT/offline/<cid>/ and compares them with the online ones (2.7.4).

Writes, under --out:
  attempts.parquet        one row per attempt: cell, outcome, flags, failed checks, onset and end sources, onset lag,
                          labelled span, overrun, remediation, the gate's layer results, telemetry coverage, windows
  checks.parquet          one row per gate check of every attempt (layer, name, target, pass)
  signatures.parquet      every fault signature evaluated on every attempt (signatures_all_faults)
  faults.csv              per campaign and run type: cells, attempts, outcomes, repeats, flags, onset lag, span, overrun
  sham_false_passes.csv   per campaign and signature: sham attempts, false passes, and the SIGNATURE_FALSE_PASS rule of
                          2.5 (two or more false passes)
  cross_signatures.csv    per campaign, run type and signature: the share of labelled attempts on which that signature
                          passed (recorded by the gate, decides nothing)
  labels.csv              per campaign at 30 s: windows per segment and label, exclusions by rule, telemetry_ok,
                          hours per segment type, prevalence
  offline_gate.csv        per attempt, MATCH or DIFFER (with --offline-gate)
  gate_summary.json       headline counts per campaign and in total, and every cross-check with its result
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import math
import re
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

PIPE = Path(__file__).resolve().parents[1]
ORCH = PIPE.parent / "orchestrator"
CAMPAIGNS = ["ec-a", "ec-b", "ec-c", "iot-a", "iot-b", "iot-c"]
RUN_TYPES = ["F0", "F1", "F2", "F4", "F5", "F6", "F7", "F8"]
FAULTS = [r for r in RUN_TYPES if r != "F0"]
OUTCOMES = ["VALID", "VALID_REMEDIATED", "GATE_FAILED", "TELEMETRY_FAILED", "ABORTED"]
LABELLED = ("VALID", "VALID_REMEDIATED")
FLAGS = ["F7_TM_LOSS", "OVERRUN", "AFTER_REDEPLOY", "AFTER_RESUBMIT", "REMEDIATED", "HEALTH_EVENT", "SERIES_GAP"]
LAYERS = ["MECHANISM", "SIGNATURE", "ENVELOPE", "TIMING", "TELEMETRY"]
RESULT_KEYS = ["mechanism", "signature", "envelope", "timing", "telemetry", "gate"]
WINDOW_S = 30
EVAL_LABELS = ("NORMAL", "ANOMALOUS", "RECOVERY", "SHAM", "SHAM_POST")
OFFLINE_LINE = re.compile(r"^(\S+) (\S+) (MATCH|DIFFER)$")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _check_sidecar(path: Path):
    side = path.parent / (path.name + ".sha256")
    if not side.is_file() or side.read_text().strip() != _sha256(path):
        raise SystemExit(f"ERROR: {path} does not match its .sha256 sidecar (11.9)")


# ---------------------------------------------------------------- gate side, from the campaign copy
def gate_tables(copy: Path) -> dict:
    """Attempt, check and signature rows of one campaign copy, from its manifest, plan and verification records."""
    m = json.loads((copy / "manifest" / "manifest.json").read_text())
    plan = json.loads((copy / "plan" / "plan.json").read_text())
    cid = m["campaign_id"]
    cells = {c["cell_key"]: {**c, "block": b["block"]} for b in plan["blocks"] for c in b["cells"]}
    att_rows, check_rows, sig_rows = [], [], []
    for a in m["attempts"]:
        r = {"campaign_id": cid, "workload": m["workload"], "campaign_index": m["campaign_index"],
             "attempt_key": a["attempt_key"], "cell_key": a["cell_key"], "block": a["block"],
             "position": a["position"], "attempt": a["attempt"], "run_type": a["run_type"],
             "fault_name": a.get("fault_name"), "status": a["status"], "outcome": a["outcome"],
             "labelled": a["outcome"] in LABELLED, "flags": json.dumps(sorted(a["flags"])),
             "failed_checks": json.dumps(a["failed_checks"]), "applied": a["applied"],
             "onset_source": a["onset_source"], "end_source": a["end_source"],
             "planned_onset_at": a["planned_onset_at"], "onset_verified_at": a["onset_verified_at"],
             "labelled_end_at": a["labelled_end_at"], "configured_duration_s": a["configured_duration_s"],
             "overrun_s": a["overrun_s"],
             "remediation": bool(a["recovery_gate"]["remediation"]["performed"]),
             "remediation_actions": json.dumps(a["recovery_gate"]["remediation"].get("actions") or [])}
        r["onset_lag_s"] = (a["onset_verified_at"] - a["planned_onset_at"]) if a["onset_verified_at"] is not None else None
        r["span_s"] = (a["labelled_end_at"] - a["onset_verified_at"]) \
            if a["onset_verified_at"] is not None and a["labelled_end_at"] is not None else None
        cell = cells.get(a["cell_key"], {})
        r["plan_run_type"] = cell.get("run_type")
        r["plan_sham_duration_s"] = cell.get("sham_duration_s") if a["run_type"] == "F0" else None
        vkey = a.get("verification_key")
        if vkey:
            vpath = copy / vkey
            v = json.loads(vpath.read_text())
            r["verification_sha_ok"] = _sha256(vpath) == a["verification_sha256"]
            r["verification_ids_ok"] = (v["attempt_key"] == a["attempt_key"] and v["run_id"] == a["run_id"]
                                        and v["run_type"] == a["run_type"])
            r["mode"] = v["mode"]
            for k in RESULT_KEYS:
                r[f"result_{k}"] = bool(v["result"][k])
            r["n_checks"] = len(v["checks"])
            r["n_checks_failed"] = sum(not c["pass"] for c in v["checks"])
            for L in LAYERS:
                r[f"n_checks_{L.lower()}"] = sum(c["layer"] == L for c in v["checks"])
            untargeted = [t for t in v["telemetry"] if not t["targeted"]]
            r["telemetry_min_untargeted_coverage"] = min((t["coverage_frac"] for t in untargeted), default=None)
            sc = [s for s in v["scrapes"] if not s["targeted"]]
            r["scrape_max_untargeted_missed"] = max((s["missed_count"] for s in sc), default=None)
            failed_names = []
            for c in v["checks"]:
                check_rows.append({"campaign_id": cid, "attempt_key": a["attempt_key"], "run_type": a["run_type"],
                                   "outcome": a["outcome"], "layer": c["layer"], "name": c["name"],
                                   "target": c["target"], "pass": bool(c["pass"]), "reason": c.get("reason")})
                if not c["pass"] and c["name"] not in failed_names:
                    failed_names.append(c["name"])
            r["failed_checks_match_record"] = failed_names == list(a["failed_checks"])
            f0_checks = {c["target"]: c["pass"] for c in v["checks"] if c["name"] == "f0_signature_false_pass"}
            for s in v["signatures_all_faults"]:
                sig_rows.append({"campaign_id": cid, "attempt_key": a["attempt_key"], "run_type": a["run_type"],
                                 "outcome": a["outcome"], "labelled": r["labelled"], "signature": s["run_type"],
                                 "pass": bool(s["pass"]),
                                 "f0_check_pass": f0_checks.get(s["run_type"]) if a["run_type"] == "F0" else None})
        else:
            r.update({"verification_sha_ok": None, "verification_ids_ok": None, "mode": None})
        att_rows.append(r)
    att = pd.DataFrame(att_rows)
    meta = {"campaign_id": cid, "status": m["status"], "halts": m["halts"], "n_halts": len(m["halts"]),
            "health_events": [{k: e.get(k) for k in ("event_type_code", "service", "status", "start_at", "end_at")}
                              for e in m["health_events"]],
            "kubernetes_version": m.get("kubernetes_version"), "node_ami_release": m.get("node_ami_release"),
            "node_inventory_snapshots": len(m.get("node_inventory") or []),
            "node_count": len((m.get("node_inventory") or [{}])[-1].get("instance_ids") or []),
            "clock_skew_s": m.get("clock_skew_s"), "plan_cells": len(cells),
            "phase_s": plan.get("phase_s"), "gate_commit": m["gate"]["git_commit"],
            "thresholds_sha256": m["gate"]["thresholds_sha256"]}
    return {"attempts": att, "checks": pd.DataFrame(check_rows), "signatures": pd.DataFrame(sig_rows),
            "cells": cells, "meta": meta}


def fault_table(att: pd.DataFrame, cells: dict) -> pd.DataFrame:
    rows = []
    cid = att["campaign_id"].iloc[0]
    for rt in RUN_TYPES:
        a = att[att["run_type"] == rt]
        planned = sum(c["run_type"] == rt for c in cells.values())
        lab = a[a["labelled"]]
        per_cell = a.groupby("cell_key").size()
        r = {"campaign_id": cid, "run_type": rt, "cells_planned": planned, "attempts": len(a),
             "labelled_runs": len(lab), "repeats": int((per_cell - 1).sum()) if len(per_cell) else 0}
        for o in OUTCOMES:
            r[o] = int((a["outcome"] == o).sum())
        fl = collections.Counter(f for x in a["flags"] for f in json.loads(x))
        for f in FLAGS:
            r[f"flag_{f}"] = int(fl.get(f, 0))
        other = sorted(set(fl) - set(FLAGS))
        r["flags_other"] = json.dumps({k: fl[k] for k in other})
        for col, name in (("onset_lag_s", "onset_lag"), ("span_s", "span"), ("overrun_s", "overrun")):
            x = pd.to_numeric(lab[col], errors="coerce").dropna()
            r[f"{name}_n"] = int(len(x))
            r[f"{name}_median_s"] = float(x.median()) if len(x) else None
            r[f"{name}_min_s"] = float(x.min()) if len(x) else None
            r[f"{name}_max_s"] = float(x.max()) if len(x) else None
        r["onset_sources"] = json.dumps(dict(collections.Counter(lab["onset_source"].astype(str))))
        r["end_sources"] = json.dumps(dict(collections.Counter(lab["end_source"].astype(str))))
        r["remediated"] = int(a["remediation"].sum())
        rows.append(r)
    return pd.DataFrame(rows)


def sham_false_passes(sig: pd.DataFrame, cid: str) -> pd.DataFrame:
    """2.5: per signature, false passes over every sham attempt with a verification record, the failed ones included.
    Two or more set SIGNATURE_FALSE_PASS for every run of that fault in the campaign (applied by the analysis)."""
    sh = sig[sig["run_type"] == "F0"] if len(sig) else sig
    rows = []
    for f in FAULTS:
        s = sh[sh["signature"] == f] if len(sh) else sh
        n = int(s["attempt_key"].nunique()) if len(s) else 0
        fp = int(s["pass"].sum()) if len(s) else 0
        fp_check = int((s["f0_check_pass"] == False).sum()) if len(s) else 0  # noqa: E712
        rows.append({"campaign_id": cid, "signature": f, "sham_attempts": n, "false_passes": fp,
                     "false_passes_from_checks": fp_check, "signature_false_pass_flag": fp >= 2})
    return pd.DataFrame(rows)


def cross_signatures(sig: pd.DataFrame, cid: str) -> pd.DataFrame:
    if not len(sig):
        return pd.DataFrame()
    lab = sig[sig["labelled"]]
    g = lab.groupby(["run_type", "signature"])["pass"].agg(["mean", "sum", "count"]).reset_index()
    g.insert(0, "campaign_id", cid)
    return g.rename(columns={"mean": "pass_share", "sum": "passes", "count": "attempts"})


# ---------------------------------------------------------------- label side, from the preprocessing outputs
def label_tables(derived: Path, cid: str) -> tuple[dict, pd.DataFrame]:
    base = derived / cid
    lp, wp, tp = base / f"labels_{WINDOW_S}s.parquet", base / f"windows_{WINDOW_S}s.parquet", base / "timeline.parquet"
    for p in (lp, wp, tp):
        _check_sidecar(p)
    br = json.loads((base / "build_report.json").read_text())
    lab = pd.read_parquet(lp)
    ok = pq.read_table(wp, columns=["window_id", "meta__telemetry_ok"]).to_pandas()
    lab = lab.merge(ok, on="window_id", how="left", validate="one_to_one")
    tl = pd.read_parquet(tp)
    tl["dur_s"] = tl["end_at"] - tl["start_at"]
    by = lab.groupby(["segment_type", "label"]).size()
    rec = {"campaign_id": cid, "windows_total": int(len(lab))}
    rec["_windows"] = {f"{seg}/{l}": int(n) for (seg, l), n in by.items()}
    for (seg, l), n in by.items():
        rec[f"w_{seg}__{l}"] = int(n)
    ev_att = lab[lab["attempt_key"].notna() & lab["label"].isin(EVAL_LABELS)]
    rec["attempt_eval_windows"] = int(len(ev_att))
    rec["anomalous_windows"] = int((lab["label"] == "ANOMALOUS").sum())
    rec["anomalous_prevalence_attempt_windows"] = rec["anomalous_windows"] / len(ev_att) if len(ev_att) else None
    rec["excluded_windows_failed_attempts"] = int((lab["attempt_key"].notna() & (lab["label"] == "EXCLUDED")).sum())
    nth = br["normal_time_health"][str(WINDOW_S)]
    for seg, v in nth.items():
        rec[f"health_{seg}_windows"] = int(v["windows"])
        rec[f"health_{seg}_excluded"] = int(v["excluded"])
        rec[f"health_{seg}_failed_conditions"] = json.dumps(v["failed_conditions"], sort_keys=True)
    rec["telemetry_not_ok_windows"] = int((~lab["meta__telemetry_ok"].astype(bool)).sum())
    rec["telemetry_not_ok_eval_windows"] = int((~ev_att["meta__telemetry_ok"].astype(bool)).sum())
    hours = tl.groupby("segment_type")["dur_s"].sum() / 3600.0
    for seg, h in hours.items():
        rec[f"hours_{seg}"] = float(h)
    rec["hours_total"] = float(tl["dur_s"].sum() / 3600.0)
    rec["baseline_hours_from_windows"] = rec.get("w_BASELINE__NORMAL", 0) * WINDOW_S / 3600.0
    rec["trailing_hours_from_windows"] = rec.get("w_TRAILING__TRAILING", 0) * WINDOW_S / 3600.0
    rec["build_warnings"] = len(br.get("warnings") or [])
    rec["config_sha256"] = br.get("config_sha256")
    # per attempt window counts, for the cross-checks
    att = lab[lab["attempt_key"].notna()]
    per = att.groupby(["attempt_key", "label"]).size().unstack(fill_value=0)
    flags = att.groupby("attempt_key")["flags"].first()
    per["flags_windows"] = flags.map(lambda s: json.dumps(sorted(json.loads(s))))
    per["outcome_windows"] = att.groupby("attempt_key")["outcome"].first()
    inj = tl[tl["segment_type"] == "INJECTION"].set_index("attempt_key")
    per["tl_injection_start"] = inj["start_at"].reindex(per.index)
    per["tl_injection_end"] = inj["end_at"].reindex(per.index)
    rec["_build_report_windows"] = br["windows"][str(WINDOW_S)]
    rec["_build_report_by_outcome"] = br["attempts"]["by_outcome"]
    return rec, per.reset_index()


# ---------------------------------------------------------------- offline gate, 7.1 step 2
def parse_offline(stdout: str) -> pd.DataFrame:
    rows, cur = [], None
    for line in stdout.splitlines():
        m = OFFLINE_LINE.match(line.strip())
        if m and not line.startswith(" "):
            cur = {"attempt_key": m.group(1), "outcome": m.group(2), "result": m.group(3), "diffs": []}
            rows.append(cur)
        elif line.startswith("   ") and cur is not None:
            cur["diffs"].append(line.strip())
    return pd.DataFrame([{**r, "diffs": json.dumps(r["diffs"])} for r in rows],
                        columns=["attempt_key", "outcome", "result", "diffs"])


def run_offline_gate(copy: Path, out: Path) -> tuple[pd.DataFrame, int]:
    out.mkdir(parents=True, exist_ok=True)
    p = subprocess.run([sys.executable, "-m", "orchestrator", "gate-offline", "--store", str(copy), "--out", str(out)],
                       cwd=ORCH, capture_output=True, text=True)
    return parse_offline(p.stdout), p.returncode


# ---------------------------------------------------------------- cross-checks
def cross_checks(cid: str, g: dict, lab_rec: dict, per: pd.DataFrame, phase_s: dict) -> list[dict]:
    att = g["attempts"]
    out = []

    def add(name, ok, detail=""):
        out.append({"campaign_id": cid, "check": name, "pass": bool(ok), "detail": str(detail)})

    oc = att["outcome"].value_counts().to_dict()
    add("outcomes: manifest equals build_report", oc == lab_rec["_build_report_by_outcome"], oc)
    lab = att[att["labelled"]]
    plan_keys = set(g["cells"])
    add("plan: every cell has exactly one labelled attempt",
        sorted(lab["cell_key"]) == sorted(plan_keys) and not lab["cell_key"].duplicated().any(),
        f"{lab['cell_key'].nunique()} of {len(plan_keys)}")
    add("plan: run types equal the plan", (att["run_type"] == att["plan_run_type"]).all())
    add("verification: file hashes equal the manifest", att["verification_sha_ok"].dropna().all())
    add("verification: ids equal the manifest", att["verification_ids_ok"].dropna().all())
    add("verification: failed_checks equal the failing checks", att["failed_checks_match_record"].dropna().all())
    add("verification: every labelled attempt has gate true", lab["result_gate"].all())
    add("verification: no labelled attempt has a failing check", (lab["n_checks_failed"] == 0).all())
    bad = att[~att["labelled"] & att["result_gate"].fillna(False).astype(bool)]
    add("verification: no unlabelled attempt has gate true", len(bad) == 0, list(bad["attempt_key"])[:3])
    # windows of every labelled attempt against its spans
    pre_n = int(phase_s["pre_fault"] // WINDOW_S) if phase_s and "pre_fault" in phase_s else None
    rec_n = int(phase_s["recovery"] // WINDOW_S) if phase_s and "recovery" in phase_s else None
    x = lab.merge(per, on="attempt_key", how="left")
    pos = np.where(x["run_type"] == "F0", x.get("SHAM", 0), x.get("ANOMALOUS", 0))
    post = np.where(x["run_type"] == "F0", x.get("SHAM_POST", 0), x.get("RECOVERY", 0))
    want_pos = np.floor(x["span_s"].astype(float) / WINDOW_S + 1e-9).astype(int)
    add("windows: INJECTION windows equal floor(labelled span / 30 s)", (pos == want_pos).all(),
        f"{int((pos != want_pos).sum())} attempts differ")
    if pre_n is not None:
        add(f"windows: {pre_n} PRE_FAULT windows per labelled attempt", (x["NORMAL"] == pre_n).all())
    if rec_n is not None:
        add(f"windows: {rec_n} RECOVERY or SHAM_POST windows per labelled attempt", (post == rec_n).all())
    sh = x[x["run_type"] == "F0"]
    add("shams: labelled span equals the plan's sham length",
        np.allclose(sh["span_s"].astype(float), sh["plan_sham_duration_s"].astype(float), atol=1e-6))
    add("timeline: INJECTION segment equals onset and labelled end",
        np.allclose(x["tl_injection_start"], x["onset_verified_at"], atol=1e-6)
        and np.allclose(x["tl_injection_end"], x["labelled_end_at"], atol=1e-6))
    add("labels: attempt flags in the windows equal the manifest", (x["flags_windows"] == x["flags"]).all())
    add("labels: attempt outcome in the windows equals the manifest", (x["outcome_windows"] == x["outcome"]).all())
    brw = lab_rec["_build_report_windows"]
    got = {k: v for k, v in lab_rec["_windows"].items() if not k.endswith("/EXCLUDED")}
    add("labels: window counts equal build_report", got == {k: v for k, v in brw.items() if not k.endswith("EXCLUDED")},
        got)
    for seg, lbl in (("BASELINE", "NORMAL"), ("TRAILING", "TRAILING")):
        w_h = (lab_rec["_windows"].get(f"{seg}/{lbl}", 0) + lab_rec.get(f"health_{seg}_excluded", 0)) * WINDOW_S / 3600
        add(f"labels: {seg} windows (kept and health-excluded) tile the timeline's {seg} hours",
            abs(w_h - lab_rec.get(f"hours_{seg}", 0)) < 1e-6, f"{w_h} vs {lab_rec.get(f'hours_{seg}')}")
    sfp = sham_false_passes(g["signatures"], cid)
    add("shams: false passes from signatures_all_faults equal the f0 checks",
        (sfp["false_passes"] == sfp["false_passes_from_checks"]).all())
    if len(g["signatures"]):
        s = g["signatures"]
        self_pass = s[(s["run_type"] == s["signature"]) & s["labelled"]]
        add("signatures: every labelled fault passes its own signature", self_pass["pass"].all(),
            f"{int((~self_pass['pass']).sum())} do not")
    return out


# ---------------------------------------------------------------- driver
def summarise(copies: Path, derived: Path, out: Path, campaigns=CAMPAIGNS, offline_gate: bool = False,
              suffix: str = "-final") -> dict:
    out.mkdir(parents=True, exist_ok=True)
    A, CK, SG, FT, SF, CS, LB, XC, OG = [], [], [], [], [], [], [], [], []
    summary = {"campaigns": {}, "window_s": WINDOW_S}
    for cid in campaigns:
        copy = copies / f"{cid}{suffix}"
        g = gate_tables(copy)
        if g["meta"]["campaign_id"] != cid:
            raise SystemExit(f"ERROR: {copy} holds {g['meta']['campaign_id']}, expected {cid}")
        lab_rec, per = label_tables(derived, cid)
        A.append(g["attempts"])
        CK.append(g["checks"])
        SG.append(g["signatures"])
        ft = fault_table(g["attempts"], g["cells"])
        FT.append(ft)
        sf = sham_false_passes(g["signatures"], cid)
        SF.append(sf)
        CS.append(cross_signatures(g["signatures"], cid))
        LB.append({k: v for k, v in lab_rec.items() if not k.startswith("_")})
        xc = cross_checks(cid, g, lab_rec, per, g["meta"]["phase_s"])
        XC.extend(xc)
        rec = {"meta": g["meta"], "attempts": int(len(g["attempts"])),
               "outcomes": g["attempts"]["outcome"].value_counts().to_dict(),
               "labelled_runs": int(g["attempts"]["labelled"].sum()),
               "repeats": int(ft["repeats"].sum()),
               "flags": dict(collections.Counter(f for x in g["attempts"]["flags"] for f in json.loads(x))),
               "sham_false_passes": int(sf["false_passes"].sum()),
               "signature_false_pass_flags": sf.loc[sf["signature_false_pass_flag"], "signature"].tolist(),
               "anomalous_windows": lab_rec["anomalous_windows"],
               "anomalous_prevalence_attempt_windows": lab_rec["anomalous_prevalence_attempt_windows"],
               "baseline_hours": lab_rec["baseline_hours_from_windows"],
               "trailing_hours": lab_rec["trailing_hours_from_windows"],
               "cross_checks_passed": int(sum(c["pass"] for c in xc)), "cross_checks": int(len(xc))}
        if offline_gate:
            og, rc = run_offline_gate(copy, out / "offline" / cid)
            og.insert(0, "campaign_id", cid)
            OG.append(og)
            finished = int((g["attempts"]["status"] == "FINISHED").sum())
            rec["offline_gate"] = {"returncode": rc, "attempts": int(len(og)), "finished_attempts": finished,
                                   "match": int((og["result"] == "MATCH").sum()),
                                   "differ": int((og["result"] == "DIFFER").sum())}
            XC.append({"campaign_id": cid, "check": "offline gate: every FINISHED attempt matches the online record",
                       "pass": bool(rc == 0 and len(og) == finished and (og["result"] == "MATCH").all()),
                       "detail": json.dumps(rec["offline_gate"])})
        summary["campaigns"][cid] = rec
    att = pd.concat(A, ignore_index=True)
    att.to_parquet(out / "attempts.parquet", index=False)
    pd.concat(CK, ignore_index=True).to_parquet(out / "checks.parquet", index=False)
    pd.concat(SG, ignore_index=True).to_parquet(out / "signatures.parquet", index=False)
    pd.concat(FT, ignore_index=True).to_csv(out / "faults.csv", index=False)
    pd.concat(SF, ignore_index=True).to_csv(out / "sham_false_passes.csv", index=False)
    pd.concat(CS, ignore_index=True).to_csv(out / "cross_signatures.csv", index=False)
    pd.DataFrame(LB).to_csv(out / "labels.csv", index=False)
    xc = pd.DataFrame(XC)
    xc.to_csv(out / "cross_checks.csv", index=False)
    if OG:
        pd.concat(OG, ignore_index=True).to_csv(out / "offline_gate.csv", index=False)
    summary["total"] = {
        "attempts": int(len(att)), "labelled_runs": int(att["labelled"].sum()),
        "outcomes": att["outcome"].value_counts().to_dict(),
        "repeats": int(sum(r["repeats"] for r in summary["campaigns"].values())),
        "halts": int(sum(r["meta"]["n_halts"] for r in summary["campaigns"].values())),
        "sham_false_passes": int(sum(r["sham_false_passes"] for r in summary["campaigns"].values())),
        "flags": dict(sum((collections.Counter(r["flags"]) for r in summary["campaigns"].values()),
                          collections.Counter())),
        "cross_checks_passed": int(xc["pass"].sum()), "cross_checks": int(len(xc))}
    if OG:
        og = pd.concat(OG, ignore_index=True)
        summary["total"]["offline_gate"] = {"attempts": int(len(og)), "match": int((og["result"] == "MATCH").sum()),
                                            "differ": int((og["result"] == "DIFFER").sum())}
    summary["inputs"] = {cid: {"manifest_sha256": _sha256(copies / f"{cid}{suffix}" / "manifest" / "manifest.json"),
                               "labels_sha256": _sha256(derived / cid / f"labels_{WINDOW_S}s.parquet"),
                               "timeline_sha256": _sha256(derived / cid / "timeline.parquet")}
                         for cid in campaigns}
    (out / "gate_summary.json").write_text(json.dumps(summary, indent=1, default=str) + "\n")
    return summary


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--copies", required=True, type=Path)
    p.add_argument("--derived", required=True, type=Path)
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--campaigns", nargs="*", default=CAMPAIGNS)
    p.add_argument("--offline-gate", action="store_true")
    p.add_argument("--copy-suffix", default="-final", help="campaign folder is <cid><suffix>; '' for campaign/<cid>/")
    a = p.parse_args(argv)
    s = summarise(a.copies, a.derived, a.out, a.campaigns, a.offline_gate, a.copy_suffix)
    t = s["total"]
    print(f"gate summary: {t['attempts']} attempts, {t['labelled_runs']} labelled, {t['repeats']} repeats, "
          f"{t['sham_false_passes']} sham false passes, cross-checks {t['cross_checks_passed']} of {t['cross_checks']}"
          + (f", offline gate {t['offline_gate']['match']} of {t['offline_gate']['attempts']} match"
             if "offline_gate" in t else ""))
    return 0 if t["cross_checks_passed"] == t["cross_checks"] else 1


if __name__ == "__main__":
    sys.exit(main())
