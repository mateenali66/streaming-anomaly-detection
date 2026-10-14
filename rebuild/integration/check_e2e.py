"""Assertions of the end-to-end run (run_e2e.sh) on one dumped campaign and its pipeline outputs.

Every expectation is recomputed from the orchestrator's own records (the dumped manifest and summary.json) and from the
protocol's rules, never from the pipeline's functions, so a pipeline that agrees with itself but not with the orchestrator
fails here.

  check_e2e.py --dump DIR --derived DIR [--expect GATE_FAILED=5 --expect TELEMETRY_FAILED=1 --expect ABORTED=1]
               [--expect-flag AFTER_REDEPLOY] [--expect-outcome VALID_REMEDIATED] [--train-validate]

Checks:
  1. the S3 mirror holds only 11.7 objects, nothing under manifest/ but manifest.json, and the orchestrator's resume state is
     on the volume only (C1)
  2. every second of the campaign belongs to exactly one timeline segment, and the timeline equals the manifest's segments
  3. the windows of every attempt and baseline stretch are exactly floor(segment length / w) per labelled segment, for every
     window size, with the label 3.3 gives for the attempt's outcome and run type
  4. attempts that are not VALID or VALID_REMEDIATED are excluded whole (every window and timeline row EXCLUDED), and valid
     attempts lose no window
  5. no fold leakage: one role per window per fold, every window of a block shares its block's role, the test block's
     attempts have no window in train, cal or hpo_val, BASELINE is train and TRAILING eval_only in every fold; with
     --train-validate also training/train.py validate_data for every fold
  6. the outcomes the orchestrator reported equal the outcomes in the windows, and the scenario's expected outcomes and
     flags occurred
  7. against the simulated cluster's own ground truth (summary.json ground_truth, REVIEW4 MIN-1): no NORMAL, RECOVERY,
     SHAM, SHAM_POST or TRAILING window overlaps an interval in which the cluster was disturbed, every ANOMALOUS window lies
     inside one interval of its own attempt, and a BASELINE or TRAILING window is EXCLUDED only near a disturbance
  8. window grids are anchored at their segment (PRE_FAULT at the planned onset, INJECTION at the verified onset,
     RECOVERY at the labelled end, BASELINE and TRAILING at their start)
Exit 0 when every check passes. Prints one line per check.
"""
from __future__ import annotations

import argparse
import json
import math
import re
import sys
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

VALID = ("VALID", "VALID_REMEDIATED")
WINDOWED = ("PRE_FAULT", "INJECTION", "RECOVERY", "BASELINE", "TRAILING")
KEY = r"(ecommerce|iot)-c[0-5]-b(0[1-9]|10)-p[1-8]-a[1-9]"
LAYOUT = [re.compile(p) for p in (
    r"^plan/plan\.json$", r"^plan/plan\.json\.sha256$", r"^run/versions\.json$", r"^run/images\.lock\.yaml$",
    r"^run/feature_allowlist\.yaml$", r"^run/thresholds\.json$", r"^manifest/manifest\.json$",
    r"^stats/baseline_stats\.json$", r"^baseline/(features|verification)/chunk-\d+\.parquet$",
    rf"^export/attempts/{KEY}/(features|verification)\.parquet$",
    r"^export/trailing/(features|verification)/chunk-\d+\.parquet$", r"^probes/(baseline|trailing)/chunk-\d+\.parquet$",
    rf"^attempts/{KEY}/(probes\.parquet|verification\.json|injector\.log)$",
    rf"^attempts/{KEY}/chaos/(APPLY_PLUS_10S|MID|BEFORE_DELETE|AFTER_DELETE)-[^/]+\.yaml$",
    r"^logs/orchestrator-\d+\.log\.gz$", r"^checksums/SHA256SUMS$", r"^COMPLETE$",
)]

failures = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global failures
    print(("ok   " if ok else "FAIL ") + name + ("" if ok else f"\n       {detail}"))
    failures += 0 if ok else 1


def overlaps(a0: float, a1: float, b0: float, b1: float, tol: float = 0.002) -> bool:
    return min(a1, b1) - max(a0, b0) > tol


def expected_label(segment_type: str, run_type: str | None, outcome: str | None) -> str:
    """Protocol 3.3, written out again here."""
    if segment_type == "BASELINE":
        return "NORMAL"
    if segment_type == "TRAILING":
        return "TRAILING"
    if outcome not in VALID:
        return "EXCLUDED"
    if segment_type == "PRE_FAULT":
        return "NORMAL"
    if segment_type == "INJECTION":
        return "SHAM" if run_type == "F0" else "ANOMALOUS"
    if segment_type == "RECOVERY":
        return "SHAM_POST" if run_type == "F0" else "RECOVERY"
    return "EXCLUDED"


def ms(x: float) -> int:
    return int(round(x * 1000))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dump", type=Path, required=True)
    ap.add_argument("--derived", type=Path, required=True, help="<out>/derived/<campaign_id>")
    ap.add_argument("--expect", action="append", default=[], help="OUTCOME=COUNT")
    ap.add_argument("--expect-flag", action="append", default=[])
    ap.add_argument("--expect-outcome", action="append", default=[])
    ap.add_argument("--train-validate", action="store_true")
    ap.add_argument("--expect-runs", default=None, help="exit codes of the orchestrator runs, 'killed' for a SIGKILL")
    ap.add_argument("--expect-redeploy", action="store_true", help="the simulated Flink was redeployed at least once")
    ap.add_argument("--expect-failed-check", action="append", default=[], help="a failed attempt names this check")
    ap.add_argument("--expect-window-flag", action="append", default=[],
                    help="RUN_TYPE:FLAG, every window of that run type's attempts carries the flag")
    ap.add_argument("--expect-no-window-flag", action="append", default=[], help="no window carries this flag")
    ap.add_argument("--expect-health-excluded", action="append", default=[],
                    help="SEGMENT, at least one window of it EXCLUDED by the NORMAL-time health rule (decision 24)")
    a = ap.parse_args()
    s3 = a.dump / "s3"
    summary = json.loads((a.dump / "summary.json").read_text())
    m = json.loads((s3 / "manifest" / "manifest.json").read_text())

    # 1. layout and the volume
    keys = sorted(p.relative_to(s3).as_posix() for p in s3.rglob("*") if p.is_file())
    off = [k for k in keys if not any(r.match(k) for r in LAYOUT)]
    check("S3 mirror holds only 11.7 objects", not off, str(off[:5]))
    check("manifest/ holds manifest.json alone (C1)", [k for k in keys if k.startswith("manifest/")] == ["manifest/manifest.json"])
    sums = (s3 / "checksums" / "SHA256SUMS").read_text().splitlines()
    listed = [ln.split("  ", 1)[1] for ln in sums]
    check("SHA256SUMS lists every object but itself and COMPLETE, nothing else",
          listed == [k for k in keys if k not in ("COMPLETE", "checksums/SHA256SUMS")])
    vol = summary["volume_files"]
    check("resume state stays on the volume under state/ (C1)",
          "manifest.json" in vol and all(f == "manifest.json" or f.startswith("state/") for f in vol)
          and any(f.startswith("state/attempts/") and f.endswith("facts.json") for f in vol), str(vol[:6]))

    # 2. every second
    tl = pd.read_parquet(a.derived / "timeline.parquet").sort_values("start_at", kind="stable").reset_index(drop=True)
    starts, ends = tl["start_at"].map(ms).to_numpy(), tl["end_at"].map(ms).to_numpy()
    t0, t1 = ms(m["timeline_start_at"]), ms(m["timeline_end_at"])
    contiguous = starts[0] == t0 and ends[-1] == t1 and (starts[1:] == ends[:-1]).all() and (ends >= starts).all()
    check("timeline rows cover the campaign with no gap or overlap", bool(contiguous),
          f"{t0}..{t1} vs {starts[0]}..{ends[-1]}")
    pos = tl[ends > starts]
    secs = np.arange(math.ceil(t0 / 1000), math.floor(t1 / 1000))
    owner = np.searchsorted(pos["end_at"].map(ms).to_numpy(), secs * 1000, side="right")
    covered = (owner < len(pos)) & (pos["start_at"].map(ms).to_numpy()[np.clip(owner, 0, len(pos) - 1)] <= secs * 1000)
    check(f"every one of {len(secs)} seconds belongs to exactly one segment", bool(covered.all()))
    mine = sorted([(ms(x["start_at"]), ms(x["end_at"]), x["segment_type"]) for x in m["campaign_segments"]]
                  + [(ms(x["start_at"]), ms(x["end_at"]), x["segment_type"]) for at in m["attempts"] for x in at["segments"]])
    theirs = sorted(zip(starts.tolist(), ends.tolist(), tl["segment_type"].tolist()))
    check("timeline equals the manifest's campaign and attempt segments", mine == theirs, f"{len(mine)} vs {len(theirs)}")
    by_att = {at["attempt_key"]: at for at in m["attempts"]}
    bad_tl = [r.attempt_key for r in tl.itertuples() if r.attempt_key is not None and
              r.label != expected_label(r.segment_type, by_att[r.attempt_key]["run_type"], by_att[r.attempt_key]["outcome"])]
    check("timeline labels follow 3.3 for every attempt segment", not bad_tl, str(bad_tl[:5]))

    # 3. and 4. windows per segment, labels, exclusion
    sizes = sorted(int(re.match(r"^windows_(\d+)s\.parquet$", p.name).group(1)) for p in a.derived.glob("windows_*s.parquet"))
    check("window sizes follow 11.9", sizes == ([30] if m["campaign_index"] != 1 else
                                                 ([15, 30, 60, 120, 300] if m["workload"] == "iot" else [15, 30, 60, 120])),
          str(sizes))
    win_all = {}
    for w in sizes:
        win = pd.read_parquet(a.derived / f"windows_{w}s.parquet")
        win_all[w] = win
        exp = Counter()
        for at in m["attempts"]:
            for sg in at["segments"]:
                if sg["segment_type"] in WINDOWED:
                    exp[(at["attempt_key"], sg["segment_type"])] += (ms(sg["end_at"]) - ms(sg["start_at"])) // (w * 1000)
        for sg in m["campaign_segments"]:
            if sg["segment_type"] in WINDOWED:
                exp[(None, sg["segment_type"])] += (ms(sg["end_at"]) - ms(sg["start_at"])) // (w * 1000)
        got = Counter((r.attempt_key, r.segment_type) for r in win[["attempt_key", "segment_type"]].itertuples(index=False))
        exp = +exp
        check(f"{w}s: window count per attempt segment and baseline stretch is floor(length / {w})", got == exp,
              str([(k, exp.get(k), got.get(k)) for k in set(exp) | set(got) if exp.get(k) != got.get(k)][:5]))
        # BASELINE and TRAILING windows may also be EXCLUDED by the NORMAL-time health rule (decision 24); section 7 checks
        # each such exclusion against the ground truth
        lab_ok = all(r.label == expected_label(r.segment_type, r.run_type, r.outcome)
                     or (r.segment_type in ("BASELINE", "TRAILING") and r.label == "EXCLUDED") for r in
                     win[["segment_type", "run_type", "outcome", "label"]].itertuples(index=False))
        check(f"{w}s: every window carries the 3.3 label of its segment, run type and outcome", lab_ok)
        invalid = win[win["attempt_key"].notna() & ~win["outcome"].isin(VALID)]
        valid = win[win["attempt_key"].notna() & win["outcome"].isin(VALID)]
        check(f"{w}s: invalid attempts excluded whole ({invalid['attempt_key'].nunique()} attempts, {len(invalid)} windows)",
              bool((invalid["label"] == "EXCLUDED").all()))
        check(f"{w}s: valid attempts keep every window ({valid['attempt_key'].nunique()} attempts)",
              not (valid["label"] == "EXCLUDED").any())
        check(f"{w}s: window outcomes equal the orchestrator's", all(
            by_att[k]["outcome"] == o for k, o in valid[["attempt_key", "outcome"]].drop_duplicates().itertuples(index=False))
              and all(by_att[k]["outcome"] == o for k, o in invalid[["attempt_key", "outcome"]].drop_duplicates()
                      .itertuples(index=False)))
    reported = Counter(x["outcome"] for x in summary["attempts"])
    check("orchestrator's reported outcomes equal the manifest's", reported == Counter(x["outcome"] for x in m["attempts"]))

    # 5. folds
    folds = pd.read_parquet(a.derived / "folds.parquet")
    allw = pd.concat(win_all.values(), ignore_index=True)
    n_folds = folds["fold"].nunique()
    per = folds.groupby(["fold", "window_id"]).size()
    check(f"one role per window in each of {n_folds} folds", bool((per == 1).all())
          and len(folds) == n_folds * len(allw) and set(folds["window_id"]) == set(allw["window_id"]))
    j = folds.merge(allw[["window_id", "segment_type", "block", "attempt_key"]], on="window_id", how="left")
    seg = j["segment_type"]
    check("BASELINE is train and TRAILING eval_only in every fold",
          bool((j.loc[seg == "BASELINE", "role"] == "train").all() and (j.loc[seg == "TRAILING", "role"] == "eval_only").all()))
    att = j[j["attempt_key"].notna()]
    roles_per_block = att.groupby(["fold", "block"])["role"].nunique()
    check("every window of a block shares its block's role in each fold", bool((roles_per_block == 1).all()))
    leak = []
    for f, g in att.groupby("fold"):
        test_keys = set(g.loc[g["role"] == "test", "attempt_key"])
        other = set(g.loc[g["role"].isin(["train", "cal", "hpo_val"]), "attempt_key"])
        if test_keys & other or g.loc[g["role"] == "test", "block"].nunique() != 1:
            leak.append(f)
        if m["campaign_index"] != 0:
            br = g.groupby("block")["role"].first()
            if sorted(br.tolist()).count("test") != 1 or sorted(br.tolist()).count("cal") != 1 \
                    or sorted(br.tolist()).count("hpo_val") != 1:
                leak.append(f)
    check("no fold leakage: one test block per fold, its attempts never in train, cal or hpo_val", not leak, str(leak))
    if a.train_validate:
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipeline"))
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipeline" / "training"))
        import train  # noqa: PLC0415
        bad = []
        for w, win in win_all.items():
            for f in sorted(folds["fold"].unique()):
                try:
                    train.validate_data(win, folds, int(f))
                except SystemExit:
                    bad.append((w, int(f)))
        check("training/train.py validate_data passes for every fold and window size", not bad, str(bad))

    # 6. scenario expectations
    got = Counter(x["outcome"] for x in m["attempts"])
    for e in a.expect:
        k, _, n = e.partition("=")
        check(f"scenario: {n} {k} attempt(s)", got.get(k, 0) == int(n), str(dict(got)))
    for o in a.expect_outcome:
        check(f"scenario: at least one {o} attempt", got.get(o, 0) >= 1, str(dict(got)))
    flags = Counter(f for x in m["attempts"] for f in x["flags"])
    for fl in a.expect_flag:
        check(f"scenario: at least one attempt flagged {fl}", flags.get(fl, 0) >= 1, str(dict(flags)))
    if a.expect_runs is not None:
        runs = ["killed" if r.get("killed") else str(r["exit_code"]) for r in summary["runs"]]
        check(f"scenario: orchestrator runs ended {a.expect_runs}", runs == a.expect_runs.split(","), str(runs))
        aborted = [x for x in m["attempts"] if x["outcome"] == "ABORTED"]
        if not aborted:  # a halt by the halt rules (sham2) leaves no attempt RUNNING, so nothing to finish as ABORTED
            check("scenario: every halt was RESUMED", bool(m["halts"]) and all(h["resolution"] == "RESUMED" for h in m["halts"]))
    if a.expect_runs is not None and any(x["outcome"] == "ABORTED" for x in m["attempts"]):
        resumed = [h for h in m["halts"] if h["resolution"] == "RESUMED" and h["attempt_key"] in {x["attempt_key"] for x in aborted}]
        check("scenario: the interrupted attempt was finished as ABORTED and its halt RESUMED", bool(aborted) and bool(resumed))
        nxt = [x for x in m["attempts"] if aborted and x["cell_key"] == aborted[0]["cell_key"]
               and x["attempt"] == aborted[0]["attempt"] + 1]
        check("scenario: the ABORTED attempt is repeated with the next attempt number and passes",
              bool(nxt) and nxt[0]["outcome"] in VALID)
        idle = [s_ for s_ in m["campaign_segments"] if s_["segment_type"] == "IDLE"
                and aborted and s_["start_at"] >= aborted[0]["ended_at"] - 1e-6 and s_["end_at"] <= nxt[0]["started_at"] + 1e-6]
        check("scenario: the restart, cleanup and recovery gate after the resume are IDLE (11.5)",
              bool(idle) and sum(x["end_at"] - x["start_at"] for x in idle) >= 300 - 1e-6)
    if a.expect_redeploy:
        check("scenario: Flink was redeployed (new JobManager pod)", len(summary["redeploys"]) >= 1, str(summary["redeploys"]))
        after = [x for x in m["attempts"] if "AFTER_REDEPLOY" in x["flags"]]
        settle_ok = all(next(sg for sg in x["segments"] if sg["segment_type"] == "SETTLE")["end_at"]
                        - x["started_at"] >= 600 for x in after)
        check("scenario: an attempt after the redeploy has the 600 s extra settle (1.3)", bool(after) and settle_ok)
    for fc in a.expect_failed_check:
        check(f"scenario: a failed attempt names {fc}", any(fc in x["failed_checks"] for x in m["attempts"]),
              str([x["failed_checks"] for x in m["attempts"] if x["failed_checks"]]))
    w30 = win_all.get(30, next(iter(win_all.values())))
    for spec in a.expect_window_flag:
        rt, _, fl = spec.partition(":")
        sel = w30[w30["run_type"] == rt]
        check(f"scenario: every {rt} window carries {fl}", len(sel) > 0 and all(fl in json.loads(x) for x in sel["flags"]))
    for fl in a.expect_no_window_flag:
        check(f"scenario: no window carries {fl}", not any(fl in json.loads(x) for x in w30["flags"]))

    # 7. ground truth of the simulated cluster (MIN-1)
    # Chaos Mesh event timestamps have one-second precision (RFC 3339 without fractions), so a labelled onset or end taken
    # from them (2.4) can sit up to 1 s from the moment the simulated fault really started or stopped. The ground-truth
    # comparison allows that 1 s and nothing more; Review 4 probe 9 moved an end by 50 s.
    EVENT_PRECISION_S = 1.0
    gt = summary.get("ground_truth") or []
    effects = [g for g in gt if g["kind"] == "effect"]
    clean = ("NORMAL", "RECOVERY", "SHAM", "SHAM_POST", "TRAILING")
    for w, win in win_all.items():
        bad = []
        for r in win[win["label"].isin(clean)][["window_id", "start_at", "end_at", "label"]].itertuples(index=False):
            hit = [g for g in effects if overlaps(r.start_at, r.end_at, g["start"], g["end"], tol=EVENT_PRECISION_S)]
            if hit:
                bad.append((r.window_id, r.label, hit[0]["run_type"], hit[0]["pod"]))
        check(f"{w}s: no NORMAL, RECOVERY, SHAM or TRAILING window overlaps a ground-truth disturbance", not bad, str(bad[:4]))
        anom = win[win["label"] == "ANOMALOUS"]
        outside = []
        for r in anom[["window_id", "attempt_key", "start_at", "end_at"]].itertuples(index=False):
            own = [g for g in gt if g["attempt_key"] == r.attempt_key]
            if not any(g["start"] - EVENT_PRECISION_S <= r.start_at and r.end_at <= g["end"] + EVENT_PRECISION_S for g in own):
                outside.append(r.window_id)
        check(f"{w}s: every ANOMALOUS window lies inside a ground-truth interval of its attempt ({len(anom)} windows)",
              not outside, str(outside[:4]))
        ex = win[win["segment_type"].isin(["BASELINE", "TRAILING"]) & (win["label"] == "EXCLUDED")]
        unjust = [r.window_id for r in ex[["window_id", "start_at", "end_at"]].itertuples(index=False)
                  if not any(overlaps(r.start_at - 20, r.end_at + 20, g["start"], g["end"]) for g in gt)]
        check(f"{w}s: every BASELINE or TRAILING window the health rule EXCLUDED lies near a disturbance ({len(ex)})",
              not unjust, str(unjust[:4]))
    for sg in a.expect_health_excluded:
        n = int(((w30["segment_type"] == sg) & (w30["label"] == "EXCLUDED")).sum())
        check(f"scenario: the NORMAL-time health rule EXCLUDED {n} {sg} windows at {30 if 30 in win_all else sizes[0]} s", n >= 1)

    # 8. grid anchoring (3.2)
    anchor_bad = []
    for w, win in win_all.items():
        wm = w * 1000
        for at in m["attempts"]:
            mine = win[win["attempt_key"] == at["attempt_key"]]
            for sg in at["segments"]:
                part = mine[mine["segment_type"] == sg["segment_type"]]
                if part.empty:
                    continue
                st, en = part["start_at"].map(ms).to_numpy(), part["end_at"].map(ms).to_numpy()
                if sg["segment_type"] == "PRE_FAULT":
                    ok = en.max() == ms(sg["end_at"]) and ((ms(sg["end_at"]) - en) % wm == 0).all()
                else:
                    ok = st.min() == ms(sg["start_at"]) and ((st - ms(sg["start_at"])) % wm == 0).all()
                if not ok:
                    anchor_bad.append((w, at["attempt_key"], sg["segment_type"]))
        for sg in m["campaign_segments"]:
            if sg["segment_type"] in ("BASELINE", "TRAILING"):
                part = win[(win["segment_type"] == sg["segment_type"]) & (win["start_at"] >= sg["start_at"] - 1e-6)
                           & (win["end_at"] <= sg["end_at"] + 1e-6)]
                if not part.empty and not ((part["start_at"].map(ms).to_numpy() - ms(sg["start_at"])) % wm == 0).all():
                    anchor_bad.append((w, None, sg["segment_type"]))
    check("window grids are anchored at their segments (PRE_FAULT at the planned onset, the others at their start)",
          not anchor_bad, str(anchor_bad[:4]))
    cells = Counter(x["cell_key"] for x in m["attempts"] if x["outcome"] in VALID)
    plan = json.loads((s3 / "plan" / "plan.json").read_text())
    check("every plan cell ends with exactly one valid attempt",
          sorted(cells) == sorted(c["cell_key"] for b in plan["blocks"] for c in b["cells"]) and set(cells.values()) == {1})
    print(f"{summary['campaign_id']}: {len(m['attempts'])} attempts {dict(got)}, {len(tl)} segments, "
          f"{sum(len(x) for x in win_all.values())} windows over {sizes}, {failures} failed checks")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
