"""make numbers (protocol 7.1 step 10, 7.2): numbers.tex and claims.json from the analysis outputs.

Added on 18 October 2026, after registration (D4). Every number in the abstract, results, discussion and conclusion
is written as \\R{key}. numbers.tex defines each key once, and \\R stops the LaTeX run on a key it does not know.
claims.json maps every key to its value, the output file and the field that produced it, and the script.

Formats (5.6): AUC values and differences to three decimals, differences signed, p-values to three significant
digits, counts as integers, shares as percentages with no decimals, seconds to the nearest second.

    python3 -m analysis.numbers_tex --out DIR --tex numbers.tex --claims claims.json
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

import pandas as pd

PIPE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPE))
from analysis.sources import DEEP, PANEL  # noqa: E402
from streamprep.contract import sha256_file  # noqa: E402

WL = {"ecommerce": "ec", "iot": "iot"}


def f3(x):
    return "n/a" if x is None or (isinstance(x, float) and math.isnan(x)) else f"{x:.3f}"


def s3(x):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "n/a"
    s = f"{x:+.3f}"
    return s if not s.startswith("-0.000") else "0.000"


def pfmt(p):
    if p is None or math.isnan(p):
        return "n/a"
    if p == 0:
        return "0"
    if p < 1e-4:
        e = math.floor(math.log10(p))
        return f"{p / 10 ** e:.2f}\\times 10^{{{e}}}"
    return f"{p:.3g}"


def tex_num(s: str) -> str:
    if s.startswith("-") or s.startswith("+") or "\\times" in s:
        return "\\ensuremath{" + s.replace("-", "-") + "}"
    return s


OPS = {"min": min, "max": max, "sum": sum, "mean": lambda x: sum(x) / len(x),
       "maxabsdiff": lambda x: max(abs(x[i] - x[i + 1]) for i in range(0, len(x), 2))}


class Book:
    def __init__(self):
        self.vals, self.claims = {}, {}

    def add(self, key, text, value, source, field, script):
        if key in self.vals:
            raise SystemExit(f"ERROR: number key {key} defined twice")
        self.vals[key] = text
        self.claims[key] = {"text": text, "value": value, "source": source, "field": field, "script": script}

    def derive(self, key, op, keys, fmt=None):
        """A number computed from other keys: op is min, max, sum or mean. check.py recomputes it from the claims
        of those keys, which are themselves re-read from their sources."""
        vals = [self.claims[k]["value"] for k in keys]
        if any(v is None for v in vals):
            raise SystemExit(f"ERROR: {key} derives from a key with no value")
        v = OPS[op](vals)
        if fmt is None and "per_h" in keys[0]:
            fmt = lambda x: f"{x:.2f}"
        if fmt is None:
            src = self.vals[keys[0]]
            fmt = (lambda x: tex_num(s3(x))) if ("+" in src or "ensuremath" in src) else \
                  (lambda x: f"{x:,}") if isinstance(v, int) else (lambda x: f3(x))
        self.add(key, fmt(v), v, "derived", f"{op}:" + "|".join(keys), "analysis/numbers_tex.py")

    def est(self, key, est, source, field, script):
        """An estimate dict with mean and ci95: key.est, key.lo, key.hi, key.p."""
        self.add(f"{key}.est", tex_num(s3(est["mean"])), est["mean"], source, f"{field}.mean", script)
        self.add(f"{key}.lo", tex_num(s3(est["ci95_lo"])), est["ci95_lo"], source, f"{field}.ci95_lo", script)
        self.add(f"{key}.hi", tex_num(s3(est["ci95_hi"])), est["ci95_hi"], source, f"{field}.ci95_hi", script)
        if "p_t" in est:
            self.add(f"{key}.p", tex_num(pfmt(est["p_t"])), est["p_t"], source, f"{field}.p_t", script)


def confirm_numbers(b: Book, res: dict):
    src, sc = "confirm.json", "analysis/confirm.py"
    for w, W in res["workloads"].items():
        k = WL[w]
        for h, p in W["family_p"].items():
            b.add(f"{h}.{k}.familyp", tex_num(pfmt(p)), p, src, f"workloads.{w}.family_p.{h}", sc)
            b.add(f"{h}.{k}.holmp", tex_num(pfmt(W["family_p_holm"][h])), W["family_p_holm"][h], src,
                  f"workloads.{w}.family_p_holm.{h}", sc)
        H = W["hypotheses"]
        for h in ("C1", "C2a", "C2b", "C3", "C4", "C6"):
            b.add(f"{h}.{k}.outcome", H[h]["outcome"], H[h]["outcome"], src, f"workloads.{w}.hypotheses.{h}.outcome",
                  sc)
            for ck, r in H[h]["contrasts"].items():
                name = ck.replace("|", ".")
                f = f"workloads.{w}.hypotheses.{h}.contrasts.{ck}"
                b.est(f"{h}.{k}.{name}", r["pooled"], src, f"{f}.pooled", sc)
                b.add(f"{h}.{k}.{name}.decision", r["decision"]["outcome"], r["decision"]["outcome"], src,
                      f"{f}.decision.outcome", sc)
                b.add(f"{h}.{k}.{name}.replication", r["replication"]["status"], r["replication"]["status"], src,
                      f"{f}.replication.status", sc)
                for c, v in r["per_campaign"].items():
                    b.add(f"{h}.{k}.{name}.{c}", tex_num(s3(v["mean"])), v["mean"], src, f"{f}.per_campaign.{c}.mean",
                          sc)
        mx = H["C3"]["mixed"]
        b.add(f"C3.{k}.mixed.coef", tex_num(s3(mx["coef"])), mx["coef"], src, f"workloads.{w}.hypotheses.C3.mixed.coef", sc)
        b.add(f"C3.{k}.mixed.p", tex_num(pfmt(mx["p"])), mx["p"], src, f"workloads.{w}.hypotheses.C3.mixed.p", sc)
        b.add(f"C6.{k}.nequivalent", str(len(H["C6"]["equivalent_models"])), len(H["C6"]["equivalent_models"]), src,
              f"workloads.{w}.hypotheses.C6.equivalent_models#len", sc)
    c5 = res["workloads"]["ecommerce"]["hypotheses"]["C5"]
    f = "workloads.ecommerce.hypotheses.C5"
    b.add("C5.rho", f3(c5["median_rho"]), c5["median_rho"], src, f"{f}.median_rho", sc)
    b.add("C5.p", tex_num(pfmt(c5["p_perm"])), c5["p_perm"], src, f"{f}.p_perm", sc)
    b.add("C5.nperm", f"{c5['n_perm']:,}", c5["n_perm"], src, f"{f}.n_perm", sc)
    b.add("C5.timeref", tex_num(s3(c5["time_reference_rho"])), c5["time_reference_rho"], src,
          f"{f}.time_reference_rho", sc)
    for w in ("ecommerce", "iot"):
        b.add(f"C5.ceiling.{WL[w]}", f3(c5["ceiling"][w]["median_rho"]), c5["ceiling"][w]["median_rho"], src,
              f"{f}.ceiling.{w}.median_rho", sc)
    for d, v in c5["per_detector_rho"].items():
        b.add(f"C5.rho.{d}", f3(v), v, src, f"{f}.per_detector_rho.{d}", sc)
    n_rep, n_all = 0, 0
    for w, D in res["C7"].items():
        for d, v in D.items():
            n_all += 1
            n_rep += bool(v["reproducible"])
            b.add(f"C7.{WL[w]}.{d}.mean", f3(v["pooled_mean"]), v["pooled_mean"], src, f"C7.{w}.{d}.pooled_mean", sc)
            b.add(f"C7.{WL[w]}.{d}.maxdev", f3(v["max_abs_deviation"]), v["max_abs_deviation"], src,
                  f"C7.{w}.{d}.max_abs_deviation", sc)
    b.add("C7.nreproducible", str(n_rep), n_rep, src, "C7.*.*.reproducible", sc)
    b.add("C7.npairs", str(n_all), n_all, src, "C7.*.*", sc)
    tof = res["time_only_flags"]
    vals = [v["mean"] for v in tof.values()]
    b.add("timeonly.min", f"{min(vals):.2f}", min(vals), src, "time_only_flags.*.mean", sc)
    b.add("timeonly.max", f"{max(vals):.2f}", max(vals), src, "time_only_flags.*.mean", sc)
    b.add("timeonly.nflagged", str(sum(v["flag"] for v in tof.values())), sum(v["flag"] for v in tof.values()), src,
          "time_only_flags.*.flag", sc)


def posthoc_numbers(b: Book, ph: dict):
    src, sc = "posthoc.json", "analysis/posthoc.py"
    for w, W in ph["within_run"].items():
        k = WL[w]
        for m, r in W["C1_within_run_auc"].items():
            b.est(f"PH.C1.{k}.{m}", r["pooled"], src, f"within_run.{w}.C1_within_run_auc.{m}.pooled", sc)
            b.add(f"PH.C1.{k}.{m}.decision", r["decision"]["outcome"], r["decision"]["outcome"], src,
                  f"within_run.{w}.C1_within_run_auc.{m}.decision.outcome", sc)
        b.est(f"PH.C3.{k}", W["C3_within_run_auc"]["pooled"], src, f"within_run.{w}.C3_within_run_auc.pooled", sc)
        x = ph["shams_needed"][w]
        b.add(f"PH.shamsd.{k}", f"{x['median_sd_sham']:.2f}", x["median_sd_sham"], src, f"shams_needed.{w}.median_sd_sham", sc)
        b.add(f"PH.withinsd.{k}", f"{x['median_sd_within_run']:.2f}", x["median_sd_within_run"], src,
              f"shams_needed.{w}.median_sd_within_run", sc)
        lo, hi = x["range_shams_needed"]
        b.add(f"PH.shamsneeded.{k}.lo", str(lo), lo, src, f"shams_needed.{w}.range_shams_needed.0", sc)
        b.add(f"PH.shamsneeded.{k}.hi", str(hi), hi, src, f"shams_needed.{w}.range_shams_needed.1", sc)
    sp = [v["mean"] for d in PANEL + ["RF", "RuleBased", "EWMA"] for c, v in ph["sham_placebo"][d].items()
          if not c.endswith("-a")]
    b.add("PH.shammean.min", f"{min(sp):.2f}", min(sp), src, "sham_placebo.*.*.mean", sc)
    b.add("PH.shammean.max", f"{max(sp):.2f}", max(sp), src, "sham_placebo.*.*.mean", sc)


def describe_numbers(b: Book, out: Path):
    src, sc = "describe/performance.parquet", "analysis/describe.py"
    perf = pd.read_parquet(out / "describe" / "performance.parquet")
    for r in perf.itertuples():
        k = f"perf.{WL[r.workload]}.{r.detector}.{r.metric}"
        for part, v in (("pooled", r.pooled_mean), ("c1", r.c1_mean), ("lo", r.pooled_lo), ("hi", r.pooled_hi)):
            b.add(f"{k}.{part}", f3(v), v, src, f"workload={r.workload},detector={r.detector},metric={r.metric}.{part}",
                  sc)
    src = "describe/operating.parquet"
    oper = pd.read_parquet(out / "describe" / "operating.parquet")
    for r in oper.itertuples():
        k = f"oper.{WL[r.workload]}.{r.detector}.{r.metric.replace('_fpb_', '.b')}"
        b.add(f"{k}.pooled", f"{r.pooled_mean:.2f}" if "per_h" in r.metric else f3(r.pooled_mean), r.pooled_mean, src,
              f"workload={r.workload},detector={r.detector},metric={r.metric}.pooled", sc)
    src = "describe/latency.parquet"
    lat = pd.read_parquet(out / "describe" / "latency.parquet")
    for r in lat[lat["scope"] == "all faults"].itertuples():
        k = f"lat.{WL[r.workload]}.{r.set}.{r.detector}.{r.budget.replace('fpb_', 'b')}"
        b.add(f"{k}.rate", f"{100 * r.detection_rate:.0f}\\%", r.detection_rate, src, f"{k}.detection_rate", sc)
        b.add(f"{k}.rmst", f"{r.rmst_s:.0f}", r.rmst_s, src, f"{k}.rmst_s", sc)
    src = "describe/categories.parquet"
    cat = pd.read_parquet(out / "describe" / "categories.parquet")
    for r in cat.itertuples():
        k = f"cat.{WL[r.workload]}.{r.variant}.{r.metric}"
        b.add(f"{k}.diff", tex_num(s3(r.diff_pooled)), r.diff_pooled, src, f"{k}.diff_pooled", sc)


# Exploratory, gate and cost outputs: (file under the results folder, key prefix, id columns, value columns).
# Keys are <prefix>.<id values>.<column>, with the workload shortened to ec or iot.
TABLES = [
    ("gate/faults.csv", "gate", ["campaign_id", "run_type"],
     ["labelled_runs", "attempts", "repeats", "VALID", "flag_F7_TM_LOSS", "onset_lag_median_s", "span_median_s",
      "span_min_s", "span_max_s"]),
    ("gate/labels.csv", "labels", ["campaign_id"],
     ["windows_total", "w_BASELINE__NORMAL", "w_INJECTION__ANOMALOUS", "w_INJECTION__SHAM", "w_PRE_FAULT__NORMAL",
      "w_RECOVERY__RECOVERY", "w_RECOVERY__SHAM_POST", "w_TRAILING__TRAILING", "attempt_eval_windows",
      "anomalous_prevalence_attempt_windows"]),
    ("explore/replay.csv", "replay", ["workload", "detector", "op", "metric"], ["pooled_c2_c3", "campaign_1"]),
    ("explore/drift.csv", "drift", ["campaign_id", "test", "features"], ["cv_auc"]),
    ("explore/per_fault.csv", "fault", ["workload", "detector", "fault", "metric"], ["pooled_c2_c3", "campaign_1"]),
    ("explore/prevalence.csv", "prev", ["workload", "detector", "op", "prevalence"], ["f1", "always_fire_f1"]),
    ("explore/iqr.csv", "iqr", ["workload", "detector"], ["pooled_c2_c3_f1", "pooled_c2_c3_fpr"]),
    ("explore/carryover.csv", "carry", ["scope"], ["H", "p", "n_attempts"]),
    ("explore/load_phase_rayleigh.csv", "rayleigh", ["workload", "scope", "run_type"], ["p", "n"]),
    ("explore/load_phase_covariate.csv", "phasecov", ["workload", "detector"], ["p_phase"]),
    ("explore/lofo.csv", "lofo", ["workload", "detector", "held_out", "metric"], ["pooled_c2_c3"]),
    ("explore/three_state.csv", "three", ["workload", "detector", "regime", "metric"], ["pooled_c2_c3"]),
    ("explore/windows.csv", "win", ["campaign_id", "detector", "window_s"], ["pooled_auc", "within_run_auc"]),
    ("explore/rf_importance_by_source.csv", "rfimp", ["campaign_id", "source"], ["share_of_top50_importance"]),
    ("cost/cost.csv", "cost", ["workload", "model"], ["fit_s", "score_ms_per_window", "single_ms_p50"]),
    ("ewma_mechanism/ablation_summary.csv", "ewmaab", ["workload", "variant"],
     ["d_pooled_auc", "d_pooled_auc_lo", "d_pooled_auc_hi", "p_pooled_auc", "d_within_run_auc",
      "d_within_run_auc_lo", "d_within_run_auc_hi"]),
    ("ewma_mechanism/saturation_summary.csv", "sat", ["workload", "campaign_id", "detector"],
     ["auc", "auc_best_case", "frac_at_max"]),
    ("ewma_mechanism/magnitude_faults_contrast.csv", "magf", ["workload", "detector"], ["d_mean", "d_lo", "d_hi", "p"]),
    ("ewma_mechanism/argmax_category.csv", "argmax", ["workload", "operating_point", "kind", "category"],
     ["share", "feature_share"]),
    ("posthoc_review/matched_c1.csv", "phm", ["workload", "detector"], ["est", "lo", "hi"]),
    ("posthoc_review/carryover.csv", "phc", ["workload", "detector"],
     ["within_run_auc_all", "within_run_auc_without", "r1_placebo_all", "r1_placebo_all_lo", "r1_placebo_all_hi",
      "r1_placebo_without", "c1_without_est", "c1_without_lo", "c1_without_hi"]),
    ("posthoc_review/without_f8.csv", "phf8", ["workload", "detector"],
     ["auc_all", "auc_without_f8", "f8_vs_normal", "f8_share_of_pos", "c1_est", "c1_lo", "c1_hi"]),
    ("posthoc_review/magnitude_without_tm_loss.csv", "phmag", ["workload", "detector"], ["d_mean", "d_lo", "d_hi"]),
    ("posthoc_review/normal_fpr.csv", "phfpr", ["workload", "detector", "budget", "set"], ["fpr"]),
    ("posthoc_review/false_alert_share.csv", "phfas", ["workload", "detector", "budget"], ["recovery_share"]),
    ("posthoc_review/ewma_r1_inputs.csv", "phewma", ["workload", "reference", "metric"], ["est", "lo", "hi", "c1"]),
    ("posthoc_review/drift_blocked.csv", "phdrift", ["campaign_id", "test"], ["cv_auc_blocked"]),
    ("posthoc_review/c5_power.csv", "phc5", ["test"], ["critical_median_rho"]),
    ("gate/sham_false_passes.csv", "sfp", ["campaign_id", "signature"], ["sham_attempts", "false_passes"]),
    ("gate/cross_signatures.csv", "xsig", ["campaign_id", "run_type", "signature"], ["passes", "attempts"]),
    ("cost/ec2_phase_b_train_time.csv", "ec2", ["workload", "model"], ["median_s"]),
    ("classical/classical_summary.csv", "classical", ["workload", "detector"],
     ["d_pooled_auc_vs_RuleBased", "d_pooled_auc_vs_RuleBased_lo", "d_pooled_auc_vs_RuleBased_hi",
      "d_within_run_auc_vs_RuleBased"]),
]
INTS = {"labelled_runs", "attempts", "repeats", "VALID", "flag_F7_TM_LOSS", "n_attempts", "n", "windows_total",
        "attempt_eval_windows", "sham_attempts", "false_passes", "passes"}
SIGNED_PREFIX = {"phm", "phmag", "phewma"}          # their est, lo, hi and c1 are differences
SIGNED_COLS = {"est", "lo", "hi", "c1", "c1_est", "c1_lo", "c1_hi", "c1_without_est", "c1_without_lo", "c1_without_hi"}


def fmt_cell(col: str, v) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "n/a"
    if col in INTS or col.startswith("w_"):
        return f"{int(v):,}"
    if col == "p" or col.startswith("p_"):
        return tex_num(pfmt(float(v)))
    if col.startswith("d_") or col.startswith("coef_"):
        return tex_num(s3(float(v)))
    if col.endswith("_s") and col != "fit_s":
        return f"{float(v):.0f}" if abs(float(v)) >= 10 else f"{float(v):.1f}"
    if col == "fit_s" or col.endswith("_ms_per_window") or col.endswith("_p50"):
        x = float(v)
        return f"{x:.3g}" if x < 100 else f"{x:.0f}"
    if col.startswith("share") or col.startswith("frac") or col.endswith("_share") or "share_of" in col:
        return f"{100 * float(v):.0f}\\%"
    if col in ("H",):
        return f"{float(v):.2f}"
    return f3(float(v))


def table_numbers(b: Book, out: Path):
    for src, prefix, ids, cols in TABLES:
        path = out / src
        if not path.is_file():
            continue
        df = pd.read_csv(path)
        for _, r in df.iterrows():
            idv = [WL.get(str(r[c]), str(r[c])).replace(" ", "_") for c in ids]   # "all three" -> all_three
            cond = ",".join(f"{c}={r[c]}" for c in ids)
            for col in cols:
                if col not in df.columns:
                    raise SystemExit(f"ERROR: {src} has no column {col}")
                v = r[col]
                val = None if pd.isna(v) else (v.item() if hasattr(v, "item") else v)
                if val is None and col not in df.columns:
                    continue
                signed = (col in SIGNED_COLS and (prefix in SIGNED_PREFIX or col.startswith("c1"))
                          and str(r.get("reference", "")) != "absolute")
                if signed and val is not None:
                    b.add(".".join([prefix, *idv, col]), tex_num(s3(float(val))), val, src, f"{cond}.{col}",
                          "analysis/posthoc_review.py")
                    continue
                text = (f"{val:.2f}" if (val is not None and "metric" in ids and "per_h" in str(r["metric"]))
                        else fmt_cell(col, val))
                b.add(".".join([prefix, *idv, col]), text, val, src, f"{cond}.{col}",
                      "analysis/" + src.split("/")[0].replace("explore", "explore.py").replace("gate", "gate_summary.py")
                      .replace("cost", "cost.py").replace("ewma_mechanism", "ewma_mechanism.py")
                      .replace("classical", "classical.py"))


def facts_numbers(b: Book, facts: dict):
    """facts.json flattened: facts.<path>, integers with thousands separators, hours to one decimal."""
    def walk(obj, path):
        if isinstance(obj, dict):
            for k, v in obj.items():
                walk(v, path + [str(k)])
            return
        key = ".".join(["facts", *[WL.get(x, x) for x in path]])
        if isinstance(obj, bool) or obj is None:
            return
        text = f"{obj:,}" if isinstance(obj, int) else (f"{obj:.1f}" if path[-1].endswith("hours") else f3(obj))
        b.add(key, text, obj, "facts.json", ".".join(path), "analysis/facts.py")
    walk(facts, [])


AE = ["LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE"]
DETS = PANEL + ["RF", "RuleBased", "EWMA"]


def derived_numbers(b: Book):
    """Ranges and totals the manuscript quotes, each recomputed by check.py from the keys it names."""
    have = lambda k: k in b.vals
    for w in ("ec", "iot"):
        b.derive(f"rng.C1.{w}.pooled.min", "min", [f"C1.{w}.{m}.pooled_auc.est" for m in DEEP])
        b.derive(f"rng.C1.{w}.pooled.max", "max", [f"C1.{w}.{m}.pooled_auc.est" for m in DEEP])
        b.derive(f"rng.C1.{w}.wrpc.lo.min", "min", [f"C1.{w}.{m}.within_run_pc.lo" for m in DEEP])
        b.derive(f"rng.C1.{w}.wrpc.hi.max", "max", [f"C1.{w}.{m}.within_run_pc.hi" for m in DEEP])
        b.derive(f"rng.PH.C1.{w}.AE.min", "min", [f"PH.C1.{w}.{m}.est" for m in AE])
        b.derive(f"rng.PH.C1.{w}.AE.max", "max", [f"PH.C1.{w}.{m}.est" for m in AE])
        for grp, members in (("AE", AE), ("point", ["IF", "OC_SVM", "RuleBased"]), ("svdddagmm", ["DeepSVDD", "DAGMM"]),
                             ("panel", PANEL), ("dets", DETS)):
            for m in ("pooled_auc", "within_run_auc", "pr_auc"):
                ks = [f"perf.{w}.{d}.{m}.pooled" for d in members]
                b.derive(f"rng.perf.{w}.{grp}.{m}.min", "min", ks)
                b.derive(f"rng.perf.{w}.{grp}.{m}.max", "max", ks)
        for op in ("b0.01", "b0.05"):
            for m in ("episode_rate", "fpr", "trailing_fa_per_h", "sham_episode"):
                ks = [f"oper.{w}.{d}.{m}.{op}.pooled" for d in DETS]
                b.derive(f"rng.oper.{w}.{m}.{op}.min", "min", ks)
                b.derive(f"rng.oper.{w}.{m}.{op}.max", "max", ks)
            ks = [f"lat.{w}.pooled.{d}.{op}.rate" for d in DETS]
        wl = {"ec": "ecommerce", "iot": "iot"}[w]
        for m in ("false_per_h", "trailing_episodes_per_h", "detection_rate", "alert_windows_per_h"):
            ks = [f"replay.{w}.{d}.fpb_0.01.{m}.pooled_c2_c3" for d in DETS]
            b.derive(f"rng.replay.{w}.{m}.min", "min", ks)
            b.derive(f"rng.replay.{w}.{m}.max", "max", ks)
        ks = [f"fault.{w}.{d}.F8.pooled_auc.pooled_c2_c3" for d in DETS]
        b.derive(f"rng.fault.{w}.F8.min", "min", ks)
        b.derive(f"rng.fault.{w}.F8.max", "max", ks)
        others = [f"fault.{w}.{d}.{f}.pooled_auc.pooled_c2_c3" for d in DETS for f in ("F1", "F2", "F4", "F5", "F7")]
        b.derive(f"rng.fault.{w}.F12457.min", "min", others)
        cs = ["ec-a", "ec-b", "ec-c"] if w == "ec" else ["iot-a", "iot-b", "iot-c"]
        b.derive(f"sum.gate.{w}.F7_TM_LOSS", "sum", [f"gate.{c}.F7.flag_F7_TM_LOSS" for c in cs])
        b.derive(f"sum.gate.{w}.valid", "sum", [f"gate.{c}.{rt}.VALID" for c in cs
                                                for rt in ("F0", "F1", "F2", "F4", "F5", "F6", "F7", "F8")])
        ks = [f"sat.{w}.{c}.{d}.frac_at_max" for c in cs[1:] for d in AE]
        b.derive(f"rng.sat.{w}.AE.frac.min", "min", ks, fmt=lambda x: f"{100 * x:.0f}\\%")
        b.derive(f"rng.sat.{w}.AE.frac.max", "max", ks, fmt=lambda x: f"{100 * x:.0f}\\%")
        ks = [f"rfimp.{c}.jvm.share_of_top50_importance" for c in cs]
        b.derive(f"rng.rfimp.{w}.jvm.min", "min", ks, fmt=lambda x: f"{100 * x:.0f}\\%")
        b.derive(f"rng.rfimp.{w}.jvm.max", "max", ks, fmt=lambda x: f"{100 * x:.0f}\\%")
    for w in ("ec", "iot"):
        cs = ["ec-b", "ec-c"] if w == "ec" else ["iot-b", "iot-c"]
        ks = [f"sat.{w}.{c}.{d}.auc_best_case" for c in cs for d in AE]
        b.derive(f"rng.sat.{w}.AE.best.min", "min", ks)
        b.derive(f"rng.sat.{w}.AE.best.max", "max", ks)
        ks = [f"magf.{w}.{d}.d_mean" for d in DETS if d != "EWMA" and have(f"magf.{w}.{d}.d_mean")]
        b.derive(f"rng.magf.{w}.others.min", "min", ks)
        b.derive(f"rng.magf.{w}.others.max", "max", ks)
        for grp, vs in (("single", ["broker", "consumer", "flink", "jvm"]),
                        ("leaveout", ["no_broker", "no_consumer", "no_flink", "no_jvm"])):
            ks = [f"cat.{w}.{v}.pooled_auc.diff" for v in vs]
            b.derive(f"rng.cat.{w}.{grp}.min", "min", ks)
            b.derive(f"rng.cat.{w}.{grp}.max", "max", ks)
        ks = [f"rayleigh.{w}.all_three.{f}.p" for f in ("F1", "F2", "F4", "F5", "F6", "F7", "F8")]
        b.derive(f"rng.rayleigh.{w}.min", "min", ks, fmt=lambda x: tex_num(pfmt(x)))
        ks = [f"lofo.{w}.{d}.{f}.f1_cost.pooled_c2_c3" for d in DETS for f in ("F1", "F2", "F4", "F5", "F6", "F7", "F8")
              if have(f"lofo.{w}.{d}.{f}.f1_cost.pooled_c2_c3")]
        b.derive(f"mean.lofo.{w}.f1_cost", "mean", ks)
        b.derive(f"rng.lofo.{w}.f1_cost.max", "max", ks)
        ks = [f"iqr.{w}.{d}.pooled_c2_c3_f1" for d in DETS]
        b.derive(f"rng.iqr.{w}.f1.min", "min", ks)
        b.derive(f"rng.iqr.{w}.f1.max", "max", ks)
        ks = [f"prev.{w}.{d}.fpb_0.01.0.01.f1" for d in DETS if have(f"prev.{w}.{d}.fpb_0.01.0.01.f1")]
        b.derive(f"rng.prev.{w}.1pct.f1.min", "min", ks)
        b.derive(f"rng.prev.{w}.1pct.f1.max", "max", ks)
        ks = [f"phasecov.{w}.{d}.p_phase" for d in DETS if have(f"phasecov.{w}.{d}.p_phase")]
        b.derive(f"rng.phasecov.{w}.min", "min", ks, fmt=lambda x: tex_num(pfmt(x)))
        ks = [f"lat.{w}.pooled.{d}.b0.01.rmst" for d in DETS]
        b.derive(f"rng.lat.{w}.b0.01.rmst.min", "min", ks, fmt=lambda x: f"{x:.0f}")
        b.derive(f"rng.lat.{w}.b0.01.rmst.max", "max", ks, fmt=lambda x: f"{x:.0f}")
        ks = [f"lat.{w}.pooled.{d}.b0.01.rate" for d in DETS]
        b.derive(f"rng.lat.{w}.b0.01.rate.min", "min", ks, fmt=lambda x: f"{100 * x:.0f}\\%")
        b.derive(f"rng.lat.{w}.b0.01.rate.max", "max", ks, fmt=lambda x: f"{100 * x:.0f}\\%")
        ks = [f"cost.{w}.{d}.score_ms_per_window" for d in DETS]
        b.derive(f"rng.cost.{w}.batch.max", "max", ks, fmt=lambda x: f"{x:.2f}")
        ks = [f"cost.{w}.{d}.fit_s" for d in DEEP]
        b.derive(f"rng.cost.{w}.deepfit.min", "min", ks, fmt=lambda x: f"{x:.1f}")
        b.derive(f"rng.cost.{w}.deepfit.max", "max", ks, fmt=lambda x: f"{x:.1f}")
        ks = [f"three.{w}.{d}.recovery.auc.pooled_c2_c3" for d in DETS]
        b.derive(f"rng.three.{w}.recovery.auc.min", "min", ks)
        b.derive(f"rng.three.{w}.recovery.auc.max", "max", ks)
        ks = [f"classical.{w}.{d}.d_pooled_auc_vs_RuleBased" for d in ("LODA", "COPOD")]
        b.derive(f"rng.classical.{w}.lodacopod.min", "min", ks)
        b.derive(f"rng.classical.{w}.lodacopod.max", "max", ks)
    allc = ["ec-a", "ec-b", "ec-c", "iot-a", "iot-b", "iot-c"]
    for w in ("ec", "iot"):
        if have(f"phm.{w}.LSTM_AE.est"):
            for grp, mem in (("AE", AE), ("deep", DEEP)):
                b.derive(f"rng.phm.{w}.{grp}.min", "min", [f"phm.{w}.{d}.est" for d in mem])
                b.derive(f"rng.phm.{w}.{grp}.max", "max", [f"phm.{w}.{d}.est" for d in mem])
        if have(f"phc.{w}.IF.within_run_auc_all"):
            b.derive(f"rng.phc.{w}.wr.maxabsdiff", "maxabsdiff",
                     [k for d in DETS for k in (f"phc.{w}.{d}.within_run_auc_all", f"phc.{w}.{d}.within_run_auc_without")])
            b.derive(f"rng.phc.{w}.r1.maxabsdiff", "maxabsdiff",
                     [k for d in DETS for k in (f"phc.{w}.{d}.r1_placebo_all", f"phc.{w}.{d}.r1_placebo_without")])
            b.derive(f"rng.phc.{w}.c1.AE.min", "min", [f"phc.{w}.{d}.c1_without_est" for d in AE])
            b.derive(f"rng.phc.{w}.c1.AE.max", "max", [f"phc.{w}.{d}.c1_without_est" for d in AE])
            b.derive(f"rng.phc.{w}.c1.AE.hi.max", "max", [f"phc.{w}.{d}.c1_without_hi" for d in AE])
        if have(f"phf8.{w}.IF.f8_vs_normal"):
            b.derive(f"rng.phf8.{w}.normal.min", "min", [f"phf8.{w}.{d}.f8_vs_normal" for d in DETS])
            b.derive(f"rng.phf8.{w}.normal.max", "max", [f"phf8.{w}.{d}.f8_vs_normal" for d in DETS])
            b.derive(f"rng.phf8.{w}.without.min", "min", [f"phf8.{w}.{d}.auc_without_f8" for d in DETS])
            b.derive(f"rng.phf8.{w}.without.max", "max", [f"phf8.{w}.{d}.auc_without_f8" for d in DETS])
            b.derive(f"rng.phf8.{w}.c1.min", "min", [f"phf8.{w}.{d}.c1_est" for d in DEEP])
            b.derive(f"rng.phf8.{w}.c1.max", "max", [f"phf8.{w}.{d}.c1_est" for d in DEEP])
        if have(f"phmag.{w}.IF.d_mean"):
            ks = [f"phmag.{w}.{d}.d_mean" for d in DETS if d != "EWMA"]
            b.derive(f"rng.phmag.{w}.others.min", "min", ks)
            b.derive(f"rng.phmag.{w}.others.max", "max", ks)
        if have(f"phfpr.{w}.IF.0.01.normal.fpr"):
            ks = [f"phfpr.{w}.{d}.0.01.normal.fpr" for d in DETS if d != "EWMA"]
            b.derive(f"rng.phfpr.{w}.normal.min", "min", ks)
            b.derive(f"rng.phfpr.{w}.normal.max", "max", ks)
            ks = [f"phfpr.{w}.{d}.0.01.recovery.fpr" for d in DETS]
            b.derive(f"rng.phfpr.{w}.recovery.min", "min", ks)
            b.derive(f"rng.phfpr.{w}.recovery.max", "max", ks)
        if have(f"phfas.{w}.IF.0.01.recovery_share"):
            ks = [f"phfas.{w}.{d}.0.01.recovery_share" for d in DETS if d != "EWMA"]
            pct = lambda x: f"{100 * x:.0f}\\%"
            b.derive(f"rng.phfas.{w}.min", "min", ks, fmt=pct)
            b.derive(f"rng.phfas.{w}.max", "max", ks, fmt=pct)
        ks = [f"perf.{w}.{d}.r1_placebo.pooled" for d in DETS]
        b.derive(f"rng.perf.{w}.r1placebo.min", "min", ks)
        b.derive(f"rng.perf.{w}.r1placebo.max", "max", ks)
        ks = [f"fault.{w}.{d}.{f}.pooled_auc.pooled_c2_c3" for d in DETS for f in ("F1", "F2")]
        b.derive(f"rng.fault.{w}.F12.min", "min", ks)
        ks = [f"fault.{w}.{d}.{f}.pooled_auc.pooled_c2_c3" for d in DETS for f in ("F1", "F2", "F5", "F7")]
        b.derive(f"rng.fault.{w}.F1257.min", "min", ks)
        if have(f"ec2.{w}.LSTM_AE.median_s"):
            ks = [f"ec2.{w}.{d}.median_s" for d in DEEP if have(f"ec2.{w}.{d}.median_s")]
            b.derive(f"rng.ec2.{w}.deep.min", "min", ks, fmt=lambda x: f"{x:.1f}")
            b.derive(f"rng.ec2.{w}.deep.max", "max", ks, fmt=lambda x: f"{x:.1f}")
    if have("rng.oper.ec.trailing_fa_per_h.b0.01.min") and have("rng.oper.iot.trailing_fa_per_h.b0.01.min"):
        b.derive("rng.oper.trailing_fa_per_h.b0.01.min", "min", ["rng.oper.ec.trailing_fa_per_h.b0.01.min",
                                                                 "rng.oper.iot.trailing_fa_per_h.b0.01.min"])
        b.derive("rng.oper.trailing_fa_per_h.b0.01.max", "max", ["rng.oper.ec.trailing_fa_per_h.b0.01.max",
                                                                 "rng.oper.iot.trailing_fa_per_h.b0.01.max"])
    if have("rng.phc.ec.wr.maxabsdiff") and have("rng.phc.iot.wr.maxabsdiff"):
        b.derive("rng.phc.wr.maxabsdiff", "max", ["rng.phc.ec.wr.maxabsdiff", "rng.phc.iot.wr.maxabsdiff"])
        b.derive("rng.phc.r1.maxabsdiff", "max", ["rng.phc.ec.r1.maxabsdiff", "rng.phc.iot.r1.maxabsdiff"])
    sigs = ["F1", "F2", "F4", "F5", "F6", "F7", "F8"]
    if have("sfp.ec-a.F1.sham_attempts"):
        b.derive("sum.sfp.attempts", "sum", [f"sfp.{c}.{s}.sham_attempts" for c in allc for s in sigs])
        b.derive("sum.sfp.passes", "sum", [f"sfp.{c}.{s}.false_passes" for c in allc for s in sigs])
    if have("xsig.ec-a.F1.F2.attempts"):
        off = [(c, r, s) for c in allc for r in sigs for s in sigs if r != s]
        b.derive("sum.xsig.offdiag.attempts", "sum", [f"xsig.{c}.{r}.{s}.attempts" for c, r, s in off])
        b.derive("sum.xsig.offdiag.passes", "sum", [f"xsig.{c}.{r}.{s}.passes" for c, r, s in off])
    if have("phdrift.ec-a.first6h_vs_last6h.cv_auc_blocked"):
        ks = [f"phdrift.{c}.{t}.cv_auc_blocked" for c in allc for t in ("first6h_vs_last6h", "leading_vs_trailing")]
        b.derive("rng.phdrift.min", "min", ks, fmt=lambda x: f"{x:.2f}")
        b.derive("rng.phdrift.max", "max", ks, fmt=lambda x: f"{x:.2f}")
    ks = [f"drift.{c}.{t}.{f}.cv_auc" for c in allc for t in ("first6h_vs_last6h", "leading_vs_trailing")
          for f in ("all_features", "without_jvm_buffer_pool")]
    b.derive("rng.drift.min", "min", ks, fmt=lambda x: f"{x:.2f}")
    ks = [f"labels.{c}.anomalous_prevalence_attempt_windows" for c in allc]
    b.derive("rng.labels.prev.min", "min", ks)
    b.derive("rng.labels.prev.max", "max", ks)
    b.derive("sum.labels.windows", "sum", [f"labels.{c}.windows_total" for c in allc])
    b.derive("sum.gate.valid", "sum", ["sum.gate.ec.valid", "sum.gate.iot.valid"])
    b.derive("rng.facts.nseries.min", "min", [f"facts.build.{c}.n_series" for c in allc])
    b.derive("rng.facts.nseries.max", "max", [f"facts.build.{c}.n_series" for c in allc])


def write(b: Book, tex: Path, claims: Path, inputs: dict):
    lines = ["% Generated by analysis/numbers_tex.py. Do not edit. Use \\R{key} in the manuscript.",
             "% Inputs: " + ", ".join(f"{k} {v[:12]}" for k, v in sorted(inputs.items())),
             "\\makeatletter",
             "\\newcommand{\\R}[1]{\\ifcsname r@#1\\endcsname\\csname r@#1\\endcsname"
             "\\else\\errmessage{numbers.tex has no key #1}\\fi}"]
    for k in sorted(b.vals):
        if any(ch in k for ch in "{}\\%#$&~^ "):
            raise SystemExit(f"ERROR: number key {k!r} has a character LaTeX cannot carry in a name")
        lines.append(f"\\expandafter\\def\\csname r@{k}\\endcsname{{{b.vals[k]}}}")
    lines.append("\\makeatother")
    tex.write_text("\n".join(lines) + "\n")
    claims.write_text(json.dumps({"inputs": inputs, "numbers": b.claims}, indent=1, default=str) + "\n")


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--out", required=True)
    p.add_argument("--tex", required=True)
    p.add_argument("--claims", required=True)
    a = p.parse_args(argv)
    out = Path(a.out)
    b = Book()
    res = json.loads((out / "confirm.json").read_text())
    confirm_numbers(b, res)
    if (out / "posthoc.json").is_file():
        posthoc_numbers(b, json.loads((out / "posthoc.json").read_text()))
    if (out / "describe" / "performance.parquet").is_file():
        describe_numbers(b, out)
    table_numbers(b, out)
    if (out / "facts.json").is_file():
        facts_numbers(b, json.loads((out / "facts.json").read_text()))
    derived_numbers(b)
    inputs = {str(f.relative_to(out)): sha256_file(f) for f in [out / "confirm.json", out / "posthoc.json",
              *sorted((out / "describe").glob("*.parquet")), *[out / s for s, *_ in TABLES], out / "facts.json"] if f.is_file()}
    write(b, Path(a.tex), Path(a.claims), inputs)
    print(f"numbers: {len(b.vals)} keys to {a.tex}")


if __name__ == "__main__":
    main()
