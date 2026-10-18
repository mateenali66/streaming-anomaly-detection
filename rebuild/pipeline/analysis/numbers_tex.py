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


class Book:
    def __init__(self):
        self.vals, self.claims = {}, {}

    def add(self, key, text, value, source, field, script):
        if key in self.vals:
            raise SystemExit(f"ERROR: number key {key} defined twice")
        self.vals[key] = text
        self.claims[key] = {"text": text, "value": value, "source": source, "field": field, "script": script}

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
        b.add(f"{k}.pooled", f3(r.pooled_mean), r.pooled_mean, src,
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
    ("classical/classical_summary.csv", "classical", ["workload", "detector"],
     ["d_pooled_auc_vs_RuleBased", "d_pooled_auc_vs_RuleBased_lo", "d_pooled_auc_vs_RuleBased_hi",
      "d_within_run_auc_vs_RuleBased"]),
]
INTS = {"labelled_runs", "attempts", "repeats", "VALID", "flag_F7_TM_LOSS", "n_attempts", "n", "windows_total",
        "attempt_eval_windows"}


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
    if col.startswith("share") or col.startswith("frac") or col == "feature_share":
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
                b.add(".".join([prefix, *idv, col]), fmt_cell(col, val), val, src, f"{cond}.{col}",
                      "analysis/" + src.split("/")[0].replace("explore", "explore.py").replace("gate", "gate_summary.py")
                      .replace("cost", "cost.py").replace("ewma_mechanism", "ewma_mechanism.py")
                      .replace("classical", "classical.py"))


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
    inputs = {str(f.relative_to(out)): sha256_file(f) for f in [out / "confirm.json", out / "posthoc.json",
              *sorted((out / "describe").glob("*.parquet")), *[out / s for s, *_ in TABLES]] if f.is_file()}
    write(b, Path(a.tex), Path(a.claims), inputs)
    print(f"numbers: {len(b.vals)} keys to {a.tex}")


if __name__ == "__main__":
    main()
