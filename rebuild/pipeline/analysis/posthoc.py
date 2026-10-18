"""Post hoc analyses of D5 (protocol/DEVIATIONS.md), labelled post hoc wherever they are reported (5.9, last line).

Added on 18 October 2026, after C1 to C7 had been computed. Nothing here changes a confirmatory decision.

1. C1 and C3 on the plain within-run AUC (4.5.2, a registered metric) in place of the placebo-corrected one. Same
   contrasts, pooling and decision flags as confirm.py, with no Holm adjustment (5.7.5).
2. The sham placebo itself (4.5.3), per detector and campaign: its mean, which should be 0.5, and its fold-to-fold
   spread.
3. How many sham runs per block the placebo-corrected test would need. For a paired contrast, the fold variance of
   the corrected difference is about the variance of the plain within-run difference plus the variance of the sham
   difference, and the sham part shrinks with the number of shams k averaged per block. The pooled 95% interval over
   the confirmatory folds reaches half-width delta when
       sd_within^2 + sd_sham^2 / k <= (delta * sqrt(n) / t_975(n - K))^2,
   with sd_within and sd_sham measured on the registered design (k = 1).

    python3 -m analysis.posthoc --out DIR      (reads DIR/fold_metrics.parquet, writes DIR/posthoc.json)
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats as st

PIPE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPE))
from analysis import confirm as C  # noqa: E402
from analysis import stats as S  # noqa: E402
from analysis.sources import DEEP, POINT  # noqa: E402


def _summ(r: dict) -> dict:
    d = S.decide(r["pooled"], C.DELTA, None, C.ALPHA, r["two_sided"])
    return {"pooled": r["pooled"], "per_campaign": {c: {k: v[k] for k in ("mean", "ci95_lo", "ci95_hi", "p_t")}
                                                    for c, v in r["per_campaign"].items()},
            "decision": d, "label": "post hoc"}


def within_run_contrasts(fm: pd.DataFrame) -> dict:
    out = {}
    for w in C.WORKLOADS:
        c1 = {m: _summ(C.contrast(fm, m, "RuleBased", "within_run_auc", w)) for m in DEEP}
        c1["EWMA"] = _summ(C.contrast(fm, "EWMA", "RuleBased", "within_run_auc", w, two_sided=True))
        diffs = {}
        for c in C.WORKLOADS[w]:
            x = pd.DataFrame({m: C.fold_series(fm, m, c, "within_run_auc") for m in DEEP + POINT})
            diffs[c] = (x[DEEP].mean(axis=1) - x[POINT].mean(axis=1)).to_numpy()
        c3 = C.contrast_from_diffs(diffs, w, two_sided=True)
        out[w] = {"C1_within_run_auc": c1, "C3_within_run_auc": _summ(c3),
                  "no_deep_beats_on_within_run_auc": all(c1[m]["decision"]["not_superior"] for m in DEEP)}
    return out


def sham_placebo(fm: pd.DataFrame) -> dict:
    out = {}
    for d in sorted(fm["detector"].unique()):
        out[d] = {}
        for c in sorted(fm["campaign_id"].unique()):
            s = fm[(fm["detector"] == d) & (fm["campaign_id"] == c)]["sham_placebo"].to_numpy()
            if len(s) != 10:
                continue
            w = S.within_campaign(s - 0.5)
            out[d][c] = {"mean": float(s.mean()), "sd": float(s.std(ddof=1)), "ci95_lo": 0.5 + w["ci95_lo"],
                         "ci95_hi": 0.5 + w["ci95_hi"]}
    return out


def shams_needed(fm: pd.DataFrame, delta: float = C.DELTA) -> dict:
    out = {}
    for w, cs in C.CONFIRMATORY.items():
        rows = {}
        n = 10 * len(cs)
        target = delta * math.sqrt(n) / st.t.ppf(0.975, n - len(cs))
        for m in DEEP:
            dw, ds, dp = [], [], []
            for c in cs:
                a = lambda k: C.fold_series(fm, m, c, k) - C.fold_series(fm, "RuleBased", c, k)
                dw.append(a("within_run_auc").to_numpy())
                ds.append(a("sham_placebo").to_numpy())
                dp.append(a("within_run_pc").to_numpy())
            sd_w = float(np.sqrt(np.mean([x.var(ddof=1) for x in dw])))
            sd_s = float(np.sqrt(np.mean([x.var(ddof=1) for x in ds])))
            sd_p = float(np.sqrt(np.mean([x.var(ddof=1) for x in dp])))
            room = target ** 2 - sd_w ** 2
            k = math.ceil(sd_s ** 2 / room) if room > 0 else None
            rows[m] = {"sd_within_run": sd_w, "sd_sham": sd_s, "sd_corrected": sd_p,
                       "halfwidth_registered": float(st.t.ppf(0.975, n - len(cs)) * sd_p / math.sqrt(n)),
                       "shams_per_block_needed": k}
        ks = [r["shams_per_block_needed"] for r in rows.values() if r["shams_per_block_needed"]]
        out[w] = {"per_model": rows, "target_fold_sd": float(target),
                  "median_shams_needed": float(np.median(ks)) if ks else None,
                  "range_shams_needed": [min(ks), max(ks)] if ks else None,
                  "median_sd_sham": float(np.median([r["sd_sham"] for r in rows.values()])),
                  "median_sd_within_run": float(np.median([r["sd_within_run"] for r in rows.values()]))}
    return out


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--out", required=True)
    a = p.parse_args(argv)
    out = Path(a.out)
    fm = pd.read_parquet(out / "fold_metrics.parquet")
    res = {"label": "post hoc (D5)", "within_run": within_run_contrasts(fm), "sham_placebo": sham_placebo(fm),
           "shams_needed": shams_needed(fm)}
    from analysis.run import _jsonable
    (out / "posthoc.json").write_text(json.dumps(_jsonable(res), indent=1) + "\n")
    print(f"posthoc: wrote {out / 'posthoc.json'}")


if __name__ == "__main__":
    main()
