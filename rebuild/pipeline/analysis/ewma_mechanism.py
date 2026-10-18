#!/usr/bin/env python3
"""Exploratory analysis 5.9.7: what the EWMA score is made of (R7.1b). Exploratory, no adjustment (5.7.5).

Added on 18 October 2026, after registration. The choices 5.9.7 leaves open are recorded in protocol/DEVIATIONS.md.
It ports the analyses of analysis/cross_workload/ewma_mechanism.py, the R2 draft's script behind the Table 27 variants
that 5.9.7 names (sha256 034986d7d039b4943fe7a5609b2f3e86f8711e07e9889474de89ef3153e530c8, dated 13 October 2026 and not
in the registered commit), to the rebuilt data.

A. Saturation bound. For each detector, pooled AUC if every tie at the top score were broken in its favour.
B. Ablation. score_variant changes one component of the published EWMA at a time. Every variant gets the fold data of
   training/train.py, the same features as EWMA in analysis/baselines.py, and the published variant must equal those
   EWMA scores exactly. Running centre restarts at the start of every contiguous stretch (seq_group, seq_index).
   drop_dominant removes the three features that most often set the published maximum on the cal block.
C. Per fault. Within-run AUC of every detector per fault, and for each detector the paired fold difference between the
   faults with direct magnitude shifts (F4, F7) and the other five. The sham placebo is the same for every run of a
   fold, so it cancels in that difference.
D. For EWMA's alerted windows, the telemetry category of the feature that sets the maximum, against that category's
   share of the fold's selected features.

    python3 analysis/ewma_mechanism.py score --derived DIR --campaign ec-b --out DIR --freeze-marker FILE   (needs torch)
    python3 analysis/ewma_mechanism.py summarize --derived DIR --phase-b DIR --baselines DIR --extra DIR \
        --results DIR --out DIR
"""
from __future__ import annotations

import argparse
import json
import platform
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.special import expit

PIPE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPE))

WINDOW_S = 30
ALPHA = 0.3            # published EWMA smoothing constant
VARIANTS = ["published", "centre_train_mean", "running_centre", "scale_global_sd",
            "static_z", "mean_sq_pool", "mean_sq_centre_train_mean", "sigmoid_squash",
            "scale_floor", "drop_dominant"]
SPLITS = (("test", "test"), ("cal", "cal"), ("trailing", "eval_only"))
MAGNITUDE_FAULTS = ["F4", "F7"]
OTHER_FAULTS = ["F1", "F2", "F5", "F6", "F8"]
R1_SCRIPT_SHA256 = "034986d7d039b4943fe7a5609b2f3e86f8711e07e9889474de89ef3153e530c8"


# ewma_reference, running_centres and score_variant are copied from analysis/cross_workload/ewma_mechanism.py (sha256
# above). tests/test_analysis_ewma_mechanism.py checks that every variant gives the R1 scores.
def ewma_reference(X_train):
    """Published centre (last smoothed training value) and scale (mean EWMA std)."""
    mu = pd.DataFrame(X_train).ewm(alpha=ALPHA, adjust=False).mean().to_numpy()
    sd = pd.DataFrame(X_train).ewm(alpha=ALPHA, adjust=False).std().to_numpy()
    sd_global = np.nanmean(sd, axis=0)
    sd_global = np.where(sd_global > 1e-9, sd_global, 1.0)
    return mu[-1], sd_global


def running_centres(X_eval, start, segments):
    """Causal EWMA centre for each eval row: the smoothed value of the preceding rows of
    the same contiguous run, restarted at `start` at each run boundary."""
    c = np.empty_like(X_eval)
    cur = start.copy()
    for i in range(len(X_eval)):
        if segments[i]:
            cur = start.copy()
        c[i] = cur
        cur = ALPHA * X_eval[i] + (1 - ALPHA) * cur
    return c


def score_variant(variant, X_train, X_eval, seg_eval, drop=None):
    mu_last, sd_ewma = ewma_reference(X_train)
    centre, scale, pool = mu_last, sd_ewma, "max"
    if variant == "centre_train_mean":
        centre = X_train.mean(axis=0)
    elif variant == "scale_global_sd":
        scale = np.where(X_train.std(axis=0) > 1e-9, X_train.std(axis=0), 1.0)
    elif variant == "static_z":
        centre = X_train.mean(axis=0)
        scale = np.where(X_train.std(axis=0) > 1e-9, X_train.std(axis=0), 1.0)
    elif variant == "mean_sq_pool":
        pool = "mean_sq"
    elif variant == "mean_sq_centre_train_mean":
        pool = "mean_sq"
        centre = X_train.mean(axis=0)
    elif variant == "running_centre":
        centre = running_centres(X_eval, mu_last, seg_eval)
    elif variant == "scale_floor":
        # floor each feature's scale at 0.1 of its training standard deviation
        scale = np.maximum(sd_ewma, 0.1 * np.where(X_train.std(axis=0) > 1e-9, X_train.std(axis=0), 1.0))
    z = np.abs(X_eval - centre) / scale
    if variant == "drop_dominant" and drop is not None:
        z[:, drop] = 0.0
    s = np.nanmax(z, axis=1) if pool == "max" else np.nanmean(z ** 2, axis=1)
    if variant == "sigmoid_squash":
        # the autoencoders' score function applied to the published EWMA score:
        # sigmoid of the score standardised by its training distribution, kept in float32
        z_tr = np.abs(X_train - mu_last) / sd_ewma
        s_tr = np.nanmax(z_tr, axis=1)
        s = expit((s - s_tr.mean()) / (s_tr.std() + 1e-8)).astype(np.float32)
    return s


def segment_starts(f: pd.DataFrame) -> np.ndarray:
    """True where a row (in time order) starts a contiguous stretch: a new seq_group or a gap in seq_index."""
    g, i = f["seq_group"].to_numpy(), f["seq_index"].to_numpy()
    if len(g) == 0:
        return np.zeros(0, dtype=bool)
    return np.r_[True, (g[1:] != g[:-1]) | (i[1:] != i[:-1] + 1)]


def detector_name(variant: str) -> str:
    return f"EWMA_{variant}"


# ---------------------------------------------------------------- score (training environment)
def score_campaign(derived: Path, cid: str, out: Path, freeze_marker: str | None, folds_only=None) -> dict:
    sys.path.insert(0, str(PIPE / "training"))
    import train as T  # noqa: E402  the registered training code, unchanged
    from analysis.baselines import gate
    from streamprep.contract import sha256_file
    from streamprep.fold_fit import feature_columns
    cfg = T.load_training_config(None)
    idx = gate(cid, cfg, freeze_marker)
    base = derived / cid
    windows, folds_path = base / f"windows_{WINDOW_S}s.parquet", base / "folds.parquet"
    df, folds = T.load_inputs(windows, folds_path, cfg["_pipeline"])
    cols = feature_columns(df)
    fold_list = folds_only or list(range(1, int(folds["fold"].max()) + 1))
    t0, rec_folds = time.time(), {}
    for k in fold_list:
        T.validate_data(df, folds, k)
        d = T.prepare_fold_data(df, folds, k, cols, cfg)
        tr = d["X_normal"]
        sel = np.array(d["transform"].selected)
        mu_last, sd_ewma = ewma_reference(tr)
        am_cal = np.nanargmax(np.abs(d["X_cal"] - mu_last) / sd_ewma, axis=1)
        dominant = [int(x) for x in np.argsort(-np.bincount(am_cal, minlength=tr.shape[1]), kind="stable")[:3]]
        for v in VARIANTS:
            rows = []
            for split, role in SPLITS:
                X, f = d[f"X_{split}"], d[f"df_{split}"]
                if len(f) == 0:
                    continue
                s = np.asarray(score_variant(v, tr, X, segment_starts(f), drop=dominant), dtype=np.float64)
                if len(s) != len(f) or not np.isfinite(s).all():
                    raise SystemExit(f"ERROR: {v} {cid} fold {k} {split}: bad scores")
                wr = f["within_run"].astype(object)
                rows.append(pd.DataFrame({"window_id": f["window_id"].to_numpy(), "fold": np.int32(k), "role": role,
                                          "model": detector_name(v), "category": "all", "score": s,
                                          "within_run": wr.where(wr.notna(), None).to_numpy()}))
            dest = out / cid / detector_name(v)
            dest.mkdir(parents=True, exist_ok=True)
            pd.concat(rows, ignore_index=True).to_parquet(dest / f"{detector_name(v)}_fold{k}.parquet", index=False)
        side = []
        for split, role in SPLITS:
            X, f = d[f"X_{split}"], d[f"df_{split}"]
            if len(f) == 0:
                continue
            am = np.nanargmax(np.abs(X - mu_last) / sd_ewma, axis=1)
            side.append(pd.DataFrame({"window_id": f["window_id"].to_numpy(), "fold": np.int32(k), "role": role,
                                      "argmax_feature": sel[am], "argmax_category": [c.split("__")[1] for c in sel[am]]}))
        dest = out / cid / "EWMA_argmax"
        dest.mkdir(parents=True, exist_ok=True)
        pd.concat(side, ignore_index=True).to_parquet(dest / f"EWMA_argmax_fold{k}.parquet", index=False)
        share = pd.Series([c.split("__")[1] for c in sel]).value_counts(normalize=True).to_dict()
        rec_folds[k] = {"n_selected": int(len(sel)), "category_share": share,
                        "dominant_cal_features": [str(sel[i]) for i in dominant]}
    rec = {"campaign_id": cid, "campaign_index": idx, "window_s": WINDOW_S, "variants": VARIANTS,
           "folds": rec_folds, "elapsed_s": round(time.time() - t0, 1),
           "inputs": {p.name: sha256_file(p) for p in (windows, folds_path)},
           "code": {"analysis/ewma_mechanism.py": sha256_file(Path(__file__)),
                    "training/train.py": sha256_file(PIPE / "training" / "train.py")},
           "r1_script_sha256": R1_SCRIPT_SHA256, "python": platform.python_version(), "machine": platform.machine()}
    (out / cid).mkdir(parents=True, exist_ok=True)
    (out / cid / "ewma_mechanism_run.json").write_text(json.dumps(rec, indent=1) + "\n")
    return rec


# ---------------------------------------------------------------- summarize (analysis environment)
def saturation_rows(camp, sc: pd.DataFrame, detector: str, fold: int) -> dict:
    from analysis.metrics import EVAL_LABELS, auc
    t = sc[sc["role"] == "test"]
    lab = camp.labels.loc[t["window_id"], "label"].to_numpy()
    ev = np.isin(lab, EVAL_LABELS)
    s = t["score"].to_numpy(np.float64)[ev]
    y = (lab[ev] == "ANOMALOUS").astype(int)
    a = auc(y, s)
    tied = s == s.max()
    npos, nneg = y.sum(), (1 - y).sum()
    upper = a + 0.5 * (tied & (y == 1)).sum() * (tied & (y == 0)).sum() / (npos * nneg)
    return {"campaign_id": camp.cid, "detector": detector, "fold": fold, "auc": a, "auc_best_case": upper,
            "frac_at_max": float(tied.mean()), "frac_normal_at_max": float((tied & (y == 0)).sum() / nneg),
            "modal_tie": float(pd.Series(s).value_counts().iloc[0] / len(s))}


def paired(fm: pd.DataFrame, a: str, b: str, metric: str, conf: list, all_c: list) -> dict:
    from analysis import stats as S
    ds = {}
    for c in all_c:
        x = fm[(fm["detector"] == a) & (fm["campaign_id"] == c)].set_index("fold")[metric].sort_index()
        y = fm[(fm["detector"] == b) & (fm["campaign_id"] == c)].set_index("fold")[metric].sort_index()
        ds[c] = (x - y).to_numpy()
    per = {c: S.within_campaign(v) for c, v in ds.items()}
    pooled = S.pooled({c: ds[c] for c in conf})
    return {"per_campaign": per, "pooled": pooled}


def summarize(derived: Path, phase_b: Path, baselines: Path, extra: Path, results: Path, out: Path):
    from analysis import metrics as M
    from analysis import sources as SRC
    from analysis.confirm import CONFIRMATORY, WORKLOADS
    out.mkdir(parents=True, exist_ok=True)
    cids = SRC.CAMPAIGNS
    camps = {c: M.load_campaign(derived, c) for c in cids}

    # B. ablation metrics, and the published variant against analysis/baselines.py EWMA
    srcs, max_diff = [], 0.0
    for c in cids:
        for k in range(1, 11):
            for v in VARIANTS:
                p = extra / c / detector_name(v) / f"{detector_name(v)}_fold{k}.parquet"
                srcs.append({"campaign_id": c, "detector": detector_name(v), "family": "ewma_variant", "fold": k,
                             "run": "ewma_mechanism", "path": str(p)})
            a = pd.read_parquet(extra / c / "EWMA_published" / f"EWMA_published_fold{k}.parquet")
            b = pd.read_parquet(baselines / c / "EWMA" / f"EWMA_fold{k}.parquet")
            if not (a["window_id"].to_numpy() == b["window_id"].to_numpy()).all():
                raise SystemExit(f"ERROR: published EWMA rows differ from analysis/baselines.py for {c} fold {k}")
            max_diff = max(max_diff, float(np.abs(a["score"].to_numpy() - b["score"].to_numpy()).max()))
    if max_diff != 0.0:
        raise SystemExit(f"ERROR: published EWMA differs from analysis/baselines.py EWMA by {max_diff}")
    fm, runs, hashes = M.compute(derived, srcs)
    # modal tie and alert rate at the F1-optimal threshold, on test windows
    extra_rows = []
    for s in srcs:
        sc = pd.read_parquet(s["path"])
        r = fm[(fm["detector"] == s["detector"]) & (fm["campaign_id"] == s["campaign_id"]) & (fm["fold"] == s["fold"])].iloc[0]
        t = sc[sc["role"] == "test"]
        lab = camps[s["campaign_id"]].labels.loc[t["window_id"], "label"]
        st_ = t["score"].to_numpy()[lab.isin(M.EVAL_LABELS).to_numpy()]
        extra_rows.append({"campaign_id": s["campaign_id"], "detector": s["detector"], "fold": s["fold"],
                           "modal_tie": float(pd.Series(st_).value_counts().iloc[0] / len(st_)),
                           "alert_rate_f1opt": float((st_ >= r["threshold_f1opt"]).mean())})
    fm = fm.merge(pd.DataFrame(extra_rows), on=["campaign_id", "detector", "fold"])
    fm.to_parquet(out / "ablation_fold_metrics.parquet", index=False)
    runs.to_parquet(out / "ablation_run_metrics.parquet", index=False)

    summ = []
    for w, cs in WORKLOADS.items():
        for v in VARIANTS:
            dname = detector_name(v)
            x = fm[(fm["detector"] == dname) & fm["campaign_id"].isin(cs)]
            row = {"workload": w, "variant": v}
            for c in cs:
                xc = x[x["campaign_id"] == c]
                row.update({f"{m}_{c}": float(xc[m].mean()) for m in ("pooled_auc", "within_run_auc", "within_run_pc")})
                row[f"modal_tie_{c}"] = float(xc["modal_tie"].mean())
            if v != "published":
                for m in ("pooled_auc", "within_run_auc"):
                    pr = paired(fm, dname, "EWMA_published", m, CONFIRMATORY[w], cs)
                    p = pr["pooled"]
                    row.update({f"d_{m}": p["mean"], f"d_{m}_lo": p["ci95_lo"], f"d_{m}_hi": p["ci95_hi"],
                                f"p_{m}": p["p_t"]})
                    row.update({f"d_{m}_{c}": pr["per_campaign"][c]["mean"] for c in cs})
            summ.append(row)
    abl = pd.DataFrame(summ)
    abl.to_csv(out / "ablation_summary.csv", index=False)

    # A. saturation bound over the panel, the random forest, max-z and EWMA
    main_src = SRC.frozen_sources(phase_b, baselines, cids)
    sat = []
    for s in main_src:
        if s["detector"] in SRC.PANEL + ["RF", "RuleBased", "EWMA"]:
            sat.append(saturation_rows(camps[s["campaign_id"]], pd.read_parquet(s["path"]), s["detector"], s["fold"]))
    sat = pd.DataFrame(sat)
    sat.to_csv(out / "saturation_per_fold.csv", index=False)
    sat["workload"] = sat["campaign_id"].map(lambda c: "ecommerce" if c.startswith("ec-") else "iot")
    sat_sum = sat.groupby(["workload", "campaign_id", "detector"])[
        ["auc", "auc_best_case", "frac_at_max", "frac_normal_at_max", "modal_tie"]].mean().reset_index()
    sat_sum.to_csv(out / "saturation_summary.csv", index=False)

    # C. per fault, from the main run metrics
    rm = pd.read_parquet(results / "run_metrics.parquet")
    fmain = pd.read_parquet(results / "fold_metrics.parquet")
    dets = SRC.PANEL + ["RF", "RuleBased", "EWMA"]
    rm = rm[rm["detector"].isin(dets)].copy()
    sham = rm[rm["run_type"] == "F0"].set_index(["detector", "campaign_id", "fold"])["within_run_auc"]
    fr = rm[rm["run_type"] != "F0"].copy()
    fr["within_run_pc"] = fr["within_run_auc"].to_numpy() - sham.reindex(
        pd.MultiIndex.from_frame(fr[["detector", "campaign_id", "fold"]])).to_numpy()
    pf = fr.groupby(["detector", "campaign_id", "run_type"])[["within_run_auc", "within_run_pc"]].mean().reset_index()
    pooled_cols = [f"pooled_auc_{f}" for f in SRC_FAULTS()]
    pp = fmain[fmain["detector"].isin(dets)].groupby(["detector", "campaign_id"])[pooled_cols].mean().reset_index()
    pf.to_csv(out / "per_fault_within_run.csv", index=False)
    pp.to_csv(out / "per_fault_pooled_auc.csv", index=False)
    from analysis import stats as S
    mag = []
    for w, cs in WORKLOADS.items():
        for dname in dets:
            ds = {}
            for c in cs:
                x = fr[(fr["detector"] == dname) & (fr["campaign_id"] == c)]
                a = x[x["run_type"].isin(MAGNITUDE_FAULTS)].groupby("fold")["within_run_auc"].mean()
                b = x[x["run_type"].isin(OTHER_FAULTS)].groupby("fold")["within_run_auc"].mean()
                ds[c] = (a - b).sort_index().to_numpy()
            p = S.pooled({c: ds[c] for c in CONFIRMATORY[w]})
            mag.append({"workload": w, "detector": dname, "d_mean": p["mean"], "d_lo": p["ci95_lo"],
                        "d_hi": p["ci95_hi"], "p": p["p_t"], **{f"d_{c}": float(ds[c].mean()) for c in cs}})
    mag = pd.DataFrame(mag)
    mag.to_csv(out / "magnitude_faults_contrast.csv", index=False)

    # D. category of the arg-max feature on EWMA's alerted windows
    arg_rows, share_rows = [], []
    for c in cids:
        rec = json.loads((extra / c / "ewma_mechanism_run.json").read_text())
        for k in range(1, 11):
            side = pd.read_parquet(extra / c / "EWMA_argmax" / f"EWMA_argmax_fold{k}.parquet")
            sc = pd.read_parquet(extra / c / "EWMA_published" / f"EWMA_published_fold{k}.parquet")
            j = side.merge(sc[["window_id", "score"]], on="window_id")
            j = j[j["role"] == "test"]
            lab = camps[c].labels.loc[j["window_id"]]
            j = j.assign(label=lab["label"].to_numpy(), run_type=lab["run_type"].to_numpy())
            j = j[j["label"].isin(M.EVAL_LABELS)]
            r = fm[(fm["detector"] == "EWMA_published") & (fm["campaign_id"] == c) & (fm["fold"] == k)].iloc[0]
            for op in ("f1opt", "fpb_0.05"):
                al = j["score"].to_numpy() >= r[f"threshold_{op}"]
                for kind, m in (("flagged_fault", al & (j["label"] == "ANOMALOUS").to_numpy()),
                                ("false_alarm", al & (j["label"] != "ANOMALOUS").to_numpy())):
                    for cat_, rt in zip(j.loc[m, "argmax_category"], j.loc[m, "run_type"]):
                        arg_rows.append({"campaign_id": c, "fold": k, "operating_point": op, "kind": kind,
                                         "run_type": rt if kind == "flagged_fault" else "negative", "category": cat_})
            for cat_, v in rec["folds"][str(k)]["category_share"].items():
                share_rows.append({"campaign_id": c, "fold": k, "category": cat_, "share": v})
    arg = pd.DataFrame(arg_rows)
    arg["workload"] = arg["campaign_id"].map(lambda c: "ecommerce" if c.startswith("ec-") else "iot")
    arg_sum = arg.groupby(["workload", "operating_point", "kind", "category"]).size().rename("n").reset_index()
    arg_sum["share"] = arg_sum["n"] / arg_sum.groupby(["workload", "operating_point", "kind"])["n"].transform("sum")
    shares = pd.DataFrame(share_rows)
    shares["workload"] = shares["campaign_id"].map(lambda c: "ecommerce" if c.startswith("ec-") else "iot")
    fshare = shares.groupby(["workload", "category"])["share"].mean().rename("feature_share").reset_index()
    arg_sum = arg_sum.merge(fshare, on=["workload", "category"], how="left")
    arg_sum.to_csv(out / "argmax_category.csv", index=False)
    arg.groupby(["workload", "operating_point", "kind", "run_type", "category"]).size().rename("n").reset_index() \
        .to_csv(out / "argmax_category_by_fault.csv", index=False)

    (out / "inputs_sha256.json").write_text(json.dumps(dict(sorted(hashes.items())), indent=1) + "\n")
    report(out, abl, sat_sum, mag, arg_sum, pf, pp)


def SRC_FAULTS():
    from analysis.metrics import FAULTS
    return FAULTS


def report(out, abl, sat_sum, mag, arg_sum, pf, pp):
    from analysis.confirm import CONFIRMATORY
    L = ["EWMA MECHANISM (protocol 5.9.7, exploratory). Pooled values are over campaigns 2 and 3, with t-based 95%",
         "intervals. Campaign 1 is shown beside them.", ""]
    L.append("B. Ablation, variant minus published EWMA (paired over folds)")
    with pd.option_context("display.width", 250, "display.max_columns", 40):
        cols = ["workload", "variant"] + [c for c in abl.columns if c.startswith("d_pooled_auc") or
                                          c.startswith("d_within_run_auc") or c.startswith("pooled_auc_")]
        L.append(abl[cols].round(3).to_string(index=False))
        L += ["", "A. Saturation bound (mean over folds)"]
        L.append(sat_sum.round(3).to_string(index=False))
        L += ["", "C. F4 and F7 minus the other five faults, within-run AUC (paired over folds)"]
        L.append(mag.round(3).to_string(index=False))
        L += ["", "C. Per-fault within-run AUC, mean over folds"]
        L.append(pf.pivot_table(index=["campaign_id", "run_type"], columns="detector",
                                values="within_run_auc").round(3).to_string())
        L += ["", "D. Category of EWMA's arg-max feature on alerted test windows, against its share of the features"]
        L.append(arg_sum.round(3).to_string(index=False))
    (out / "ewma_mechanism.txt").write_text("\n".join(L) + "\n")


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("score")
    s.add_argument("--derived", required=True, type=Path)
    s.add_argument("--campaign", required=True)
    s.add_argument("--out", required=True, type=Path)
    s.add_argument("--freeze-marker", default=None)
    s.add_argument("--fold", type=int, action="append")
    m = sub.add_parser("summarize")
    for a in ("--derived", "--phase-b", "--baselines", "--extra", "--results", "--out"):
        m.add_argument(a, required=True, type=Path)
    a = p.parse_args(argv)
    if a.cmd == "score":
        rec = score_campaign(a.derived, a.campaign, a.out, a.freeze_marker, a.fold)
        print(f"{a.campaign}: {len(rec['folds'])} folds x {len(VARIANTS)} variants in {rec['elapsed_s']} s")
    else:
        summarize(a.derived, a.phase_b, a.baselines, a.extra, a.results, a.out)
        print(f"wrote {a.out}")


if __name__ == "__main__":
    main()
