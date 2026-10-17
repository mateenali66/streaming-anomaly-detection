#!/usr/bin/env python3
"""Cross-workload fits of protocol 4.3.2 and 6.3, the input of C6.

Added on 17 October 2026, after registration, to carry out the registered rule with the registered code. The model,
training and scoring functions are those of training/train.py, imported unchanged, and feature selection is
streamprep.fold_fit. The choices 4.3.2 leaves open are recorded as D2 in protocol/DEVIATIONS.md.

One fit per model and source campaign, 9 models x 6 campaigns = 54 fits (6.3).
  * Features are the f__ columns present in the 30 s windows of all six campaigns (--shared, from --write-shared).
  * The source split is the one of 4.3.1. Blocks 1 to 8 and the BASELINE train, block 9 is hpo_val (early stopping on
    its NORMAL windows), block 10 is cal and TRAILING is eval_only. Selection, imputation and scaling are fitted on the
    source's train NORMAL windows, as in every fit.
  * The configuration is the source workload's frozen one (6.1.2). The seed is 1000 x campaign index + 42 + 0, fold 0
    standing for this split.
Each fit then scores the test block of every fold of every other campaign of both workloads. For target fold k the
imputation and scaling are refitted on the target's fold-k train NORMAL windows over the source's selected columns, the
target scaler of 4.3.2 that avoids the R1 source-scaler artefact. The selection stays the source's, so the model sees
the columns it was trained on. The same fit scores both the cross-workload targets and the same-workload targets, so the
within-workload control uses the same features and configuration.

    python3 training/cross_train.py --write-shared derived/shared_30s.json --derived derived
    python3 training/cross_train.py --derived derived --source ec-b --model IF --frozen-params frozen/ecommerce.json \
        --freeze-marker frozen/FREEZE.json --shared derived/shared_30s.json --output results/cross/ec-b/IF

writes scores/<model>_<target>.parquet (window_id, fold, role, model, source, target, score, within_run), auc.csv
(pooled AUC per target and fold, 4.5.1), models/ and summary.json.
"""
from __future__ import annotations

import argparse
import json
import pickle
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from sklearn.metrics import roc_auc_score

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import train as T  # noqa: E402  the registered training code, unchanged
from streamprep.contract import campaign_index_from_id, sha256_file  # noqa: E402
from streamprep.fold_fit import FEATURE_PREFIX, FoldTransform  # noqa: E402

WINDOW_S = 30
SOURCE_BLOCKS = {**{b: "train" for b in range(1, 9)}, 9: "hpo_val", 10: "cal"}  # 4.3.1
CROSS_MODELS = ["IF", "OC_SVM", "LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE", "DeepSVDD", "DAGMM", "RF"]
CROSS_FOLD = 0


def campaigns(derived: Path) -> list[str]:
    return sorted(p.name for p in derived.iterdir() if (p / f"windows_{WINDOW_S}s.parquet").is_file())


def write_shared(derived: Path, out: Path, expect: int = 6):
    cids = campaigns(derived)
    if len(cids) != expect:
        raise SystemExit(f"expected {expect} campaigns under {derived}, found {cids}")
    cols, hashes = None, {}
    for cid in cids:
        p = derived / cid / f"windows_{WINDOW_S}s.parquet"
        T._check_sidecar(p)
        names = {c for c in pq.read_schema(p).names if c.startswith(FEATURE_PREFIX)}
        cols = names if cols is None else cols & names
        hashes[cid] = sha256_file(p)
    rec = {"window_s": WINDOW_S, "campaigns": cids, "windows_sha256": hashes, "n_shared": len(cols),
           "columns": sorted(cols)}
    out.write_text(json.dumps(rec, indent=1) + "\n")
    print(f"wrote {out}: {len(cols)} feature columns shared by {', '.join(cids)}")


def gate(cid: str, cfg: dict, freeze_marker: str | None):
    """A replicate campaign is not read before the freeze tag exists (6.1, 7.1), as in train.py."""
    _wl, idx = campaign_index_from_id(cid)
    if idx not in (0, int(cfg["search_campaign_index"])):
        if not freeze_marker or not Path(freeze_marker).is_file() or Path(freeze_marker).stat().st_size == 0:
            raise SystemExit(f"ERROR: {cid} is a replicate campaign. It is not read before the freeze tag exists "
                             "(6.1, 7.1). Pass --freeze-marker.")
    return idx


def source_roles(df: pd.DataFrame) -> pd.Series:
    """streamprep.folds.assign_folds with the 4.3.1 block map in place of the rotation."""
    seg = df["segment_type"].to_numpy()
    blk = df["block"].to_numpy()
    role = np.empty(len(df), dtype=object)
    role[seg == "BASELINE"] = "train"
    role[seg == "TRAILING"] = "eval_only"
    att = (seg != "BASELINE") & (seg != "TRAILING")
    blocks = sorted(int(b) for b in np.unique(blk[att]))
    if blocks != sorted(SOURCE_BLOCKS):
        raise SystemExit(f"ERROR: the source split needs blocks 1 to 10, found {blocks}")
    for b, r in SOURCE_BLOCKS.items():
        role[att & (blk == b)] = r
    if any(r is None for r in role):
        raise SystemExit("ERROR: a source window received no role")
    return pd.Series(role, index=df.index)


def refit_on_target(src: FoldTransform, X_train_normal: pd.DataFrame) -> tuple[FoldTransform, dict]:
    """Imputation and scaling of streamprep.fold_fit, refitted on the target's train NORMAL windows over the source's
    selected columns. A column with no value in any target train NORMAL window maps to 0, the train NORMAL centre."""
    A = X_train_normal[src.selected].to_numpy(np.float64)
    all_nan = np.isnan(A).all(axis=0)
    with np.errstate(all="ignore"):
        medians = np.nanmedian(np.where(all_nan[None, :], 0.0, A), axis=0)
    A = np.where(np.isnan(A), medians[None, :], A)
    constant = A.max(axis=0) == A.min(axis=0)
    means = np.where(constant, A[0], A.mean(axis=0))
    std = A.std(axis=0)
    scales = np.where(~constant & (std > 0), std, 1.0)
    t = FoldTransform(src.variance_threshold, src.correlation_threshold, src.protected_patterns, src.exempt_from,
                      selected=list(src.selected), medians=medians, means=means, scales=scales)
    rep = {"target_all_nan_columns": int(all_nan.sum()), "target_constant_columns": int(constant.sum()),
           "target_train_normal": int(len(A))}
    return t, rep


def fit_source(model_name, df, cols, cfg, params, seed):
    role = source_roles(df)
    lab = df["label"]
    m = {"normal": (role == "train") & (lab == "NORMAL"),
         "train_rf": (role == "train") & lab.isin(["NORMAL", "ANOMALOUS"]),
         "hpo_val_normal": (role == "hpo_val") & (lab == "NORMAL")}
    for k in ("normal", "train_rf"):
        if lab[m[k]].isin(T.FIT_FORBIDDEN).any():
            raise SystemExit(f"ERROR: a forbidden label entered the {k} fit set")
    transform = FoldTransform.from_config(cfg)
    transform.fit(df.loc[m["normal"], cols])
    X = {k: transform.transform(df.loc[v]) for k, v in m.items()}
    y_rf = (lab[m["train_rf"]] == "ANOMALOUS").astype(int).to_numpy()
    T.set_split_groups(**{k: (df.loc[v, "seq_group"].to_numpy(), df.loc[v, "seq_index"].to_numpy())
                          for k, v in m.items()})
    T.MODEL_SEED = T.SAMPLER_SEED = seed
    T.set_seeds(seed)
    if T.HAS_TORCH:
        T.torch.backends.cudnn.deterministic = True
        T.torch.backends.cudnn.benchmark = False
    n_features = X["normal"].shape[1]
    fit = {"transform": transform, "n_features": n_features, "seq_length": None,
           "sizes": {k: int(v.sum()) for k, v in m.items()}}
    if model_name == "IF":
        model = T.train_isolation_forest(X["normal"], n_estimators=params.get("n_estimators", 200),
                                         contamination=params.get("contamination", 0.05),
                                         max_features=params.get("max_features", 1.0),
                                         max_samples=params.get("max_samples", 1.0))
        scaler = T.fit_score_scaler(-model.decision_function(X["normal"]))
        fit.update(model=model, scaler=scaler, score=lambda A: T.score_isolation_forest(model, A, scaler))
    elif model_name == "OC_SVM":
        model = T.train_ocsvm(X["normal"], nu=params.get("nu", 0.05), kernel=params.get("kernel", "rbf"),
                              gamma=params.get("gamma", "scale"))
        scaler = T.fit_score_scaler(-model.decision_function(X["normal"]))
        fit.update(model=model, scaler=scaler, score=lambda A: T.score_ocsvm(model, A, scaler))
    elif model_name in T.DEEP_MODELS:
        model, ref, seq_length, kind = T.train_deep_model_final(model_name, n_features, X["normal"],
                                                                X["hpo_val_normal"], params)
        fit.update(model=model, ref=ref, seq_length=seq_length, kind=kind)
    elif model_name == "RF":
        model = T.train_random_forest(X["train_rf"], y_rf, n_estimators=params.get("n_estimators", 200),
                                      max_depth=params.get("max_depth", None),
                                      min_samples_leaf=params.get("min_samples_leaf", 1),
                                      max_features=params.get("max_features", "sqrt"))
        fit.update(model=model, score=lambda A: T.score_random_forest(model, A))
    else:
        raise SystemExit(f"ERROR: {model_name} is not a cross-workload model")
    return fit


def score_target(model_name, fit, dft, folds, fold_id, cfg):
    T.validate_data(dft, folds, fold_id)
    role = T.fold_roles(dft, folds, fold_id)
    lab = dft["label"]
    normal = (role == "train") & (lab == "NORMAL")
    test = (role == "test") & lab.isin(T.EVAL_LABELS)
    t, rep = refit_on_target(fit["transform"], dft.loc[normal])
    X = t.transform(dft.loc[test])
    df_test = dft.loc[test].reset_index(drop=True)
    if model_name in T.DEEP_MODELS:
        T.set_split_groups(test=(df_test["seq_group"].to_numpy(), df_test["seq_index"].to_numpy()))
        s, e = T._score_deep(fit["model"], fit["kind"], X, "test", fit["seq_length"], fit["ref"])
    else:
        s, e = fit["score"](X), np.arange(len(X))
    wr_all = df_test["within_run"].notna().to_numpy()
    if int(wr_all[e].sum()) != int(wr_all.sum()):
        raise SystemExit(f"ERROR: {model_name} scores {int(wr_all[e].sum())} of the {int(wr_all.sum())} within-run "
                         f"negatives of fold {fold_id} (decision 27)")
    sub = df_test.iloc[e].reset_index(drop=True)
    y = (sub["label"] == "ANOMALOUS").astype(int).to_numpy()
    s = np.asarray(s, dtype=float)
    ok = len(y) and y.min() != y.max() and np.isfinite(s).all()
    auc = float(roc_auc_score(y, s)) if ok else float("nan")
    rows = pd.DataFrame({"window_id": sub["window_id"].to_numpy(), "fold": np.int32(fold_id), "role": "test",
                         "score": s, "within_run": sub["within_run"].astype(object).where(sub["within_run"].notna(), None)})
    return rows, {"fold": fold_id, "auc_roc": auc, "n_scored": int(len(s)), "n_anomalous": int(y.sum()),
                  "within_run_negatives": int(wr_all.sum()), **rep}


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--derived", required=True, type=Path)
    p.add_argument("--write-shared", type=Path, default=None)
    p.add_argument("--expect-campaigns", type=int, default=6, help="campaigns --write-shared must find")
    p.add_argument("--source")
    p.add_argument("--model", choices=CROSS_MODELS)
    p.add_argument("--frozen-params")
    p.add_argument("--freeze-marker")
    p.add_argument("--shared", type=Path)
    p.add_argument("--targets", nargs="*", default=None, help="default: every other campaign under --derived")
    p.add_argument("--output", type=Path)
    a = p.parse_args(argv)
    if a.write_shared:
        return write_shared(a.derived, a.write_shared, a.expect_campaigns)
    for k in ("source", "model", "frozen_params", "shared", "output"):
        if getattr(a, k) is None:
            p.error(f"--{k.replace('_', '-')} is required")
    cfg = T.load_training_config(None, None)
    T.MAX_EPOCHS = int(cfg["max_epochs"])
    sl = cfg["evaluation"]["seq_length"]
    T.SEQ_LENGTH_MIN, T.MAX_SEQ_LENGTH = int(sl["min"]), int(sl["max"])
    shared = json.loads(a.shared.read_text())
    cols = list(shared["columns"])
    params = json.loads(Path(a.frozen_params).read_text())
    if a.model not in params:
        raise SystemExit(f"ERROR: {a.frozen_params} has no configuration for {a.model}")
    params = dict(params[a.model])
    src_idx = gate(a.source, cfg, a.freeze_marker)
    src_path = a.derived / a.source / f"windows_{WINDOW_S}s.parquet"
    df, _ = T.load_inputs(src_path, a.derived / a.source / "folds.parquet", cfg["_pipeline"])
    if df["campaign_id"].iloc[0] != a.source:
        raise SystemExit(f"ERROR: {src_path} holds {df['campaign_id'].iloc[0]}")
    missing = sorted(set(cols) - set(df.columns))
    if missing:
        raise SystemExit(f"ERROR: {len(missing)} shared columns are missing from {a.source}")
    seed = T.seed_for_fit(src_idx, CROSS_FOLD, cfg)
    t0 = time.time()
    fit = fit_source(a.model, df, cols, cfg, params, seed)
    fit_s = round(time.time() - t0, 2)
    print(f"{a.model} on {a.source}: {fit['n_features']} of {len(cols)} shared columns after selection, "
          f"{fit['sizes']}, {fit_s} s")
    out = a.output
    (out / "scores").mkdir(parents=True, exist_ok=True)
    (out / "models").mkdir(parents=True, exist_ok=True)
    targets = a.targets or [c for c in campaigns(a.derived) if c != a.source]
    aucs, inputs = [], {"source": {"windows_sha256": sha256_file(src_path)}}
    for tgt in targets:
        gate(tgt, cfg, a.freeze_marker)
        tpath = a.derived / tgt / f"windows_{WINDOW_S}s.parquet"
        fpath = a.derived / tgt / "folds.parquet"
        dft, folds = T.load_inputs(tpath, fpath, cfg["_pipeline"])
        inputs[tgt] = {"windows_sha256": sha256_file(tpath), "folds_sha256": sha256_file(fpath)}
        parts = []
        for k in range(1, int(folds["fold"].max()) + 1):
            rows, rec = score_target(a.model, fit, dft, folds, k, cfg)
            parts.append(rows)
            aucs.append({"source": a.source, "target": tgt, "model": a.model,
                         "same_workload": campaign_index_from_id(tgt)[0] == campaign_index_from_id(a.source)[0], **rec})
        sc = pd.concat(parts, ignore_index=True)
        sc.insert(3, "model", a.model)
        sc.insert(4, "source", a.source)
        sc.insert(5, "target", tgt)
        sc.to_parquet(out / "scores" / f"{a.model}_{tgt}.parquet", index=False)
        mean = np.nanmean([r["auc_roc"] for r in aucs if r["target"] == tgt])
        print(f"  {a.source} -> {tgt}: mean pooled AUC over folds {mean:.4f}")
    pd.DataFrame(aucs).to_csv(out / "auc.csv", index=False)
    prefix = f"{a.model}_{a.source}"
    if a.model in T.DEEP_MODELS:
        T.torch.save(fit["model"].state_dict(), out / "models" / f"{prefix}.pt")
        (out / "models" / f"{prefix}_score_ref.json").write_text(json.dumps({"kind": fit["kind"], "ref": list(fit["ref"])}))
    else:
        with open(out / "models" / f"{prefix}.pkl", "wb") as fh:
            pickle.dump({"model": fit["model"], "score_scaler": list(fit.get("scaler", ()))}, fh)
    (out / "models" / f"source_transform_{prefix}.json").write_text(json.dumps(fit["transform"].to_dict(), indent=1))
    summary = {"model": a.model, "source": a.source, "seed": seed, "device": T.DEVICE, "window_s": WINDOW_S,
               "frozen_params": params, "frozen_params_file_sha256": sha256_file(Path(a.frozen_params)),
               "shared_file_sha256": sha256_file(a.shared), "n_shared": len(cols), "n_selected": fit["n_features"],
               "seq_length": fit["seq_length"], "source_split": "blocks 1-8 train, 9 hpo_val, 10 cal (4.3.1)",
               "fit_sets": fit["sizes"], "fit_time_s": fit_s, "inputs": inputs,
               "mean_auc_by_target": {t: round(float(np.nanmean([r["auc_roc"] for r in aucs if r["target"] == t])), 4)
                                      for t in targets},
               "pipeline_config_sha256": cfg["pipeline_config_sha256"],
               "train_py_sha256": sha256_file(HERE / "train.py")}
    (out / "summary.json").write_text(json.dumps(summary, indent=1, default=str) + "\n")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
