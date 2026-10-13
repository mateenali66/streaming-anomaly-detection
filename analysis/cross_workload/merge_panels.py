"""Merge the per-run results of both workloads into cross-workload analysis panels.

Inputs:
  data/v3/iot/results/{cpu-1,gpu-1,gpu-2}/[sensitivity_ws_{ws}/]/{model_results,per_fault_type_results}.csv
  data/results_v3/merged/{model_results,per_fault_type_results}.csv  (e-commerce 30s)
  data/v3/results/ecom-gapfill/{model_results,per_fault_type_results}.csv  (e-commerce DSVDD+DAGMM)

Outputs (under analysis/cross_workload/):
  panel_overall.csv     -> workload, window_s, model, fold, f1, auc, ...
  panel_per_fault.csv   -> workload, window_s, model, fold, fault_id, f1, ...
"""
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "analysis" / "cross_workload"
OUT.mkdir(parents=True, exist_ok=True)


def collect_iot():
    base = ROOT / "data" / "v3" / "iot" / "results"
    overall, per_fault = [], []
    for instance_dir in sorted(base.iterdir()):
        if not instance_dir.is_dir():
            continue
        # Primary 30s lives directly under instance_dir
        primary_overall = instance_dir / "model_results.csv"
        primary_per_fault = instance_dir / "per_fault_type_results.csv"
        if primary_overall.exists():
            df = pd.read_csv(primary_overall)
            df["workload"] = "iot"
            df["window_s"] = 30
            overall.append(df)
        if primary_per_fault.exists():
            df = pd.read_csv(primary_per_fault)
            df["workload"] = "iot"
            df["window_s"] = 30
            per_fault.append(df)
        # Sensitivity windows under sensitivity_ws_<ws>/
        for sub in instance_dir.glob("sensitivity_ws_*"):
            tail = sub.name.split("_")[-1]
            if not (tail.endswith("s") and tail[:-1].isdigit()):
                continue
            ws = int(tail.rstrip("s"))
            for kind, sink in [("model_results.csv", overall),
                                ("per_fault_type_results.csv", per_fault)]:
                p = sub / kind
                if p.exists():
                    df = pd.read_csv(p)
                    df["workload"] = "iot"
                    df["window_s"] = ws
                    sink.append(df)
    return pd.concat(overall, ignore_index=True), pd.concat(per_fault, ignore_index=True)


def collect_ecommerce():
    overall, per_fault = [], []
    # Existing v3 (6 models) at 30s
    base = ROOT / "data" / "results_v3" / "merged"
    df = pd.read_csv(base / "model_results.csv")
    df["workload"] = "ecommerce"; df["window_s"] = 30
    overall.append(df)
    df = pd.read_csv(base / "per_fault_type_results.csv")
    df["workload"] = "ecommerce"; df["window_s"] = 30
    per_fault.append(df)
    # Gap-fill (DSVDD + DAGMM) at 30s
    gap = ROOT / "data" / "v3" / "results" / "ecom-gapfill"
    df = pd.read_csv(gap / "model_results.csv")
    df["workload"] = "ecommerce"; df["window_s"] = 30
    overall.append(df)
    df = pd.read_csv(gap / "per_fault_type_results.csv")
    df["workload"] = "ecommerce"; df["window_s"] = 30
    per_fault.append(df)
    # Sensitivity windows for e-commerce (from existing v3 instance dirs, 15/60/120s)
    for inst in ["cpu-1", "gpu-1", "gpu-2"]:
        inst_dir = ROOT / "data" / "results_v3" / inst
        for sub in inst_dir.glob("sensitivity_ws_*"):
            tail = sub.name.split("_")[-1]
            if not (tail.endswith("s") and tail[:-1].isdigit()):
                continue
            ws = int(tail.rstrip("s"))
            for kind, sink in [("model_results.csv", overall),
                                ("per_fault_type_results.csv", per_fault)]:
                p = sub / kind
                if p.exists():
                    df = pd.read_csv(p)
                    df["workload"] = "ecommerce"; df["window_s"] = ws
                    sink.append(df)
    return pd.concat(overall, ignore_index=True), pd.concat(per_fault, ignore_index=True)


def main():
    iot_overall, iot_pf = collect_iot()
    ecom_overall, ecom_pf = collect_ecommerce()

    overall = pd.concat([iot_overall, ecom_overall], ignore_index=True)
    per_fault = pd.concat([iot_pf, ecom_pf], ignore_index=True)

    # Reorder for readability
    front = ["workload", "window_s", "model", "fold", "category"]
    overall_cols = front + [c for c in overall.columns if c not in front]
    overall = overall[overall_cols]
    pf_front = ["workload", "window_s", "model", "fold", "fault_id", "category"]
    pf_cols = pf_front + [c for c in per_fault.columns if c not in pf_front]
    per_fault = per_fault[pf_cols]

    overall.to_csv(OUT / "panel_overall.csv", index=False)
    per_fault.to_csv(OUT / "panel_per_fault.csv", index=False)

    # Quick summary
    print("=== panel_overall ===")
    print(f"shape: {overall.shape}")
    print(overall.groupby(["workload", "window_s", "model"]).size().unstack(fill_value=0))
    print()
    print("=== panel_per_fault ===")
    print(f"shape: {per_fault.shape}")
    print(per_fault.groupby(["workload", "window_s"])["fault_id"].nunique().to_string())


if __name__ == "__main__":
    main()
