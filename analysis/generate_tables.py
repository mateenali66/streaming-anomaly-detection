#!/usr/bin/env python3
"""
LaTeX sources for three e-commerce tables, written to tables/:
  tab_confusion.tex       confusion matrices, mean over 10 folds, F1-optimal thresholds
  tab_rf_importance.tex   Random Forest feature importance, top 20, mean over 10 folds
  tab_unsupervised.tex    label-free (IQR) thresholds against labelled thresholds, for the
                          primary run's models. The paper's table adds Deep SVDD and DAGMM
                          from data/v3/results/ecom-gapfill/model_results_unsupervised.csv.
Detection latency is in analysis/cross_workload/detection_latency_table.py.
"""

from pathlib import Path

import numpy as np
import pandas as pd

RESULTS_DIR = Path("data/results_v3/merged")
OUTPUT_DIR = Path("tables")


def parse_feature_name(raw: str) -> tuple[str, str, str, str]:
    """Extract (category, metric_short, instance, aggregation) from raw Prometheus feature name."""
    parts = raw.split("__")
    category = parts[0].upper() if parts[0] in ("broker", "consumer", "flink", "jvm") else parts[0]

    agg = parts[-1] if parts[-1] in ("mean", "std", "min", "max", "slope", "iqr") else "?"

    if len(parts) < 2:
        return category, raw, "", agg

    core = parts[1]

    instance_parts = []
    for p in parts:
        if "kafka_brokers_0" in p: instance_parts.append("b0")
        elif "kafka_brokers_1" in p: instance_parts.append("b1")
        elif "kafka_brokers_2" in p: instance_parts.append("b2")
        if "G1 Eden" in p: instance_parts.append("Eden")
        elif "G1 Old" in p: instance_parts.append("Old")
        elif "Compressed Class" in p: instance_parts.append("CCS")
        elif "Metaspace" in p: instance_parts.append("Meta")
        elif "CodeCache" in p or "CodeHeap" in p: instance_parts.append("Code")
        if "Enrichment" in p: instance_parts.append("enrich")
        elif "Aggregation" in p: instance_parts.append("agg")
        elif "Fraud" in p: instance_parts.append("fraud")
        if "partition_8" in p: instance_parts.append("p8")
        if "request_" in p and "version" in p:
            req_type = p.split("request_")[1].split(",")[0]
            instance_parts.append(req_type)
    instance = "/".join(instance_parts) if instance_parts else ""

    core = core.replace("flink_taskmanager_", "tm.")
    core = core.replace("flink_jobmanager_", "jm.")
    core = core.replace("kafka_network_", "net.")
    core = core.replace("kafka_server_", "srv.")
    core = core.replace("jvm_memory_pool_", "mem.pool.")
    core = core.replace("jvm_memory_", "mem.")
    core = core.replace("jvm_buffer_pool_", "buf.pool.")
    core = core.replace("jvm_threads_", "threads.")
    core = core.replace("jvm_gc_collection_", "gc.")
    core = core.replace("job_task_operator_KafkaSourceReader_topic_partition_", "src.partition.")
    core = core.replace("Status_JVM_CPU_", "cpu.")
    core = core.replace("Status_JVM_Memory_NonHeap_", "mem.nonheap.")
    core = core.replace("requestmetrics_requestspersec_total", "req/s")
    core = core.replace("_bytes_total", "")
    core = core.replace("_bytes", "")
    core = core.replace("allocated", "alloc")

    if len(core) > 35:
        core = core[:32] + "..."

    return category, core, instance, agg


def generate_confusion_matrix_table():
    """Aggregated confusion matrices from F1-optimal thresholds."""
    df = pd.read_csv(RESULTS_DIR / "model_results.csv")

    rows = []
    for model in sorted(df["model"].unique()):
        mdf = df[df["model"] == model]
        tp_list, fp_list, fn_list, tn_list = [], [], [], []

        for _, row in mdf.iterrows():
            tp = round(row["recall"] * row["true_anomalies"])
            fp = round(row["predicted_anomalies"] - tp) if row["predicted_anomalies"] > tp else 0
            fn = round(row["true_anomalies"] - tp)
            tn = round(row["total_samples"] - tp - fp - fn)
            tp_list.append(tp)
            fp_list.append(fp)
            fn_list.append(fn)
            tn_list.append(tn)

        tp_mean = np.mean(tp_list)
        fp_mean = np.mean(fp_list)
        fn_mean = np.mean(fn_list)
        tn_mean = np.mean(tn_list)
        total = tp_mean + fp_mean + fn_mean + tn_mean
        tpr = tp_mean / (tp_mean + fn_mean) if (tp_mean + fn_mean) > 0 else 0
        fpr = fp_mean / (fp_mean + tn_mean) if (fp_mean + tn_mean) > 0 else 0

        rows.append({
            "model": model,
            "TP": tp_mean, "FP": fp_mean, "FN": fn_mean, "TN": tn_mean,
            "TPR": tpr, "FPR": fpr, "N": total,
        })

    lines = []
    lines.append(r"\begin{table}[t]")
    lines.append(r"\caption{Confusion matrix summary (mean across 10 folds, F1-optimal thresholds).}")
    lines.append(r"\label{tab:confusion}")
    lines.append(r"\centering")
    lines.append(r"\small")
    lines.append(r"\begin{tabular}{lrrrrrr}")
    lines.append(r"\toprule")
    lines.append(r"Model & TP & FP & FN & TN & TPR & FPR \\")
    lines.append(r"\midrule")

    for r in sorted(rows, key=lambda x: x["model"]):
        m = r["model"].replace("_", r"\_")
        lines.append(
            f"  {m} & {r['TP']:.1f} & {r['FP']:.1f} & {r['FN']:.1f} & {r['TN']:.1f} "
            f"& {r['TPR']:.3f} & {r['FPR']:.3f} \\\\"
        )

    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    lines.append(r"\end{table}")

    table = "\n".join(lines)
    (OUTPUT_DIR / "tab_confusion.tex").write_text(table)
    print("Confusion matrix table")
    print(table)
    print()
    return table


def generate_rf_importance_table():
    """Top 20 RF features by mean importance across 10 folds."""
    df = pd.read_csv(RESULTS_DIR / "rf_feature_importance.csv")

    top = df.groupby("feature")["importance"].agg(["mean", "std", "count"]).sort_values("mean", ascending=False).head(20)

    lines = []
    lines.append(r"\begin{table}[t]")
    lines.append(r"\caption{Top 20 Random Forest features by mean Gini importance across 10 folds.}")
    lines.append(r"\label{tab:rf-importance}")
    lines.append(r"\centering")
    lines.append(r"\small")
    lines.append(r"\begin{tabular}{cllllr}")
    lines.append(r"\toprule")
    lines.append(r"Rank & Cat. & Metric & Instance & Agg. & Imp. \\")
    lines.append(r"\midrule")

    for rank, (feat, row) in enumerate(top.iterrows(), 1):
        cat, metric, instance, agg = parse_feature_name(feat)
        metric_escaped = metric.replace("_", r"\_").replace("...", r"\ldots")
        inst_escaped = instance.replace("_", r"\_") if instance else "--"
        lines.append(
            f"  {rank} & {cat} & \\texttt{{{metric_escaped}}} & {inst_escaped} & {agg} & {row['mean']:.4f} \\\\"
        )

    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    lines.append(r"\end{table}")

    table = "\n".join(lines)
    (OUTPUT_DIR / "tab_rf_importance.tex").write_text(table)
    print("RF feature importance table")
    print(table)
    print()

    cat_summary = df.copy()
    cat_summary["category"] = cat_summary["feature"].apply(lambda f: f.split("__")[0].upper())
    cat_imp = cat_summary.groupby("category")["importance"].sum().sort_values(ascending=False)
    total_imp = cat_imp.sum()
    print("Category importance shares:")
    for cat, imp in cat_imp.items():
        print(f"  {cat}: {imp:.4f} ({imp/total_imp*100:.1f}%)")
    print()

    return table


def generate_unsupervised_table():
    """Unsupervised (IQR) vs labeled (F1-optimal) threshold comparison."""
    df_labeled = pd.read_csv(RESULTS_DIR / "model_results.csv")
    df_unsup = pd.read_csv(RESULTS_DIR / "model_results_unsupervised.csv")

    # Only semi-supervised models have unsupervised thresholds
    semi_sup = ["IF", "OC_SVM", "LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE"]

    lines = []
    lines.append(r"\begin{table}[t]")
    lines.append(r"\caption{Labeled (F1-optimal) vs.\ unsupervised (IQR) threshold performance (mean $\pm$ std, 10 folds).}")
    lines.append(r"\label{tab:unsupervised}")
    lines.append(r"\centering")
    lines.append(r"\small")
    lines.append(r"\begin{tabular}{lcccc}")
    lines.append(r"\toprule")
    lines.append(r" & \multicolumn{2}{c}{Labeled (F1-opt)} & \multicolumn{2}{c}{Unsupervised (IQR)} \\")
    lines.append(r"\cmidrule(lr){2-3} \cmidrule(lr){4-5}")
    lines.append(r"Model & F1 & Precision & F1 & Precision \\")
    lines.append(r"\midrule")

    for model in semi_sup:
        lab = df_labeled[df_labeled["model"] == model]
        uns = df_unsup[df_unsup["model"] == model]

        lab_f1 = f"{lab['f1_score'].mean():.3f}$\\pm${lab['f1_score'].std():.3f}"
        lab_p = f"{lab['precision'].mean():.3f}$\\pm${lab['precision'].std():.3f}"
        uns_f1 = f"{uns['f1_score'].mean():.3f}$\\pm${uns['f1_score'].std():.3f}"
        uns_p = f"{uns['precision'].mean():.3f}$\\pm${uns['precision'].std():.3f}"

        m = model.replace("_", r"\_")
        lines.append(f"  {m} & {lab_f1} & {lab_p} & {uns_f1} & {uns_p} \\\\")

    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    lines.append(r"\end{table}")

    table = "\n".join(lines)
    (OUTPUT_DIR / "tab_unsupervised.tex").write_text(table)
    print("Unsupervised vs labeled thresholds table")
    print(table)
    print()
    return table


if __name__ == "__main__":
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    generate_confusion_matrix_table()
    generate_rf_importance_table()
    generate_unsupervised_table()
    print(f"All tables saved to {OUTPUT_DIR}/")
