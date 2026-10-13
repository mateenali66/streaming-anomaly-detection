"""Did the broker faults take effect? Evidence from the raw campaign metrics.

F1 kills one Kafka broker pod and F2 partitions one broker from the others. Either should
leave traces in the brokers' own JMX metrics, which are exported as raw values every 10 s:

  restarts     a killed broker's JVM restarts, so its cumulative counters fall back towards
               zero. Counted as any decrease in a broker's messages-in counter.
  replication  a dead or partitioned broker drops out of the in-sync replica sets, which
               raises IsrShrinks and UnderReplicatedPartitions on the others.
  traffic      per-broker message rate during injection, as a ratio to the run's pre-fault
               rate. A dead or isolated broker stops taking writes.

Reads the campaign metrics and manifests from the dataset archives (ecommerce_v6.tar.gz and
iot_v6.tar.gz unpacked at the repository root, or the same files under data/).

Run:  python analysis/cross_workload/injection_check.py
Output: injection_check_runs.csv (one row per fault run), injection_check.txt
"""
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).parent
ALL = [f"F{i}" for i in range(1, 9)]
# The runs in the published dataset: e-commerce F1-F4 from the first campaign file and F5-F8
# from the second (the first file's F5-F8 attempts were superseded), IoT from one file.
CAMPAIGNS = {
    "ecommerce": [("campaign_v1_metrics.parquet", "campaign_manifest_v1.json", ALL[:4]),
                  ("campaign_v2_metrics.parquet", "campaign_manifest_v2.json", ALL[4:])],
    "iot": [("campaign_iot.parquet", "campaign_manifest_iot.json", ALL)],
}
DIRS = {"ecommerce": [ROOT / "ecommerce", ROOT / "data" / "campaign"],
        "iot": [ROOT / "iot", ROOT / "data" / "v3" / "iot"]}
COUNTER = "broker__kafka_server_brokertopicmetrics_messagesinpersec_total__job_kafka_brokers__kafka_broker_streaming_kafka_brokers_{b}"


def find(workload, name):
    for d in DIRS[workload]:
        if (d / name).exists():
            return d / name
    raise SystemExit(f"{name} not found; unpack {workload}_v6.tar.gz at the repository root")


def main():
    rows, resets = [], []
    for wl, files in CAMPAIGNS.items():
        for metrics, manifest, faults in files:
            d = pd.read_parquet(find(wl, metrics))
            t = d["timestamp"].astype(float)
            isr = [c for c in d.columns if "isrshrinks_total" in c]
            urp = [c for c in d.columns if "underreplicatedpartitions" in c]
            msg = [COUNTER.format(b=b) for b in range(3)]
            for b, c in enumerate(msg):
                v = d[c].dropna().to_numpy()
                resets.append(dict(workload=wl, file=metrics, broker=b, samples=len(v),
                                   counter_resets=int((np.diff(v) < 0).sum())))
            for r in json.load(open(find(wl, manifest)))["runs"]:
                if r["fault_id"] not in faults:
                    continue
                inj, pre = r["phases"]["injection"], r["phases"]["pre_fault"]
                w = (t >= inj["start_epoch"]) & (t <= inj["end_epoch"])
                p = (t >= pre["start_epoch"]) & (t < inj["start_epoch"])
                if not w.any() or not p.any():
                    continue    # F6 logs a near-zero injection phase: its Job runs asynchronously
                rec = dict(workload=wl, fault=r["fault_id"], repetition=r["repetition"],
                           isr_shrinks=float(np.nansum(np.nanmax(d.loc[w | p, isr].to_numpy(), 0)
                                                       - np.nanmin(d.loc[w | p, isr].to_numpy(), 0))),
                           max_under_replicated=float(np.nanmax(d.loc[w, urp].to_numpy())))
                for b, c in enumerate(msg):
                    rec[f"msg_rate_ratio_b{b}"] = (np.nanmean(np.diff(d.loc[w, c].to_numpy()))
                                                   / np.nanmean(np.diff(d.loc[p, c].to_numpy())))
                rows.append(rec)
    runs = pd.DataFrame(rows)
    runs.round(6).to_csv(OUT / "injection_check_runs.csv", index=False)
    res = pd.DataFrame(resets)
    res.to_csv(OUT / "injection_check_counter_resets.csv", index=False)
    ratio = [f"msg_rate_ratio_b{b}" for b in range(3)]
    summ = runs.groupby(["workload", "fault"]).agg(
        runs=("repetition", "size"), runs_with_isr_shrink=("isr_shrinks", lambda s: int((s > 0).sum())),
        runs_with_under_replication=("max_under_replicated", lambda s: int((s > 0).sum())),
        **{f"{c}_mean": (c, "mean") for c in ratio})
    txt = ("BROKER COUNTER RESETS OVER EACH WHOLE CAMPAIGN (a JVM restart resets them)\n"
           + res.to_string(index=False)
           + "\n\nPER FAULT: runs with replication disturbance, and per-broker message rate "
             "during injection as a ratio to pre-fault\n" + summ.round(3).to_string() + "\n")
    (OUT / "injection_check.txt").write_text(txt)
    print(txt)


if __name__ == "__main__":
    main()
