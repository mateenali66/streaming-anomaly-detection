"""Facts about the data and the training runs that the manuscript's methods and results quote (D6).

Added on 18 October 2026, after registration. It reads the build reports, the shared feature list, the training runs'
completion records and summaries, and the freeze record, and writes facts.json. numbers_tex.py turns each value
into a key, so no count in the text is typed by hand.

    python3 -m analysis.facts --derived DIR --search-run DIR --phase-b DIR --frozen DIR --out DIR
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

CAMPAIGNS = ["ec-a", "ec-b", "ec-c", "iot-a", "iot-b", "iot-c"]
PANEL = ["IF", "OC_SVM", "LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE", "DeepSVDD", "DAGMM", "RF"]


def build_facts(derived: Path) -> dict:
    out = {}
    for c in CAMPAIGNS:
        f = json.loads((derived / c / "build_report.json").read_text())["features"]
        out[c] = {"n_series": f["n_series"], "n_counter_series": f["n_counter_series"],
                  "n_feature_columns_30s": f["n_feature_columns"]["30"]}
    shared = json.loads((derived / "shared_30s.json").read_text())
    out["shared_feature_columns"] = shared["n_shared"]
    return out


def run_facts(search: Path, phase_b: Path) -> dict:
    s = json.loads((search / "DONE.json").read_text())
    b = json.loads((phase_b / "DONE.json").read_text())
    sel = {}
    for c in CAMPAIGNS:
        n = []
        for k in range(1, 11):
            summ = json.loads((phase_b / c / "w30-frozen" / "IF" / f"fold{k}" / "summary_IF.json").read_text())
            n.append(int(summ["per_fold"][0]["n_features"]))
        sel[c] = {"min": min(n), "max": max(n)}
    return {"search_jobs": s["total"], "search_done": s["done"], "search_not_estimable": len(s["failed_final"]),
            "search_hours": s["elapsed_h"], "phase_b_jobs": b["total"], "phase_b_done": b["done"],
            "phase_b_failed": len(b["failed_final"]), "phase_b_hours": b["elapsed_h"],
            "selected_features_IF": sel,
            "selected_features_min": min(v["min"] for v in sel.values()),
            "selected_features_max": max(v["max"] for v in sel.values())}


def frozen_facts(frozen: Path) -> dict:
    fr = json.loads((frozen / "FREEZE.json").read_text())
    out = {}
    for w, models in fr["workloads"].items():
        out[w] = {m: models[m]["consensus"].get("seq_length") for m in models if "seq_length" in models[m]["consensus"]}
    return {"seq_length": out}


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    for k in ("derived", "search-run", "phase-b", "frozen", "out"):
        p.add_argument(f"--{k}", required=True, type=Path)
    a = p.parse_args(argv)
    facts = {"build": build_facts(a.derived), "runs": run_facts(a.search_run, a.phase_b),
             "frozen": frozen_facts(a.frozen)}
    (a.out / "facts.json").write_text(json.dumps(facts, indent=1) + "\n")
    print(json.dumps(facts, indent=1))


if __name__ == "__main__":
    main()
