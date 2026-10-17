#!/usr/bin/env python3
"""Run the campaign-1 training matrix of protocol 6.1 and 6.3 on one multi-GPU machine.

Added on 17 October 2026, after registration. It only schedules training/train.py, unchanged, one process per model and
fold, and never passes a setting the registered code does not already take. It adds no model, metric or rule.

Phases
  search   Nested HPO on campaign 1 at the 30 s anchor (6.1.1): 8 semi-supervised models at 100 trials and the
           random forest at 50, per fold, both workloads. Trial counts come from training/config.json.
  window   Window-size sensitivity on campaign 1 (6.1.5): the 8 semi-supervised models at 30 trials per fold, at
           15, 60 and 120 s on both workloads and 300 s on IoT.

Each job writes to <out>/<campaign>/w<size>/<model>/fold<k>/, so no two processes share a results file or an Optuna
database. A job whose summary and score file exist is skipped, and Optuna resumes a partial study from its database,
so a rerun after an interruption continues where it stopped.

    python3 training/run_matrix.py --derived /opt/sad/derived --out /opt/sad/results --phases search window \
        --gpus 4 --per-gpu 5 --cpu-slots 12 [--s3-uri s3://bucket/training/runs/<run>] [--sns-topic arn]
"""
from __future__ import annotations

import argparse
import heapq
import json
import os
import platform
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

HERE = Path(__file__).resolve().parent
TRAIN = HERE / "train.py"
SEMI = ["Trans_AE", "CNN_AE", "DeepSVDD", "DAGMM", "LSTM_AE", "LSTM_VAE", "OC_SVM", "IF"]
GPU_MODELS = {"LSTM_AE", "Trans_AE", "CNN_AE", "LSTM_VAE", "DeepSVDD", "DAGMM"}
# Relative cost per trial, measured on 17 Oct 2026 (CAMPAIGN_LOG). Only used to start the longest jobs first.
COST = {"Trans_AE": 13.0, "CNN_AE": 6.4, "DeepSVDD": 5.5, "DAGMM": 4.0, "OC_SVM": 3.9, "LSTM_AE": 3.8,
        "LSTM_VAE": 2.7, "IF": 0.3, "RF": 0.3}
WINDOW_SIZES = {"ecommerce": [15, 60, 120], "iot": [15, 60, 120, 300]}


@dataclass(order=True)
class Job:
    priority: float
    name: str = field(compare=False)
    model: str = field(compare=False)
    cmd: list = field(compare=False)
    out: Path = field(compare=False)
    gpu: bool = field(compare=False)
    attempts: int = field(default=0, compare=False)


def workload_of(cid: str) -> str:
    return "ecommerce" if cid.startswith("ec-") else "iot"


def is_done(out: Path, model: str, fold: int) -> bool:
    return (out / f"summary_{model}.json").is_file() and (out / "scores" / f"{model}_fold{fold}.parquet").is_file()


def build_jobs(args) -> list[Job]:
    jobs = []
    py = sys.executable
    for cdir in sorted(Path(args.derived).iterdir()):
        cid = cdir.name
        if not cid.endswith("-a"):  # campaign 1 only; replicates wait for the freeze (6.1.3)
            continue
        plan = []
        if "search" in args.phases:
            plan += [(30, m, None) for m in SEMI + ["RF"]]
        if "window" in args.phases:
            plan += [(w, m, 30) for w in WINDOW_SIZES[workload_of(cid)] for m in SEMI]
        for w, model, n_trials in plan:
            windows = cdir / f"windows_{w}s.parquet"
            if not windows.is_file():
                raise SystemExit(f"missing {windows}")
            for fold in range(1, 11):
                out = Path(args.out) / cid / f"w{w}" / model / f"fold{fold}"
                if is_done(out, model, fold):
                    continue
                cmd = [py, "-u", str(TRAIN), "--model", model, "--windows", str(windows),
                       "--folds", str(cdir / "folds.parquet"), "--fold", str(fold), "--output", str(out)]
                if n_trials:
                    cmd += ["--n-trials", str(n_trials)]
                trials = n_trials or (50 if model == "RF" else 100)
                # search before window, then longest first; heapq pops the smallest priority
                prio = (0 if n_trials is None else 1e6) - COST[model] * trials * (2.0 if w == 15 else 30.0 / w)
                jobs.append(Job(prio, f"{cid}/w{w}/{model}/fold{fold}", model, cmd, out, model in GPU_MODELS))
    return jobs


class Runner:
    def __init__(self, args, jobs):
        self.args, self.lock = args, threading.Lock()
        self.queue = list(jobs)
        heapq.heapify(self.queue)
        self.total = len(jobs)
        self.running: dict[str, dict] = {}
        self.done: list[dict] = []
        self.failed: list[dict] = []
        self.gpu_free = {g: args.per_gpu for g in range(args.gpus)}
        self.cpu_free = args.cpu_slots
        self.started = time.time()
        self.alerted = False

    def env_for(self, job: Job, gpu: int | None) -> dict:
        env = dict(os.environ)
        env.update({"OMP_NUM_THREADS": str(self.args.threads), "MKL_NUM_THREADS": str(self.args.threads),
                    "OPENBLAS_NUM_THREADS": str(self.args.threads), "LOKY_MAX_CPU_COUNT": str(self.args.loky_cpus),
                    "PYTHONHASHSEED": "0"})
        env["CUDA_VISIBLE_DEVICES"] = str(gpu) if gpu is not None else ""
        if gpu is None:
            env["STREAMPREP_DEVICE"] = "cpu"
        return env

    def take_slot(self, job: Job):
        if job.gpu:
            g = max(self.gpu_free, key=lambda k: self.gpu_free[k])
            if self.gpu_free[g] > 0:
                self.gpu_free[g] -= 1
                return g
            return None
        if self.cpu_free > 0:
            self.cpu_free -= 1
            return -1
        return None

    def free_slot(self, job: Job, slot):
        if job.gpu:
            self.gpu_free[slot] += 1
        else:
            self.cpu_free += 1

    def run_one(self, job: Job, slot):
        job.out.mkdir(parents=True, exist_ok=True)
        t0 = time.time()
        with open(job.out / "job.log", "a") as log:
            log.write(f"# {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} attempt {job.attempts + 1} "
                      f"slot {'gpu' + str(slot) if job.gpu else 'cpu'}\n# {' '.join(job.cmd)}\n")
            log.flush()
            rc = subprocess.call(job.cmd, stdout=log, stderr=subprocess.STDOUT,
                                 env=self.env_for(job, slot if job.gpu else None), cwd=str(HERE.parent))
        rec = {"job": job.name, "rc": rc, "seconds": round(time.time() - t0, 1), "attempt": job.attempts + 1}
        with self.lock:
            self.running.pop(job.name, None)
            self.free_slot(job, slot)
            fold = int(job.name.rsplit("fold", 1)[1])
            if rc == 0 and is_done(job.out, job.model, fold):
                self.done.append(rec)
            elif job.attempts + 1 < 2:
                job.attempts += 1
                heapq.heappush(self.queue, job)
                rec["requeued"] = True
                self.failed.append(rec)
            else:
                self.failed.append(rec)
                self.notify(f"training job failed twice: {job.name} rc {rc}")

    def notify(self, msg: str):
        if not self.args.sns_topic:
            return
        try:
            subprocess.run(["aws", "sns", "publish", "--region", "us-east-2", "--topic-arn", self.args.sns_topic,
                            "--subject", "streaming-anomaly-detection training", "--message", msg],
                           capture_output=True, timeout=60)
        except Exception:
            pass

    def status(self) -> dict:
        with self.lock:
            return {"updated_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "elapsed_h": round((time.time() - self.started) / 3600, 3), "total": self.total,
                    "done": len(self.done), "running": sorted(self.running), "queued": len(self.queue),
                    "failed_final": [f for f in self.failed if not f.get("requeued")],
                    "retried": [f for f in self.failed if f.get("requeued")],
                    "done_jobs": self.done[-50:], "phases": self.args.phases,
                    "host": platform.node(), "python": platform.python_version()}

    def write_status(self, final=False):
        p = Path(self.args.out) / ("DONE.json" if final else "status.json")
        p.write_text(json.dumps(self.status(), indent=1))

    def sync(self):
        if self.args.s3_uri:
            subprocess.run(["aws", "s3", "sync", "--quiet", "--region", "us-east-2", self.args.out, self.args.s3_uri],
                           capture_output=True, timeout=1800)

    def loop(self):
        last_sync = 0.0
        while True:
            with self.lock:
                while self.queue:
                    job = self.queue[0]
                    slot = self.take_slot(job)
                    if slot is None:
                        # the head job cannot start; try one job of the other kind before waiting
                        other = next((j for j in self.queue if j.gpu != job.gpu), None)
                        if other is None:
                            break
                        slot = self.take_slot(other)
                        if slot is None:
                            break
                        self.queue.remove(other)
                        heapq.heapify(self.queue)
                        job = other
                    else:
                        heapq.heappop(self.queue)
                    self.running[job.name] = {"since": time.time()}
                    threading.Thread(target=self.run_one, args=(job, slot), daemon=True).start()
                idle = not self.queue and not self.running
            self.write_status()
            if time.time() - last_sync > self.args.sync_every:
                self.sync()
                last_sync = time.time()
            if idle:
                break
            time.sleep(10)
        self.write_status(final=True)
        self.sync()
        st = self.status()
        self.notify(f"training phases {self.args.phases} finished: {st['done']} of {st['total']} jobs done, "
                    f"{len(st['failed_final'])} failed, {st['elapsed_h']} h")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--derived", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--phases", nargs="+", choices=["search", "window"], default=["search"])
    p.add_argument("--gpus", type=int, default=4)
    p.add_argument("--per-gpu", type=int, default=4)
    p.add_argument("--cpu-slots", type=int, default=8)
    p.add_argument("--threads", type=int, default=2, help="OMP/MKL threads per process")
    p.add_argument("--loky-cpus", type=int, default=4, help="cores a scikit-learn n_jobs=-1 fit may use")
    p.add_argument("--s3-uri", default=None)
    p.add_argument("--sns-topic", default=None)
    p.add_argument("--sync-every", type=int, default=300)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    jobs = build_jobs(args)
    if args.dry_run:
        for j in sorted(jobs):
            print(j.name, "gpu" if j.gpu else "cpu", " ".join(j.cmd[3:]))
        print(len(jobs), "jobs")
        return
    Path(args.out).mkdir(parents=True, exist_ok=True)
    Runner(args, jobs).loop()


if __name__ == "__main__":
    main()
