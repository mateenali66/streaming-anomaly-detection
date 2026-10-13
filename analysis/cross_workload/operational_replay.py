"""Chronological operational replay of the alert stream.

Operational alert burden can be derived analytically from TPR, FPR and an assumed
prevalence, or replayed chronologically. This script does the replay, and includes the EWMA
baseline in it.

The analytic estimate derives expected false alerts per hour as FPR x 120 for
30-second windows. That treats every window as an independent alerting opportunity. Real
on-call does not work that way: consecutive firing windows are one incident, and a detector
that fires 40 windows in a row produces one page, not 40.

This replays each fold's test split in wall-clock order, applies each model's per-fold
threshold calibrated on the cal split, and measures the alert stream an operator would see:

  alert episode   maximal run of alerting windows, merged across gaps of at most
                  SUPPRESSION_WINDOWS non-alerting windows (on-call deduplication)
  true episode    alert episode overlapping a fault episode
  false episode   alert episode overlapping no fault episode
  detection lag   time from fault-episode start to the first alerting window inside it
  monitored time  sum of covered window durations, NOT the span from first to last window,
                  because a fold's test runs are separated by cooldowns and by other
                  repetitions' runs. Counting those gaps would deflate the alert rate.

Scores come from the raw per-window arrays retained by the published runs, so this replays
the published models rather than refitting them. The EWMA baseline is the published one
(classical_baselines.score_ewma, via ewma_mechanism.py) at its stored cal-split threshold.
Sequence models score fewer windows than
the fold's test split (dense alignment drops the first seq_length - 1), and their scores
align to the tail of the split.

Output:
  operational_replay_episodes.csv   one row per alert episode
  operational_replay_per_fold.csv   per model and fold
  operational_replay_summary.csv    per model, aggregated over folds
  operational_replay.txt            human-readable, with the analytic estimate alongside
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(OUT))
from ewma_mechanism import feat_cols as ewma_feat_cols, fold_data, score_variant  # noqa: E402

PARQUET = ROOT / "data" / "features_v3" / "features_30s.parquet"
PANEL = OUT / "panel_overall.csv"
SCORE_TREES = [ROOT / "data" / "results_v3" / "merged" / "raw_scores",
               ROOT / "data" / "v3" / "results" / "ecom-gapfill" / "raw_scores"]

WORKLOAD = "ecommerce"
WINDOW_S = 30
SUPPRESSION_WINDOWS = 4   # merge alert runs separated by <= 4 quiet windows (2 min)
N_FOLDS = 10


def find_scores(model, fold):
    for tree in SCORE_TREES:
        s = tree / f"{model}_fold{fold}_scores.npy"
        l = tree / f"{model}_fold{fold}_labels.npy"
        if s.exists() and l.exists():
            return np.load(s), np.load(l)
    return None, None


def episodes(flags, max_gap=0):
    """Maximal runs of True in `flags`, merged across gaps of at most max_gap Falses."""
    idx = np.flatnonzero(flags)
    if idx.size == 0:
        return []
    groups, start, prev = [], idx[0], idx[0]
    for i in idx[1:]:
        if i - prev - 1 > max_gap:
            groups.append((start, prev))
            start = i
        prev = i
    groups.append((start, prev))
    return groups


def main():
    df = pd.read_parquet(PARQUET)
    panel = pd.read_csv(PANEL)
    panel = panel[(panel["workload"] == WORKLOAD) & (panel["window_s"] == WINDOW_S)
                  & (panel["category"] == "all")]
    thr_lookup = {(r["model"], int(r["fold"])): float(r["threshold"])
                  for _, r in panel.iterrows() if pd.notna(r["threshold"])}
    cb = pd.read_csv(Path(__file__).parent / "classical_baselines_per_fold.csv")
    ewma_thr = {int(r["fold"]): float(r["threshold"])
                for _, r in cb[(cb["model"] == "EWMA") & (cb["workload"] == WORKLOAD)].iterrows()}
    models = sorted(panel["model"].unique())

    ep_rows, fold_rows = [], []

    for fold in range(1, N_FOLDS + 1):
        col = f"fold_{fold}_split"
        te = df[(df[col] == "test") & (df["label"] != "EXCLUDED")].reset_index(drop=True)
        y_te = (te["label"] == "ANOMALOUS").to_numpy()

        work = {}
        for model in models:
            s, lab = find_scores(model, fold)
            if s is None:
                continue
            n = len(s)
            rows = te.iloc[len(te) - n:].reset_index(drop=True)   # tail alignment
            if not np.array_equal(lab.astype(bool), (rows["label"] == "ANOMALOUS").to_numpy()):
                print(f"  label mismatch {model} fold {fold}, skipping")
                continue
            thr = thr_lookup.get((model, fold))
            if thr is None:
                continue
            work[model] = (s, rows, thr)

        # the published EWMA baseline, at the threshold it was calibrated to on the cal split
        Xtr, _, Xte, _, _, _, te_full = fold_data(df, ewma_feat_cols(df), fold)
        assert np.array_equal(te_full["window_start"].to_numpy(), te["window_start"].to_numpy())
        te_s = score_variant("published", Xtr, Xte, None)
        work["EWMA"] = (te_s, te.copy(), ewma_thr[fold])

        for model, (s, rows, thr) in work.items():
            t = rows["window_start"].to_numpy(np.float64)
            y = (rows["label"] == "ANOMALOUS").to_numpy()
            alert = s >= thr

            fault_eps = episodes(y, max_gap=0)
            alert_eps = episodes(alert, max_gap=SUPPRESSION_WINDOWS)

            fault_spans = [(a, b) for a, b in fault_eps]
            n_true, n_false = 0, 0
            for a, b in alert_eps:
                hit = any(not (b < fa or a > fb) for fa, fb in fault_spans)
                n_true += hit
                n_false += (not hit)
                ep_rows.append(dict(
                    model=model, fold=fold, start_idx=int(a), end_idx=int(b),
                    n_windows=int(b - a + 1),
                    duration_s=float((b - a + 1) * WINDOW_S),
                    start_time=float(t[a]), kind="true" if hit else "false",
                    fault_id=rows["fault_id"].iloc[a] if hit else ""))

            # Detection lag is only meaningful when the detector was quiet going into the
            # fault. An always-on detector "detects" every fault at lag 0 while never
            # having been informative, so those episodes are counted separately.
            lags, lags_from_quiet, n_already_firing = [], [], 0
            for fa, fb in fault_spans:
                fired = np.flatnonzero(alert[fa:fb + 1])
                lag = float(fired[0] * WINDOW_S) if fired.size else np.nan
                lags.append(lag)
                was_quiet = (fa == 0) or (not alert[fa - 1])
                if was_quiet:
                    lags_from_quiet.append(lag)
                else:
                    n_already_firing += 1
            detected = int(np.sum(~np.isnan(lags)))

            monitored_h = len(rows) * WINDOW_S / 3600.0
            fp_windows = int(np.sum(alert & ~y))
            tn_windows = int(np.sum(~alert & ~y))
            fpr = fp_windows / max(fp_windows + tn_windows, 1)
            duty = float(alert.mean())
            longest = max((b - a + 1) for a, b in alert_eps) if alert_eps else 0

            fold_rows.append(dict(
                model=model, fold=fold, n_windows=len(rows),
                monitored_hours=monitored_h,
                n_fault_episodes=len(fault_spans), n_detected_episodes=detected,
                episode_recall=detected / max(len(fault_spans), 1),
                n_alert_episodes=len(alert_eps),
                n_true_episodes=n_true, n_false_episodes=n_false,
                false_episodes_per_hour=n_false / monitored_h if monitored_h else np.nan,
                alert_duty_cycle=duty,
                longest_alert_episode_windows=int(longest),
                longest_alert_episode_frac=longest / max(len(rows), 1),
                n_faults_detector_already_firing=n_already_firing,
                median_detection_lag_s=float(np.nanmedian(lags)) if detected else np.nan,
                median_lag_from_quiet_s=(float(np.nanmedian(lags_from_quiet))
                                         if np.any(~np.isnan(lags_from_quiet)) else np.nan),
                false_alert_minutes_per_hour=fpr * 60.0,
                fp_windows=fp_windows, window_fpr=fpr,
                analytic_fp_per_hour=fpr * (3600.0 / WINDOW_S),
                threshold=thr))

    ep = pd.DataFrame(ep_rows)
    fd = pd.DataFrame(fold_rows)
    ep.to_csv(OUT / "operational_replay_episodes.csv", index=False)
    fd.to_csv(OUT / "operational_replay_per_fold.csv", index=False)

    agg = fd.groupby("model").agg(
        folds=("fold", "nunique"),
        monitored_hours=("monitored_hours", "sum"),
        duty_cycle=("alert_duty_cycle", "mean"),
        longest_ep_frac=("longest_alert_episode_frac", "mean"),
        episode_recall=("episode_recall", "mean"),
        false_eps_per_hour=("false_episodes_per_hour", "mean"),
        analytic_fp_per_hour=("analytic_fp_per_hour", "mean"),
        false_alert_min_per_hour=("false_alert_minutes_per_hour", "mean"),
        lag_from_quiet_s=("median_lag_from_quiet_s", "median"),
        faults_already_firing=("n_faults_detector_already_firing", "sum"),
        n_fault_episodes=("n_fault_episodes", "sum"),
        n_false_episodes=("n_false_episodes", "sum"),
    ).reset_index()
    # A detector alerting most of the time produces one unending alert rather than many
    # discrete pages, so its episode counts understate the burden instead of overstating it.
    agg["always_on"] = agg["duty_cycle"] > 0.5
    agg = agg.sort_values(["always_on", "false_eps_per_hour"])
    agg.to_csv(OUT / "operational_replay_summary.csv", index=False)

    show = ["model", "duty_cycle", "longest_ep_frac", "false_alert_min_per_hour",
            "n_false_episodes", "false_eps_per_hour", "analytic_fp_per_hour",
            "episode_recall", "faults_already_firing", "lag_from_quiet_s", "always_on"]

    lines = [
        "CHRONOLOGICAL OPERATIONAL REPLAY", "",
        f"Workload: {WORKLOAD}, {WINDOW_S}s windows, {N_FOLDS} folds, published thresholds.",
        f"Alert episodes merge runs separated by <= {SUPPRESSION_WINDOWS} quiet windows "
        f"({SUPPRESSION_WINDOWS * WINDOW_S / 60:.0f} min on-call deduplication).",
        "Monitored time is the sum of covered window durations, not the first-to-last span.",
        "",
        "Read duty_cycle FIRST. Episode counts only describe a detector that is sometimes",
        "quiet. A detector alerting most of the time collapses into one unending alert, so",
        "its false-episode count falls toward zero while it is at its least usable. That is",
        "the opposite of the analytic FPR x 120 estimate's failure mode, which counts every",
        "window as a separate page.",
        "",
        "  duty_cycle                fraction of monitored windows in an alerting state",
        "  longest_ep_frac           longest single alert episode, as a fraction of the fold",
        "  false_alert_min_per_hour  minutes per hour alerting outside any fault",
        "  analytic_fp_per_hour      the analytic FPR x 120 estimate, for comparison",
        "  faults_already_firing     fault episodes the detector was already alerting into",
        "  lag_from_quiet_s          detection lag, only for faults entered from a quiet state",
        "",
        agg[show].round(3).to_string(index=False),
        "",
        "Episode recall is the fraction of fault episodes with at least one alerting window.",
    ]
    text = "\n".join(lines)
    (OUT / "operational_replay.txt").write_text(text)
    print(text)
    print(f"\nSaved: {OUT}/operational_replay_summary.csv")


if __name__ == "__main__":
    main()
