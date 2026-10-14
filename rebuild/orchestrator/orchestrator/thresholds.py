"""Gate thresholds as registered (run/thresholds.json, protocol 2.4, 1.6, 2.6, 3.4).

The fixed values are the numbers of protocol 2.4 and 1.6. The config-driven values are the section 10
decision values. The whole object is written once as canonical JSON. Its SHA-256 goes into the manifest and
into every verification record, and a restart refuses to continue if the recomputed bytes differ.
"""
from __future__ import annotations

from . import SCHEMA_VERSION, ids, queries
from .canonical import canonical_bytes, sha256_hex


def build(cfg: dict) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "gate": {
            "lead_s": 30,                       # 2.4: injection samples after a 30 s lead
            "F1": {
                "presence_window_s": 60,        # within 60 s of onset
                "probe_port": ids.KAFKA_PORT_REPLICATION,
                "urp_min": 1,
                "urp_presence_check": bool(cfg["f1_f2_urp_presence_check"]),
                "apply_max_s": cfg["fixed"]["apply_max_s"],
            },
            "F2": {
                "probe_ports": [ids.KAFKA_PORT_CONTROLLER, ids.KAFKA_PORT_REPLICATION],
                "probe_fail_during_frac_min": 0.80,
                "probe_ok_pre_frac_min": 0.95,
                "fenced_min": 1,
                "urp_min": 1,
                "urp_presence_check": bool(cfg["f1_f2_urp_presence_check"]),
                "client_port": ids.KAFKA_PORT_PLAIN,
                "client_ok_during_frac_min": 0.95,
                "apply_max_s": cfg["fixed"]["apply_max_s"],
            },
            "F4": {"cpu_ratio_min": 2.0, "cpu_cores_min": 1.6, "container": ids.FLINK_CONTAINER,
                   "apply_max_s": cfg["fixed"]["apply_max_s"]},
            "F5": {"kills": 5, "interval_s": 60, "new_uid_min": 5, "uid_window_after_end_s": 60,
                   "kill_apply_max_s": cfg["fixed"]["apply_max_s"],
                   "job_recovery_limit_s": cfg["f5_job_recovery_limit_s"],
                   # 11.7 and 11.11 (C13, C24): the stored probe records hold a checkpoint path relative to this campaign's
                   # scratch prefix s3://<results-bucket>/scratch/<campaign_id>/, so the bucket never enters the record.
                   # A path elsewhere keeps its key with the bucket removed and fails this prefix test.
                   "checkpoint_path_prefix": "flink-checkpoints/"},
            "F6": {"ready_lead_s": 30, "acked_frac_min": 0.95, "apply_max_s": 5, "consumer_frac_min": 0.90},
            "F7": {"loss_during_min": 0.30, "loss_pre_max": 0.02, "apply_max_s": cfg["fixed"]["apply_max_s"]},
            "F8": {"target_limit_frac": cfg["f8_target_limit_permille"] / 1000.0,
                   "min_stressor_mib": cfg["f8_min_stressor_mib"], "increase_frac_of_stressor_min": 0.5,
                   "sizing_window_s": 60, "container": ids.FLINK_CONTAINER,
                   "apply_max_s": cfg["fixed"]["apply_max_s"],
                   # REVIEW4 L8: container_oom_events_total of flink-main-container may not increase from onset to the
                   # end of RECOVERY (a killed stressor leaves no OOMKilled reason)
                   "oom_events_increase_max": 0},
            # 2.4: the labelled end must fall within 60 s of onset plus duration. An overrun of 60 to 120 s is flagged OVERRUN, above
            # 120 s fails, and an underrun below -60 s fails (REVIEW4 CAM-2 item 3).
            "timing": {"end_tolerance_s": 60, "overrun_fail_s": 120, "underrun_fail_s": -60, "not_found_max_s": 60},
            "telemetry": {
                "segments": ["PRE_FAULT", "INJECTION", "RECOVERY"],
                # Decision 21 (REVIEW4 PIL-1): TELEMETRY_FAILED comes from scrape health. A scrape target is a broker,
                # JobManager or TaskManager pod while kube-state-metrics shows it; a scrape is missed when Prometheus `up`
                # for that pod is absent or 0 at a 10 s step. More than scrape_miss_max misses of one untargeted target in
                # one segment fail the attempt.
                "scrape_miss_max": 3,
                "scrape_roles": ["broker", "jobmanager", "taskmanager"],
                # The fault's own targets are exempt in these segments (decision 21).
                "exempt_segments": ["INJECTION", "RECOVERY"],
                # Each fault's own targets: TARGET_BROKER is the broker in targets_expected, taskmanager is every
                # TaskManager pod, including the replacements F5 creates.
                "targeted": {"F1": ["TARGET_BROKER"], "F2": ["TARGET_BROKER"], "F4": ["taskmanager"],
                             "F5": ["taskmanager"], "F7": ["taskmanager"], "F8": ["taskmanager"]},
                # Task-level series presence is recorded as a value and a flag, never a gate (decision 21): an untargeted
                # (source, scope) below this coverage flags the attempt SERIES_GAP.
                "series_gap_flag_below": 0.95,
                # (source, role, expected pods per 10 s step). Brokers are per node ID.
                "sources": [
                    {"source": "broker", "role": "broker", "per_broker": True, "pods": 1},
                    {"source": "jvm", "role": "broker", "per_broker": True, "pods": 1},
                    {"source": "flink", "role": "jobmanager", "per_broker": False, "pods": 1},
                    {"source": "flink", "role": "taskmanager", "per_broker": False, "pods": ids.EXPECTED_TASKMANAGERS},
                    {"source": "jvm", "role": "taskmanager", "per_broker": False, "pods": ids.EXPECTED_TASKMANAGERS},
                    {"source": "consumer", "role": "taskmanager", "per_broker": False, "pods": ids.EXPECTED_TASKMANAGERS},
                ],
            },
            # Decision 24 (REVIEW4 CAM-3): PRE_FAULT must be healthy. Envelope checks over the PRE_FAULT segment.
            "pre_fault": {"taskmanagers_registered": ids.EXPECTED_TASKMANAGERS, "tcp_ok_frac_min": 0.95,
                          "packet_loss_frac_max": 0.02,
                          "expected_pods": {"broker": ids.EXPECTED_BROKERS, "jobmanager": 1,
                                            "taskmanager": ids.EXPECTED_TASKMANAGERS}},
            # REVIEW4 MIN-1: onset and labelled end recomputed from the stored chaos records and injector log.
            "record_time_tolerance_s": 0.002,
        },
        "recovery_gate": {
            "hold_s": cfg["fixed"]["gate_hold_s"],
            "timeout_s": cfg["fixed"]["gate_timeout_s"],
            "remediation_timeout_s": cfg["fixed"]["remediation_timeout_s"],
            "poll_s": 10,
            "brokers_ready": ids.EXPECTED_BROKERS,
            "jobmanagers_ready": 1,
            "taskmanagers_ready": ids.EXPECTED_TASKMANAGERS,
            "registered_taskmanagers": ids.EXPECTED_TASKMANAGERS,
            "task_slots": ids.EXPECTED_TASKMANAGERS * ids.SLOTS_PER_TASKMANAGER,
            "checkpoints_since_end_min": 2,
            "failed_checkpoint_window_s": 120,
            "lag_sd_multiplier": 3,
            "rate_match_tolerance_frac": 0.05,
            "rate_window_s": 60,
            "tcp_ok_frac_min": 0.95,
            "packet_loss_frac_max": 0.02,
            "probe_window_s": 120,
            "probe_max_age_s": 30,
            # REVIEW4 L3: the dead-letter rate comes from the verification consumer's dead_letter_count over 60 s, a
            # counter that always exists, instead of the broker's lazily created per-topic meter.
            "dead_letter_window_s": 60,
        },
        "health_gate": {"clock_skew_max_s": 1.0},
        # REVIEW4 L1 (C40): the job settings every expected job must run with, read back from the Flink REST API at every
        # start and after every resubmission. They are the settings rebuild/infra/images/flink-jobs/run-job.sh passes;
        # tests/test_review4.py compares the two.
        "flink_job_settings": {"parallelism": 4, "checkpoint_mode": "exactly_once", "checkpoint_interval_ms": 60000,
                               "checkpoint_timeout_ms": 120000, "checkpoint_min_pause_ms": 30000,
                               "externalization_enabled": True, "delete_on_cancellation": False},
        "campaign_rules": {
            "halt_consecutive_failed": cfg["halt_consecutive_failed"],
            "halt_total_failed_gt": cfg["halt_total_failed_gt"],
            "trailing_baseline": bool(cfg["trailing_baseline"]),
            "pause_abandon_s": cfg["fixed"]["pause_abandon_s"],
            "s3_failure_halt_s": cfg["fixed"]["s3_failure_halt_s"],
            "max_attempt_number": 9,
            # REVIEW4 MIN-3: before TRAILING, the health gate, then a fixed settle of settle_max seconds, plus the 600 s
            # extra settle when the last remediation redeployed Flink or resubmitted a job.
            "trailing_settle_s": cfg["settle_max_s"],
        },
        "queries": queries.build(cfg),
        "prometheus_jobs": dict(cfg["prometheus_jobs"]),
    }


def thresholds_bytes(th: dict) -> bytes:
    return canonical_bytes(th)


def thresholds_sha(th: dict) -> str:
    return sha256_hex(thresholds_bytes(th))
