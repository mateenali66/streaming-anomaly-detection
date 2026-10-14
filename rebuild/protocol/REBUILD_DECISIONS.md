# Rebuild decisions

Author decisions for the re-run, recorded 13 October 2026. The background is in `FOUNDATION_RECORD.md`.

1. Re-run both workloads from scratch with fixed injections, rather than describing the failures as a limitation of the old data.
2. Earlier versions (Zenodo v8, the R1 manuscript) stay as they are. The R2 version sets the record, and the cover letter tells the editor what was found and what changed.
3. Three fully independent campaigns per workload, six in total, each on a fresh cluster with its own 24-hour normal baseline before its 80 fault runs.
4. Fault order is randomised within each repetition block from a recorded seed.
5. Every run must pass an injection check against the pipeline's own metrics, or it is repeated. Failed runs are never labelled.
6. A pilot comes first: every fault once on each workload. The pilot is approved up to $60.
7. The full campaigns need a separate approval after the pilot passes. The estimate is about $870 in total: about $680 for the campaigns, $60 for the pilot, $100 for training and $30 for other items.
8. Account is the shared staging account (changed from the sandbox on 13 October), region us-east-2. The region allows 5 VPCs with 3 already used by other teams, and 5 Elastic IPs, so the clusters share one new VPC. Other teams' VPCs and resources are never touched. No account names or IDs go into any public file.
9. Hyperparameters are tuned on campaign 1 and frozen for campaigns 2 and 3.
10. The target is still 27 October 2026. If the pilot fails or a quota blocks the parallel campaigns, the author sends the editor an extension request that day.
11. EKS no longer offers Kubernetes 1.30. The re-run uses a standard-support version (1.34 to 1.37), so every component is checked against it. Extended-support versions cost about $0.60 per cluster hour instead of $0.10.
12. If the work cannot finish within 10 days (by 23 October), the author asks the editor for an extension.

13. After three independent reviews (REVIEW1_FAULTS.md, REVIEW2_PIPELINE.md, REVIEW3_INFRA.md), nothing runs until the orchestrator, preprocessing, Flink setup and Terraform are rebuilt, tested offline and re-reviewed.
14. F3 is dropped. Chaos Mesh's disk-fault tool does not run on ARM nodes, so F3 never ran in R1. The paper says so. Seven faults plus a sham run remain.
15. F1 is a forced pod kill of a broker that is not the KRaft controller. The controller's identity is recorded at onset.
16. All nodes, volumes and the NAT gateway sit in one availability zone. The paper notes the change from R1.
17. Cumulative counters become rates, and Flink workers are aggregated by role.
18. Hyperparameter search optimises AUC-ROC, not F1.
19. Cooldown between runs is 300 s. The recovery gate must still pass before the next run.
20. The extension request waits until after the pilot (author decision, 13 October). Data plus paper will not fit in 10 days, so it is expected.

## Live prices used (us-east-2, on demand, 13 October 2026)

| Item | Price |
|---|---|
| r6g.xlarge | $0.2016 per hour |
| r6g.2xlarge | $0.4032 per hour |
| g5.xlarge | $1.006 per hour |
| EKS control plane | $0.10 per hour |
| NAT gateway | $0.045 per hour plus $0.045 per GB |
| gp3 storage | $0.08 per GB-month |

One cluster (3 × r6g.xlarge, 3 × r6g.2xlarge, the EKS fee, 600 GB gp3) costs about $1.98 per hour.

## Fixes adopted after review 4 (13 October 2026, author to confirm with the pilot results)

Review 4 (`REVIEW4_DATA_VALIDITY.md`, `REVIEW4_OPERATIONS.md`, `REVIEW4_LIVE_CONTRACT.md`) found defects the offline tests could not reach. These fixes go in before the pilot. Nothing is registered yet, so they are pre-registration changes.

21. TELEMETRY_FAILED comes from scrape health, not series presence. An attempt fails telemetry when an untargeted scrape target misses more than 3 scrapes in one segment (Prometheus `up` absent or 0). Task-level series presence is recorded as a value and a flag, never a gate. The exemptions for the fault's own targets cover INJECTION and RECOVERY.
22. A sham whose signature passes is GATE_FAILED and repeated, as protocol 2.4 says. False passes are counted over every F0 attempt. The analysis, not the gate, flags fault runs of a signature that false-passed twice.
23. Every rule the preprocessing applies to a valid attempt is also a gate check, so a broken record becomes a repeat during the campaign and never an unbuildable campaign afterwards.
24. NORMAL time must be healthy. PRE_FAULT gets an envelope check (every job RUNNING with the same job ID, three TaskManagers registered, no pod replaced or restarted, probes at their pass level). BASELINE and TRAILING windows that fail the same conditions are EXCLUDED by this fixed rule, and the counts are reported.
25. The variance filter never removes the TaskManager count, the derived features or the named event counters (under-replicated partitions, ISR changes, failed checkpoints and the like). Constant columns keep unit scale. TaskManager aggregates get explicit missingness features, so a lost TaskManager is never filled in silently.
26. Isolation Forest and OC-SVM scores are scaled once per fold with a scaler fitted on train NORMAL scores, never on the set being scored. The same applies to DAGMM's non-finite energies and Deep SVDD's degenerate check. This is a change from the R1 model code and the paper says so.
27. Sequence length is searched on 5 to 11 windows. Within-run AUC uses the same negatives for every model: the last 10 pre-fault windows of each attempt. The placebo compares the late 5 with the early 5 of those. The time-only reference for C5 is block position. This needs the 600 s pre-fault, so cost option C (300 s) is no longer workable without dropping the sequence models from the within-run analysis.
28. The F6 injector opens no Kafka connection before its start time, so F6 pre-fault windows carry no injector traffic.
29. The results bucket name carries no account ID, and the exporter strips the registry host from image labels, so no account identifier enters the deposited data.
30. Long Terraform applies and destroys run detached with a log and are polled with short commands. No single command waits longer than 10 minutes.

## Choices made while fixing review 4 (14 October 2026, author to confirm with the pilot results)

31. Flink jobs now run with R1's settings: parallelism 4, checkpoints every 60 s exactly once, timeout 120 s, RocksDB incremental. Before this fix the submit path gave parallelism 1 with checkpointing off, which a real Flink 1.19.3 run in the built image confirmed. Two settings differ from R1. Checkpoints are retained on cancel, so the remediation can restore from the last one. The restart strategy is failure-rate (20 failures in 10 minutes, 30 s delay) instead of R1's fixed-delay with 10 attempts. A fixed-delay budget counts over a job's whole life, and ten F5 attempts per campaign would use it up early, after which every job failure would be permanent. The pilot runs with failure-rate. The paper names both changes.
32. The protected columns of decision 25 are also exempt from the correlation filter, not only the variance filter, because protocol 4.1.5 keeps the derived features. The narrower reading is one config line.
33. The time-only reference for C5 is each fault's mean block position, with Spearman rho between the two workloads.
34. Decision 25 is extended to brokers. A broker whose series are missing gets a missingness feature, so a down broker under F1 is never median-filled silently.
35. At 60, 120 and 300 s windows the pre-fault holds 10, 5 and 2 windows, so sequence models cannot score the full within-run set there. Training records it as incomplete, and the within-run analysis uses the window sizes where it is complete.
36. The EKS CoreDNS add-on schema keeps its keys behind a `$ref`. The version capture check now follows it. The captured versions are Kubernetes 1.35 (standard support to 26 March 2027), node AMI 1.35.8-20261003, CoreDNS v1.13.2-eksbuild.50, kube-proxy v1.35.3-eksbuild.29, VPC CNI v1.22.4-eksbuild.3, Pod Identity agent v1.3.10-eksbuild.3 and EBS CSI v1.67.0-eksbuild.1, each the AWS default for 1.35 on 14 October 2026.
37. No standard on-demand vCPU was in use in the region in the 7 days before the pilot (no AWS/Usage datapoint), so the 72 vCPUs of two pilots and the 216 of six campaigns fit the 1,152 quota.

## Decisions after the pilot (14 October 2026, author: "I'll go with your recommendations")

The pilot ran on 14 October 2026 (`PILOT_LOG.md`). Every fault took effect and passed on both workloads, F1 and F2 included. The author took the recommendations below. Nothing is registered yet, so all of them are pre-registration changes.

38. Section 10 of the protocol is settled as follows.
    1. Cost option A as written, trailing baseline kept. It is the out-of-time NORMAL check for the drift confound, and B saves about $95 by dropping it. The requote after the pilot is about $970 of new spend (six campaigns about $860, training about $100, network and storage about $10), about $1,010 with the pilot's actual cost of about $41. The campaigns still need the author's yes on that figure (item 10).
    2. The values stay as written: the F5 job-recovery limit of 180 s, the F8 fill target of 85% with a 256 MiB minimum, and the halt after two consecutive failed attempts or more than 8 in total. The pilot found no mechanical problem with any of them (all jobs running 38 s after the last F5 kill, F8 at about 85% with no OOM, no failed attempt).
    3. Margin delta 0.03 AUC.
    4. Under-replicated partitions stay a presence-only check in the F1 and F2 gate. In the pilot the presence tests found 33 and 34 samples.
    5. Registration is a signed git tag on the repository, pushed to the public mirror after the identifier scan. Pilot data and any abandoned campaign go into the deposit, labelled as such and never analysed.
    6. The dataset reuses the reserved v9 DOI with the rebuilt files. v8 stays as it is.
    7. RQ1 keeps "broker failure", because F1 and F2 now take effect. RQ4 names reproducibility across independent clusters.
    8. Flink operator 1.10.0, which ran the pilot on EKS 1.35.
    9. The F1 label is a fixed 300 s, with the not-Ready span as the secondary labelling of 3.6. The pilot measured 37 s and 61 s of disturbance.
    10. Open until the author approves the requote in item 1.
39. Decisions 21 to 37 are confirmed with the pilot data. Decision 21 exempts every fault's own targets, which the pilot exercised (missed scrapes only on targeted pods).
40. Fixes from the pilot, each with a test: the F4 CPU sample over 60 s (C41), three instants rounded to the millisecond (C42), training on a one-block pilot with `--frozen-params`, `mark_complete.py` refusing an existing marker, and the identifier scan skipping exactly the files git ignores.
41. The restart strategy allows 40 failures in 10 minutes instead of 20. A job with no shuffle restarts four regions per TaskManager loss, so five F5 kills on one such job reach 20. The pilot saw 12 in one cell. The Flink jobs image must be rebuilt and its digest locked before registration.
42. Kept out of the features (`counters.exclude_series`): the JVM pools that grow with process age (Metaspace, CodeHeap, Compressed Class Space, G1 Old Gen, the non-heap area and Flink's NonHeap and Metaspace gauges), which the monotone guard stopped, and two families of Kafka meters whose presence follows fault history (REVIEW4_LIVE_CONTRACT L3): the dead-letter topic's broker meters and request types off the data path. Broker request rates keep Produce, Fetch, FetchConsumer, FetchFollower and Metadata, the produce and fetch set of R1. Rebuilt with these rules, no pilot column that is complete in the baseline is missing in more than 5.2% of later windows.
43. The derived features stay as coded. On this deployment `watermark_lag` is undefined (the jobs assign no watermarks) and fold_fit drops it, and `rebalance_frequency` is always 0 (Flink's KafkaSource never joins a consumer-group rebalance). The paper says six of the eight carry information and describes F5 as TaskManager loss, not a consumer rebalance.
44. F7 stays at 50% packet loss. A TaskManager lost to the heartbeat timeout is flagged F7_TM_LOSS as the protocol intends (1 of 6 TaskManagers in the pilot).
45. The e-commerce load profile stays LOW, 1,000 events/s with a 20% sinusoid, the rate R1 measured.
46. The earlier recommendation of a preferred leader election after F1 is withdrawn. The controller's own count shows all 29 displaced leaders restored within 5 to 8 minutes after every F1 and F2, so leadership was never the cause. The lasting change was the first-use meters of item 42.
47. Kafka disk. The export holds no volume metric, so the 70% check of infra README section 6 was bounded from the brokers' produce rates instead. IoT writes about 3.5 MB/s, so 2 h of raw and enriched events, one unrolled segment per partition and 24 h of aggregated metrics come to about 60 GiB per broker, about 20% of 300 GiB.
