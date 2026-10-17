# Deviations from the registered protocol

Every change made after the registration of 14 October 2026 is recorded here, with the date, what changed, why, which campaigns and attempts it touches, and the author's approval. The paper reports each one. A change that is not recorded here did not happen.

## D1. The JVM buffer pool gauges pass the monotone guard (17 October 2026)

All six campaigns had finished before this change, so it touches no campaign data and no attempt. One line of `pipeline/config/pipeline.json` changed. `monotone_guard.allow_metric_patterns` went from an empty list to `["^jvm_buffer_pool_(used|capacity)_bytes$"]`. The code, the plans, the thresholds, the image lock and every other config value are as registered.

| | Registered | After D1 |
|---|---|---|
| SHA-256 of `pipeline/config/pipeline.json` | `a0b70b7cd2f1bc49cd7a83b5d3ef13b40370c63d5e15777098391e92e9bd6510` | `709aedb95909e252366a7fa60de74995415e56024d4488deec0cf8e5f26958d8` |
| `config_sha256` in build and training reports | `9b81d27e84ef7f528dbe60d8d2ea741e0f3da1c88bb6a69bfc12838485260b20` | `5e379394852faff090fa9a141772dc91f718cb03d69b90402d9eb177008efc5e` |

The registered config stopped the ec-a build at the monotone guard (protocol section 4.1, item 8). The guard stopped 2 of the 14,937 ec-a series. Both are broker 2's JVM direct buffer pool, `jvm_buffer_pool_used_bytes` and `jvm_buffer_pool_capacity_bytes` with pool `direct`, at Spearman 0.992. Over the 24 h leading baseline the gauge crept up by under 0.1 MiB near 0.6 MiB. After the baseline it moved with the faults between 0 and 9 MiB and fell 31 times. The guard stopped no series in iot-a, ec-b, iot-b, ec-c or iot-c. Until this was resolved, ec-a could not be built, and no campaign could be trained.

Three ways out were tested on the six verified campaign copies before the change, using the registered functions with the config changed in memory only.

1. An `exclude_series` rule for the direct pool, in the form of decision 42, would have removed the same 6 series (both gauges on each of the three brokers) from every campaign.
2. Switching the guard's policy from error to drop would have removed only the 2 ec-a series, so the ec-a features would differ from the other five.
3. Allowing the two gauges past the guard keeps them as features in every campaign.

The author chose option 3 on 17 October 2026.

Every other buffer pool series already passed the guard under the registered config and was a feature. So D1 changes no feature in iot-a, ec-b, iot-b, ec-c or iot-c. In ec-a it adds the 2 stopped series to the features, named by the registered naming rules. With D1 the guard stops no series in any of the six campaigns, and the pipeline tests pass (145 of 145).

The guard exists to keep out gauges that act as clocks, so that risk now stays in for these 2 series. A measurement made after the choice shows the gauge carries campaign history in all 18 direct pool series (6 per campaign), not only the 2 the guard stopped. On every broker of every campaign, `jvm_buffer_pool_used_bytes` with pool `direct` has a median of 0.54 to 0.93 MiB over the leading baseline and 6.83 to 8.97 MiB over the trailing baseline. Every trailing step lies above the leading baseline's maximum, and so do 97 to 99% of all steps after the leading baseline. On this gauge alone, the drift check (section 1.5, item 4) can tell the leading baseline from the trailing one in every campaign. It can also tell the first six hours of the leading baseline from the last six in every campaign, because the gauge creeps up inside the baseline as well. Its last six hours lie entirely above its first six on 17 of the 18 series, and the remaining one (iot-c broker 1) gives a single-series AUC of 0.717. The author was shown this before any build and kept option 3 on 17 October 2026. The paper reports the drift check as it comes out. The registered analysis limits what drift can do to the results. The within-run AUC compares each run's ANOMALOUS windows with the same attempt's own pre-fault windows (section 4.5, item 2), which all come after the baseline. A pooled result is flagged when its campaign's time-only AUC excludes 0.5 or its drift check fails (section 4.6, item 4, and section 1.5, item 4). A ranking claim needs both the pooled and the placebo-corrected within-run AUC (section 4.6, item 2). The paper reports D1 in its deviations appendix and in the response to the reviewers.
