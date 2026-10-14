# Registration of the rebuilt campaigns

Registered on 14 October 2026, after the pilot of the same day and before any of the six campaigns launched. This file, the commit that holds it and its signed tag are the pre-registration of protocol section 7.6.

## What is registered

1. The protocol, `protocol/REBUILD_PROTOCOL.md`, with the interface contract of its section 11 and the contract changes in `CONTRACT_CHANGES.md` (C1 to C42).
2. The author's decisions 1 to 47, `protocol/REBUILD_DECISIONS.md`. Decisions 38 to 47 settle protocol section 10 and record what the pilot changed.
3. The pilot record, `protocol/PILOT_LOG.md`.
4. The code under this directory: the orchestrator, the preprocessing and training, the infrastructure, and the integration tests.
5. The six campaign plans, the gate thresholds, the image lock and the cluster add-on versions, with the hashes below.

Every change after this point goes into `protocol/DEVIATIONS.md` and is reported in the paper.

## What the pilot changed

The pilot came first. Code fixes found by running it: the F4 CPU sample over 60 s (C41), three instants rounded to the millisecond (C42), training on a one-block pilot with `--frozen-params`, a refusal in `infra/scripts/mark_complete.py`, and the identifier scan of `infra/tests/check_manifests.py`. One system setting: the Flink restart limit from 20 to 40 failures in 10 minutes. One feature rule: the series of protocol 4.1.8, chosen from how series behave over time (growth with process age, presence after a restart), never from labels, detection results or any score. The pilot tuned no gate threshold, and `plans/thresholds.json` is unchanged. The Isolation Forest dry run on pilot data was a code test and chose nothing.

## Hashes

| Item | SHA-256 |
|---|---|
| Protocol, `protocol/REBUILD_PROTOCOL.md` | `b9cddfa9d1731c7147b7461d374182d9d633ea9be188ed215a2cf45176d0eaf2` |
| Decisions, `protocol/REBUILD_DECISIONS.md` | `6f95a43b379834a26a1c51ccc9cd30a9ac56bc60081bb687788d018b2cc21bc1` |
| Pilot log, `protocol/PILOT_LOG.md` | `39e51995db62f7c538218166c3b33a085669388fc98f236e017c58beb3e5f407` |
| Contract changes, `CONTRACT_CHANGES.md` | `2e11eccfd1414872e6561d4daeee0b00a0048926e23a76f672d61716b7a10b7a` |
| Gate thresholds, `plans/thresholds.json` | `44db541541e2fd11eef7bf148a739bd1c18ee52cd5bd6d014daeb388622ead8e` |
| Image lock, `infra/images/images.lock.yaml` | `1f2b51e1ed8ec454ec6bf98d83bb1662da81d5fbbc591b522819cf24c69864e2` |
| Cluster add-on versions, `infra/terraform/cluster/addon-versions.auto.tfvars` | `9fe1daa99ac15537478f5aef00ba1ca4ef8362895194b46bdc0222fc6df3aa55` |
| Preprocessing configuration, `pipeline/config/pipeline.json` | `a0b70b7cd2f1bc49cd7a83b5d3ef13b40370c63d5e15777098391e92e9bd6510` |
| Training configuration, `pipeline/training/config.json` | `f5b37d8ccaacc48a292d3c5579fab003b266363a19aab2913240d0c7c05049e1` |

The orchestrator writes its code identity into every manifest (`orchestrator.git_commit`, `gate.git_commit`) and the hash of the image lock it ran with (`images_lock_sha256`). For the registered code they are:

| Orchestrator code identity | `sha256-tree:f75a89e0b0000e90cc7d91a4a15eb4b9614f9fb1becece547986b63204fb862c` |
|---|---|
| Image lock as mounted | `1f2b51e1ed8ec454ec6bf98d83bb1662da81d5fbbc591b522819cf24c69864e2` |

## Campaign plans

| Campaign | Workload | Index | Load generator seed | Plan SHA-256 |
|---|---|---|---|---|
| ec-a | ecommerce | 1 | 1422086788 | `339171a5ed2cea07f88a894d77d4ae3672b9b9f660fde1150061e8c0dcb27e93` |
| ec-b | ecommerce | 2 | 768628644 | `e70177cc33857b533a6563a268693de70d9ca50c53a7ec9acf17991518e8d4f7` |
| ec-c | ecommerce | 3 | 1750366755 | `ee2b251d3a2fb448e35c2ff66057156eafe34f67dc947156069eb1388e1a516d` |
| iot-a | iot | 1 | 678964778 | `98ba716f670d20229d01a59dc1e354e2fc157683f32c48fbe4dfd32192903a31` |
| iot-b | iot | 2 | 1948754625 | `fb91b782e39c5e034ca94b951657321493e354438a9a34c322315f3d54557afc` |
| iot-c | iot | 3 | 2091541298 | `c27c377e07e1abf6ba1c2242eacc3067b15c6c48b10fdc37071013700ca2b2b1` |

Each plan is drawn by `integration/plan_tool.sh` with the orchestrator image's NumPy, and the orchestrator redraws it and compares bytes at every start (protocol 1.4.4).

## Images built from this code

| Image | Tag | Digest |
|---|---|---|
| orchestrator | r2-002 | `sha256:e72cd301be53264df16a3817577d925105c95c768ced8a1b994afd9aad5d0dfe` |
| f6-injector | r2-002 | `sha256:0d3e0c441a44ead9d1524ad05fcbb42b74788b97ec700d6d1c5d67b8cbf430ba` |
| flink-jobs | r2-002 | `sha256:f5686ff117fd41ba000747bb4adb3dc523d53a29ca1d64d905f513879154d887` |
| load-generator | r2-001 | `sha256:625a94f7ba11e2109c7a85cc118f4c8df8227eb1a21559226da5a29957a8e9e9` |

Every other image is a digest-pinned mirror listed in `infra/images/images.lock.yaml`. Registry hosts are left out of this file on purpose.
