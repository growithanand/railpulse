# Full-source failure-horizon profile

## Purpose and execution

This read-only profile applies `cycle-failure-horizon-v1` to the complete configured local inputs.
It rebuilds accepted Silver telemetry and failure-event views from the Bronze Delta tables, uses the
latest accepted telemetry timestamp as `label_observation_end`, and labels the materialized
`gold.loaded_cycles` snapshot. It does not write a horizon table or modify any existing Delta data.

Run it from the verified Ubuntu WSL environment:

```bash
.venv-wsl/bin/python -m railpulse.features.failure_horizon_profile --master "local[4]"
```

The default horizon is two hours (`7,200` seconds). Another positive integer can be inspected with
`--horizon-seconds`, and the selected value is retained in the JSON output.

## Reconciliation contract

`cycle-failure-horizon-profile-v1` verifies that:

- accepted telemetry has one complete dataset/source/ingestion lineage and an observation end;
- accepted failure events have unique record IDs, unique source rows, and one complete lineage;
- telemetry and failure inputs use the same dataset version;
- every Gold cycle receives exactly one known horizon status;
- positive, negative, and null targets agree with their status and match evidence;
- positive cycle counts reconcile both by status and by accepted failure event; and
- events with no positive cycle remain visible with a zero count.

## Verified complete-source result

The command was run locally on 2026-09-09 with the default two-hour horizon.

| Input or boundary | Verified value |
| --- | ---: |
| Accepted telemetry records | 1,516,948 |
| Gold loaded cycles | 15,766 |
| Accepted failure events | 4 |
| Label observation end | `2020-09-01 03:59:50` |
| Failure events with at least one positive cycle | 3 |

Status reconciliation:

| Horizon status | Cycle count | Binary target |
| --- | ---: | --- |
| `positive` | 22 | `true` |
| `negative` | 15,676 | `false` |
| `horizon_censored` | 3 | null |
| `inside_failure_interval` | 2 | null |
| `missing_prediction_boundary` | 63 | null |
| **Total** | **15,766** | — |

Positive-cycle allocation by accepted failure event:

| Source row | Failure start | Failure end | Positive cycles |
| ---: | --- | --- | ---: |
| 1 | `2020-04-18 00:00:00` | `2020-04-18 23:59:00` | 0 |
| 2 | `2020-05-29 23:30:00` | `2020-05-30 06:00:00` | 4 |
| 3 | `2020-06-05 10:00:00` | `2020-06-07 14:30:00` | 5 |
| 4 | `2020-07-15 14:30:00` | `2020-07-15 19:00:00` | 13 |

Source identity:

| Identity | Verified value |
| --- | --- |
| Dataset version | `uci-791-aab991a970e5` |
| Telemetry SHA-256 | `db30ccb4ea402e3c8bf2c99db06e288d4f2a772f6928f9dbe26a920d69793e24` |
| Telemetry ingestion batch | `12cdc6af63e42b12d14b0db7d4ea7304e4944a80a05eec519c0285c641b92171` |
| Failure transcription SHA-256 | `3a9e02204190394c65abf65d24b1553a1764030138162cc4779abf8d8b90fce5` |
| Failure source-document SHA-256 | `b00fac0e8899854078309bef4adaa480d82ecf14dc81c5097c3646973e824127` |
| Failure ingestion batch | `fa13c29f3df64f5d29df6291b6118fcc62faf73c145e45cfb23517461b4a7712` |

## Interpretation and limitations

These are target-coverage counts, not model predictions or performance metrics. The first published
failure receives zero positive cycles because no eligible cycle stop falls in its two-hour label
window under the current contract. That does not show that the event was unpredictable or that no
warning evidence exists in row-level telemetry.

The profile reads the configured Gold snapshot but does not independently rebuild it or attach its
source lineage to each cycle row. The earlier Gold build provides separate reconciliation evidence.
The provisional loaded-cycle signal, sparse four-event reference, source ambiguities, conservative
failure-interval endpoints, and absence of post-failure recovery exclusions all remain important.
No event recall, false-alarm rate, warning lead time, health conclusion, or model quality is measured
by this profile.
