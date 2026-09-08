# Full-source loaded-cycle profile

## Purpose and method

This read-only profile tests the provisional loaded-cycle semantics against the complete verified
MetroPT-3 telemetry artifact independently of Gold persistence. The command reads the path-backed
Bronze Delta table, rebuilds `telemetry-validation-v2` in memory, retains accepted Silver records,
derives cycle boundaries and identifiers, and aggregates them without writing a Gold table.

Run from the configured Ubuntu WSL environment:

```bash
.venv-wsl/bin/python -m railpulse.features.cycle_profile --master "local[4]"
```

The command emits deterministic JSON tied to the source and transformation contract versions. The
profile was executed successfully on 2026-09-07.

## Reproducibility identity

| Field | Verified value |
| --- | --- |
| Dataset version | `uci-791-aab991a970e5` |
| Telemetry SHA-256 | `db30ccb4ea402e3c8bf2c99db06e288d4f2a772f6928f9dbe26a920d69793e24` |
| Ingestion batch ID | `12cdc6af63e42b12d14b0db7d4ea7304e4944a80a05eec519c0285c641b92171` |
| Silver validation contract | `telemetry-validation-v2` |
| Cycle identifier contract | `loaded-cycle-v1` |
| Profile contract | `loaded-cycle-profile-v1` |

## Verified telemetry quality

| Metric | Value |
| --- | ---: |
| Total Bronze records | 1,516,948 |
| Accepted Silver records | 1,516,948 |
| Quarantined Silver records | 0 |
| Material forward gaps | 354 |

The first profiling run exposed 189 false quarantines at four verified analogue extrema. The source
stores those values with floating-point representation tails, such as motor current
`0.0199999999999995`. Silver had rounded the comparison bounds despite parsing the original tokens
as doubles. Version 2 uses the exact extrema recorded in the dataset manifest; an independent
regression fixture preserves those literal tokens.

## Loaded-cycle results

| Metric | Value |
| --- | ---: |
| Visible loaded segments | 15,766 |
| Observed starts | 15,584 |
| Left-censored starts | 182 |
| Observed stops | 15,703 |
| Right-censored segments | 63 |
| Complete observed start-and-stop cycles | 15,536 |
| Loaded telemetry observations | 243,638 |
| Segments with observed duration | 15,703 |
| Minimum observed duration | 9 seconds |
| Median observed duration | 129 seconds |
| Approximate 95th percentile duration | 178 seconds |
| Maximum observed duration | 91,907 seconds |

The classifications reconcile: observed plus left-censored starts equal the segment count, and
observed stops plus right-censored segments also equal the segment count. Every observed duration
is positive.

The duration distribution has a substantial tail that must not be silently trimmed. The longest
segment is left-censored, contains 9,272 loaded observations, and runs from the first visible record
at 2020-04-18 00:23:59 to an observed stop at 2020-04-19 01:55:46. It overlaps the first published
failure interval. Other inspected continuous segments last 42,339 and 41,634 seconds. These are
observations of the documented `DV_eletric` signal, not proof of normal cycles, sensor correctness,
or failure causation.

## Limitations and next decision

- `DV_eletric = 1` remains a provisional loaded-operation signal.
- The profile includes failure, maintenance, and ordinary operating periods; it is not a normal-only
  baseline.
- The 95th percentile uses Spark `percentile_approx` with fixed accuracy 10,000.
- Segment-ID propagation currently uses one globally ordered window because the dataset describes
  one compressor. Spark reports the resulting single-partition execution; a multi-asset pipeline
  will require an explicit equipment partition key.
- Right-censored aggregates can gain a later stop. The tested Gold merge contract now allows that
  monotonic update without changing the stable cycle identifier.
- No pressure, current, temperature, failure, or maintenance interpretation is added here.

The same reconciled transformation is now connected to Gold through the reproducible command
documented in [the Gold loaded-cycle build](gold_cycle_build.md).
