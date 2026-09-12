# Full-source motor-current feature profile

## Purpose and execution

This read-only profile applies `motor-current-15m-v1` to the complete configured local inputs. It
rebuilds accepted Silver telemetry from the Bronze Delta table and uses the materialized
`gold.loaded_cycles` stop timestamps as prediction boundaries. It does not write a feature table or
modify any existing Delta data.

Run it from the verified Ubuntu WSL environment:

```bash
.venv-wsl/bin/python -m railpulse.features.temporal_feature_profile --master "local[4]"
```

The command prints deterministic JSON containing source lineage, feature availability, and
descriptive observation-support statistics.

## Reconciliation contract

`motor-current-15m-profile-v1` verifies that:

- accepted telemetry has one complete dataset/source/ingestion lineage;
- Gold cycles have unique, non-null identifiers and a stop-timestamp field;
- every Gold cycle receives exactly one expected feature status;
- available features have positive observation counts, complete statistics, and contributing
  timestamps strictly inside `(t - 15 minutes, t]`;
- unavailable features retain zero counts and null statistics with status-consistent boundaries;
- available counts reconcile between status and support summaries; and
- the configured and observed dataset versions agree.

## Verified complete-source result

The command was run locally on 2026-09-12.

| Input or contract | Verified value |
| --- | ---: |
| Accepted telemetry records | 1,516,948 |
| Gold loaded cycles | 15,766 |
| Feature version | `motor-current-15m-v1` |
| Window | 900 seconds |

Feature availability:

| Status | Cycle count | Interpretation |
| --- | ---: | --- |
| `available` | 15,703 | An exact accepted telemetry observation exists at prediction time. |
| `missing_prediction_boundary` | 63 | The Gold cycle has no observed stop. |
| `missing_prediction_observation` | 0 | A stop exists but its telemetry anchor is absent. |
| **Total** | **15,766** | Every Gold cycle is reconciled. |

Available-window support:

| Statistic | Observation count | First-to-last span |
| --- | ---: | ---: |
| Minimum | 3 | 19 seconds |
| 5th percentile | 75 | 891 seconds |
| Median | 91 | 892 seconds |
| 95th percentile | 91 | 893 seconds |
| Maximum | 91 | 899 seconds |

Source identity:

| Identity | Verified value |
| --- | --- |
| Dataset version | `uci-791-aab991a970e5` |
| Telemetry SHA-256 | `db30ccb4ea402e3c8bf2c99db06e288d4f2a772f6928f9dbe26a920d69793e24` |
| Telemetry ingestion batch | `12cdc6af63e42b12d14b0db7d4ea7304e4944a80a05eec519c0285c641b92171` |
| Telemetry validation | `telemetry-validation-v2` |

## Interpretation and limitations

The 15,703 available features exactly cover the cycles with observed stop boundaries; the other 63
cycles are already known to be right-censored. Zero missing prediction observations confirms that
every materialized stop timestamp has an accepted telemetry anchor under the current source and
validation contract.

The median available window contains 91 observations spanning 892 seconds. The minimum contains
only three observations spanning 19 seconds, so availability alone is not sufficient evidence of
feature completeness. These statistics describe observed support; they do not establish a minimum
training-eligibility rule. The low-support tail must be inspected before such a rule is selected.

The profile does not persist features, summarize motor-current values as health states, test
predictive usefulness, or measure failure warning performance. The source contains one compressor,
so the current unpartitioned event-time window is not yet a multi-asset implementation.
