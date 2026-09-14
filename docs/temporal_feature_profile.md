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

`motor-current-15m-profile-v5` verifies that:

- accepted telemetry has one complete dataset/source/ingestion lineage;
- Gold cycles have unique, non-null identifiers and a stop-timestamp field;
- every Gold cycle receives exactly one expected feature status;
- available features have positive observation counts, complete statistics, and contributing
  timestamps strictly inside `(t - 15 minutes, t]`;
- unavailable features retain zero counts and null statistics with status-consistent boundaries;
- available counts reconcile between status and support summaries;
- the observation-count and observed-span tails are counted independently around their observed
  fifth-percentile cutoffs, and their weakest examples retain deterministic cycle, time-span, and
  preceding-gap evidence;
- strict membership below the two cutoffs is separated into both, count-only, and span-only groups
  that reconcile with the independent strict-tail totals;
- count-only and span-only examples are limited, deterministically ordered, and reconcile with
  their group sizes; and
- the configured and observed dataset versions agree.

## Verified complete-source result

The version 5 command was run locally on 2026-09-14.

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

Low-support observation-count inspection:

| Relationship to observed 5th-percentile count | Cycles |
| --- | ---: |
| Below 75 observations | 235 |
| Exactly 75 observations | 4,582 |
| At or below 75 observations | 4,817 |

Low-support observed-span inspection:

| Relationship to observed 5th-percentile span | Cycles |
| --- | ---: |
| Below 891 seconds | 237 |
| Exactly 891 seconds | 796 |
| At or below 891 seconds | 1,033 |

Strict-tail membership overlap:

| Relationship to the two observed cutoffs | Cycles |
| --- | ---: |
| Below both 75 observations and 891 seconds | 182 |
| Below 75 observations only | 53 |
| Below 891 seconds only | 55 |
| Below either cutoff | 290 |

Deterministic one-sided examples:

| Example group | Observation count | First-to-last span | Leading unobserved time | First observation follows gap | Preceding interval |
| --- | ---: | ---: | ---: | ---: | ---: |
| Count-only, 10 of 53 | 15-49 | 892-899 seconds | 1-8 seconds | 0 of 10 | 9-10 seconds |
| Span-only, 10 of 55 | 75-84 | 733-822 seconds | 78-167 seconds | 10 of 10 | 367-88,833 seconds |

The JSON output retains the ten weakest windows for each measure in deterministic order. In this
source, the ten lowest-count and ten shortest-span examples are the same windows. Their observation
counts range from 3 to 7, first-to-last spans range from 19 to 59 seconds, and leading unobserved
time ranges from 841 to 881 seconds. All ten first contributing observations follow a recorded
material forward gap; their preceding intervals range from 2,006 to 74,617 seconds.

It also retains up to ten examples from each one-sided group. Count-only examples are ordered by
lowest observation count and then longest span. Span-only examples are ordered by shortest span and
then highest observation count. Prediction timestamp and cycle identifier provide stable final
tie-breakers.

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
training-eligibility rule.

The approximate 5th-percentile observation count is 75, but only 235 windows fall strictly below
it because another 4,582 windows tie at the cutoff. Treating all 4,817 windows at or below 75 as
exactly five percent of the population would therefore be incorrect. The ten weakest examples show
a direct relationship with long source gaps, but they do not prove that every lower-count window
has inadequate time coverage.

The approximate 5th-percentile observed span is 891 seconds. Its 237 strictly lower windows are
close in number to the 235 strictly below the count cutoff, but only 796 windows tie at the span
cutoff, producing a narrower at-or-below tail of 1,033 windows.

The strict tails share 182 cycles, while 53 are below only the count cutoff and 55 are below only
the span cutoff. Their union therefore contains 290 cycles, including 108 whose classification
depends on which support measure is used. This confirms that observation count and observed span
are related but not interchangeable. The cutoffs remain descriptive evidence, not a selected
eligibility rule.

The ten count-only examples span almost the complete 15-minute window but contain only 15-49
observations. Their first observations are preceded by nominal 9-10 second intervals rather than
material gaps. This rules out leading-edge truncation in those examples, but the current profile
does not yet distinguish internal gaps from other causes of sparse sampling.

The ten span-only examples contain 75-84 observations over 733-822 seconds. Every first observation
follows a material gap, leaving 78-167 seconds uncovered at the leading edge. Span therefore
captures a gap-related coverage loss that observation count alone misses in these examples.

The profile does not persist features, summarize motor-current values as health states, test
predictive usefulness, quantify internal gaps inside each feature window, or measure failure warning
performance. The source contains one compressor, so the current unpartitioned event-time window is
not yet a multi-asset implementation.
