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

`motor-current-15m-profile-v10` verifies that:

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
  their group sizes;
- material forward-gap markers after the first contributing observation are counted across every
  count-only window and reconcile with that group;
- explicit leading-or-internal gap intersection is compared with the strict percentile-tail union,
  and both partitions reconcile with every available window;
- bounded examples from both disagreement groups retain deterministic support, leading-gap, and
  internal-gap evidence and reconcile with their group sizes;
- complete disagreement groups are partitioned by tail trigger and gap position, with support and
  gap-magnitude summaries that reconcile with their group sizes;
- strict-tail capture is reconciled at increasing maximum in-window gap thresholds, and the counts
  are monotonic as the threshold increases; and
- the configured and observed dataset versions agree.

## Verified complete-source result

The version 10 command was run locally on 2026-09-15.

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

Count-only internal-gap reconciliation:

| Measure | Verified value |
| --- | ---: |
| Count-only windows | 53 |
| With an internal material forward gap | 53 |
| Without an internal material forward gap | 0 |
| Internal material forward-gap markers | 54 |
| Maximum internal interval | 765 seconds |

Explicit gap intersection versus strict percentile-tail membership:

| Membership | Windows |
| --- | ---: |
| Strict tail and explicit gap | 264 |
| Strict tail only | 26 |
| Explicit gap only | 27 |
| Neither | 15,386 |
| **Available total** | **15,703** |

| Independent total | Windows |
| --- | ---: |
| Strict count-or-span tail | 290 |
| Leading-or-internal material gap | 291 |
| First observation follows a material gap | 207 |
| Contains a later internal material gap | 89 |

Leading and internal gap counts are not mutually exclusive.

Deterministic disagreement examples:

| Example group | Observation count | First-to-last span | Leading unobserved time | Leading gap | Internal gap |
| --- | ---: | ---: | ---: | ---: | ---: |
| Tail-only, 10 of 26 | 75 | 889-890 seconds | 10-11 seconds | 0 of 10 | 0 of 10 |
| Gap-only, 10 of 27 | 75-91 | 892-897 seconds | 3-8 seconds | 2 of 10 | 8 of 10 |

Tail-only examples are ordered by lowest observation count and then shortest span. Their preceding
intervals are 12-13 seconds, and none contains an internal material gap. Gap-only examples are
ordered by the largest leading or internal gap interval. The first two have leading intervals of
8,008 and 186 seconds; the remaining eight have internal intervals of 161-174 seconds.

Complete tail-only characterization:

| Strict-tail trigger | Windows |
| --- | ---: |
| Below count only | 0 |
| Below span only | 26 |
| Below both | 0 |
| **Tail-only total** | **26** |

Across all 26 tail-only windows, observation counts range from 75 to 90 and first-to-last spans
range from 889 to 890 seconds.

Complete gap-only characterization:

| Explicit-gap position | Windows |
| --- | ---: |
| Leading only | 2 |
| Internal only | 25 |
| Leading and internal | 0 |
| **Gap-only total** | **27** |

| Gap-only measure | Minimum | Median | Maximum |
| --- | ---: | ---: | ---: |
| Observation count | 75 | — | 91 |
| First-to-last span | 892 seconds | — | 899 seconds |
| Leading uncovered time, leading-gap windows | 8 seconds | 8 seconds | 8 seconds |
| Full preceding interval, leading-gap windows | 186 seconds | 186 seconds | 8,008 seconds |
| Largest internal interval per internal-gap window | 20 seconds | 104 seconds | 174 seconds |

The 25 internal-only windows contain 25 internal material-gap markers, so each contains exactly one.

Strict-tail sensitivity to maximum in-window gap magnitude:

| Minimum maximum in-window gap | Gap-intersecting windows | In strict tail | Outside strict tail | Tail capture |
| ---: | ---: | ---: | ---: | ---: |
| 1 second | 291 | 264 | 27 | 90.7% |
| 20 seconds | 285 | 260 | 25 | 91.2% |
| 60 seconds | 247 | 234 | 13 | 94.7% |
| 120 seconds | 237 | 226 | 11 | 95.4% |
| 300 seconds | 179 | 179 | 0 | 100.0% |
| 600 seconds | 101 | 101 | 0 | 100.0% |

For a leading gap, in-window magnitude is the uncovered time from the window start to the first
observation, not the complete preceding source interval. For an internal gap, it is the recorded
interval ending at the gap marker. If both positions occur, the larger in-window value is used. The
1-second row represents every explicit gap intersection; 20 seconds is the existing Silver
material-gap boundary, and the higher fixed values are diagnostic sensitivity points only.

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
material gaps. Across the complete count-only group, however, all 53 windows contain at least one
material forward-gap marker after their first contributing observation. The group contains 54 such
markers in total, with a maximum interval of 765 seconds. Excluding the first observation from this
calculation distinguishes internal interruption from leading-edge truncation.

The ten span-only examples contain 75-84 observations over 733-822 seconds. Every first observation
follows a material gap, leaving 78-167 seconds uncovered at the leading edge. Span therefore
captures a gap-related coverage loss that observation count alone misses in these examples.

Together, these results show that low count identifies gaps inside nearly full-span windows while
low span identifies missing leading coverage. Across all available windows, 264 belong to both the
strict percentile-tail union and the explicit gap-intersection group. Another 26 are tail-only and
27 are gap-only. The two totals are nearly identical at 290 and 291, but the 53 disagreements prove
that neither signal can substitute for the other by count alone.

The complete characterization confirms the tail-only sample pattern. All 26 are below the span
cutoff only, with 75-90 observations over 889-890 seconds. They therefore miss the 891-second span
boundary by only one or two seconds, never fall below the count cutoff, and contain no explicit
material gap. Small cadence variation can cross this strict population-derived boundary without a
material source interruption.

The gap-only group shows the converse. Two windows have a leading gap but still retain 91
observations over 892 seconds because only eight seconds of each source interval overlap the feature
window. The other 25 each contain one internal gap while retaining 75-91 observations over 892-899
seconds. Their largest internal intervals range from the 20-second material-gap boundary to 174
seconds, with a median of 104 seconds. Explicit gap evidence therefore describes source continuity,
while the percentile tails describe endpoint support and sample count. The full-group evidence
explains the disagreement; the sensitivity profile below tests how it changes as in-window gap
magnitude increases.

The sensitivity curve shows that strict-tail capture increases with in-window gap magnitude. It
captures 260 of the 285 windows at or above the 20-second material boundary, 226 of 237 at or above
120 seconds, and every one of the 179 windows at or above 300 seconds. This supports using count and
span as indicators of severe coverage loss, but not as complete detectors of shorter interruptions.
It also does not resolve the 26 tail-only windows caused by one- or two-second span deficits. These
results narrow the policy question; they do not establish that 300 seconds, or any other tested
value, is a valid training-eligibility threshold.

The profile does not persist features, summarize motor-current values as health states, test
predictive usefulness, select a feature-coverage rule, or measure failure warning performance. The
source contains one compressor, so the current unpartitioned event-time window is not yet a
multi-asset implementation.
