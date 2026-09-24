# Project status

## Current phase

**Phase 6 — Temporal features and failure horizons (underway)**

Bronze ingestion is implemented and verified against both official inputs. Silver now has typed
telemetry, parsing and digital-domain validation, binary digital normalization, and duplicate source
index/event-time detection. It now derives adjacent event-time intervals, flags material forward
gaps, quarantines out-of-order records, and produces reconciled in-memory quality counts. Published
failure events now have a separate typed structural-quality split. Accepted and quarantined
telemetry and failure events can now be persisted to separate path-backed Silver Delta tables with
record-level rerun idempotency. The versioned four-event failure transcription has been reconciled
through the Bronze reader and Silver validator. Analogue values outside the verified complete-file
dataset envelope are quarantined as possible contract drift without being treated as equipment
health limits. Telemetry quality summaries now have stable source-and-validation identities and an
idempotent Delta output. Phase 5 now has provisional loaded-cycle boundary rules based on continuous
`DV_eletric` transitions, with explicit left-censoring at data starts and material gaps. Loaded rows
and observed stop boundaries now receive deterministic segment identifiers and retain whether the
visible segment start was observed or left-censored. Identified segments can now be reduced to one
cycle-level row with visible boundary evidence, loaded-observation counts, honest duration
semantics, and explicit right-censoring. The read-only full-source profile now reconciles all
1,516,948 Bronze records through Silver validation and reports 15,766 provisional loaded segments.
A tested Gold Delta merge boundary inserts new cycle IDs, monotonically extends or closes existing
right-censored rows, and rejects attempts to change stable start evidence or reopen closed cycles.
The source-bound `loaded-cycle-build-v1` command now connects that boundary to the complete local
Bronze source. Its first run inserted 15,766 reconciled rows into `gold.loaded_cycles`; a second run
inserted or updated none and reported all 15,766 source cycles unchanged.
A tested, read-only Spark SQL inspection now reconciles all four visible-start/right-censoring
groups and their descriptive duration tails against the materialized Gold table.
Phase 6 now has a pure, versioned cycle-to-failure horizon transformation. It uses observed cycle
stops as prediction boundaries, matches only strictly future failure starts, excludes timestamps
inside published failure intervals, and preserves incomplete outcomes as null rather than negative.
A source-lineage-aware, read-only full-source profile now applies that contract to all 15,766 Gold
cycles. It reconciles 22 positive cycles across three accepted failure events, 15,676 negative
cycles, and 68 rows with explicit censoring or in-failure statuses.
The first temporal feature contract now computes minimum, mean, and maximum motor current over the
strict trailing 15 minutes at each prediction timestamp. It records observation support and has a
regression test proving that appended future telemetry cannot change existing feature values.
A source-lineage-aware, read-only full-source profile now applies that contract to all 15,766 Gold
cycles. It reconciles 15,703 available features, the 63 cycles without observed stops, and zero
missing telemetry anchors. Available windows have a median of 91 observations, while the minimum of
3 shows that availability alone is not yet a sufficient training-eligibility rule.
The version 10 profile now inspects count and observed-span tails independently, retains deterministic
one-sided examples, measures internal material gaps, compares the strict percentile-tail union with
explicit gap intersection, and characterizes both disagreement groups. All 26 tail-only windows are
below the span cutoff only. The 27 gap-only windows split into 2 leading-only and 25 internal-only
gaps. A fixed-threshold sensitivity table shows strict-tail capture increasing from 264 of 291
windows with any explicit gap overlap to all 179 windows with at least 300 seconds of in-window gap
time. These summaries show that marginal endpoint-support cutoffs and material source
discontinuities describe different properties.
A separate version 1 policy comparison now joins every available window to the default two-hour
horizon output. Five fixed candidates retain 15,388-15,703 windows. Every candidate retains all 22
positive and all 5 null-label windows, while its exclusions are entirely negative. This is
diagnostic coverage evidence only: no feature-eligibility rule has been selected from full-source
labels. A separate label-independent version 1 contract now uses the existing 20-second telemetry
material-gap boundary as its only coverage cutoff. It retains percentile tails as diagnostics,
fails closed when feature or coverage context is unavailable, and does not inspect horizon labels.
A source-lineage-aware, read-only full-source profile now applies that contract to every Gold cycle.
It reconciles 15,418 eligible cycles and 348 ineligible cycles: 285 for material in-window gaps and
63 for missing prediction boundaries. No missing prediction anchors, missing/invalid coverage
contexts, or unsupported feature statuses occur.

## Environment observed on 2026-09-01

| Tool | Observed state |
| --- | --- |
| Python | 3.13.5 via Anaconda |
| pytest | 8.3.4 |
| Java | 23.0.2 |
| Git | 2.49.0.windows.1 |
| Spark / PySpark | Not installed |
| Databricks CLI | Not installed |
| Ruff | Not initially installed |
| Python build frontend | Not initially installed |

## Phase 3 runtime verified on 2026-09-02

| Component | Verified state |
| --- | --- |
| Ubuntu WSL | Ubuntu 24.04.1 LTS |
| Python | 3.12.3 in ignored `.venv-wsl/` |
| Java | Eclipse Temurin 21.0.12.1 LTS in ignored `.tools/` |
| Apache Spark / PySpark | 4.2.0 |
| Delta Lake | 4.4.0 |
| Local Spark master | `local[4]` for full ingestion; bounded 3 GiB driver heap |

## Implemented capabilities

- `src`-layout Python package boundaries.
- Typed TOML configuration loading with repository-relative path resolution.
- Deterministic package and configuration tests.
- Dataset, Delta, Spark, streaming, MLflow, model, secret, and local-state exclusions.
- Planning, decisions, status, and data-placement documentation.
- Minimal Databricks Asset Bundle entry point; not CLI-validated or deployed.
- SHA-256 manifest for the official UCI archive and both members.
- Complete-file CSV inspection covering 1,516,948 rows and 15 sensors.
- Source/next-Bronze contracts, sensor dictionary, and separate four-row failure reference.
- Reusable standard-library inspector and deterministic metadata/contract tests.
- Separate explicit all-string schemas for telemetry and failure-report Bronze records.
- Source/file/document checksums, modification/ingestion times, dataset version, and deterministic
  record/batch identifiers.
- Path-backed Delta tables with merge-based rerun idempotency and input/target reconciliation.
- Deterministic Spark integration fixtures for raw-token preservation, corrupt-row retention,
  duplicate-key rejection, and zero-insert reruns.
- A non-writing Silver telemetry projection for source-index, timezone-free timestamp, and sensor
  parsing that keeps invalid raw tokens available for later rejection reasons.
- Deterministic parsing rejection reasons and a lazy accepted/quarantine split that reconciles every
  input record without applying later domain or engineering rules.
- Digital-domain validation for all eight binary sensors; accepted values are normalized to byte
  integers while invalid raw values and explicit reasons remain available for quarantine.
- Dataset-envelope validation for all seven analogue sensors, with inclusive observed bounds and
  deterministic per-sensor reasons while preserving raw and parsed outlier values. Version 2 uses
  exact manifest extrema so floating-point representation tails in the verified source remain valid.
- Separate `silver.telemetry_accepted` and `silver.telemetry_quarantine` path-backed Delta tables,
  with insert-only `record_id` merges, duplicate-key rejection, and per-output count reconciliation.
- Separate `silver.failure_events_accepted` and `silver.failure_events_quarantine` tables using the
  same pre-write key checks, idempotent merge boundary, and count reconciliation.
- Window-based duplicate detection marks every row sharing a non-null source index or event
  timestamp while leaving null parsing results to their existing parsing reasons.
- Source-index-ordered sequence metadata distinguishes nominal/jittered intervals, material forward
  gaps, and out-of-order timestamps without quarantining valid measurements after forward gaps.
- A deterministic telemetry quality summary reconciles total, accepted, quarantined, and forward-gap
  counts and reports each rejection reason independently.
- A `silver.telemetry_quality_metrics` Delta table persists that summary under a deterministic
  source-batch and validation-contract identity, preserving earlier contract versions on rerun.
- Failure-event validation types source rows and timezone-free interval bounds, preserves report
  labels and ambiguous text, and rejects malformed, incomplete, duplicated, or reversed records.
- Causal loaded-cycle boundary annotations use only current and predecessor `DV_eletric` states,
  avoid transitions across forward gaps, and distinguish observed starts from left-censored segments.
- Stable loaded-cycle identifiers are anchored to the first visible active record, propagate only
  forward, and carry segment start provenance onto loaded rows and their exclusive stop boundary.
- Cycle-level aggregation retains observed and censored boundaries, counts loaded observations,
  and calculates visible duration only when an exclusive stop timestamp is available.
- A reproducible, lineage-bound full-source profile reconciles cycle start/stop classifications and
  records censoring and duration distributions without writing a Gold table.
- A path-backed `gold.loaded_cycles` merge contract inserts new cycles, updates only open cycle
  state, preserves stable identifiers and start evidence, and rejects state regression before write.
- A reproducible full-source Gold command profiles and persists the same cycle snapshot, reports
  source and contract identity, and reconciles logical merge outcomes on first write and rerun.
- A checked-in Gold SQL query and read-only runner reconcile start/stop censoring groups, loaded
  observations, and duration-tail statistics without creating health or failure labels.
- A versioned two-hour cycle-to-failure horizon contract uses observed cycle stops, deterministic
  event matching, explicit in-failure exclusions, and observation-aware null labels.
- A read-only full-source horizon profile rebuilds accepted Silver views from Bronze, derives the
  label observation end, and reconciles every cycle status and failure-event match count.
- A past-only 15-minute motor-current window uses event-time range semantics, exposes observed
  support, and rejects future-row leakage at the tested prediction boundary.
- A read-only full-source motor-current profile reconciles feature statuses and reports
  observation-count and observed-span percentiles, independently counted cutoff tails, strict-tail
  membership overlap, deterministic weakest and one-sided examples, and count-only internal-gap
  evidence. It compares explicit leading-or-internal gap intersection with the strict percentile-
  tail union and retains bounded examples plus complete trigger, position, and magnitude summaries
  for both disagreement groups. It also profiles strict-tail capture at increasing in-window gap
  magnitudes without writing or selecting a coverage rule.
- A source-lineage-aware, read-only coverage-policy profile joins available motor-current windows
  to two-hour horizon labels and reconciles retained and excluded positive, negative, and null-label
  counts across five fixed diagnostic candidates without selecting a rule.
- A versioned motor-current eligibility transformation applies the existing 20-second material-gap
  boundary independently of horizon labels and preserves explicit reasons for every ineligible row.
- A read-only full-source eligibility profile validates feature and coverage-context keys, exact
  status/reason semantics, source lineage, and one reconciled eligibility result per Gold cycle.
- A Gold feature-snapshot schema contract defines the primary key, column order, version invariants,
  lineage columns, and insert-only update semantics for the persisted feature table.
- A deterministic feature-snapshot builder now validates unique non-null cycle keys, fixed component
  versions, required columns, and nonempty source lineage before projecting the exact Gold contract.
  Future failure labels remain outside the snapshot so training and scoring share one feature table.
- An insert-only Delta persistence boundary now inserts new feature-snapshot keys, treats identical
  reruns as unchanged, rejects conflicting immutable rows or incompatible targets, and reconciles
  source, inserted, unchanged, and target counts.
- A reproducible Gold command now connects Bronze telemetry validation, Gold cycle boundaries,
  past-only features, gap-based eligibility, snapshot construction, and Delta persistence. Its
  focused fixture test verifies first-write and zero-insert rerun behavior. The official-data run
  inserted 15,766 reconciled snapshots; its verified rerun inserted none and left all 15,766 rows
  unchanged.
- A versioned chronological modelling-view contract now defines the feature, target, time, lineage,
  and row-status columns. It permits training only for feature-eligible positive or negative
  horizons and retains censored, in-failure, and missing-boundary rows with explicit exclusions.
- A deterministic modelling-view transformation now enforces complete one-to-one cycle joins,
  feature-window and prediction-boundary alignment, supported component versions, label semantics,
  and failure-source lineage before assigning ordered exclusion reasons.
- A read-only full-source modelling-view profile reconciles all 15,766 cycles into 15,413 trainable
  and 353 excluded rows. The trainable set contains all 22 positive cycle labels, 15,391 negatives,
  and positive coverage for three of four accepted failure events across 2020-02-01 to 2020-08-31.
- Three fixed, versioned calendar split candidates assign trainable rows from prediction time alone.
  A read-only full-source comparison reconciles all 15,413 trainable rows for every candidate and
  reports period-level rows, labels, prediction-time spans, and represented failure events. Only
  `validation-2020-06_test-2020-07` has positive-event representation in train, validation, and test.

Official local Bronze evidence:

| Logical table | Source rows | Target rows | Verified rerun inserts |
| --- | ---: | ---: | ---: |
| `bronze.telemetry_raw` | 1,516,948 | 1,516,948 | 0 |
| `bronze.failure_reports_raw` | 4 | 4 | 0 |

Versioned failure-reference Silver evidence:

| Input records | Accepted records | Quarantined records | Preserved duplicate report labels |
| ---: | ---: | ---: | ---: |
| 4 | 4 | 0 | 2 (`#1`) |

The integration check also verifies every parsed interval boundary, the transcription and source
document identities, and the unresolved maintenance-date note on source row 2.

Full-source cycle profile evidence:

| Accepted telemetry | Loaded segments | Complete cycles | Left-censored | Right-censored | Median observed duration |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1,516,948 | 15,766 | 15,536 | 182 | 63 | 129 seconds |

The maximum observed duration is 91,907 seconds and overlaps a published failure interval. It is
retained as source behavior requiring later analysis, not removed as an assumed anomaly. See
`docs/cycle_profile.md` for source identities, reconciled counts, tail inspection, and limitations.

Full-source Gold write evidence:

| Run | Source cycles | Inserted | Updated | Unchanged | Target rows |
| --- | ---: | ---: | ---: | ---: | ---: |
| Initial build | 15,766 | 15,766 | 0 | 0 | 15,766 |
| Verified rerun | 15,766 | 0 | 0 | 15,766 | 15,766 |

Verified Gold inspection evidence:

| Visible start | Right-censored | Cycles | Loaded observations | Median duration | Maximum duration |
| --- | --- | ---: | ---: | ---: | ---: |
| Left-censored | No | 167 | 14,338 | 218 seconds | 91,907 seconds |
| Left-censored | Yes | 15 | 15,172 | — | — |
| Observed | No | 15,536 | 188,052 | 129 seconds | 42,339 seconds |
| Observed | Yes | 48 | 26,076 | — | — |

Verified full-source two-hour horizon evidence:

| Positive | Negative | Horizon-censored | Inside failure | Missing cycle stop | Matched failures |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 22 | 15,676 | 3 | 2 | 63 | 3 of 4 |

The label observation end is `2020-09-01 03:59:50`. Positive-cycle counts by published source row
are 0, 4, 5, and 13. These are label-coverage results, not predictions or event-recall measurements.

Verified full-source 15-minute motor-current feature evidence:

| Available | Missing prediction boundary | Missing telemetry anchor | Total cycles |
| ---: | ---: | ---: | ---: |
| 15,703 | 63 | 0 | 15,766 |

| Support statistic | Observation count | First-to-last span |
| --- | ---: | ---: |
| Minimum | 3 | 19 seconds |
| 5th percentile | 75 | 891 seconds |
| Median | 91 | 892 seconds |
| 95th percentile | 91 | 893 seconds |
| Maximum | 91 | 899 seconds |

The observed 5th-percentile count is 75 observations. There are 235 windows strictly below that
value, 4,582 exactly equal to it, and 4,817 at or below it. All ten weakest examples begin after a
material forward gap, but the large tie means count alone is not a defensible eligibility rule.

The observed 5th-percentile span is 891 seconds. There are 237 windows strictly below that value,
796 exactly equal to it, and 1,033 at or below it. The ten shortest-span examples are the same ten
windows as the lowest-count examples.

| Strict-tail relationship | Cycles |
| --- | ---: |
| Below both count and span cutoffs | 182 |
| Below the count cutoff only | 53 |
| Below the span cutoff only | 55 |
| Below either cutoff | 290 |

The 108 one-sided memberships show that count and time coverage are related but not interchangeable.

| Deterministic examples | Observation count | First-to-last span | Leading unobserved time | First observation follows gap |
| --- | ---: | ---: | ---: | ---: |
| Count-only, 10 of 53 | 15-49 | 892-899 seconds | 1-8 seconds | 0 of 10 |
| Span-only, 10 of 55 | 75-84 | 733-822 seconds | 78-167 seconds | 10 of 10 |

All 53 count-only windows contain at least one material forward gap after their first contributing
observation. They contain 54 internal gap markers in total, with a maximum interval of 765 seconds.
The span-only examples directly show leading-edge loss after source gaps. These results establish
complementary gap positions without selecting an eligibility rule.

| Gap-intersection versus strict-tail membership | Windows |
| --- | ---: |
| In both groups | 264 |
| Strict tail only | 26 |
| Explicit gap only | 27 |
| In neither group | 15,386 |
| **Available total** | **15,703** |

The strict tail contains 290 windows and the explicit leading-or-internal gap group contains 291.
There are 207 windows whose first observation follows a material gap and 89 with a later internal
gap; these position counts are not mutually exclusive. All 53 membership disagreements now have
complete trigger, position, support-range, and gap-magnitude summaries.

| Disagreement examples | Observation count | First-to-last span | Leading unobserved time | Gap evidence |
| --- | ---: | ---: | ---: | --- |
| Tail-only, 10 of 26 | 75 | 889-890 seconds | 10-11 seconds | No leading or internal material gap |
| Gap-only, 10 of 27 | 75-91 | 892-897 seconds | 3-8 seconds | 2 leading; 8 internal |

The selected tail-only windows miss the 891-second span cutoff by only one or two seconds and have
nominal 12-13 second preceding intervals. The strongest gap-only examples include two leading gaps
whose intervals are 8,008 and 186 seconds and eight internal gaps of 161-174 seconds, even though
their counts and endpoint spans do not fall below either strict cutoff.

| Complete tail-only trigger | Windows |
| --- | ---: |
| Below count only | 0 |
| Below span only | 26 |
| Below both | 0 |

All 26 tail-only windows have 75-90 observations over 889-890 seconds. They miss only the
891-second span cutoff and contain no explicit material gap.

| Complete gap-only position | Windows |
| --- | ---: |
| Leading only | 2 |
| Internal only | 25 |
| Leading and internal | 0 |

The two leading-only windows each lose eight seconds at the feature-window boundary; their complete
preceding intervals are 186 and 8,008 seconds. The 25 internal-only windows contain one marker each,
with largest intervals ranging from 20 to 174 seconds and a median of 104 seconds. Their support
remains at 75-91 observations over 892-899 seconds.

| Minimum maximum in-window gap | Gap windows | In strict tail | Outside strict tail |
| ---: | ---: | ---: | ---: |
| 1 second | 291 | 264 | 27 |
| 20 seconds | 285 | 260 | 25 |
| 60 seconds | 247 | 234 | 13 |
| 120 seconds | 237 | 226 | 11 |
| 300 seconds | 179 | 179 | 0 |
| 600 seconds | 101 | 101 | 0 |

Leading gaps use only their uncovered time inside the feature window; internal gaps use their
recorded interval. The strict tails capture every tested gap window at or above 300 seconds, but
still miss 25 at the existing 20-second material boundary. This is diagnostic evidence, not an
eligibility threshold.

Verified coverage-policy comparison:

| Policy | Retained | Excluded | Retained positive | Retained negative | Retained null |
| --- | ---: | ---: | ---: | ---: | ---: |
| Available baseline | 15,703 | 0 | 22 | 15,676 | 5 |
| Exclude strict tail | 15,413 | 290 | 22 | 15,386 | 5 |
| Exclude material gaps at 20 seconds | 15,418 | 285 | 22 | 15,391 | 5 |
| Exclude strict tail or material gaps | 15,388 | 315 | 22 | 15,361 | 5 |
| Exclude strict tail or gaps at 120 seconds | 15,402 | 301 | 22 | 15,375 | 5 |

All excluded windows are currently negative, but this four-event, complete-source comparison is not
used to choose a rule or claim predictive value. See `docs/coverage_policy_profile.md` for policy
definitions, reconciliation boundaries, lineage, and limitations.

Verified label-independent eligibility result:

| Status or reason | Cycles |
| --- | ---: |
| Eligible | 15,418 |
| Ineligible: material window gap | 285 |
| Ineligible: missing prediction boundary | 63 |
| Ineligible: all other reasons | 0 |
| **Total** | **15,766** |

Every cycle has one eligibility status, and every ineligible cycle has exactly one reconciled
reason. The profile does not load failure labels or write a feature table. See
`docs/eligibility_profile.md` for execution, lineage, and limitations.

## Not implemented

- Persisted failure-horizon, temporal-feature, and eligibility data; additional sensor features;
  and later Gold transformations.
- Advanced SQL analytics, models, MLflow runs, alerts, dashboard, streaming, or policy simulation.
- CI workflow and Databricks deployment resources.

No data-quality, model-performance, failure-detection, warning-lead-time, false-alarm, or cost result
has been measured.

## Known constraints and next phase

The source contradictions documented in Phase 2 remain unresolved. Bronze deliberately performs no
type conversion, sensor-range validation, deduplication, timestamp normalization, or source-value
repair. Native Windows Spark is not the verified runtime because its Hadoop layer requires a
separate Windows helper; Ubuntu WSL is the tested local path. Databricks remains untested. Silver
output merges are insert-only: reclassifying an existing `record_id` after validation rules change
will require an explicit versioned rebuild rather than silently moving records between tables.

## Next milestone

Select and freeze the reviewed chronological split, then build an interpretable training-only
engineering baseline without consulting the held-out test period.
