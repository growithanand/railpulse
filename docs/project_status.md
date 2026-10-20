# Project status

## Current phase

**Databricks migration — managed feature-snapshot idempotency verified**

The local Bronze-to-Gold engineering path, chronological modelling view, development-only baseline
investigations, and version 2 feature snapshot are verified. The Databricks bundle and serverless
wheel runtime are also verified, and the three Unity Catalog layer schemas are provisioned. The
current increment completes the first governed file boundary for the checksum-verified inputs. The
managed Volume is deployed and independently verified, and its two landed inputs match their
manifest-backed byte sizes and SHA-256 identities. The gated two-task Bronze job is also deployed,
unscheduled, and verified through both its first successful write and an unchanged zero-insert
rerun. Unity Catalog contains separate managed telemetry and failure-reference Bronze tables with
1,516,948 and four rows respectively.
The first managed Silver increment now provides an allowlisted catalog writer for the
five existing Silver outputs, with fixed merge keys and serverless-safe count reconciliation.
The managed telemetry operation composes accepted, quarantined, and deterministic
quality-summary outputs from the same validated frame and reconciles all three writes.
The managed failure-event operation composes accepted and quarantined outputs from
one prevalidated frame and reconciles both writes.
A packaged Silver runner reads both allowlisted managed Bronze tables, builds
both validation plans before writing, and invokes the reviewed telemetry and failure operations.
A deployed, unscheduled Silver workflow gates that runner behind a successful invocation of the
existing managed Bronze workflow. The reviewed first-run contract froze the remote baseline, five
expected table counts, telemetry quality evidence, independent post-run checks, and retry procedure
before execution. An initial approved start was rejected safely by a temporary account-level
compute gate on 2026-10-15. The unchanged workflow ran successfully on 2026-10-16. Independent
read-only checks found exactly the five reviewed Silver tables and reconciled 1,516,948 accepted
telemetry rows, zero telemetry quarantine rows, one quality-summary row, four accepted failure rows,
and zero failure quarantine rows. The quality row used `telemetry-validation-v2`, reported 354
material forward gaps and no rejection reasons, and reconciled accepted plus quarantined records to
the total. Accepted telemetry and failure identifiers were non-null and unique.
The unscheduled managed Gold workflow is also deployed and has completed its first approved run
through the full Bronze-to-Silver dependency chain. The upstream unchanged merges inserted zero
rows. Gold then inserted 15,766 deterministic `loaded_cycles` rows from an empty target. Independent
SQL confirmed a managed Delta table with all nine contracted columns, non-null unique cycle IDs,
243,638 loaded observations, 15,703 observed durations, 63 right-censored cycles, the four expected
boundary/censoring groups, and zero structural-invariant violations.

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
- Validated and deployed Databricks Asset Bundle with a successful serverless Python-wheel preflight
  run.
- Tested Unity Catalog identifier and layer-mapping contract with bundle-configurable catalog and
  separate Bronze, Silver, and Gold schema targets.
- Tested idempotent Databricks schema provisioner with current-catalog protection, pre/post inventory
  reconciliation, missing-only creation, and no-op rerun behavior. Its first managed run created and
  reconciled all three target schemas; its second managed run created nothing.
- Tested Databricks source-landing contract with deterministic dataset-versioned Volume paths,
  shared identifier validation, fixed source filenames, and CLI-path conversion. The bundle now
  manages one deletion-protected Volume under the Bronze schema. Its reviewed deployment and
  independent Catalog API check succeeded. A separate controlled upload landed exactly the two
  contracted inputs, whose remote byte sizes and downloaded SHA-256 identities match the manifest.
- Tested, read-only managed source-preflight contract that reloads the committed manifest, rebuilds
  both Volume paths, fails on size or SHA-256 mismatch, and emits versioned reconciliation evidence.
  Its unscheduled bundle job is deployed. After a command-line adapter correction, the managed run
  reconciled both inputs successfully and an independent inventory confirmed that no table exists.
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
- A catalog-backed Silver adapter maps only the five existing Silver outputs to
  fully qualified Unity Catalog names. It uses `record_id` for validated record tables and
  `quality_batch_id` for telemetry metrics, supports empty contracted outputs, performs insert-only
  reconciliation, and avoids serverless-incompatible DataFrame persistence.
- A managed telemetry persistence operation prevalidates record keys, rejection reasons,
  source lineage, and reconciled quality counts before writing. It then writes accepted, quarantine,
  and quality-metric tables through the allowlisted adapter and makes interrupted multi-table work
  safe to retry through deterministic insert-only keys.
- A managed failure-event persistence operation prevalidates record keys and rejection
  reasons before writing, then reconciles accepted and quarantined source counts with the complete
  validated input. Deterministic insert-only keys make interrupted two-table work safe to retry.
- A versioned `databricks_silver_processing` wheel entry point resolves both managed Bronze tables,
  reads and validates both sources before the first write, then returns nested reconciliation
  evidence from the managed telemetry and failure persistence operations.
- A deployed, unscheduled two-task Silver workflow invokes the existing managed Bronze job, then
  runs the packaged Silver entry point only after Bronze succeeds. It permits one concurrent run,
  queues additional requests, and has completed its first successful managed run.
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
- A catalog-backed `gold.loaded_cycles` writer maps only that established Gold output to its fully
  qualified Unity Catalog table, preserves the same monotonic merge rules, reconciles logical write
  counts, and avoids serverless-incompatible DataFrame persistence.
- A versioned `databricks_gold_cycle_processing` wheel entry point reads the allowlisted managed
  Silver accepted-telemetry table, derives the established cycle snapshot, and invokes the managed
  writer without introducing a second cycle contract.
- An unscheduled two-task Gold workflow declaration invokes the existing managed Silver job, then
  runs the packaged Gold entry point only after Silver succeeds. It permits one concurrent run,
  queues additional requests, and passes strict development bundle validation. The workflow is
  deployed and its first complete run materialized and reconciled 15,766 managed loaded cycles.
- A reproducible full-source Gold command profiles and persists the same cycle snapshot, reports
  source and contract identity, and reconciles logical merge outcomes on first write and rerun.
- A checked-in Gold SQL query and read-only runner reconcile start/stop censoring groups, loaded
  observations, and duration-tail statistics without creating health or failure labels.
- A versioned two-hour cycle-to-failure horizon contract uses observed cycle stops, deterministic
  event matching, explicit in-failure exclusions, and observation-aware null labels.
- An allowlisted managed `gold.failure_horizons` persistence boundary validates the fixed two-hour
  version and target semantics, inserts unseen cycle keys without DataFrame caching, reconciles
  identical reruns, and rejects conflicting changes to existing labels.
- A versioned `databricks_gold_failure_horizon_processing` wheel entry point reads the managed
  loaded cycles and accepted Silver tables, verifies compatible single-source lineages, derives the
  accepted telemetry observation boundary, applies the existing horizon contract, and invokes the
  managed writer. A local catalog integration test proves its first write and zero-insert rerun.
- An unscheduled two-task failure-horizon workflow declaration invokes the existing Gold cycle job,
  then runs the packaged horizon entry point only after that dependency succeeds. It permits one
  concurrent run and queues additional requests. Development bundle validation, selected plan
  review, deployment, first execution, and independent table reconciliation all pass.
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
- `calendar-chronological-split-selection-v1` freezes that June-validation/July-test candidate for
  downstream work. Test rows are now unavailable for model and alert-threshold selection.
- An interpretable robust-deviation baseline now fits the median and median absolute deviation of
  15-minute mean motor current from 6,392 negative training rows. Its development-only profile
  reconciles 6,396 training and 4,747 validation rows without reporting test scores. Validation
  negatives have a median deviation score of 13.924 versus 0.576 for validation positives, showing
  that the single global-current baseline is not a defensible high-score alert rule.
- A development-only monthly feature profile localizes the drift. Negative median 15-minute mean
  current changes from 0.796 A in February to 2.948 A in April and 5.575 A in June, while June
  positives have a 0.928 A median. This supports explicit regime or directional-rule investigation
  rather than a symmetric global-deviation threshold.
- A development-only comparison now evaluates five fixed low-current thresholds. The 1.0 A rule
  captures four of five validation positives but also flags 293 negative cycles; its validation
  precision is 1.35% and its training false-positive rate is 43.70%. No threshold is selected.
- A past-only 15-minute pressure-context transformation summarizes signed and absolute differences
  between `TP3` and `Reservoirs`. Its full-source profile reconciles 15,703 available cycles and 63
  missing prediction boundaries. The median absolute window mean is 0.00154 bar and the maximum is
  0.01086 bar, confirming close tracking but not predictive usefulness by itself.
- A test-isolated development profile compares that pressure context across 6,548 training and
  4,790 validation cycles. Training positives and negatives have nearly identical median absolute
  means (0.00171 and 0.00165 bar), while validation distributions overlap and contain only five
  positive cycles. The feature is not promoted into a new snapshot version.
- A past-only cycle operating-context contract now derives complete current and previous loaded
  durations plus their intervening idle time from Gold cycle boundaries alone. Explicit statuses
  retain first-cycle, censored, incomplete-predecessor, and invalid-order cases without labels.
- Its label-independent full-source profile reconciles all 15,766 Gold cycles, with complete context
  for 15,375. There are 63 missing prediction boundaries, 167 left-censored current cycles, one
  missing predecessor, 160 incomplete predecessors, and no invalid predecessor intervals. Median
  loaded duration is 129 seconds; median preceding idle time is 843 seconds.
- A test-isolated development comparison retains 6,373 training and 4,749 validation cycles with
  complete context and observed labels. Positive median current duration is 178.5 seconds in train
  and 129 seconds in validation, versus 119 and 60 seconds for negatives. Positive previous-cycle
  duration is also higher in both periods. The context is approved for a versioned snapshot
  expansion, not for a threshold or performance claim.
- A version 2 snapshot contract now targets the separate `gold.feature_snapshots_v2` table. Its
  tested in-memory builder preserves all v1 motor-current and eligibility fields, adds the six cycle
  context fields, enforces one-to-one keys and supported versions, and excludes future labels. Its
  immutable Delta writer inserts unseen keys, accepts identical reruns, rejects changed existing
  rows, and reconciles before and after counts. The existing `gold.feature_snapshots` schema and rows
  remain unchanged.
- An allowlisted managed `gold.feature_snapshots_v2` persistence boundary now carries that same
  insert-only contract into Unity Catalog. It requires one fixed source lineage, rejects lineage
  mixing and conflicting existing rows, reconciles first writes and identical retries, and avoids
  DataFrame caching for the serverless runtime. Focused Spark tests exercise the managed and local
  writers together.
- A managed feature-snapshot v2 runner now reads only governed `silver.telemetry_accepted` and
  `gold.loaded_cycles`, constructs the approved past-only motor-current features, eligibility, and
  cycle context, and writes through the catalog boundary. It never reads failure labels. Its result
  records source lineage, fixed contract versions, eligibility counts, source populations, and
  write reconciliation. A catalog-backed first-write and identical-rerun test passes without
  DataFrame caching.
- An unscheduled `gold_feature_snapshot_v2_processing` bundle workflow now gates the feature task
  behind the existing loaded-cycle workflow, which already refreshes managed Silver. The feature
  task uses the reviewed wheel entry point, four namespace parameters, serverless environment
  version 4, queueing, and a one-run concurrency limit. Bundle validation passes. A resource-scoped
  bundle plan reported one job to create, three
  transitive upstream jobs to update, zero resources to delete, and five resources not selected.
  The planned upstream updates are Bronze, Silver, and loaded-cycle processing because the selected
  workflow carries their nested dependency chain and shared wheel artifact.
- The reviewed deployment applied that exact scope: the feature workflow was created, the three
  transitive upstream jobs were updated, the shared wheel and bundle files were uploaded, and no
  resource was deleted. A read-only workspace check immediately after deployment confirmed that
  the development workflow existed with zero recorded runs, so deployment itself did not start
  compute.
- The approved first managed feature run then completed successfully in approximately 6 minutes 32
  seconds. Its upstream refresh and feature tasks both succeeded. The feature task read 1,516,948
  accepted telemetry rows and 15,766 loaded cycles, then inserted 15,766 snapshots into an empty
  managed Delta table. Independent SQL confirmed 15,766 non-null unique keys, exact key equality
  with loaded cycles, 15,418 eligible and 348 ineligible rows, the expected feature and context
  distributions, one fixed contract and lineage, and zero violations across eight semantic,
  boundary, lineage, and label-isolation checks. The validation warehouse was stopped immediately.
- On 2026-10-20, the identical managed workflow reran successfully in approximately 6 minutes 35
  seconds. It read the same 1,516,948 accepted telemetry rows and 15,766 loaded cycles, inserted zero
  snapshots, matched all 15,766 source snapshots as unchanged, and preserved 15,766 target rows.
  Feature, eligibility, reason, context, fixed-version, threshold, and source-lineage values remained
  unchanged. No separate SQL warehouse was started for this rerun.
- The full-source version 2 build materializes 15,766 rows from the immutable version 1 table and
  Gold cycle context. Its identical rerun inserts zero rows and reports all 15,766 rows unchanged;
  a column-for-column source comparison confirms version 1 remains at 15,766 unchanged rows.
- A separate [version 2 modelling-view schema](modeling_view_v2_contract.md) now preserves the
  original five model inputs and adds current-cycle duration, previous-cycle duration, and previous
  idle time from the immutable version 2 snapshot. Context identifiers and statuses remain metadata,
  unavailable context has an explicit future exclusion reason, the selected chronological split
  remains unchanged, and failure labels remain outside the model-input list. Its tested Spark
  builder reuses the version 1 key, lineage, horizon, and prediction-boundary guards, adds explicit
  v2 snapshot and context validation, and keeps incomplete-context rows as ordered exclusions.
- Its [read-only full-source profile](modeling_view_v2_profile.md) reconciles all 15,766 rows into
  15,272 trainable and 494 excluded rows. Requiring complete cycle context excludes 141 rows that
  were trainable in version 1, while retaining all 22 positive labels and the same three represented
  failure events. This is population evidence only; no model or threshold has been selected.
- An allowlisted managed `gold.modeling_view_v2` persistence boundary now validates the fixed
  feature, eligibility, context, horizon, and modelling-view contracts; one feature lineage; one
  failure lineage; one label-observation boundary; supported target and row-status semantics;
  ordered exclusion reasons; and feature-window alignment. Its immutable merge inserts unseen cycle
  keys, reconciles identical retries as unchanged, rejects changed rows or mixed source identities,
  and avoids DataFrame caching. Focused Spark/Delta tests pass for first-write, zero-insert rerun,
  conflict, lineage, invalid-state, and prediction-boundary cases.

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

## Databricks handoff preflight verified on 2026-10-01

The official Databricks CLI 1.18.0 is installed in the local development environment. Browser-based
OAuth authentication remains machine-local. Strict validation against the development workspace
returns `Validation OK!`. The bundle then deployed one development-only serverless Python-wheel job,
and its first run terminated successfully.

The job imported RailPulse package version 0.1.0, initialized Spark 4.2.0, and reported the expected
`workspace.default` catalog context through the versioned `databricks-runtime-preflight-v1` output
contract. It did not create or modify project tables. No credentials, ignored local data, workspace
URLs, user identifiers, or deployment state were written to the repository.

The namespace-aware version 2 preflight configuration also returns `Validation OK!`. Its deployment
updated the one existing preflight job and added or deleted no resources. The managed run then
terminated successfully with package 0.1.0, Spark 4.2.0, runtime `client.4.10`, the active
`workspace.default` context, and the three planned layer schemas. A subsequent catalog API check
confirmed that only `default` and `information_schema` exist; the preflight created no schemas or
tables.

The schema-provisioning job configuration also returns `Validation OK!`. Its reviewed deployment
added one unscheduled job, updated the existing preflight job's shared wheel reference, and deleted
nothing. The first managed run on 2026-10-02 returned
`databricks-schema-provision-v1`, reported no preexisting target schemas, and made all three target
schemas available. A separate catalog API check confirmed each schema and its layer comment. No
tables or project data were created.

The second managed provisioner run also terminated successfully. It reported all three target
schemas as preexisting, returned an empty `newly_available_schemas` list, and preserved the same
three-schema inventory.

The governed source-landing contract now targets `workspace.railpulse_bronze.source` and separates
the official telemetry and reviewed failure reference below the immutable dataset version. Bundle
validation succeeded, and the reviewed deployment created that Volume while updating two existing
jobs and deleting nothing. The Catalog API confirmed a managed Volume with the contracted comment;
an independent file listing returned empty immediately after deployment. Local preflight confirmed
the telemetry CSV and failure reference match their declared identities. A separate no-overwrite
upload then landed both files, and downloaded verification copies reconciled their remote byte sizes
and SHA-256 hashes. No job was run and no table was created.

Before this handoff checkpoint, the complete repository suite passed all 134 tests, including the
Spark and Delta integration tests. Ruff lint and formatting checks, Python bytecode compilation,
and an isolated wheel build also passed.

For the source-landing increment, all 65 non-Spark tests passed while 96 Spark tests were
deliberately deselected. Ruff lint, the 145-file format check, bytecode compilation, the configured
wheel build, strict bundle validation, the remote Volume inventory, and the read-only bundle plan
also passed.

For the managed source-preflight increment, all 73 non-Spark tests passed while 96 Spark tests were
deselected. Ruff lint and formatting, bytecode compilation, the system-Python wheel build, strict
bundle validation, and both reviewed deployment plans passed. The initial managed run failed safely
before source access because named parameters were not parsed; the regression-tested correction was
deployed, and the second run reconciled both sizes and SHA-256 identities successfully. A post-run
catalog inventory remained empty and both source-file listings were unchanged.

For the managed Bronze deployment checkpoint on 2026-10-08, all 75 non-Spark tests passed, Ruff
lint and formatting checks passed, the current wheel built successfully, and strict bundle
validation returned no errors or warnings. The scoped deployment plan contained one job creation,
zero updates, and zero deletions. Post-deployment inspection confirmed an unscheduled, UI-locked
two-task workflow: `ingest_bronze_tables` depends on `verify_landed_sources` and runs only after its
success. The run history remained empty. Spark integration tests were not rerun at this checkpoint
because the local WSL Java prerequisite was unavailable; the earlier complete-suite evidence above
remains the latest full Spark verification.

The first managed write attempts then exposed two Spark Connect compatibility differences while
leaving the Bronze schema empty: serverless rejected explicit DataFrame persistence, then rejected
the long-form `errorifexists` write-mode alias. The managed adapter was changed to avoid persistence
and use the supported `error` alias, while the path-backed local adapter retained its existing disk
cache. Each repair passed the 75 non-Spark tests, lint, formatting, wheel build, strict bundle
validation, and a scoped one-job deployment plan. The subsequent job run completed both tasks and
reported 1,516,948 telemetry inserts plus four failure-reference inserts from empty targets. An
independent Unity Catalog inventory confirmed both managed Delta tables and their reconciled row
statistics.

The managed Bronze acceptance rerun on 2026-10-08 was deployed from committed source after strict
bundle validation. Its scoped plan updated one job, selected no other resources, and added or
deleted nothing. Both workflow tasks terminated successfully. Source preflight reproduced the same
two file identities; telemetry matched all 1,516,948 existing records and inserted zero, while the
failure reference matched all four existing records and inserted zero. Before and after counts were
therefore unchanged for both managed Delta tables. This closes the managed Bronze idempotency
checkpoint without making a model or maintenance-impact claim.

For the initial managed Silver persistence-boundary increment, all 77 non-Spark tests passed and
all six Silver storage Spark/Delta integration tests passed in the verified WSL runtime. Ruff lint
and formatting checks plus Python bytecode compilation also passed. The integration fixture proved
first-write and zero-insert rerun counts, empty-output table creation, schema-type preservation, and
the absence of serverless-incompatible DataFrame persistence. No bundle resource was declared or
deployed, and no remote Silver table was created.

For the managed telemetry composition increment, all 77 non-Spark tests passed and all seven Silver
storage Spark/Delta integration tests passed in WSL. Ruff lint and formatting checks plus Python
bytecode compilation also passed. The focused managed test proved reconciled first writes of two
accepted fixture rows, one quarantined row, and one quality-summary row; an identical rerun inserted
zero into all three tables and preserved the same quality-batch identity. The serverless path called
no DataFrame cache operation. No bundle resource was declared or deployed, and no remote Silver
table was created.

For the managed failure-event composition increment, all 77 non-Spark tests passed and all eight
Silver storage Spark/Delta integration tests passed in WSL. Ruff lint and formatting checks plus
Python bytecode compilation also passed. The focused managed test proved reconciled first writes of
one accepted and one quarantined failure record; an identical rerun inserted zero into both tables.
The serverless path called no DataFrame cache operation. No bundle resource was declared or
deployed, and no remote Silver table was created.

For the resource-independent Silver job-boundary increment, all 79 non-Spark tests passed. Ruff
lint and formatting checks, Python bytecode compilation, a clean wheel build, and the existing
eight-test Silver storage Spark/Delta integration suite also passed. Wheel inspection confirmed the
new runner and `databricks_silver_processing` console entry point are packaged. Focused unit evidence
confirms that both fully qualified Bronze tables are read before validation and that both validation
results are routed to the correct managed persistence operation. No bundle resource was declared or
deployed, and no remote Silver table was created.

For the unscheduled Silver workflow declaration, all 79 non-Spark tests passed together with Ruff,
formatting, and bytecode checks. A fresh wheel built successfully and contained both the runner and
its console entry point. Strict bundle validation completed without warnings or errors. The scoped
read-only plan reported one Silver job creation, one update to the referenced Bronze job, zero
deletions, zero unchanged selected resources, and four resources outside the selection. At that
declaration checkpoint, nothing had been deployed or run and no remote Silver table existed.

For the managed Silver deployment checkpoint on 2026-10-12, the final strict validation and scoped
plan reproduced the reviewed one-create, one-update, zero-delete scope. Deployment created the
unscheduled Silver workflow, updated the existing Bronze workflow to the same newly packaged wheel,
uploaded 12 bundle files, and deleted nothing. A remote read confirmed the expected two-task graph,
Bronze job reference, `ALL_SUCCESS` gate, serverless wheel environment, queueing, and one-run
concurrency limit. Its run history was empty after deployment. The workflow was not executed and no
managed Silver table was created; an independent Unity Catalog listing confirmed the Silver schema
remained empty.

For the managed Silver pre-run checkpoint on 2026-10-13, a fresh read-only inventory confirmed that
the two Bronze sources remain managed Delta tables with 1,516,948 telemetry rows and four failure
rows, the Silver schema still contains zero tables, and the deployed Silver workflow still has zero
runs. The reviewed run contract records expected first-run counts of 1,516,948 accepted telemetry
rows, zero telemetry quarantine rows, one quality-metrics row with 354 material forward gaps, four
accepted failure rows, and zero failure quarantine rows. No workflow was started and no remote
table was changed.

On 2026-10-15, the reviewed Silver workflow start was attempted from the committed bundle. The
workspace API rejected it before assigning a run ID with the message that new organization runs are
temporarily disabled. Independent post-attempt checks again found zero Silver workflow runs and an
empty `workspace.railpulse_silver` schema, so no Bronze refresh or Silver write began. Databricks
Free Edition documents this behavior as a compute-usage gate that can remain active for the rest of
the day, or longer in exceptional cases. The correct retry is the unchanged full gated workflow
after compute access resumes; bypassing the Bronze dependency or altering the reviewed job would not
address the account-level block.

On 2026-10-16, compute access resumed and the unchanged deployed Silver workflow completed its
first successful run. Independent Databricks SQL checks confirmed exactly the reviewed output
counts: 1,516,948 rows in `telemetry_accepted`, zero in `telemetry_quarantine`, one in
`telemetry_quality_metrics`, four in `failure_events_accepted`, and zero in
`failure_events_quarantine`. The quality row reported `telemetry-validation-v2`, 1,516,948 total
and accepted records, zero quarantined records, 354 material forward gaps, and an empty rejection
map. Separate identifier checks returned total, non-null, and distinct counts of 1,516,948 for
accepted telemetry and four for accepted failure events. This closes the managed Silver execution
and remote reconciliation checkpoint without making a model or maintenance-impact claim.

Later on 2026-10-16, the declared Gold workflow passed bundle validation and a selected read-only
deployment plan. The plan would create the Gold job, update the existing Bronze and Silver jobs only
to reference the newly built wheel, delete nothing, and leave four unrelated resources outside the
selection. A separate catalog inventory confirmed that `workspace.railpulse_gold` still contains no
tables. The expected 15,766-row first write, independent SQL checks, unchanged-rerun evidence, and
retry policy are now frozen in the managed Gold first-run contract. The reviewed selection was then
deployed: one Gold job was created, the Bronze and Silver wheel references were updated, and no
resource or file was deleted. Independent readback confirmed the expected two-task configuration,
no schedule, no run history, and an empty Gold schema. No workflow was executed.

The first approved Gold execution then completed successfully on 2026-10-16. Its nested Bronze and
Silver work found the sources unchanged and inserted zero upstream rows. The Gold write reported
15,766 source and inserted cycles, zero updates, zero unchanged cycles, and a target change from zero
to 15,766 rows. A separate Databricks SQL statement confirmed exactly one managed Delta table with
the nine reviewed columns; total, non-null-ID, and distinct-ID counts of 15,766; 243,638 loaded
observations; 15,703 durations; 63 right-censored cycles; all four expected start/censoring groups;
and zero counts for all seven structural-invariant violations. Spark emitted the documented global
window performance warning for this single-compressor dataset, but the Gold processing task
completed successfully. The later failure-horizon workflow refresh closed the unchanged managed
cycle rerun on 2026-10-19.

On 2026-10-17, the failure-horizon workflow passed a selected read-only deployment plan. The plan
would create one unscheduled job, update the existing Bronze, Silver, and Gold-cycle jobs only to
reference the newly built wheel, delete no resources, and leave four unrelated resources outside
the selection. Detailed structured review confirmed that no existing task structure, parameters,
concurrency setting, or schedule would change. No workspace resource was updated and no compute was
started during this checkpoint.

On 2026-10-18, the reviewed selection was deployed without execution. One failure-horizon job was
created, the Bronze, Silver, and Gold-cycle jobs received only the planned wheel-reference refresh,
12 bundle files were uploaded, and no resource or file was deleted. Independent remote readback
confirmed the new job is UI-locked and unscheduled, permits one concurrent run, queues additional
requests, invokes the existing Gold-cycle job first, and runs the packaged horizon task only after
that dependency succeeds. Its run history was empty, and the target `failure_horizons` table did not
exist after deployment. No job compute was started.

On 2026-10-19, the complete failure-horizon workflow succeeded in approximately 5 minutes 49
seconds. Bronze and Silver inserted zero rows, and the loaded-cycle refresh reported zero inserts,
zero updates, 15,766 unchanged cycles, and 15,766 target rows before and after. This closes the
previously pending managed loaded-cycle idempotency rerun. The horizon task then inserted 15,766
labels into an empty managed Delta target in approximately 45 seconds. Independent SQL confirmed
15,766 non-null unique keys; the exact 22 positive, 15,676 negative, three horizon-censored, two
inside-failure, and 63 missing-boundary counts; one fixed version, two-hour horizon, and observation
boundary; and zero violations across nine table-semantic checks plus Silver failure-reference and
interval checks. The validation warehouse was stopped immediately after the read-only audit.

## Not implemented

- The packaged managed modelling-view runner, workflow, deployment, and first write; additional
  sensor features; and later Gold transformations. The unchanged failure-horizon rerun also remains
  pending.
- Advanced SQL analytics, models, MLflow runs, alerts, dashboard, streaming, or policy simulation.
- CI workflow and production Databricks table-processing resources. Development managed Bronze,
  Silver, and the first three Gold-table workflows now exist and have independently validated
  managed runs. Later managed Gold products remain pending.

No predictive-model performance, failure-detection, warning-lead-time, false-alarm, or cost result
has been measured.

## Known constraints and next phase

The source contradictions documented in Phase 2 remain unresolved. Bronze deliberately performs no
type conversion, sensor-range validation, deduplication, timestamp normalization, or source-value
repair. Native Windows Spark is not the verified runtime because its Hadoop layer requires a
separate Windows helper; Ubuntu WSL is the tested local path. Databricks bundle deployment and the
serverless Python-wheel runtime are verified, and the first catalog-backed Bronze writes now pass.
Silver output merges are insert-only: reclassifying an existing `record_id` after validation rules
change will require an explicit versioned rebuild rather than silently moving records between tables.

## Next milestone

Commit the managed modelling-view persistence boundary and its focused tests. The next engineering
increment is a packaged runner that reads the validated managed feature snapshots and failure
horizons, builds the fixed version 2 modelling view, and writes only through this boundary. The
unchanged failure-horizon rerun also remains useful but requires compute approval. Do not commit
credentials, local paths, deployment state, or ignored source data.
