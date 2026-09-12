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

Repository-local Git identity:

- Name: `Anand Mullasseril Shajahan`
- Email: `anand.mullassheri@gmail.com`

## Implemented in the working tree

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
  observation-count and observed-span percentiles without writing or selecting a coverage rule.

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

## Not implemented

- Persisted failure-horizon and temporal-feature data, feature-coverage eligibility rules,
  additional sensor features, and later Gold transformations.
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

The next small increment will inspect the low-support motor-current window tail before selecting a
minimum feature-coverage eligibility rule or persisting a table.
