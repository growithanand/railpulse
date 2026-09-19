# Gold feature-snapshot schema contract

## Purpose

`motor-current-15m-snapshot-v1` defines the persisted schema for the Gold `feature_snapshots`
table. Each row stores the complete motor-current feature, coverage-gap context, and
label-independent eligibility result for one loaded cycle. The schema combines columns already
defined and verified by the temporal-feature, coverage-context, and eligibility transformations into
a single materialized table for downstream chronological splitting, modeling, and evaluation.

## Table

| Property | Value |
| --- | --- |
| Logical name | `gold.feature_snapshots` |
| Primary key | `loaded_cycle_id` |
| Key identity | Deterministic SHA-256 derived from `loaded-cycle-v1` and `loaded_cycle_start_record_id` |
| Snapshot version | `motor-current-15m-snapshot-v1` |
| Format | Delta Lake |

## Primary-key invariant

The table must contain exactly one row per `loaded_cycle_id`. Duplicate keys within a snapshot
version are prohibited. A persistence writer must validate key uniqueness before writing.

`build_feature_snapshot` enforces this invariant before it returns a snapshot. It also rejects null
keys, missing contract columns, conflicting pre-existing lineage columns, unsupported component
versions, and empty lineage values. Extra upstream columns are deliberately discarded.

## Columns

Columns appear in the fixed order defined by `FEATURE_SNAPSHOT_COLUMNS` in
`src/railpulse/features/feature_snapshot_schema.py`.

### Cycle identity

| Column | Type | Source |
| --- | --- | --- |
| `loaded_cycle_id` | `string` | Gold loaded-cycle deterministic identity |

### Motor-current feature

Defined by `motor-current-15m-v1`. See the
[temporal feature contract](temporal_feature_contract.md) for semantics.

| Column | Type |
| --- | --- |
| `motor_current_15m_feature_version` | `string` |
| `motor_current_15m_window_seconds` | `long` |
| `motor_current_15m_window_start` | `timestamp_ntz` |
| `motor_current_15m_status` | `string` |
| `motor_current_15m_observation_count` | `long` |
| `motor_current_15m_first_observation_timestamp` | `timestamp_ntz` |
| `motor_current_15m_last_observation_timestamp` | `timestamp_ntz` |
| `motor_current_15m_minimum_amperes` | `double` |
| `motor_current_15m_mean_amperes` | `double` |
| `motor_current_15m_maximum_amperes` | `double` |

### Eligibility

Defined by `motor-current-15m-eligibility-v1`. See the
[feature eligibility contract](feature_eligibility_contract.md) for semantics.

| Column | Type |
| --- | --- |
| `motor_current_eligibility_version` | `string` |
| `motor_current_eligibility_minimum_excluded_gap_seconds` | `long` |
| `motor_current_maximum_window_gap_seconds` | `long` |
| `motor_current_eligibility_status` | `string` |
| `motor_current_eligibility_reasons` | `array<string>` |

### Snapshot lineage

| Column | Type | Meaning |
| --- | --- | --- |
| `feature_snapshot_version` | `string` | Fixed snapshot contract identifier |
| `dataset_version` | `string` | UCI MetroPT-3 dataset version from configuration |
| `telemetry_source_sha256` | `string` | SHA-256 of the raw telemetry source file |
| `telemetry_ingestion_batch_id` | `string` | Bronze ingestion batch that produced the accepted telemetry |

## Version invariants

Within a single snapshot version, every row must carry the same values for:

- `motor_current_15m_feature_version` (fixed to `motor-current-15m-v1`)
- `motor_current_eligibility_version` (fixed to `motor-current-15m-eligibility-v1`)
- `feature_snapshot_version` (fixed to `motor-current-15m-snapshot-v1`)

A change to any component version requires a full rebuild of the table under a new snapshot version.

## Label separation

Failure-horizon labels are not stored in `gold.feature_snapshots`. Labels describe a future outcome,
while this table represents information available at the prediction boundary. Keeping them separate
makes the same snapshot usable for training and scoring and makes label access explicit when a later
chronological modeling view is assembled. Eligibility is therefore materialized without consulting
or retaining failure labels.

## Permitted update behavior

Within a snapshot version the table is **insert-only**: new `loaded_cycle_id` values may be added
but existing rows must not be modified or deleted. Reclassifying an existing cycle after a
transformation rule change requires a versioned full rebuild, not an in-place update.

## Deferred work

The in-memory snapshot builder and its input validation are implemented. The Delta writer,
reconciliation counts, persistence tests, and the CLI command that materializes the table are
separate increments. Chronological splitting, modeling, event evaluation, and maintenance-impact
claims depend on a persisted snapshot but are not part of this transformation scope.
