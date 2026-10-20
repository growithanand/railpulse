# First managed feature-snapshot v2 run

## Outcome

The development `gold_feature_snapshot_v2_processing` workflow completed successfully on
2026-10-19. It refreshed the existing Bronze, Silver, and loaded-cycle dependencies before building
the first managed, label-free `gold.feature_snapshots_v2` table.

The workflow took approximately 6 minutes 32 seconds. The upstream refresh task took approximately
4 minutes 52 seconds, and the feature task took approximately 1 minute 39 seconds. Both tasks
terminated successfully.

## First-write reconciliation

The feature task read 1,516,948 accepted telemetry rows and all 15,766 managed loaded cycles. Its
immutable writer reported:

| Measure | Count |
| --- | ---: |
| Source snapshots | 15,766 |
| Inserted snapshots | 15,766 |
| Unchanged snapshots | 0 |
| Target rows before | 0 |
| Target rows after | 15,766 |

Unity Catalog independently identifies the resulting table as a managed Delta table with 26
columns.

## Feature and eligibility evidence

Independent SQL reproduced the task's distributions:

| Group | State | Count |
| --- | --- | ---: |
| Motor-current feature | Available | 15,703 |
| Motor-current feature | Missing prediction boundary | 63 |
| Eligibility | Eligible | 15,418 |
| Eligibility | Ineligible | 348 |
| Ineligibility reason | Material window gap | 285 |
| Ineligibility reason | Missing prediction boundary | 63 |

The feature window is fixed at 900 seconds, and the material-gap exclusion threshold is fixed at
20 seconds.

## Cycle-context evidence

| Context state | Count |
| --- | ---: |
| Available | 15,375 |
| Missing prediction boundary | 63 |
| Left-censored current cycle | 167 |
| Missing previous cycle | 1 |
| Incomplete previous cycle | 160 |

No incomplete-current or invalid-previous-interval rows were present.

## Independent validation

The checked-in
[`managed_feature_snapshot_v2_first_run_validation.sql`](../sql/managed_feature_snapshot_v2_first_run_validation.sql)
confirmed:

- 15,766 total rows, 15,766 non-null keys, and 15,766 unique keys;
- exact population and key equality with `gold.loaded_cycles`;
- one snapshot version, feature version, eligibility version, context version, window size, gap
  threshold, and source lineage;
- zero fixed-contract, lineage, feature-semantic, eligibility-semantic, context-semantic,
  cycle-boundary, and source-lineage violations; and
- zero feature-table columns containing failure, label, or target data.

The validation SQL warehouse was stopped immediately after the audit and independently confirmed
as stopped.

## Performance note

Spark emitted a warning that the globally ordered cycle-context window has no partition key. That
ordering is intentional because predecessor context spans the single compressor timeline. The run
completed successfully and every independent semantic check passed. The warning records a future
scalability consideration rather than a correctness failure for this dataset.

## Deferred retry

The first managed write is complete and independently validated. An identical full workflow rerun
remains a separate compute-bearing action. It must report zero inserted rows and all 15,766 source
snapshots unchanged before managed feature-snapshot idempotency is closed.
