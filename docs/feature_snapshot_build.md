# Gold feature-snapshot build

## Purpose

The `motor-current-15m-snapshot-build-v1` command materializes the verified motor-current feature
and eligibility contracts into the insert-only `gold.feature_snapshots` Delta table. It is the
reproducible bridge from feature engineering to later chronological modelling and Databricks SQL.

## Data flow

1. Read configured `bronze.telemetry_raw` and `gold.loaded_cycles` Delta paths.
2. Reapply the versioned Silver telemetry validation contract in memory.
3. Derive each cycle's observed stop as its prediction boundary.
4. Compute past-only 15-minute motor-current features.
5. Reconstruct leading and internal gap context.
6. Apply label-independent feature eligibility.
7. Bind the snapshot to dataset, source-file, and ingestion-batch lineage.
8. Insert new snapshot keys and reconcile the Delta target.

Failure-horizon labels are neither read nor written by this command.

## Run locally

From Ubuntu WSL with the documented Java 21 and Spark environment:

```bash
.venv-wsl/bin/python -m railpulse.features.feature_snapshot_build --master "local[4]"
```

The command prints deterministic JSON containing the eligibility profile and Delta write counts.
An identical second run must report zero inserted rows and all source rows unchanged.

## Verified complete-source result

The command was executed twice against the configured official local source on 2026-09-19.

| Run | Accepted telemetry | Source snapshots | Inserted | Unchanged | Target before | Target after |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Initial build | 1,516,948 | 15,766 | 15,766 | 0 | 0 | 15,766 |
| Verified rerun | 1,516,948 | 15,766 | 0 | 15,766 | 15,766 | 15,766 |

The materialized rows preserve the previously verified eligibility distribution: 15,418 eligible
cycles and 348 ineligible cycles. The ineligible set contains 285 material-window-gap rows and 63
rows without an observed prediction boundary. These are feature-coverage results, not model
performance claims.

## Databricks relevance

The core function accepts an existing `SparkSession` and all storage locations come from project
configuration. A later Databricks Asset Bundle job can therefore invoke the same orchestration while
mapping the logical Gold table into Unity Catalog. The path-backed local run verifies transformation
and Delta semantics; it does not claim that workspace deployment has already been tested.

## Limitations

- The snapshot contains features and eligibility, not labels, model splits, or predictions.
- Changing a feature or eligibility contract requires a new snapshot version and explicit rebuild.
