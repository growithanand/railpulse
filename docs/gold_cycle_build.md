# Gold loaded-cycle build

## Purpose and execution

The Gold build turns the verified Bronze telemetry source into a path-backed
`gold.loaded_cycles` Delta table through one reproducible command. It rebuilds
`telemetry-validation-v2` in memory, retains accepted telemetry, derives `loaded-cycle-v1`
segments, reconciles the same `loaded-cycle-profile-v1` statistics used by the read-only profile,
and writes through the monotonic merge contract.

Run from the configured Ubuntu WSL environment after Bronze ingestion:

```bash
.venv-wsl/bin/python -m railpulse.features.cycle_build --master "local[4]"
```

The `loaded-cycle-build-v1` JSON result contains the source checksum and ingestion batch, validation,
cycle-ID, profile, and build contract versions, reconciled telemetry and cycle counts, and logical
insert/update/unchanged counts for the Gold merge. The generated Delta files remain under ignored
local data storage and are not committed.

## Gold table contract

Each `gold.loaded_cycles` row contains:

| Column | Meaning |
| --- | --- |
| `loaded_cycle_id` | Stable SHA-256 identity derived from the first visible loaded record. |
| `loaded_cycle_start_record_id` | Bronze/Silver record anchor for tracing the visible start. |
| `loaded_cycle_start_type` | `observed` or `left_censored`. |
| `loaded_cycle_start_timestamp` | First visible loaded timestamp. |
| `loaded_cycle_stop_record_id` | Optional inactive record providing the exclusive stop boundary. |
| `loaded_cycle_stop_timestamp` | Optional exclusive stop timestamp. |
| `loaded_observation_count` | Number of visible loaded observations in the segment. |
| `is_right_censored` | Whether the current source snapshot lacks an observed stop. |
| `observed_duration_seconds` | Visible start-to-stop duration when a stop exists. |

The start record remains a joinable row-level lineage anchor. The build result supplies the exact
dataset, source checksum, ingestion batch, and transformation-contract identity for the generated
snapshot. Merge state rules are detailed in [the cycle semantics](cycle_semantics.md).

## Verified full-source result

The command was executed on 2026-09-08 against the same source identity recorded by the read-only
profile:

| Identity | Verified value |
| --- | --- |
| Dataset version | `uci-791-aab991a970e5` |
| Telemetry SHA-256 | `db30ccb4ea402e3c8bf2c99db06e288d4f2a772f6928f9dbe26a920d69793e24` |
| Ingestion batch ID | `12cdc6af63e42b12d14b0db7d4ea7304e4944a80a05eec519c0285c641b92171` |
| Validation contract | `telemetry-validation-v2` |
| Cycle-ID contract | `loaded-cycle-v1` |
| Profile contract | `loaded-cycle-profile-v1` |
| Build contract | `loaded-cycle-build-v1` |

First-write and rerun reconciliation:

| Run | Source cycles | Inserted | Updated | Unchanged | Target before | Target after |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Initial build | 15,766 | 15,766 | 0 | 0 | 0 | 15,766 |
| Verified rerun | 15,766 | 0 | 0 | 15,766 | 15,766 | 15,766 |

Both runs reproduced the profile totals: 1,516,948 accepted telemetry records, 15,766 loaded
segments, 15,536 complete observed-start-and-stop cycles, 182 left-censored starts, and 63
right-censored segments. No row was quarantined under the current validation contract.

The materialized table can be examined without another Bronze-to-Gold rebuild using the checked-in
query and runner documented in [the Gold loaded-cycle inspection](gold_cycle_inspection.md).

## Managed catalog write boundary

The Gold persistence layer also provides an allowlisted managed-table writer for
`<catalog>.<gold_schema>.loaded_cycles`. It applies the same stable cycle identity and monotonic
merge rules as the local writer: new cycles are inserted, right-censored cycles may gain
observations or close, and settled cycles cannot be rewritten or reopened. The result reconciles
inserted, updated, unchanged, and before/after target counts without caching DataFrames, keeping the
write boundary compatible with Databricks serverless compute.

This boundary is integration-tested against a local catalog. The packaged
`databricks_gold_cycle_processing` runner reads the allowlisted managed Silver accepted-telemetry
table, derives the versioned loaded cycles, and invokes the writer. Deployment review and the first
managed Databricks execution were kept as separate reviewed increments.

The checked-in Asset Bundle now declares an unscheduled `gold_cycle_processing` workflow. Its first
task invokes the existing managed Silver workflow; its Gold wheel task can run only after that
upstream job succeeds. The workflow permits one active run, queues additional requests, uses the
same four catalog/schema parameters as the preceding layers, and invokes
`databricks_gold_cycle_processing` on serverless environment version 4. Bundle validation against
the configured development workspace passed on 2026-10-16. The reviewed resource was deployed that
day and its first approved execution completed successfully through the full Bronze-to-Silver gate.
It inserted and reconciled 15,766 rows in the managed `loaded_cycles` table. Independent SQL
confirmed non-null unique cycle IDs, the expected boundary/censoring groups, and zero structural
invariant violations. The unchanged managed rerun remains pending. The reviewed expectations,
observed evidence, independent checks, and retry policy are preserved in
[the first managed Gold run record](managed_gold_first_run.md).

## Limitations

- The build currently recomputes the complete validated snapshot from Bronze; it is not a streaming
  or bounded incremental transformation.
- Segment-ID propagation uses a global ordered window for this single-compressor source. A
  multi-asset build requires an equipment key and partitioned ordering.
- The local full-source table is path-backed Delta storage; the managed Databricks table now proves
  the equivalent catalog-backed first write. The separate unchanged managed rerun is still pending.
- Loaded-cycle semantics remain provisional operating evidence, not failure labels or proof of
  compressor health.
