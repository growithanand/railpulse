# Gold feature-snapshot version 2 build

## Purpose

The full-source build expands the immutable `gold.feature_snapshots` rows with past-only cycle
operating context and writes the result to the separate `gold.feature_snapshots_v2` Delta table. It
does not recompute or update version 1 motor-current features.

## Command

Run after `gold.loaded_cycles` and `gold.feature_snapshots` have been materialized:

```bash
.venv-wsl/bin/python -m railpulse.features.feature_snapshot_v2_build --master "local[4]"
```

The command prints a JSON result containing source lineage, cycle-context coverage, version 1
before and after counts, and version 2 Delta write reconciliation.

## Build invariants

- The source table must contain one unique version 1 snapshot per cycle and one source lineage.
- The source dataset version must match the configured dataset version.
- Gold cycle and version 1 snapshot populations must be equal.
- Cycle context is derived from Gold cycle boundaries without loading failure labels.
- Version 2 rows must join one-to-one with version 1 rows.
- The version 1 table is compared column-for-column before and after the version 2 write.
- The version 2 writer inserts new keys and rejects changes to existing rows.

## Verified full-source result

The local build was verified on 2026-09-28 against dataset `uci-791-aab991a970e5`:

| Check | Initial build | Identical rerun |
| --- | ---: | ---: |
| Version 1 source rows | 15,766 | 15,766 |
| Version 2 source rows | 15,766 | 15,766 |
| Version 2 inserted rows | 15,766 | 0 |
| Version 2 unchanged rows | 0 | 15,766 |
| Version 2 target rows after write | 15,766 | 15,766 |
| Version 1 rows after build | 15,766 | 15,766 |

The materialized table uses snapshot version
`motor-current-15m-cycle-context-snapshot-v2`. Its cycle-context status counts reconcile to all
15,766 rows: 15,375 available, 63 missing prediction boundaries, 167 left-censored current cycles,
one missing predecessor, 160 incomplete predecessors, and no incomplete-current or invalid-interval
rows.

## Scope boundary

This build establishes a reproducible local Gold table and rerun evidence. It does not deploy to
Databricks, create managed catalog objects, fit a model, select an alert threshold, or evaluate the
held-out test period.
