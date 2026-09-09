# Gold loaded-cycle inspection

## Purpose and execution

The checked-in Spark SQL query groups `gold.loaded_cycles` by visible start evidence and current
right-censoring state. For each group it reports cycle count, loaded-observation count, duration
count, and minimum, median, approximate 95th percentile, and maximum observed duration.

Run the read-only inspection against the configured local Gold table:

```bash
.venv-wsl/bin/python -m railpulse.features.cycle_inspection --master "local[2]"
```

The runner registers the path-backed table as the temporary view `gold_loaded_cycles`, executes
`sql/gold_cycle_inspection.sql`, reconciles every grouped count with the source table, and emits
deterministic `loaded-cycle-inspection-v1` JSON. The SQL remains directly inspectable and can be
adapted to a catalog-backed table after Databricks deployment is tested.

## Verified full-source groups

The command was executed successfully on 2026-09-09 against the 15,766-row local Gold snapshot:

| Visible start | Right-censored | Cycles | Loaded observations | Durations | Min | Median | P95 | Max |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Left-censored | No | 167 | 14,338 | 167 | 10 s | 218 s | 724 s | 91,907 s |
| Left-censored | Yes | 15 | 15,172 | 0 | — | — | — | — |
| Observed | No | 15,536 | 188,052 | 15,536 | 9 s | 129 s | 169 s | 42,339 s |
| Observed | Yes | 48 | 26,076 | 0 | — | — | — | — |

The groups reconcile to 15,766 cycles, 243,638 loaded observations, and 15,703 observed durations.
The 63 right-censored segments have no duration statistics by contract because no exclusive stop is
visible. Material gaps can leave a preceding segment right-censored, so this category is not limited
to the final row of the complete dataset.

## Interpretation guardrails

- `Observed` and `left-censored` describe whether the visible start boundary is known; they do not
  classify healthy and unhealthy operation.
- Right-censoring records missing stop evidence in the current snapshot; it is not a failure label.
- Percentiles are descriptive Spark `percentile_approx` results with fixed accuracy 10,000.
- The long-duration tail is retained as source behavior. No threshold, outlier removal, or causal
  failure claim is introduced by this query.
