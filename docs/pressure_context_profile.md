# Full-source pressure-context profile

## Purpose

`panel-reservoir-pressure-difference-15m-profile-v1` verifies coverage and characterizes the
past-only `TP3 − Reservoirs` feature before any snapshot schema change. It does not load labels,
fit a model, choose a threshold, or write a table.

## Run locally

```bash
.venv-wsl/bin/python -m railpulse.features.pressure_context_profile --master "local[4]"
```

## Verified complete-source result

The command was executed against the configured official local source on 2026-09-26.

| Status | Cycles |
| --- | ---: |
| Available | 15,703 |
| Missing prediction boundary | 63 |
| Missing prediction-time observation | 0 |
| **Total** | **15,766** |

Available windows have the same observation support as the existing motor-current feature: a
minimum of 3 paired observations, fifth percentile of 75, median of 91, and maximum of 91.

| Pressure-difference statistic | Result |
| --- | ---: |
| Median signed window mean | 0.0000 bar |
| P10 signed window mean | -0.00213 bar |
| P90 signed window mean | 0.00090 bar |
| Median absolute window mean | 0.00154 bar |
| P90 absolute window mean | 0.00297 bar |
| P95 absolute window mean | 0.00393 bar |
| Maximum absolute window mean | 0.01086 bar |
| Median maximum point difference | 0.0060 bar |
| Maximum point difference | 0.1820 bar |

## Interpretation

`TP3` and `Reservoirs` track each other very closely across the complete source. Coverage is strong,
but the derived difference has a narrow distribution. Narrow variation does not prove that the
feature is useless; rare or label-specific changes may still matter. It does mean that snapshot
integration should wait until a development-only label comparison establishes whether the small
differences add useful separation.

These values are observed data characteristics, not equipment tolerances or leak thresholds.

The subsequent [development-only comparison](pressure_context_development_profile.md) found
substantial positive/negative overlap and did not justify adding the feature to a new snapshot
version.
