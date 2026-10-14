# Full-source modelling-view version 2 profile

## Purpose

`motor-current-cycle-context-failure-modeling-view-profile-v2` verifies the complete population
produced when the immutable version 2 feature snapshot is joined to the existing two-hour failure
horizon. It measures the eligibility effect of requiring complete cycle context; it does not fit a
model, select a threshold, inspect model performance, or write a table.

## Run locally

```bash
.venv-wsl/bin/python -m railpulse.evaluation.modeling_view_v2_profile --master "local[4]"
```

The command rebuilds accepted Silver telemetry and failure events from local Bronze, derives the
same versioned horizons from Gold cycles, reads `gold.feature_snapshots_v2`, applies the version 2
modelling-view contract, and prints deterministic JSON with source lineage.

## Reconciliation rules

- every version 2 snapshot key must have exactly one horizon row;
- every output row must use the reviewed snapshot, context, horizon, and modelling-view versions;
- trainable rows require eligible motor-current features, available cycle context, and an observed
  positive or negative horizon;
- trainable rows have no exclusion reasons and every excluded row has at least one;
- exclusion reasons are counted independently because reasons can overlap;
- positive trainable rows must reference accepted failure events.

## Verified complete-source result

The read-only command was executed against dataset version `uci-791-aab991a970e5` on 2026-10-14.

| Measure | Version 2 result |
| --- | ---: |
| Total modelling-view rows | 15,766 |
| Trainable rows | 15,272 |
| Excluded rows | 494 |
| Trainable positive labels | 22 |
| Trainable negative labels | 15,250 |
| Accepted failure events | 4 |
| Failure events with trainable positive cycles | 3 |
| Earliest trainable prediction | 2020-02-01 00:58:58 |
| Latest trainable prediction | 2020-08-31 20:00:58 |

| Exclusion reason | Cycles |
| --- | ---: |
| Feature ineligible | 348 |
| Cycle context unavailable | 391 |
| Horizon censored | 3 |
| Inside a published failure interval | 2 |
| Missing prediction boundary | 63 |
| Unsupported horizon status | 0 |

Compared with version 1, requiring complete cycle context changes 141 previously trainable rows to
excluded, reducing the trainable population from 15,413 to 15,272. The context-unavailable count is
391 because it also includes rows already excluded for another reason. All 22 positive labels remain
trainable, and the accepted failure events contribute 0, 4, 5, and 13 positive cycles in source-row
order. The earliest trainable prediction moves forward by 34 minutes and 1 second because the first
otherwise eligible row does not have complete predecessor context.

## Interpretation limits

This result establishes population coverage and label preservation only. It does not show that the
three duration inputs improve prediction, generalize across time, or support a maintenance alert.
The next reviewable step is a training/validation-only candidate comparison using the already
selected chronological split. The July test period must remain sealed until model and threshold
choices are fixed.
