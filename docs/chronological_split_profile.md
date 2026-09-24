# Full-source chronological split profile

## Purpose

`calendar-chronological-split-profile-v1` compares the three declared calendar candidates over the
same trainable modelling-view population. It assigns rows using prediction timestamps only and does
not fit a model, tune a threshold, select a candidate, or write a table.

## Run locally

```bash
.venv-wsl/bin/python -m railpulse.evaluation.chronological_split_profile --master "local[4]"
```

The command rebuilds the accepted Silver views and two-hour failure horizons, joins them to the
materialized Gold feature snapshots, applies the modelling-view contract, and prints deterministic
JSON with source lineage.

## Verified complete-source result

The command was executed against the configured official local source on 2026-09-24. Every
candidate reconciled all 15,413 trainable rows and all 22 positive labels.

| Candidate | Period | Rows | Positive | Negative | Represented failure events |
| --- | --- | ---: | ---: | ---: | ---: |
| Validation May, test July | Train | 5,004 | 0 | 5,004 | 0 |
|  | Validation | 6,139 | 9 | 6,130 | 2 |
|  | Test | 4,270 | 13 | 4,257 | 1 |
| Validation June, test July | Train | 6,396 | 4 | 6,392 | 1 |
|  | Validation | 4,747 | 5 | 4,742 | 1 |
|  | Test | 4,270 | 13 | 4,257 | 1 |
| Validation June, test August | Train | 6,396 | 4 | 6,392 | 1 |
|  | Validation | 7,070 | 18 | 7,052 | 2 |
|  | Test | 1,947 | 0 | 1,947 | 0 |

The May-validation candidate has no positive example or represented failure event in training, so
it cannot support a supervised engineering baseline. The August-test candidate has no positive
example or represented event in test, so it cannot measure held-out event detection. The June/July
candidate is the only declared option with one represented failure event in each period.

## Interpretation limits

There are only three represented failure events across the entire trainable view, one in each
period under the June/July candidate. Cycle-level positive counts are not independent failure
events, and the source does not support precise uncertainty or broad generalization claims. The
held-out test period must remain unavailable while the baseline, anomaly model, and alert threshold
are developed. Candidate selection is a separate, versioned decision.
