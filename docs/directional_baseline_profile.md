# Directional baseline candidate profile

## Purpose

`motor-current-low-directional-profile-v1` compares five fixed low-current rules on train and
validation rows only. It reports cycle-level confusion counts, precision, recall, false-positive
rate, and represented failure-event coverage. Test rows are excluded, and no threshold is selected
automatically.

## Run locally

```bash
.venv-wsl/bin/python -m railpulse.models.directional_baseline_profile --master "local[4]"
```

## Verified complete-source result

The command was executed against the configured official local source on 2026-09-26 and reconciled
all 11,143 development rows.

| Threshold | Period | TP | FN | FP | TN | Precision | Recall | False-positive rate |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0.9 A | Train | 0 | 4 | 1,924 | 4,468 | 0.00% | 0% | 30.10% |
|  | Validation | 0 | 5 | 10 | 4,732 | 0.00% | 0% | 0.21% |
| 1.0 A | Train | 2 | 2 | 2,793 | 3,599 | 0.07% | 50% | 43.70% |
|  | Validation | 4 | 1 | 293 | 4,449 | 1.35% | 80% | 6.18% |
| 1.2 A | Train | 2 | 2 | 3,405 | 2,987 | 0.06% | 50% | 53.27% |
|  | Validation | 4 | 1 | 484 | 4,258 | 0.82% | 80% | 10.21% |
| 1.5 A | Train | 2 | 2 | 3,805 | 2,587 | 0.05% | 50% | 59.53% |
|  | Validation | 4 | 1 | 668 | 4,074 | 0.60% | 80% | 14.09% |
| 2.0 A | Train | 4 | 0 | 4,129 | 2,263 | 0.10% | 100% | 64.60% |
|  | Validation | 5 | 0 | 929 | 3,813 | 0.54% | 100% | 19.59% |

Every rule at or above 1.0 A represents the single positive failure event in each development
period. Event representation does not resolve the large number of false-positive cycles.

## Decision

No threshold is selected. The 1.0 A candidate offers the strongest validation tradeoff in this grid,
but 293 false-positive cycles for four true-positive cycles is not operationally credible, and its
training behavior is substantially worse. Raising the threshold recovers more positives only by
further increasing false positives.

The evidence shows that motor-current mean alone cannot separate failure proximity from ordinary
low-current operation across regimes. The next feature increment should add past-only pressure,
temperature, or operating-state context before another baseline is attempted. Test data remains
sealed.
