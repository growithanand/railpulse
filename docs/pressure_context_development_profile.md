# Development pressure-context profile

## Purpose

`panel-reservoir-pressure-difference-development-profile-v1` compares the past-only
`TP3 - Reservoirs` feature across positive and negative two-hour horizons in the frozen training
and validation periods. It excludes the test period, does not choose a threshold, and does not
write or change the Gold feature snapshot.

The profile uses every cycle with an available pressure-context window and an observed positive or
negative horizon. This is intentionally broader than the existing motor-current eligibility policy,
so the result evaluates the pressure signal itself rather than a different feature's coverage rule.

## Run locally

```bash
.venv-wsl/bin/python -m railpulse.features.pressure_context_development_profile --master "local[4]"
```

## Verified complete-source result

The command was executed against the configured official local source on 2026-09-27. It rebuilt
Silver inputs, pressure features, and failure horizons from 1,516,948 accepted telemetry records and
four accepted failure events.

| Partition | Label | Cycles | Median signed mean | Median absolute mean | P90 absolute mean | Median maximum point difference |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Train | Positive | 4 | -0.000374 bar | 0.001714 bar | 0.001910 bar | 0.0060 bar |
| Train | Negative | 6,544 | -0.000110 bar | 0.001648 bar | 0.003187 bar | 0.0060 bar |
| Validation | Positive | 5 | -0.000264 bar | 0.001670 bar | 0.002286 bar | 0.0060 bar |
| Validation | Negative | 4,785 | 0.000000 bar | 0.000000 bar | 0.001846 bar | 0.0000 bar |
| **Development total** |  | **11,338** |  |  |  |  |

The largest maximum point difference is 0.006 bar for both positive groups, compared with 0.182
bar for training negatives and 0.016 bar for validation negatives. The largest mean absolute
difference is 0.001956 bar for training positives, 0.010857 bar for training negatives, 0.002505
bar for validation positives, and 0.006308 bar for validation negatives.

## Decision

The feature is not promoted into a new Gold snapshot version. Training positives have almost the
same median absolute mean as training negatives, and validation-positive values overlap the upper
validation-negative distribution. The evidence is also limited to four training and five
validation positive cycles. Those results do not support a stable pressure threshold or a claim
that the feature separates approaching failures.

Pressure context remains a documented research candidate. The next feature investigation should
add past-only operating-regime context rather than expanding the immutable snapshot with an
unproven column. The test period remains sealed.
