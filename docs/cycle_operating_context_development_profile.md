# Development cycle operating-context profile

## Purpose

`loaded-cycle-operating-context-development-profile-v1` compares complete cycle operating context
across positive and negative two-hour horizons in the frozen training and validation periods. It
excludes the test period, does not select a threshold, and does not write or change a Gold snapshot.

Only cycles with `cycle_context_status = available` and an observed positive or negative horizon
are included. Current duration, previous duration, and previous idle time are all known at the
current cycle's prediction boundary.

## Run locally

```bash
.venv-wsl/bin/python -m railpulse.features.cycle_operating_context_development_profile --master "local[4]"
```

## Verified complete-source result

The command was executed against dataset version `uci-791-aab991a970e5` on 2026-09-28. It rebuilt
failure horizons from 1,516,948 accepted telemetry records and four accepted failure events while
keeping July test rows unavailable.

| Partition | Label | Cycles | Median current duration | Median previous duration | Median previous idle |
| --- | --- | ---: | ---: | ---: | ---: |
| Train | Positive | 4 | 178.5 s | 168.5 s | 1,352.5 s |
| Train | Negative | 6,369 | 119.0 s | 119.0 s | 1,240.0 s |
| Validation | Positive | 5 | 129.0 s | 138.0 s | 1,219.0 s |
| Validation | Negative | 4,744 | 60.0 s | 60.0 s | 13.0 s |
| **Development total** |  | **11,122** |  |  |  |

| Partition and label | Current-duration P10-P90 | Previous-duration P10-P90 | Previous-idle P10-P90 |
| --- | ---: | ---: | ---: |
| Train positive | 129.0-242.0 s | 129.0-222.0 s | 1,321.0-1,384.7 s |
| Train negative | 48.0-159.0 s | 48.0-159.0 s | 13.0-1,933.0 s |
| Validation positive | 129.0-151.0 s | 129.0-151.0 s | 1,108.2-1,266.4 s |
| Validation negative | 47.0-149.0 s | 47.0-149.0 s | 13.0-1,120.0 s |

## Decision

Current and previous loaded durations are higher for positive cycles in both development periods.
Validation positives also follow substantially longer idle periods than the median validation
negative, although the upper negative distribution overlaps and only five validation positives are
available. The train idle distributions overlap more strongly.

This evidence justifies carrying the three values and their explicit context status into a new,
versioned feature-snapshot contract. It does not justify a duration threshold, a performance claim,
or test evaluation. Snapshot version 1 must remain immutable, and the July test period remains
sealed until the expanded modelling approach is frozen.
