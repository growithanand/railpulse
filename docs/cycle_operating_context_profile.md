# Full-source cycle operating-context profile

## Purpose

`loaded-cycle-operating-context-profile-v1` verifies the coverage and observed distributions of the
past-only cycle operating-context contract. It reads `gold.loaded_cycles`, does not load failure
labels, and does not write a feature snapshot or select a model rule.

## Run locally

```bash
.venv-wsl/bin/python -m railpulse.features.cycle_operating_context_profile --master "local[4]"
```

## Verified complete-source result

The command was executed against dataset version `uci-791-aab991a970e5` on 2026-09-27 and
reconciled all 15,766 Gold cycles.

| Status | Cycles |
| --- | ---: |
| Available | 15,375 |
| Missing prediction boundary | 63 |
| Left-censored current cycle | 167 |
| Incomplete current cycle | 0 |
| Missing previous cycle | 1 |
| Incomplete previous cycle | 160 |
| Invalid previous interval | 0 |
| **Total** | **15,766** |

Complete context is available for 97.5% of Gold cycles. The explicit non-available statuses preserve
boundary limitations rather than filling durations or assuming continuity.

| Component | Non-null cycles | Minimum | P05 | Median | P95 | Maximum |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Current loaded duration | 15,536 | 9 s | 47 s | 129 s | 169 s | 42,339 s |
| Previous loaded duration | 15,535 | 9 s | 47 s | 129 s | 169 s | 42,339 s |
| Previous idle time | 15,517 | 9 s | 12 s | 843 s | 1,904 s | 34,204 s |

Component counts exceed the fully available status where an individual value is still valid even
though another part of the context is incomplete. For example, an observed previous stop can
support an idle interval even when that previous cycle's start was left-censored.

## Interpretation

Current and previous loaded durations have matching central distributions and long upper tails.
Idle time has broader operational variation: its median is about 14 minutes and its 95th percentile
is about 32 minutes. The strong coverage and variation justify a development-only comparison by
chronological period and failure-horizon label.

This result does not establish predictive value. The test period remains sealed, and no operating
context is added to the immutable Gold feature snapshot yet.
