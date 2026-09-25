# Development motor-current drift profile

## Purpose

`motor-current-development-drift-profile-v1` explains why the first robust-deviation baseline does
not transfer from training to validation. It profiles the underlying 15-minute mean motor current
by calendar month and observed label across train and validation only. The sealed test period is not
included.

## Run locally

```bash
.venv-wsl/bin/python -m railpulse.models.development_feature_drift_profile --master "local[4]"
```

## Verified complete-source result

The command was executed against the configured official local source on 2026-09-25 and reconciled
11,143 development rows from 2020-02-01 through 2020-06-30.

| Month | Label | Rows | P10 current | Median current | P90 current |
| --- | --- | ---: | ---: | ---: | ---: |
| February | Negative | 1,151 | 0.735 A | 0.796 A | 1.215 A |
| March | Negative | 1,656 | 0.859 A | 1.157 A | 3.676 A |
| April | Negative | 2,197 | 0.868 A | 2.948 A | 3.923 A |
| May | Negative | 1,393 | 0.859 A | 1.636 A | 3.174 A |
| May | Positive | 4 | 0.922 A | 1.264 A | 1.617 A |
| June | Negative | 4,737 | 1.196 A | 5.575 A | 5.575 A |
| June | Positive | 5 | 0.919 A | 0.928 A | 1.331 A |

## Interpretation

The negative population changes materially over time. Its median rises from 0.796 A in February to
2.948 A in April, falls to 1.636 A in May, and then moves to 5.575 A in June. June positive rows
remain in the lower-current range instead of following the dominant negative regime.

This explains the failed symmetric-deviation baseline: the model treats June's common higher-current
operation as anomalous because it was centered on the earlier training median. The evidence supports
investigating explicit operating regimes or a directional low-current rule. It does not yet justify
a threshold, causal interpretation, or model-performance claim.

The next baseline revision must be developed with training and validation only. Test-period feature
values and outcomes remain sealed.
