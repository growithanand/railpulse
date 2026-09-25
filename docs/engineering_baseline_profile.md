# Full-source engineering baseline profile

## Purpose

`motor-current-robust-deviation-profile-v1` fits the transparent motor-current baseline on negative
training rows and profiles its scores on train and validation rows only. It does not score, inspect,
or report the sealed test period, select an alert threshold, or write a table.

## Run locally

```bash
.venv-wsl/bin/python -m railpulse.models.engineering_baseline_profile --master "local[4]"
```

## Verified complete-source result

The command was executed against the configured official local source on 2026-09-25. The baseline
fit 6,392 negative training rows:

| Parameter | Value |
| --- | ---: |
| Training median current | 1.1114 A |
| Training median absolute deviation | 0.3206 A |

The development profile reconciled 6,396 training rows and 4,747 validation rows. Test rows were
excluded.

| Period | Label | Rows | Minimum | Median | P90 | P95 | Maximum |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Train | Positive | 4 | 0.573 | 1.060 | 1.576 | 1.587 | 1.599 |
| Train | Negative | 6,392 | 0.000 | 1.000 | 8.769 | 8.769 | 15.363 |
| Validation | Positive | 5 | 0.401 | 0.576 | 1.092 | 1.250 | 1.408 |
| Validation | Negative | 4,742 | 0.000 | 13.924 | 13.924 | 13.924 | 14.630 |

## Interpretation

This score is not a useful high-score failure rule. Validation negatives are generally much farther
from the negative-training median than validation positives. A conventional high-deviation alert
would therefore produce many false positives while missing the observed positive pattern.

The result is still valuable as an honest engineering benchmark: a single global current center
does not transfer across the observed calendar periods. It indicates operating-regime or temporal
drift that must be understood before threshold selection. No threshold or performance claim is made
from this profile.

## Next investigation

Profile the input feature by calendar period and operating context to explain the shift. A revised
baseline should condition on a stable regime or use features that separate failure proximity from
ordinary operating changes. The held-out test period must remain sealed during that work.
