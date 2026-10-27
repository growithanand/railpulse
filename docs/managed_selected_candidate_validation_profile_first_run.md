# First managed selected-candidate validation profile

## Purpose

This record preserves the first managed execution of the retained candidate's compact validation
profile. The workflow fits `motor-current-only` on the frozen training partition, scores only the
frozen validation partition, and emits aggregate evidence without persisting cycle-level scores.

## Execution boundary

The unscheduled workflow ran successfully on 2026-10-26 from the bundle deployed at Git commit
`a8d3f9f`. The governed modelling-view refresh completed first, and the profile task ran only after
that dependency returned `SUCCESS`.

The complete workflow took approximately 8 minutes 11 seconds. The upstream modelling-view refresh
used approximately 7 minutes 24 seconds; the profile task used 2 seconds of setup and 43 seconds of
execution. No retry occurred.

The run created no score table, model version, registry entry, threshold, alert episode, or test-set
result.

## Reconciled population and fit evidence

The managed source matched the frozen comparison evidence exactly:

| Partition | Rows | Positive labels | Negative labels |
| --- | ---: | ---: | ---: |
| Training | 6,325 | 4 | 6,321 |
| Validation | 4,730 | 5 | 4,725 |

The validation positive rate was `0.0010570825`, or approximately 0.106%. The retained five-input
logistic regression completed 10 iterations and reproduced validation AUPRC
`0.0119542268`.

| Model term | Fitted value |
| --- | ---: |
| Intercept | -7.4180308230 |
| 15-minute observation count | 0.0004659318 |
| 15-minute minimum motor current | -0.0019111651 |
| 15-minute mean motor current | -0.0023554197 |
| 15-minute maximum motor current | 0.0029531757 |
| Maximum feature-window gap | -0.0007876715 |

## Validation score distribution

| Population | Rows | Minimum | p01 | p05 | p10 | p25 | Median | Mean | p75 | p90 | p95 | p99 | Maximum |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| All | 4,730 | 0.000616763 | 0.000616763 | 0.000616763 | 0.000616763 | 0.000616763 | 0.000616763 | 0.000622540 | 0.000633608 | 0.000635716 | 0.000636019 | 0.000636188 | 0.000640861 |
| Positive | 5 | 0.000635126 | 0.000635159 | 0.000635293 | 0.000635460 | 0.000635962 | 0.000636027 | 0.000635893 | 0.000636172 | 0.000636175 | 0.000636177 | 0.000636177 | 0.000636178 |
| Negative | 4,725 | 0.000616763 | 0.000616763 | 0.000616763 | 0.000616763 | 0.000616763 | 0.000616763 | 0.000622525 | 0.000633565 | 0.000635709 | 0.000636016 | 0.000636188 | 0.000640861 |

Positive cycles are concentrated near the upper end of the score distribution, but the label
populations overlap and the highest negative score exceeds every positive score. The narrow,
low-valued probabilities also mean a conventional `0.5` cutoff is not a meaningful alert rule.

## Interpretation boundary

This profile confirms reproducible managed scoring and provides the distribution evidence needed to
design validation-only alert policies. It does not establish probability calibration, a maintenance
threshold, failure-level warning lead time, acceptable false-alarm burden, or production readiness.
Only five positive validation cycles are available, so policy comparisons must remain deliberately
small and their conclusions cautious.

The [validation-only policy contract](validation_alert_policy_contract.md) now defines a limited set
covering score threshold, persistence, episode merge, and cooldown behavior, and its label-blind
episode engine plus guarded Spark adapter are implemented. The next gate is comparing policies on
validation using failure-event warning lead time and false alarms per operating period. Freeze the
complete model and policy before the one-time sealed-test evaluation.

## Repository boundary

This record deliberately omits Databricks account identifiers, workspace URLs, job and run IDs, and
local credential state.
