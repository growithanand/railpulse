# Motor-current coverage-policy profile

## Purpose and execution

This read-only profile compares a fixed set of diagnostic coverage policies for the available
`motor-current-15m-v1` windows. It joins those windows to `cycle-failure-horizon-v1` labels so each
policy reports retained and excluded positive, negative, and null-label counts. It does not select
an eligibility rule, persist features or labels, or modify an existing Delta table.

Run it from the verified Ubuntu WSL environment:

```bash
.venv-wsl/bin/python -m railpulse.features.coverage_policy_profile --master "local[4]"
```

The command rebuilds accepted Silver views from Bronze, reads `gold.loaded_cycles`, applies the
default two-hour horizon, derives past-only motor-current features, and prints deterministic JSON.
Another positive horizon can be inspected with `--horizon-seconds`.

## Compared policies

The strict tail excludes a window when either its observation count or its first-to-last observed
span is strictly below the complete-source fifth percentile. An in-window gap is the greater of:

- leading time uncovered after a source gap; and
- the largest recorded forward interval after the first contributing observation.

| Policy ID | Diagnostic exclusion |
| --- | --- |
| `available_baseline` | None; retain every available feature window |
| `exclude_strict_tail` | Count below 75 or observed span below 891 seconds |
| `exclude_material_gap_20s` | Maximum in-window gap at least 20 seconds |
| `exclude_strict_tail_or_material_gap_20s` | Either strict-tail or 20-second gap condition |
| `exclude_strict_tail_or_gap_120s` | Either strict-tail or 120-second gap condition |

The 20-second boundary is the existing telemetry material-gap contract. The 120-second boundary is
a sensitivity comparison grounded in the preceding gap-magnitude profile; it is not a selected
training threshold.

## Reconciliation contract

`motor-current-coverage-policy-profile-v1` verifies that:

- accepted telemetry and failure events have complete, compatible source lineage;
- feature and horizon outputs each reconcile with all Gold cycle IDs;
- available feature context and horizon labels have unique, non-null cycle IDs;
- every available feature has exactly one known horizon status;
- retained and excluded populations partition every available feature; and
- positive, negative, and null-label counts reconcile within both populations for every policy.

## Verified complete-source result

The command was run locally on 2026-09-15 with the 15-minute feature window and default two-hour
failure horizon.

| Input or boundary | Verified value |
| --- | ---: |
| Accepted telemetry records | 1,516,948 |
| Gold loaded cycles | 15,766 |
| Available motor-current windows | 15,703 |
| Accepted failure events | 4 |
| Fifth-percentile observation count | 75 |
| Fifth-percentile observed span | 891 seconds |

Available-window label reconciliation:

| Target category | Available cycles |
| --- | ---: |
| Positive | 22 |
| Negative | 15,676 |
| Null | 5 |
| **Total** | **15,703** |

The five null labels comprise three incomplete horizons and two prediction timestamps inside a
published failure interval. The 63 cycles without prediction boundaries also lack motor-current
features and therefore do not enter this available-window comparison.

| Policy | Retained | Excluded | Retained positive | Retained negative | Retained null |
| --- | ---: | ---: | ---: | ---: | ---: |
| Available baseline | 15,703 | 0 | 22 | 15,676 | 5 |
| Exclude strict tail | 15,413 | 290 | 22 | 15,386 | 5 |
| Exclude material gaps at 20 seconds | 15,418 | 285 | 22 | 15,391 | 5 |
| Exclude strict tail or material gaps | 15,388 | 315 | 22 | 15,361 | 5 |
| Exclude strict tail or gaps at 120 seconds | 15,402 | 301 | 22 | 15,375 | 5 |

All excluded windows are negative under the current two-hour horizon: every policy retains all 22
positive and all 5 null-label windows. This is a count reconciliation, not evidence that a policy
improves prediction or identifies healthy operation.

## Interpretation and limitations

The result shows that none of these fixed diagnostic exclusions removes a currently observed
positive window. It does not justify choosing the policy that removes the most negative rows.
Selecting a feature rule because it preserves labels across the full dataset would introduce
label-informed selection, and four accepted failure events are too sparse for a stable performance
claim. Future selection must remain label-independent or occur only within a chronological
development period, with final performance assessed on untouched later events.

The comparison covers one sensor and one feature window. It does not test model accuracy, warning
lead time, event recall, false alarms, maintenance impact, or the information content of excluded
rows. No feature, horizon, policy, or model output is persisted by this command.
