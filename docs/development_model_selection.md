# Development model selection

## Decision

RailPulse retains `motor-current-only` as the provisional candidate for development-only alert-policy
work. This is not a production model selection and does not authorize test-period scoring,
registration, deployment, or maintenance alerts.

The decision is versioned as `motor-current-logistic-development-selection-v1` and is derived from
the fixed `cycle-context-logistic-candidate-comparison-v1` managed run.

## Evidence

Both candidates used the same 6,325 training rows and 4,730 validation rows. Training contained four
positive labels; validation contained five.

| Candidate | Inputs | Validation AUPRC | Decision |
| --- | ---: | ---: | --- |
| `motor-current-only` | 5 | 0.011954 | Retain for development |
| `motor-current-plus-cycle-context` | 8 | 0.008632 | Do not carry forward |

The retained candidate had approximately 38% higher validation AUPRC and uses three fewer inputs.
The cycle-context candidate is therefore not justified under this fixed estimator, population, and
chronological split.

## Enforced boundary

`development_model_selection.py` freezes the comparison version, selection metric, candidate order,
shared population counts, exact managed-run metrics, selected candidate, reason, and sealed-test
identity. Construction fails when:

- a reviewed candidate is missing or reordered;
- candidate populations differ or label counts do not reconcile;
- a metric is non-finite or outside `[0, 1]`;
- the selected candidate is not the unique validation-AUPRC leader; or
- the comparison, scope, reason, or sealed-partition metadata changes.

The selected feature list is resolved from the existing candidate contract rather than copied into
a new model definition.

## Interpretation

Five validation positives are insufficient for a strong generalization claim. This selection simply
chooses which already-reviewed feature set may proceed to development-only score and alert-policy
work. The July test partition remains sealed until the model, threshold, persistence, merge, and
cooldown rules are frozen.

The next increment should produce validation cycle scores for this exact candidate without exposing
test rows. Alert-episode construction and operating-threshold selection remain later, separately
reviewed steps.
