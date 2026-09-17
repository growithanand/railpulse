# Full-source motor-current eligibility profile

## Purpose and execution

This read-only profile applies `motor-current-15m-eligibility-v1` to every materialized Gold cycle.
It rebuilds accepted Silver telemetry from Bronze, derives the past-only 15-minute motor-current
feature and gap context, and reconciles eligibility statuses and reasons. It does not load failure
events or horizon labels, write a feature table, or modify existing Delta data.

Run it from the verified Ubuntu WSL environment:

```bash
.venv-wsl/bin/python -m railpulse.features.eligibility_profile --master "local[4]"
```

## Reconciliation contract

`motor-current-15m-eligibility-profile-v1` verifies that:

- accepted telemetry has one complete dataset/source/ingestion lineage;
- motor-current features contain one unique row for every Gold cycle;
- available-window gap context contains one unique row for every available feature;
- eligibility output contains one unique row for every Gold cycle;
- profile, feature, eligibility, validation, window, and threshold versions are fixed;
- `eligible` rows are available, have no reason, and have a maximum gap below 20 seconds;
- each `ineligible` row has exactly one reason consistent with its feature status and gap; and
- status and reason counts independently reconcile with the Gold cycle total.

## Verified complete-source result

The command was run locally on 2026-09-16.

| Input or contract | Verified value |
| --- | ---: |
| Accepted telemetry records | 1,516,948 |
| Gold loaded cycles | 15,766 |
| Feature window | 900 seconds |
| Minimum excluded gap | 20 seconds |

Eligibility status reconciliation:

| Status | Cycles |
| --- | ---: |
| `eligible` | 15,418 |
| `ineligible` | 348 |
| **Total** | **15,766** |

Ineligibility reason reconciliation:

| Reason | Cycles |
| --- | ---: |
| `feature_missing_prediction_boundary` | 63 |
| `feature_missing_prediction_observation` | 0 |
| `missing_or_invalid_coverage_context` | 0 |
| `material_window_gap` | 285 |
| `unsupported_feature_status` | 0 |
| **Total ineligible** | **348** |

The 15,418 eligible count exactly matches the earlier diagnostic candidate that excluded material
gaps at 20 seconds. The 285 gap exclusions plus 63 cycles without causal prediction boundaries
explain every ineligible cycle. Zero missing anchors and zero missing/invalid context rows confirm
that the complete-source feature and coverage joins are internally complete.

Source identity:

| Identity | Verified value |
| --- | --- |
| Dataset version | `uci-791-aab991a970e5` |
| Telemetry SHA-256 | `db30ccb4ea402e3c8bf2c99db06e288d4f2a772f6928f9dbe26a920d69793e24` |
| Telemetry ingestion batch | `12cdc6af63e42b12d14b0db7d4ea7304e4944a80a05eec519c0285c641b92171` |
| Telemetry validation | `telemetry-validation-v2` |

## Interpretation and limitations

Eligibility is an input-coverage decision, not an equipment-health label. It does not establish
that eligible cycles are normal, ineligible cycles are abnormal, or the rule improves model
performance. No failure label, target count, or motor-current magnitude participates in the
profile's decision.

The current source represents one compressor, so its temporal windows are globally ordered and
Spark emits the known no-partition window warning. A multi-asset implementation must partition by
a stable equipment identifier. Persistence, chronological splitting, modeling, event evaluation,
and maintenance-impact claims remain separate work.
