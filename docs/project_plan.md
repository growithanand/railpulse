# RailPulse project plan

## Objective

Build an end-to-end, reproducible platform that turns MetroPT-3 compressor telemetry into validated
Delta data products, point-in-time-correct health features, operational alert episodes, and
maintenance decision support.

## Delivery phases

| Phase | Deliverable | Completion evidence |
| --- | --- | --- |
| 1 | Repository, local configuration, package, tests, and durable instructions | Local quality checks and reviewed initial commit |
| 2 | Official dataset verification and data contract | Source, license, checksum, schema, units, intervals, and ambiguities documented |
| 3 | Bronze ingestion | Explicit schemas, traceable metadata, idempotency tests, row reconciliation |
| 4 | Silver validation and quarantine | Rejection reasons, duplicates/gaps/ranges, normalized events, DQ metrics |
| 5 | Gold compressor cycles | Tested cycle/session boundaries and inspectable SQL/PySpark outputs |
| 6 | Temporal features and failure horizons | Past-only windows, label semantics, leakage tests |
| 7 | Advanced SQL and reliability KPIs | Verified CTE/window/interval-query outputs |
| 8 | Engineering baseline | Interpretable rules, threshold rationale, chronological results |
| 9 | Unsupervised anomaly model | Normal-period training, reproducible scoring, baseline comparison |
| 10 | Event-level evaluation | Lead time, event recall, false alarms/day, sensitivity, uncertainty |
| 11 | MLflow tracking | Dataset/feature/split/code versions, parameters, metrics, artifacts |
| 12 | Alert episodes | Persistence, merging, cooldown, warning/critical logic and tests |
| 13 | Dashboard | Queries, specification, reproduction steps, honest deployment status |
| 14 | Historical stream replay | Incremental files, checkpoints, duplicates, late-data tests |
| 15 | Maintenance-policy simulator | Explicit hypothetical costs and sensitivity analysis |
| 16 | CI/CD and Databricks deployment configuration | Automated checks and validated bundle where an environment exists |
| 17 | Final documentation and review | Reproduction, limitations, demo, evidence-based résumé bullets, history audit |

Phases may be split into smaller reviewed commits when implementation and verification form distinct
logical units. Models cannot begin before provenance, failure events, labels, chronological splits,
and leakage controls are documented.

## Architectural sequence

1. `bronze.telemetry_raw` and `bronze.failure_reports_raw` preserve source records and ingestion
   metadata.
2. Silver validation produces accepted telemetry, quarantine records, normalized failure events,
   and data-quality metrics.
3. Gold transformations produce cycles, temporal features, horizons, scores, alerts, reliability
   summaries, and policy results.
4. MLflow records reproducibility metadata; Databricks SQL exposes decision-support views.

## Quality gates used in every phase

- Formatting, linting, import checks, and applicable unit/integration tests pass.
- Counts, schemas, and quality assertions reconcile where data is transformed.
- Temporal work has explicit leakage and chronological-split tests.
- Generated outputs are inspected and limitations are recorded.
- `git diff --check`, status, file inventory, and the complete intended diff are reviewed.
- The user explicitly authorizes the single proposed commit before it is created.
