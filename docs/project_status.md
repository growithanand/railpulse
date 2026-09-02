# Project status

## Current phase

**Phase 2 — Official dataset verification and data contract (implementation verified)**

The official UCI MetroPT-3 archive, CSV, PDF, schema, sensor meanings, timestamp behavior, license,
and published failure table have been inspected. The manifest, contract, dictionary, failure
reference, and streaming inspector have passed local verification. Git commit state is reported in
the session handoff because it changes independently of checked-in project files.

## Environment observed on 2026-09-01

| Tool | Observed state |
| --- | --- |
| Python | 3.13.5 via Anaconda |
| pytest | 8.3.4 |
| Java | 23.0.2 |
| Git | 2.49.0.windows.1 |
| Spark / PySpark | Not installed |
| Databricks CLI | Not installed |
| Ruff | Not initially installed |
| Python build frontend | Not initially installed |

Repository-local Git identity:

- Name: `Anand Mullasseril Shajahan`
- Email: `anand.mullassheri@gmail.com`

## Implemented in the working tree

- `src`-layout Python package boundaries.
- Typed TOML configuration loading with repository-relative path resolution.
- Deterministic package and configuration tests.
- Dataset, Delta, Spark, streaming, MLflow, model, secret, and local-state exclusions.
- Planning, decisions, status, and data-placement documentation.
- Minimal Databricks Asset Bundle entry point; not CLI-validated or deployed.
- SHA-256 manifest for the official UCI archive and both members.
- Complete-file CSV inspection covering 1,516,948 rows and 15 sensors.
- Source/next-Bronze contracts, sensor dictionary, and separate four-row failure reference.
- Reusable standard-library inspector and deterministic metadata/contract tests.

## Not implemented

- Spark/Delta dependencies and sessions.
- Bronze, Silver, or Gold tables and transformations.
- SQL analytics, models, MLflow runs, alerts, dashboard, streaming, or policy simulation.
- CI workflow and Databricks deployment resources.

No data-quality, model-performance, failure-detection, warning-lead-time, false-alarm, or cost result
has been measured.

## Known constraints and next phase

The source contains documented contradictions: instance count, stated cadence/date range, duplicate
failure report number, and a maintenance date that predates its associated failure interval. They
remain explicit and unresolved. Spark/Delta compatibility still requires a supported toolchain.

The next phase will implement explicit-schema, metadata-rich, idempotent Bronze ingestion for the
telemetry CSV and separate failure reference without silently cleaning source values.
