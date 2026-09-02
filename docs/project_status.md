# Project status

## Current phase

**Phase 4 — Silver validation and quarantine (in progress: typed telemetry projection)**

Bronze ingestion is implemented and verified against both official inputs. The first Silver
increment adds a reusable typed telemetry projection while preserving every Bronze field. Silver
table writes, explicit rejection reasons, and accepted/quarantine outputs are not implemented yet.
Git commit state is reported in the session handoff because it changes independently of checked-in
project files.

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

## Phase 3 runtime verified on 2026-09-02

| Component | Verified state |
| --- | --- |
| Ubuntu WSL | Ubuntu 24.04.1 LTS |
| Python | 3.12.3 in ignored `.venv-wsl/` |
| Java | Eclipse Temurin 21.0.12.1 LTS in ignored `.tools/` |
| Apache Spark / PySpark | 4.2.0 |
| Delta Lake | 4.4.0 |
| Local Spark master | `local[4]` for full ingestion; bounded 3 GiB driver heap |

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
- Separate explicit all-string schemas for telemetry and failure-report Bronze records.
- Source/file/document checksums, modification/ingestion times, dataset version, and deterministic
  record/batch identifiers.
- Path-backed Delta tables with merge-based rerun idempotency and input/target reconciliation.
- Deterministic Spark integration fixtures for raw-token preservation, corrupt-row retention,
  duplicate-key rejection, and zero-insert reruns.
- A non-writing Silver telemetry projection for source-index, timezone-free timestamp, and sensor
  parsing that keeps invalid raw tokens available for later rejection reasons.

Official local Bronze evidence:

| Logical table | Source rows | Target rows | Verified rerun inserts |
| --- | ---: | ---: | ---: |
| `bronze.telemetry_raw` | 1,516,948 | 1,516,948 | 0 |
| `bronze.failure_reports_raw` | 4 | 4 | 0 |

## Not implemented

- Silver table writes, accepted/quarantine splitting, range/domain checks, normalized failure
  events, data-quality metrics, and all Gold transformations.
- SQL analytics, models, MLflow runs, alerts, dashboard, streaming, or policy simulation.
- CI workflow and Databricks deployment resources.

No data-quality, model-performance, failure-detection, warning-lead-time, false-alarm, or cost result
has been measured.

## Known constraints and next phase

The source contradictions documented in Phase 2 remain unresolved. Bronze deliberately performs no
type conversion, sensor-range validation, deduplication, timestamp normalization, or source-value
repair. Native Windows Spark is not the verified runtime because its Hadoop layer requires a
separate Windows helper; Ubuntu WSL is the tested local path. Databricks remains untested.

The next small increment will attach explicit telemetry parsing rejection reasons and split accepted
from quarantined records without writing Delta tables yet. Range, duplicate, gap, and failure-event
rules will follow in separate reviewed commits.
