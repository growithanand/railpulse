# Architecture decisions

This log records durable choices. Statuses are **accepted**, **provisional**, or **superseded**.

## ADR-001 — Bronze–Silver–Gold data products

- **Status:** Accepted
- **Decision:** Separate immutable source-aligned Bronze records, validated/quarantined Silver data,
  and business/ML-ready Gold products.
- **Why:** Traceability and quality investigation matter in maintenance analytics. Silent cleaning
  would make alerts difficult to defend.
- **Alternative:** A single cleaned table is simpler initially but obscures rejected records and
  transformation lineage.

## ADR-002 — Reusable code in a `src` package

- **Status:** Accepted
- **Decision:** Keep production logic in `src/railpulse/`; use notebooks only as thin exploration or
  demonstration interfaces.
- **Why:** Modules are easier to test, reuse in Databricks jobs, and discuss in code review.
- **Alternative:** Notebook-first development is faster for exploration but encourages hidden state
  and duplicated transformations.

## ADR-003 — TOML defaults with repository-relative paths

- **Status:** Accepted
- **Decision:** Load checked-in settings from `configs/default.toml`, validate required values, and
  resolve relative storage paths against an explicit project root.
- **Why:** Python 3.11+ reads TOML without a runtime dependency, and working-directory-independent
  paths improve local and job reproducibility.
- **Alternative:** YAML is common for data platforms but would add a parser dependency for current
  application configuration.

## ADR-004 — Defer Spark and Delta version selection

- **Status:** Provisional
- **Decision:** Do not pin PySpark, Delta Lake, or Databricks runtime dependencies in Phase 1.
- **Why:** The inspected machine has Python 3.13.5 and Java 23.0.2 but no Spark or Databricks CLI.
  Versions must be chosen against verified local and target-runtime compatibility, not guessed.
- **Alternative:** Immediate pins would make the scaffold look complete but could create an invalid
  Java/Python/Spark combination.

## ADR-005 — Separate telemetry and failure reports

- **Status:** Accepted
- **Decision:** Ingest telemetry and published failure/maintenance information into separate Bronze
  inputs and join only under documented point-in-time semantics.
- **Why:** Failure-report fields are not available at scoring time and can cause direct leakage.
