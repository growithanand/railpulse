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

## ADR-006 — Identify source artifacts by SHA-256

- **Status:** Accepted
- **Decision:** Use `uci-791-aab991a970e5` as the RailPulse dataset version and retain full archive
  and member hashes in a machine-readable manifest.
- **Why:** UCI does not publish a semantic artifact version. A content digest makes the exact 2026-09-02
  retrieval reproducible and detects silent source replacement.
- **Alternative:** A retrieval date alone cannot distinguish two different artifacts downloaded on
  the same day or detect later content changes.

## ADR-007 — Treat cadence as nominal 10 seconds with jitter and gaps

- **Status:** Accepted
- **Decision:** Use event timestamps as the time axis, expect a nominal 10-second interval, and
  explicitly measure shorter/longer transitions and material gaps.
- **Why:** The complete CSV has 1,337,521 exact 10-second transitions, but also timestamp jitter and
  354 material gaps. The perfectly regular source index does not represent elapsed event time.
- **Alternative:** Assuming exact 0.1 Hz would produce incorrect rolling-window durations and hide
  missing time intervals.

## ADR-008 — MetroPT-3 is not the later MetroPT dataset

- **Status:** Accepted
- **Decision:** Scope RailPulse to UCI MetroPT-3 ID 791: 2020 data, 15 sensor signals, no GPS, and a
  nominal 10-second cadence.
- **Why:** The separate MetroPT dataset described in Scientific Data contains 2022 data, 20 variables
  including GPS, and 1 Hz acquisition. Mixing their documentation would corrupt this contract.
