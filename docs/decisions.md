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

- **Status:** Superseded by ADR-009
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

## ADR-009 — Use Spark 4.2 with Delta Lake 4.4 for local Bronze work

- **Status:** Accepted
- **Decision:** Pin PySpark 4.2.0 and `delta-spark` 4.4.0. Verify local integration in Ubuntu WSL
  with Python 3.12 and Eclipse Temurin JDK 21.
- **Why:** Delta 4.4 is built and tested for Spark 4.2, Spark 4.2 supports Python 3.10+ and Java
  17/21/25, and JDK 21 is an LTS runtime. The installed Windows JDK 23 is outside that Java set, and
  native Windows Hadoop also requires an additional `winutils.exe` helper.
- **Limitation:** Databricks runtime and deployment compatibility remain unverified until a workspace
  is available.

## ADR-010 — Use source-derived keys and path-backed local Bronze tables

- **Status:** Accepted
- **Decision:** Derive `record_id` from the logical table, input SHA-256, and stable source key
  (`source_index_raw` for telemetry; `source_row_raw` for failures). Use Delta `MERGE` for reruns and
  reject duplicate source-derived keys within a batch. Store generated local tables under
  `data/delta/bronze/` while retaining `bronze.*` as their logical names.
- **Why:** The verified files provide unique stable keys. This permits deterministic idempotency
  without inventing nondeterministic row numbers, while explicit duplicate rejection prevents
  silent loss. Path-backed tables work without a local Hive metastore and map cleanly to managed
  table writes in a later Databricks adapter.

## ADR-011 — Label future failure onset from an observed cycle stop

- **Status:** Accepted
- **Decision:** Use an observed loaded-cycle stop as the prediction timestamp and label the earliest
  accepted failure start in `(prediction_timestamp, prediction_timestamp + horizon]`. Exclude cycle
  stops inside a published failure interval and preserve incomplete horizons as null targets.
- **Why:** The cycle stop is a reproducible point at which the cycle's evidence is available. Strict
  future-onset and observation-censoring rules prevent current failures and unknown outcomes from
  becoming misleading training labels.
- **Limitation:** Publisher interval-end inclusion is unspecified. Version 1 conservatively excludes
  both interval endpoints; later sensitivity analysis must keep that assumption visible.

## ADR-012 — Use trailing event-time windows at the prediction boundary

- **Status:** Accepted
- **Decision:** Build temporal sensor features from trailing event-time windows ending at the cycle
  prediction timestamp. The first feature summarizes motor current over `(t - 15 minutes, t]`.
- **Why:** The strict trailing window makes future-row exclusion explicit while retaining the
  current observation available at scoring time. Event-time ranges remain correct when the nominal
  10-second cadence has jitter or gaps.
- **Limitation:** Fifteen minutes is an initial inspectable window, not an optimized predictive
  horizon. Coverage rules, additional windows, and empirical usefulness remain unverified.
