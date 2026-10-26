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
- **Limitation:** This local runtime does not prove managed-catalog behavior. Databricks package and
  Spark compatibility were subsequently verified through the read-only managed preflight.

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

## ADR-013 — Freeze June validation and July test boundaries

- **Status:** Accepted
- **Decision:** Use train timestamps before 2020-06-01, validation timestamps from 2020-06-01 up to
  2020-07-01, and test timestamps from 2020-07-01 onward. Record the choice as
  `calendar-chronological-split-selection-v1`.
- **Why:** It is the only profiled candidate with a represented positive failure event in train,
  validation, and test. The May-validation alternative has no training positives, while the
  August-test alternative has no test positives.
- **Guardrail:** Selection used only period feasibility and label/event coverage. Test features,
  model scores, alert thresholds, and performance remain unavailable until final evaluation.
- **Limitation:** Each period represents only one published failure event, so results will be
  descriptive case-study evidence rather than a precise generalization estimate.

## ADR-014 — Preserve snapshot v1 and publish context through a v2 table

- **Status:** Accepted
- **Decision:** Keep `gold.feature_snapshots` on `motor-current-15m-snapshot-v1` and publish the
  cycle-context expansion through the separate `gold.feature_snapshots_v2` contract.
- **Why:** Existing v1 rows are immutable and already have verified rerun evidence. A separate table
  makes the schema change explicit, preserves reproducibility, and lets downstream consumers opt in.
- **Guardrail:** Version 2 remains label-free and must reconcile one-to-one with version 1 feature
  inputs and cycle-context rows before it can be written.

## ADR-015 — Reuse the workspace catalog and isolate medallion schemas

- **Status:** Accepted
- **Decision:** Use the existing `workspace` catalog for development and map RailPulse's logical
  layers to `railpulse_bronze`, `railpulse_silver`, and `railpulse_gold`. Managed workloads must use
  fully qualified three-level names produced by the validated `CatalogNamespace` contract.
- **Why:** Reusing the governed workspace catalog avoids an unnecessary top-level object while
  separate schemas preserve clear layer boundaries and allow later privilege separation. Bundle
  variables keep physical names environment-specific.
- **Evidence:** The first managed provisioner run created all three schemas, an independent catalog
  check confirmed them, and a second run created nothing. Tables and source files remain absent.

## ADR-016 — Land source files in a managed Bronze Volume

- **Status:** Accepted
- **Decision:** Declare one managed Unity Catalog Volume named `source` under the physical Bronze
  schema. Store inputs below `metropt3/<dataset-version>/`, with separate `telemetry/` and
  `reference/` directories. Build every path through the validated, versioned source-landing
  contract.
- **Why:** A managed Volume provides a governed non-tabular file boundary without a user-specific
  workspace path or a separately configured external storage location. Dataset-versioned
  directories prevent a future source revision from silently replacing the verified artifact.
- **Alternative:** Workspace files are user-oriented rather than a durable governed data boundary.
  An external Volume would add cloud-storage configuration that this development workspace does not
  currently require.
- **Guardrail:** Bundle deployment may create only the empty Volume and must be reviewed separately
  from file upload. The Volume has deletion protection, and the ingestion job must verify manifest
  size and checksum before writing a Bronze table.
- **Evidence:** The reviewed deployment created the managed Volume and updated two existing jobs
  without deleting a resource. A Catalog API read confirmed the contracted type and comment, and an
  independent file listing confirmed that the Volume was empty immediately after deployment. A
  later controlled upload used the versioned paths without overwrite; remote sizes and independently
  downloaded SHA-256 values matched the manifest for both inputs.

## ADR-017 — Allowlist managed Silver tables and fix their merge keys

- **Status:** Accepted
- **Decision:** Expose a catalog-backed Silver writer only for the existing telemetry accepted,
  telemetry quarantine, telemetry quality-metrics, failure accepted, and failure quarantine
  contracts. Record tables merge on `record_id`; the metrics table merges on `quality_batch_id`.
- **Why:** A fixed table-to-key mapping prevents callers from directing validated data to arbitrary
  catalog objects or selecting an unsafe merge key. It also keeps the managed boundary aligned with
  the already verified local Silver contracts.
- **Guardrail:** The managed writer is insert-only, reconciles counts and unmatched keys, creates an
  empty contracted table when a valid output has no rows, and avoids DataFrame persistence for
  Databricks serverless compatibility. Reclassifying an existing record after a validation-contract
  change still requires an explicit versioned rebuild.
- **Deployment boundary:** This decision adds a tested persistence adapter only. No Silver job is
  declared or deployed, and no managed Silver table is created by this increment.

## ADR-018 — Make managed Silver telemetry a prevalidated, rerunnable multi-table operation

- **Status:** Accepted
- **Decision:** Derive accepted and quarantined telemetry plus its deterministic quality summary
  from one validated frame, verify their identities and counts before writing, then persist the
  three allowlisted tables through insert-only per-table Delta transactions.
- **Why:** Delta transactions are atomic for one table, not across three tables. Complete
  prevalidation catches deterministic data errors before the first write, while stable keys make an
  identical retry safe if an infrastructure failure interrupts the operation between tables.
- **Guardrail:** The operation reports success only after all three writes reconcile with the same
  total and quality counts. It disables intermediate DataFrame caching on the serverless path and
  does not treat a retryable multi-table sequence as a cross-table transaction.
- **Deployment boundary:** This increment composes managed telemetry outputs only. Failure-event
  composition, job orchestration, deployment, and remote Silver table creation remain separate.

## ADR-019 — Make managed Silver failure events a prevalidated, rerunnable operation

- **Status:** Accepted
- **Decision:** Validate the complete failure-event frame and its classification column before
  writing, then derive and persist accepted and quarantined records through the allowlisted catalog
  adapter.
- **Why:** The two Delta tables cannot share one transaction. Validating stable record identities and
  classification inputs first catches deterministic errors before either write, while insert-only
  merges make an interrupted operation safe to retry.
- **Guardrail:** The operation reports success only when accepted and quarantined source counts
  reconcile with the validated total. It performs no DataFrame caching on the serverless path and
  does not claim cross-table transactional atomicity.
- **Deployment boundary:** This increment composes managed failure-event outputs only. Job
  orchestration, deployment, and remote Silver table creation remain separate.

## ADR-020 — Keep the first managed Silver job boundary resource-independent

- **Status:** Accepted
- **Decision:** Provide a versioned Python-wheel entry point that reads both allowlisted managed
  Bronze tables, constructs both validation splits before writing, and invokes the reviewed managed
  telemetry and failure persistence operations.
- **Why:** A resource-independent runner makes the complete Bronze-to-Silver execution path testable
  before a Databricks workflow is declared or remote Silver data is changed. Reading and validating
  both inputs first also catches missing tables and structural schema errors before the first write.
- **Guardrail:** The five Delta outputs remain separate transactions. An infrastructure failure can
  leave a partial run, so recovery is an identical retry using the deterministic merge keys rather
  than a claim of cross-table atomicity.
- **Deployment boundary:** This decision adds the packaged runner only. No bundle job resource is
  declared or deployed, and no remote managed Silver table is created.

## ADR-021 — Gate managed Silver processing with the existing Bronze workflow

- **Status:** Accepted
- **Decision:** Declare a separate, unscheduled Silver workflow whose first task invokes the existing
  managed Bronze workflow and whose second task runs the Silver wheel entry point only after the
  Bronze run succeeds.
- **Why:** Executable task ordering prevents Silver from accidentally reading stale Bronze data while
  retaining one authoritative implementation of source preflight and Bronze ingestion. Keeping the
  workflow unscheduled prevents automatic remote writes during the migration checkpoint.
- **Guardrail:** Allow one concurrent Silver run, enable queueing, and use `ALL_SUCCESS` for the
  Bronze-to-Silver gate. The resource must pass strict bundle validation and a scoped read-only plan
  before deployment is considered.
- **Deployment boundary:** This decision declares and validates the job resource only. Deployment,
  execution, and creation of remote managed Silver tables remain separate review gates.
- **Evidence:** The reviewed deployment created the unscheduled Silver job, updated only the
  referenced Bronze job's wheel dependency, and deleted nothing. A remote read confirmed the
  Bronze-gated two-task graph, serverless environment, queueing, one-run concurrency limit, and
  empty Silver run history. The workflow was not executed and created no Silver table.

## ADR-022 — Gate managed modelling-view v2 processing with feature snapshots

- **Status:** Accepted
- **Decision:** Declare a separate, unscheduled modelling-view v2 workflow whose first task invokes
  the existing managed feature-snapshot v2 workflow and whose second task runs the modelling-view
  wheel entry point only after that upstream workflow succeeds.
- **Why:** The feature workflow already owns the complete Bronze-to-Silver-to-cycle refresh and the
  label-free snapshot build. Reusing it keeps one authoritative feature path. The immutable
  failure-horizon table is consumed directly instead of launching a second duplicate upstream
  chain; the runner's population, version, boundary, and lineage checks reject incompatible inputs.
- **Guardrail:** Allow one concurrent run, enable queueing, use `ALL_SUCCESS`, and keep the workflow
  unscheduled. Strict bundle validation and a selected read-only plan must pass before deployment.
- **Deployment boundary:** This decision declares and validates the workflow only. Deployment,
  execution, and creation of `gold.modeling_view_v2` remain separate review gates.
- **Evidence:** The selected plan would create one job, update four transitive upstream jobs only to
  reference the newly built wheel, delete no resources, and leave five resources unselected. No
  existing task graph, parameters, concurrency setting, or schedule would change. The first selected
  deployment refreshed those wheel references but the account-level inactive-organization gate
  rejected creation of the new job. A read-only lookup confirmed the job remained absent, and no
  workflow or compute was started. After sign-in was restored, an unchanged retry created the job
  and changed only those wheel references. Independent readback confirmed the reviewed two-task
  graph, serverless environment, queue and concurrency controls, no schedule, zero runs, and an
  absent target table. The first separately approved execution then enforced the gate: the upstream
  feature-snapshot refresh succeeded before the modelling task inserted 15,766 rows into an empty
  managed target. Runner output exactly matched the frozen population, label, exclusion, lineage,
  and failure-event coverage counts. Read-only Catalog metadata confirmed a 44-column managed Delta
  table with 15,766 rows. The separately approved six-statement SQL audit then confirmed exact
  source projections and distributions, zero contract or leakage-boundary violations, complete
  failure lineage, and the expected event-level coverage. The validation warehouse was stopped
  immediately afterward. A separately approved unchanged workflow rerun then inserted zero rows,
  matched all 15,766 target rows as unchanged, and preserved the exact population, label, exclusion,
  lineage, and failure-event evidence.

## ADR-023 — Compare cycle context with one fixed development-only estimator

- **Status:** Accepted
- **Decision:** Compare the original five motor-current inputs with their eight-input cycle-context
  extension on the same trainable version 2 population using one fixed L2-regularized Spark
  binomial logistic regression and validation area under the precision-recall curve.
- **Why:** Holding the estimator, population, split, and fitting configuration constant makes the
  comparison about the added cycle-context inputs rather than unequal data or tuning effort.
- **Guardrail:** The comparison accepts only the reviewed train/validation data object. It cannot
  read or score the July test partition, tune an alert threshold, resample classes, search
  hyperparameters, or automatically declare a winner.
- **Evidence boundary:** The implemented engine returns partition counts, validation AUPRC,
  coefficients, intercepts, and optimizer iterations for MLflow logging. Focused contract and Spark
  tests pass. The first managed comparison later recorded both candidates without scoring the sealed
  test partition or selecting an alert threshold.

## ADR-024 — Track one comparison parent with two nested candidate runs

- **Status:** Accepted
- **Decision:** Log the fixed comparison contract once in a parent MLflow run and log each of the
  two reviewed feature sets in its own nested child run.
- **Why:** The parent preserves the shared split, estimator, code, dataset, and source-lineage
  identity, while the child runs keep each candidate's inputs and validation evidence independently
  inspectable without duplicating the complete contract.
- **Guardrail:** Validate all evidence before starting a run. Log only train/validation counts,
  validation AUPRC, iterations, coefficients, and intercepts; do not log test evidence, predictions,
  alert thresholds, a registered model, or an automatic winner.
- **Evidence boundary:** Ten focused tracking and contract tests pass using an in-memory
  MLflow-compatible recorder. The first managed comparison later verified one parent and two nested
  candidate runs remotely; it created no registered model or automatic winner.

## ADR-025 — Derive experiment lineage only from development rows

- **Status:** Accepted
- **Decision:** Make the managed comparison runner read only the allowlisted version 2 modelling
  table and derive its tracking identity from trainable rows strictly before the sealed test period.
  Require the experiment name and code version as explicit runner inputs.
- **Why:** The experiment record must identify the exact managed sources and code without allowing
  July test values or lineage to influence development metadata.
- **Guardrail:** Reject missing boundary columns, zero or mixed development identities, incompatible
  SHA-256 or version fields, and all downstream development, comparison, or tracking contract
  failures before recording MLflow evidence.
- **Evidence boundary:** Three unit tests and three Spark tests cover routing, explicit arguments,
  error propagation, pre-test context isolation, mixed-lineage rejection, and missing-column
  rejection. The first managed run later recorded the exact reviewed Git commit and development-only
  lineage in MLflow.

## ADR-026 — Gate comparison behind the managed modelling-view workflow

- **Status:** Accepted
- **Decision:** Declare a bundle-managed MLflow experiment and a separate unscheduled two-task job.
  The first task invokes the existing modelling-view workflow; the comparison entry point runs only
  after `ALL_SUCCESS` and receives the experiment resource plus `${bundle.git.commit}`.
- **Why:** Reusing the verified upstream workflow guarantees that candidate fitting starts from the
  governed modelling table, while the automatic commit identity ties experiment evidence to the
  reviewed source without manual copying.
- **Guardrail:** Use serverless environment version 4, install only the project wheel and
  `mlflow-skinny`, queue additional requests, allow one concurrent run, and define no schedule or
  trigger. Review a resource-scoped plan before deployment and require separate approval to run.
- **Evidence boundary:** Strict validation and the selected plan passed. Deployment created the
  experiment and comparison job, updated five transitive jobs only with the shared wheel reference,
  deleted nothing, and left five resources unselected. The first separately approved run completed
  successfully on 2026-10-24: upstream modelling data refreshed first, then one parent and two nested
  candidate runs finished in MLflow without test-period scoring.

## ADR-027 — Retain the simpler candidate for development-only alert-policy work

- **Status:** Accepted
- **Decision:** Retain `motor-current-only` as the candidate for development-only scoring and alert
  policy design. Do not carry the cycle-context extension into the next stage.
- **Why:** Both candidates used identical populations and estimator settings. The simpler candidate
  produced validation AUPRC `0.011954`, compared with `0.008632` for the three-feature extension,
  while requiring fewer inputs.
- **Guardrail:** Treat the decision as provisional and development-only. Keep the July test period
  sealed, and do not register, deploy, or describe the candidate as production-ready. Threshold,
  persistence, merge, and cooldown rules remain separate decisions.
- **Evidence boundary:** A pure-Python contract freezes the managed-run counts and metrics, requires
  one shared population and every reviewed candidate, and rejects any selected candidate that is not
  the unique validation-AUPRC leader. The validation partition contains only five positive labels.

## ADR-028 — Expose validation probabilities through a sealed scoring boundary

- **Status:** Accepted
- **Decision:** Fit only the retained `motor-current-only` candidate on the frozen training rows and
  return cycle probabilities only for the frozen validation rows.
- **Why:** Alert-policy work needs time-ordered probabilities and labels, but it does not need test
  rows, source metadata, feature values, raw predictions, or the fitted Spark model.
- **Guardrail:** Require the exact managed comparison populations, candidate and selection versions,
  reviewed columns, unique keys, and chronological boundaries before fitting. Emit only audit keys,
  validation labels and probabilities, and fixed scoring identities.
- **Evidence boundary:** Three Spark integration tests verify 4,730 reconciled validation scores,
  bounded probabilities, exclusion of a July row, and rejection of changed populations, July
  timestamps, and incompatible contract metadata. Full-source execution and persistence remain
  separate gates.

## ADR-029 — Profile validation probabilities without row-level persistence

- **Status:** Accepted
- **Decision:** Convert the guarded validation score frame into one compact profile containing
  fixed identities, fit evidence, label counts, positive rate, and all/positive/negative probability
  distributions. Do not write cycle-level probabilities.
- **Why:** Threshold design needs to understand score separation and scale, while the project does
  not yet need a durable prediction table. Aggregate evidence makes the next review possible with a
  smaller privacy, provenance, and accidental-test-use surface.
- **Guardrail:** Revalidate the exact managed population, reviewed columns, unique keys, candidate
  and split literals, probability bounds, and reconciled distribution counts. Fail before fitting
  when a reconstructed source does not match the managed comparison evidence.
- **Evidence boundary:** Focused Spark coverage verifies the compact profile and rejects tampered
  score identity. A read-only local full-source attempt stopped before fitting because its train and
  validation populations contained six fewer negative rows than the frozen managed evidence. No
  score summary, threshold, row-level write, or test-period result was produced. A separate managed
  runner now routes the governed modelling table through the same guard and returns only compact
  JSON evidence; its workflow declaration, deployment, and execution remain separate gates.
