# RailPulse

**Predictive maintenance and analytics for metro compressor systems**

RailPulse is an end-to-end data engineering and machine-learning portfolio project built on the
[MetroPT-3](https://archive.ics.uci.edu/dataset/791/metropt%2B3%2B) air-compressor dataset. It turns
1.5 million telemetry observations and separately published failure reports into governed Delta
tables, point-in-time features, chronological modelling data, and an eventual maintenance dashboard.

The project asks a practical question:

> Can abnormal compressor behaviour be identified early enough to support maintenance
> intervention—ideally at least two hours before a recorded failure—without creating an
> unacceptable false-alarm burden?

The first managed development-only model comparison is complete, but no sealed-test or
maintenance-impact claim is made yet. The emphasis remains reproducible evidence and a leakage-safe
evaluation design.

## Current status

The complete Bronze-to-Gold engineering path is verified locally on the official data. The
Databricks foundation is also live: the package runs on serverless compute, the three Unity Catalog
schemas exist, both source files are stored in a governed Volume, and a managed preflight job has
reproduced their manifest-backed sizes and SHA-256 hashes.

A two-task managed Bronze job is implemented, tested, and deployed. Its first task verifies the
landed files; its second task can write the two allowlisted catalog tables only after verification
succeeds. Its first successful managed run created and reconciled 1,516,948 telemetry rows and four
failure-reference rows. An unchanged managed rerun then matched every existing record, inserted
zero rows, and preserved both table counts.

An unscheduled managed Silver workflow is also deployed and has completed its first successful
run. It invoked the verified Bronze workflow first, then ran the packaged telemetry and failure
validation boundary only after Bronze succeeded. Independent read-only checks confirmed all five
reviewed Silver tables, 1,516,948 accepted telemetry rows, four accepted failure rows, empty
quarantine outputs, one reconciled quality-summary row, and complete unique identifiers in both
accepted tables.

The first managed Gold workflow has now completed the full Bronze-to-Silver gate and materialized
`loaded_cycles` as managed Delta storage. Its write reconciled 15,766 deterministic cycle rows from
an empty target. Independent SQL confirmed 15,766 non-null unique cycle IDs, 243,638 loaded
observations, the expected four boundary/censoring groups, and zero structural-invariant violations.

The next managed Gold boundary has also completed its first run. The gated workflow refreshed every
upstream layer without inserting or changing existing rows, then materialized 15,766 deterministic
two-hour labels in `failure_horizons`. Independent SQL confirmed the frozen five-status distribution,
unique cycle keys, fixed contract metadata, exact Silver failure references, and zero semantic
violations.

The managed `feature_snapshots_v2` workflow has completed both its first write and an identical
rerun. The first run inserted and independently validated 15,766 point-in-time snapshots. The rerun
inserted zero rows, matched all 15,766 existing snapshots as unchanged, and preserved the complete
feature, eligibility, context, and source-lineage contract.

The allowlisted `gold.modeling_view_v2` workflow has now completed its first approved managed
write. Its upstream feature-snapshot refresh succeeded before the modelling task started, and the
runner inserted 15,766 immutable rows into an empty target. The output exactly reproduced the
frozen profile: 15,272 trainable rows, 494 exclusions, 22 positive labels, 15,250 negative labels,
and three of the four accepted failure events represented by trainable positive cycles. A read-only
Catalog check confirmed a 44-column Unity Catalog managed Delta table with statistics for 15,766
rows. A separate six-statement, read-only SQL audit then confirmed exact feature and horizon source
projections, fixed versions and lineages, the complete label and exclusion distributions, zero
semantic or leakage-boundary violations, and exact references to the accepted failure events. The
validation warehouse was stopped immediately after the audit. An unchanged workflow rerun then
proved idempotency: the managed writer inserted zero rows, matched all 15,766 existing rows as
unchanged, and preserved the complete population and lineage evidence without starting a separate
SQL warehouse.

The unscheduled model-comparison workflow and its MLflow experiment are now deployed and have
completed their first approved run. The workflow refreshed the governed modelling view before
fitting either candidate, and MLflow recorded one parent run plus two nested candidate runs. On the
same 6,325 training rows and 4,730 validation rows, the five-input motor-current candidate achieved
validation AUPRC `0.011954`; adding three cycle-context inputs reduced it to `0.008632`. The simpler
candidate is therefore the provisional development leader, but the validation set contains only
five positive labels and the sealed July test period remains untouched.

A compact validation-score profiler is now implemented and tested. A read-only local full-source
attempt stopped before fitting because its reconstructed train/validation populations differed from
the frozen managed evidence by six negative rows. The guard was intentionally retained; no local
score summary or test-period result was produced. A managed read-only runner is now prepared to use
the governed modelling table and return only the compact profile. Its unscheduled two-task
Databricks job is declared and passes strict bundle validation. A resource-scoped plan proposes one
job addition, five existing-job updates, no deletions, and leaves seven resources outside the
selection. The selected deployment then created that job and updated the five planned dependencies
without deleting resources or starting compute. The profile workflow has not been run yet.

| Area | Verified state | Next boundary |
| --- | --- | --- |
| Source governance | Manifest, immutable paths, checksums, managed Volume | Complete |
| Local pipeline | Verified through Gold v1/v2 snapshots and modelling-view profile | Complete |
| Databricks runtime | Serverless wheel execution and source preflight | Complete |
| Managed Bronze | First load and zero-insert rerun reconciled both source-aligned tables | Complete |
| Managed Silver | First managed run and independent table, quality, and identifier checks passed | Complete |
| Managed Gold | Modelling-view first write, independent audit, and zero-insert rerun passed | Complete |
| Modelling | Guarded scorer plus deployed validation-profile workflow | Run and reconcile the compact managed profile |
| Decision support | Maintenance use case and KPI goals identified | Build KPI tables and SQL dashboard |

See [project status](docs/project_status.md) for the detailed implementation record and
[project plan](docs/project_plan.md) for the delivery sequence.

## Architecture

```text
Official telemetry CSV + published failure reports
                         |
                         v
       checksum manifest + governed source Volume
                         |
                         v
                 Bronze Delta tables
        raw strings, corrupt rows, file lineage,
           deterministic record and batch IDs
                         |
                         v
                 Silver Delta tables
      typed values, accepted/quarantine outputs,
       quality reasons, gaps, duplicate detection
                         |
                         v
                  Gold data products
       loaded cycles, point-in-time features,
       feature snapshots v1/v2, failure horizons
                         |
                         v
          chronological modelling and MLflow
                         |
                         v
        alert episodes + event-level evaluation
                         |
                         v
        Databricks SQL maintenance dashboard
```

The dataset is small enough to process on one machine. Spark and Databricks are used deliberately
to demonstrate scalable, incremental, governed patterns rather than to imply that distributed
compute is required for 1.5 million rows.

## What the project demonstrates

### Governed ingestion

- Exact source identity through versioned paths, byte sizes, and SHA-256 checksums.
- Separate provenance for telemetry, the failure transcription, and its official source document.
- All-string Bronze schemas that preserve raw tokens and malformed rows before interpretation.
- Deterministic record and batch identifiers with insert-only Delta merges and count reconciliation.
- Rerunnable writes: an identical source produces zero new rows.

### Data quality and transformation

- Explicit parsing, analogue-envelope, binary-domain, duplicate, and event-time sequence checks.
- Separate accepted and quarantine tables with record-level rejection reasons.
- Material-gap detection without incorrectly labelling measurements after a gap as invalid.
- Causal loaded-cycle segmentation with left- and right-censoring retained as data, not discarded.
- Monotonic Gold merges that prevent settled cycle history from being silently rewritten.

### Point-in-time features and evaluation

- Strict past-only feature windows ending at each cycle prediction timestamp.
- Label-independent feature eligibility and immutable feature snapshots.
- A two-hour cycle-to-failure horizon with explicit null states for incomplete or in-failure cases.
- Calendar-based train, validation, and test periods; random row splitting is prohibited.
- Tests that prove future telemetry cannot change historical feature values.

### Platform engineering

- A Python `src` package with unit and Spark/Delta integration tests.
- Databricks Asset Bundle resources, Python-wheel tasks, serverless execution, and Unity Catalog.
- Validated catalog/schema identifiers and fully qualified managed table names.
- Read-only preflight tasks separated from provisioning, upload, deployment, and table mutation.
- Generated data, Delta logs, credentials, environment files, and workspace state excluded from Git.

## Verified complete-source evidence

| Evidence | Verified result |
| --- | ---: |
| Telemetry rows reconciled through Bronze and Silver | 1,516,948 |
| Published failure records retained | 4 |
| Provisional loaded cycles | 15,766 |
| Available 15-minute motor-current features | 15,703 |
| Feature-eligible cycles | 15,418 |
| Material-window-gap exclusions | 285 |
| Missing prediction boundaries | 63 |
| Trainable modelling-view rows | 15,413 |
| Positive two-hour cycle labels | 22 |
| Negative trainable cycle labels | 15,391 |
| Excluded modelling-view rows | 353 |
| Failure events represented by positive cycles | 3 of 4 |

Local first-write and rerun checks materialized 15,766 rows in each versioned Gold feature-snapshot
table. Identical reruns inserted zero rows and left the existing snapshots unchanged. Generated
Delta storage is intentionally not committed.

## Modelling findings so far

The modelling work is intentionally conservative:

- A June-validation/July-test chronological split was selected from coverage feasibility alone.
  The held-out test period has not been used to choose features or thresholds.
- A training-only robust motor-current deviation baseline exposed substantial temporal or
  operating-regime drift and was retained as a documented benchmark failure.
- Five directional low-current thresholds were rejected because their validation false-alarm burden
  remained operationally weak.
- A pressure-balance feature showed substantial positive/negative overlap and was not promoted.
- Loaded-cycle duration context showed consistent development-period signal and was added through a
  separate immutable version 2 feature snapshot instead of rewriting version 1.
- A fixed Spark logistic-regression comparison engine now evaluates the original five-input feature
  set against the eight-input cycle-context extension using guarded train/validation frames only.
  An MLflow boundary is also implemented and tested: it records one parent comparison run and two
  nested candidate runs with fixed contract, lineage, validation AUPRC, and coefficient evidence.
  A managed runner now reads only the allowlisted modelling-view table, derives tracking identity
  from pre-test trainable rows, runs the fixed comparison, and passes its evidence to that boundary.
  Its unscheduled Databricks workflow and bundle-managed experiment were deployed through the
  reviewed selected plan. The first managed run completed successfully and recorded one parent plus
  two nested MLflow runs. On identical populations, `motor-current-only` reached validation AUPRC
  `0.011954`, while `motor-current-plus-cycle-context` reached `0.008632`. The simpler candidate is
  the provisional development leader; five validation positives and an untouched test period keep
  this from being a final performance claim.
- A versioned development-selection contract now retains `motor-current-only` for the next
  alert-policy steps. It freezes the exact managed-run evidence, requires identical candidate
  populations, and keeps the July test partition sealed. This is a development decision, not model
  registration or deployment.
- A guarded Spark scorer now fits that exact candidate on the frozen 6,325-row training population
  and can expose only the 4,730 validation keys, timestamps, labels, probabilities, and fixed
  identities. It rejects changed counts, label distributions, candidate metadata, and July
  timestamps before fitting. A compact profiler can summarize all, positive, and negative validation
  probabilities without persisting row-level output. Focused Spark tests pass. A read-only local
  attempt correctly stopped on a six-negative-row provenance mismatch, so no full-source profile or
  persistence has occurred yet. A tested managed runner now reads the governed modelling table and
  returns only aggregate profile evidence. Its unscheduled job declaration passes validation, and a
  selected plan proposed one addition, five updates, and no deletions. The selected deployment
  applied exactly that resource scope without starting compute. Execution remains a separate gate.

These negative and mixed results are part of the portfolio evidence: the project records why a
candidate was rejected rather than turning limited failure data into an inflated accuracy claim.

## Databricks progression

Completed managed steps:

1. Validate the bundle and packaged wheel on serverless compute.
2. Provision separate Bronze, Silver, and Gold schemas idempotently.
3. Create a deletion-protected managed source Volume.
4. Upload the two contracted inputs without overwrite.
5. Independently verify remote sizes and downloaded hashes.
6. Run the deployed source-preflight job successfully without creating tables.
7. Implement and test an allowlisted Unity Catalog Bronze writer.
8. Define and validate an unscheduled two-task Bronze workflow with a mandatory preflight gate.
9. Deploy only that workflow and verify its empty run history without creating tables.
10. Run the workflow successfully and reconcile 1,516,948 telemetry rows and four failure rows.
11. Redeploy from committed source and prove an identical rerun inserts zero rows.
12. Implement and test allowlisted, retry-safe managed Silver persistence and its packaged runner.
13. Deploy the unscheduled Bronze-gated Silver workflow and verify that deployment starts no run.
14. Run the reviewed Silver workflow successfully through its Bronze dependency.
15. Independently reconcile all five Silver tables, the telemetry quality summary, and accepted
    record identifiers.
16. Implement and test the allowlisted managed Gold cycle writer and packaged runner.
17. Deploy the unscheduled Silver-gated Gold workflow without starting compute.
18. Run the full workflow and independently reconcile 15,766 managed loaded cycles.
19. Implement and test retry-safe two-hour failure-horizon persistence and its packaged runner.
20. Review and deploy the unscheduled Gold-cycle-gated failure-horizon workflow, then independently
    confirm its configuration, empty run history, and absent target table without starting compute.
21. Run the complete workflow and independently reconcile all 15,766 managed failure-horizon rows.
22. Materialize and independently validate 15,766 managed feature snapshots, then prove an
    identical rerun inserts zero rows.
23. Deploy the feature-snapshot-gated modelling-view workflow without starting compute.
24. Run its first approved write and reconcile the runner output plus read-only Catalog metadata.
25. Independently audit the managed modelling view with six checked-in SQL statements, then stop
    the validation warehouse.
26. Rerun the unchanged workflow and prove the immutable modelling-view merge inserts zero rows and
    preserves all 15,766 target rows.
27. Declare and strictly validate the unscheduled model-comparison workflow and its bundle-managed
    MLflow experiment without deploying resources or starting compute.
28. Review a selected plan that creates those two resources, refreshes only five transitive wheel
    references, deletes nothing, and leaves five unrelated resources outside the selection.
29. Deploy the selected comparison resources without starting compute.
30. Run the governed dependency chain and record the finished parent and candidate runs in MLflow
    without reading or scoring the sealed test period.

The managed workflows now own:

- `<catalog>.<bronze_schema>.telemetry_raw`
- `<catalog>.<bronze_schema>.failure_reports_raw`
- `<catalog>.<silver_schema>.telemetry_accepted`
- `<catalog>.<silver_schema>.telemetry_quarantine`
- `<catalog>.<silver_schema>.telemetry_quality_metrics`
- `<catalog>.<silver_schema>.failure_events_accepted`
- `<catalog>.<silver_schema>.failure_events_quarantine`
- `<catalog>.<gold_schema>.loaded_cycles`
- `<catalog>.<gold_schema>.failure_horizons`
- `<catalog>.<gold_schema>.feature_snapshots_v2`
- `<catalog>.<gold_schema>.modeling_view_v2`

The [first managed Silver run record](docs/managed_silver_first_run.md) preserves the pre-run
baseline and quota-blocked attempt alongside the successful execution, observed outputs,
independent checks, and retry policy.
The [first managed Gold run record](docs/managed_gold_first_run.md) preserves the reviewed baseline,
deployment boundary, successful execution, independent SQL reconciliation, and verified unchanged
rerun.
The [first managed failure-horizon run record](docs/managed_failure_horizon_first_run.md) preserves
the first-write evidence, exact label distribution, semantic checks, and pending rerun policy.
The [first managed modelling-view v2 run record](docs/managed_modeling_view_v2_first_run.md)
preserves the pre-run gate, successful first-write evidence, Catalog metadata, independent SQL
audit, zero-insert rerun, and immutable retry policy.
The [first managed model-candidate comparison record](docs/managed_model_candidate_comparison_first_run.md)
preserves the deployment boundary, ordered workflow evidence, MLflow metrics, interpretation limits,
and next modelling gate.

## Roadmap to the portfolio demonstration

1. **Completed:** materialize and reconcile the two managed Bronze tables, including a zero-insert
   rerun.
2. **Completed:** run and reconcile the deployed managed Silver workflow for accepted,
   quarantine, and quality outputs.
3. **Completed:** run and independently reconcile the managed two-hour failure-horizon table.
4. **Completed:** the managed point-in-time feature-snapshot first write and unchanged idempotency
   rerun are independently reconciled.
5. **Completed:** independently audit the first managed modelling-view write and stop the validation
   warehouse.
6. **Completed:** prove an unchanged managed modelling-view rerun inserts zero rows and preserves
   all 15,766 rows.
7. **Completed:** deploy and run the guarded full-source comparison with MLflow while keeping the
   test period sealed.
8. **Completed:** retain the simpler candidate through a versioned development-only selection
   contract without opening the test period.
9. **In progress:** the compact validation-score profile, managed runner, and unscheduled job are
   implemented, validated, and deployed. Run it separately against the exact managed comparison
   population and reconcile the compact output. Afterward, convert reviewed validation scores into
   alert episodes and select a policy using warning lead time and false-alarm burden without opening
   the sealed test period.
10. Evaluate the frozen model and alert policy once on the sealed test period.
11. Publish dashboard-ready reliability KPIs and a Databricks SQL dashboard for maintenance users.
12. Add a short reproducible demo path and evidence-based résumé bullets.

## Technology

- Python 3.11+
- PySpark / Apache Spark 4.2
- Delta Lake 4.4
- Spark SQL and Databricks SQL
- Databricks Asset Bundles, serverless jobs, Volumes, and Unity Catalog
- pytest and Ruff
- MLflow experiment with one verified managed comparison and two nested candidate runs

Local Spark/Delta execution is verified in Ubuntu WSL with a supported JDK 17. Native Windows Spark
is not the verified path because its Hadoop layer requires an additional Windows helper binary.

## Quick start

Raw data is not stored in Git. Download MetroPT-3 from UCI and place the official CSV as described
in [data/README.md](data/README.md). The repository already contains the vetted four-row failure
reference and the checksum manifest.

Package checks can run in a normal virtual environment:

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install --upgrade pip
.venv\Scripts\python -m pip install -e ".[dev,spark]"
.venv\Scripts\python -m pytest -m "not spark"
.venv\Scripts\python -m ruff check .
.venv\Scripts\python -m ruff format --check .
```

Spark/Delta integration is run inside WSL with JDK 21:

```bash
python3 -m venv .venv-wsl
.venv-wsl/bin/python -m pip install --upgrade pip
.venv-wsl/bin/python -m pip install -e ".[dev,spark]"
export JAVA_HOME=/path/to/jdk-21
export PYSPARK_PYTHON="$PWD/.venv-wsl/bin/python"
export PYSPARK_SUBMIT_ARGS="--driver-memory 3g pyspark-shell"
.venv-wsl/bin/python -m pytest
```

Core local materialization commands:

```bash
.venv-wsl/bin/python -m railpulse.ingestion.bronze --master "local[4]"
.venv-wsl/bin/python -m railpulse.features.cycle_build --master "local[4]"
.venv-wsl/bin/python -m railpulse.features.feature_snapshot_build --master "local[4]"
.venv-wsl/bin/python -m railpulse.features.feature_snapshot_v2_build --master "local[4]"
```

The remaining profile commands and their expected evidence are linked from
[project status](docs/project_status.md).

## Repository layout

```text
configs/             Environment-independent local defaults
data/                Placement guide and vetted failure reference; raw data ignored
docs/                Contracts, decisions, profiles, project plan, and status
resources/           Databricks jobs and managed infrastructure definitions
src/railpulse/       Reusable ingestion, validation, feature, model, and evaluation code
tests/               Unit and Spark/Delta integration tests
sql/                 Checked-in analytical SQL
```

Reusable transformations belong in Python modules or checked-in SQL, not hidden notebook state.

## Dataset and evaluation guardrails

RailPulse uses the official UCI MetroPT-3 dataset, ID 791, DOI
[10.24432/C5VW3R](https://doi.org/10.24432/C5VW3R), under CC BY 4.0. The retrieved archive, CSV, and
source PDF are identified in [the dataset manifest](docs/dataset_manifest.json). Raw artifacts are
never committed.

The telemetry covers 2020-02-01 through 2020-09-01 at a nominal 10-second cadence with jitter and
material gaps. Failure reports remain separate from telemetry, and unresolved source ambiguities
are preserved.

Evaluation is chronological and failure-event-aware. Future-looking windows, centered features,
random row-level splits, full-dataset normalization, and test-set threshold selection are
prohibited. With only four published failures, conclusions must be presented as case-study evidence
rather than precise estimates of generalization or maintenance impact.

## Key documentation

- [Project status](docs/project_status.md)
- [Project plan](docs/project_plan.md)
- [Architecture decisions](docs/decisions.md)
- [Dataset manifest](docs/dataset_manifest.json)
- [Data contract](docs/data_contract.md)
- [Bronze ingestion](docs/bronze_ingestion.md)
- [First managed Silver run](docs/managed_silver_first_run.md)
- [Gold cycle build](docs/gold_cycle_build.md)
- [Failure-horizon contract](docs/failure_horizon_contract.md)
- [First managed failure-horizon run](docs/managed_failure_horizon_first_run.md)
- [Feature snapshot v1](docs/feature_snapshot_contract.md)
- [Feature snapshot v2](docs/feature_snapshot_v2_contract.md)
- [First managed feature-snapshot v2 run](docs/managed_feature_snapshot_v2_first_run.md)
- [Modelling-view v2 profile](docs/modeling_view_v2_profile.md)
- [First managed modelling-view v2 run](docs/managed_modeling_view_v2_first_run.md)
- [Evaluation protocol](docs/evaluation_protocol.md)
- [Chronological split evidence](docs/chronological_split_profile.md)
- [Model candidate comparison contract](docs/model_candidate_comparison_contract.md)
- [First managed model-candidate comparison](docs/managed_model_candidate_comparison_first_run.md)
- [Development model selection](docs/development_model_selection.md)
- [Selected-candidate validation scoring](docs/selected_candidate_validation.md)
- [Engineering baseline evidence](docs/engineering_baseline_profile.md)
- [Directional baseline evidence](docs/directional_baseline_profile.md)
