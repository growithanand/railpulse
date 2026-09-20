# RailPulse

**Predictive Maintenance and Analytics Platform for Metro Compressor Systems**

RailPulse is an industrial data and machine-learning portfolio project based on the
MetroPT-3 compressor telemetry dataset. Its central question is:

> Can abnormal compressor behaviour be identified early enough to support maintenance
> intervention—ideally at least two hours before a recorded failure—without producing an
> unacceptable number of false alarms?

## Current status

RailPulse currently has a locally verified, end-to-end data-engineering path from the official raw
sources to versioned Gold feature snapshots. The implementation includes checksum-backed source
provenance, idempotent Bronze ingestion, typed Silver validation and quarantine, data-quality
metrics, loaded-operation cycles, causal two-hour failure horizons, past-only motor-current
features, and label-independent feature eligibility.

The `motor-current-15m-snapshot-build-v1` command now materializes one immutable
`gold.feature_snapshots` Delta row per loaded cycle. Its complete-source run inserted 15,766 rows;
an identical rerun inserted zero and reconciled all 15,766 rows as unchanged. Generated Delta data
remains excluded from Git.

### Verified complete-source evidence

| Evidence | Verified result |
| --- | ---: |
| Accepted telemetry records | 1,516,948 |
| Gold loaded cycles | 15,766 |
| Available 15-minute motor-current features | 15,703 |
| Eligible feature snapshots | 15,418 |
| Ineligible feature snapshots | 348 |
| Material-window-gap exclusions | 285 |
| Missing prediction boundaries | 63 |
| Positive two-hour cycle labels | 22 |
| Accepted published failure events represented by positive cycles | 3 of 4 |

The project has not trained or evaluated a predictive model yet. Chronological modelling views,
baseline and anomaly models, alert episodes, event-level metrics, MLflow tracking, Databricks jobs,
Unity Catalog registration, and Databricks SQL dashboards remain planned. Consequently, no model
accuracy, warning-lead-time, false-alarm, or maintenance-impact claim is currently made.

See [the project status](docs/project_status.md) for verified environment details and
[the project plan](docs/project_plan.md) for delivery phases.

## Intended user and outcome

RailPulse is designed for a maintenance or reliability engineer who needs to inspect equipment
condition, review alert episodes, compare alerts with recorded failures, measure useful warning
lead time and false-alarm frequency, and compare maintenance policies.

The planned flow is:

```text
Official MetroPT-3 telemetry + separately documented failure reports
                              |
                              v
                     Bronze Delta tables
                  raw values + source metadata
                              |
                              v
                     Silver Delta tables
              validation + quarantine + quality metrics
                              |
                              v
                      Gold data products
          cycles + temporal features + horizons + reliability KPIs
                 + immutable feature snapshots
                              |
                              v
               baseline/anomaly scores -> alert episodes
                              |
                              v
                Databricks SQL decision-support dashboard
```

This is a production-pattern demonstration. Approximately 1.5 million records at a nominal
10-second cadence do not inherently require distributed computing; Spark is used to demonstrate
scalable, incremental, and Databricks-compatible engineering practices.

## Implemented now

- A Python `src` package with clear ingestion, validation, feature, model, evaluation, and
  monitoring boundaries.
- Checked-in TOML defaults loaded through a typed, validated configuration module.
- Deterministic package and configuration tests.
- A checksum-backed official dataset manifest, sensor dictionary, failure-event reference, and
  streaming CSV contract inspector.
- Explicit all-string Bronze schemas, source/checksum metadata, corrupt-record capture, deterministic
  identifiers, Delta `MERGE` idempotency, and row reconciliation.
- Locally materialized `bronze.telemetry_raw` and `bronze.failure_reports_raw` Delta tables; generated
  table storage remains ignored.
- Typed Silver telemetry with explicit parsing, domain, range, duplicate, and timestamp-sequence
  quality reasons, plus separate accepted and quarantine Delta outputs.
- Typed Silver failure events with structural rejection reasons and separate accepted and quarantine
  Delta outputs.
- Versioned telemetry quality summaries with reconciled row, gap, and rejection-reason counts.
- Causal loaded-cycle boundaries, deterministic segment identifiers, cycle-level aggregation, and
  a reproducible full-source profile using the documented `DV_eletric` operating signal.
- Monotonic `gold.loaded_cycles` Delta merge semantics that insert new IDs, close right-censored
  cycles without changing their IDs, and reject regressive or incompatible snapshots.
- A source-bound `loaded-cycle-build-v1` command with reconciled full-source first-write and rerun
  evidence for the local `gold.loaded_cycles` table.
- A tested Spark SQL inspection that reconciles censoring-group counts and descriptive duration
  tails without assigning health or failure meaning.
- A versioned cycle-to-failure horizon transformation that labels strictly future failure starts at
  observed cycle stops and preserves in-failure, open-cycle, and incomplete-horizon rows as null.
- A source-lineage-aware, read-only full-source horizon profile that reconciles cycle statuses and
  positive-cycle allocation across every accepted failure event.
- A tested 15-minute motor-current feature window with strict past-only event-time boundaries,
  observation-support metadata, and explicit missing-feature statuses.
- A source-lineage-aware, read-only full-source motor-current profile that reconciles feature
  availability, observation-support percentiles, independent count and span tails, their strict
  membership overlap, deterministic weakest and one-sided examples, and internal-gap evidence for
  count-only windows. It also compares explicit gap intersection with the percentile-tail union
  and retains bounded examples and complete disagreement-group summaries without setting a
  training threshold. A reconciled sensitivity table measures strict-tail capture as the maximum
  in-window gap increases.
- A read-only policy comparison that joins available motor-current windows to two-hour horizon
  labels and reconciles retained and excluded positive, negative, and null-label counts for five
  fixed coverage candidates without selecting or persisting a rule.
- A versioned, label-independent motor-current eligibility transformation that uses the existing
  20-second material-gap boundary, fails closed on missing context, and emits explicit reasons.
- A source-lineage-aware, read-only full-source eligibility profile that reconciles one status per
  Gold cycle and one exact reason per ineligible cycle without loading failure labels.
- A versioned Gold feature-snapshot schema and deterministic builder that retain past-only features,
  label-independent eligibility, and dataset/source/ingestion lineage while excluding future labels.
- An insert-only `gold.feature_snapshots` Delta writer that rejects conflicting historical rows and
  incompatible schemas, reconciles write counts, and treats identical reruns as unchanged.
- A full-source snapshot build command with verified first-write and zero-insert rerun evidence for
  all 15,766 Gold cycles.
- Data and artifact exclusion rules that allow only the placement guide and vetted reference
  metadata to be versioned under `data/`.
- A minimal Databricks Asset Bundle entry point. It has not been deployed or CLI-validated.
- Durable project planning, status, and architecture-decision documentation.

## Runtime stack

The current pipeline uses Python, PySpark 4.2.0, Apache Spark 4.2.0, Spark SQL, Delta Lake 4.4.0,
pytest, and Ruff. Later phases will add production Databricks resources, MLflow, Structured
Streaming, and GitHub Actions where each is justified. Streaming will replay historical files; it
will not be described as a live train connection.

The local Spark/Delta integration is verified in Ubuntu WSL with Python 3.12 and Eclipse Temurin JDK
21. Native Windows Spark is not the verified path because Hadoop requires a separate Windows helper.
Databricks deployment and runtime compatibility have not been tested.

The current code is ready for an initial Databricks integration increment, but the complete
decision-support dashboard depends on stable chronological modelling, prediction, alert, and
event-evaluation tables. The planned order is modelling-view contract, baseline evaluation,
dashboard-ready aggregates, and then the polished Databricks SQL dashboard.

## Quick start

Python 3.11 or newer is required. Package-only checks can run in a normal virtual environment:

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install --upgrade pip
.venv\Scripts\python -m pip install -e ".[dev,spark]"
.venv\Scripts\python -m pytest -m "not spark"
.venv\Scripts\python -m ruff check .
.venv\Scripts\python -m ruff format --check .
```

Spark/Delta integration and ingestion are run inside Ubuntu WSL with a supported JDK 21. From the
repository mounted in WSL:

```bash
python3 -m venv .venv-wsl
.venv-wsl/bin/python -m pip install --upgrade pip
.venv-wsl/bin/python -m pip install -e ".[dev,spark]"
export JAVA_HOME=/path/to/a/jdk-21
export PYSPARK_PYTHON="$PWD/.venv-wsl/bin/python"
export PYSPARK_SUBMIT_ARGS="--driver-memory 3g --conf spark.driver.extraJavaOptions=-Djdk.lang.Process.launchMechanism=VFORK pyspark-shell"
.venv-wsl/bin/python -m pytest
```

The local JDK and virtual environment belong under ignored `.tools/` and `.venv-wsl/` when kept in
the project. See [Bronze ingestion](docs/bronze_ingestion.md) for source placement, execution, table
locations, idempotency behavior, and verified counts.

To reproduce the complete-file source inspection after downloading the official CSV into ignored
raw storage:

```powershell
.venv\Scripts\python -m railpulse.validation.metropt3 `
  "data\raw\source\metropt3-uci-791\MetroPT3(AirCompressor).csv"
```

After source inspection, materialize or safely reconcile both Bronze tables:

```bash
.venv-wsl/bin/python -m railpulse.ingestion.bronze --master "local[4]"
```

Then derive, reconcile, and persist the full-source Gold cycle table:

```bash
.venv-wsl/bin/python -m railpulse.features.cycle_build --master "local[4]"
```

Inspect censoring categories and observed-duration tails without modifying Gold data:

```bash
.venv-wsl/bin/python -m railpulse.features.cycle_inspection --master "local[2]"
```

Apply and inspect the default two-hour failure horizon without persisting another table:

```bash
.venv-wsl/bin/python -m railpulse.features.failure_horizon_profile --master "local[4]"
```

Apply and inspect 15-minute motor-current feature coverage without persisting a feature table:

```bash
.venv-wsl/bin/python -m railpulse.features.temporal_feature_profile --master "local[4]"
```

Compare diagnostic coverage policies with two-hour horizon-label retention without selecting or
persisting a rule:

```bash
.venv-wsl/bin/python -m railpulse.features.coverage_policy_profile --master "local[4]"
```

Apply and reconcile the selected label-independent eligibility contract without persisting output:

```bash
.venv-wsl/bin/python -m railpulse.features.eligibility_profile --master "local[4]"
```

Materialize the versioned Gold feature snapshot after the eligibility profile has been reviewed:

```bash
.venv-wsl/bin/python -m railpulse.features.feature_snapshot_build --master "local[4]"
```

## Repository layout

```text
configs/             Checked-in, environment-independent defaults
data/                Placement instructions; actual data is ignored
docs/                Plans, decisions, contracts, protocols, and status
src/railpulse/       Reusable production code
tests/               Deterministic unit and integration tests
sql/                 Spark/Databricks SQL introduced with data products
notebooks/           Thin exploration and demonstration entry points
dashboards/          Dashboard queries and specifications
resources/           Databricks deployment resources
```

Reusable transformations belong in Python modules or SQL, not hidden notebook state.

## Verified dataset and evaluation guardrails

RailPulse uses the [official UCI MetroPT-3 dataset](https://archive.ics.uci.edu/dataset/791/metropt%2B3%2B),
ID 791 and DOI [10.24432/C5VW3R](https://doi.org/10.24432/C5VW3R), under CC BY 4.0. The retrieved
archive, CSV, and PDF are identified by SHA-256 in the
[dataset manifest](docs/dataset_manifest.json). Raw artifacts are never committed.

The actual CSV contains 1,516,948 rows, 15 sensor signals, and timestamps from 2020-02-01 through
2020-09-01. Its cadence is nominally 10 seconds with jitter and material gaps. Published failures
are stored separately from telemetry and retain unresolved source ambiguities.

Evaluation will be chronological and failure-event-aware. Future-looking windows, centered
features, random row-level splits, full-dataset normalization, and test-set threshold selection are
prohibited. Sparse failure events will be reported honestly; inconclusive results are acceptable.

## Documentation

- [Project plan](docs/project_plan.md)
- [Project status](docs/project_status.md)
- [Architecture decisions](docs/decisions.md)
- [Data placement](data/README.md)
- [Dataset manifest](docs/dataset_manifest.json)
- [Data contract](docs/data_contract.md)
- [Data dictionary](docs/data_dictionary.md)
- [Bronze ingestion](docs/bronze_ingestion.md)
- [Full-source loaded-cycle profile](docs/cycle_profile.md)
- [Gold loaded-cycle build](docs/gold_cycle_build.md)
- [Gold loaded-cycle inspection](docs/gold_cycle_inspection.md)
- [Cycle-to-failure horizon contract](docs/failure_horizon_contract.md)
- [Full-source failure-horizon profile](docs/failure_horizon_profile.md)
- [Past-only motor-current feature contract](docs/temporal_feature_contract.md)
- [Full-source motor-current feature profile](docs/temporal_feature_profile.md)
- [Motor-current coverage-policy profile](docs/coverage_policy_profile.md)
- [Motor-current feature eligibility contract](docs/feature_eligibility_contract.md)
- [Full-source motor-current eligibility profile](docs/eligibility_profile.md)
- [Gold feature-snapshot schema contract](docs/feature_snapshot_contract.md)
- [Gold feature-snapshot build](docs/feature_snapshot_build.md)

An evaluation protocol, dashboard instructions, a demo script, and evidence-based résumé bullets
will be added only when their supporting phases are implemented.
