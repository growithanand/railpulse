# Databricks source-landing contract

## Status

`databricks-source-landing-v1` is defined in code and bundle configuration. Its managed Volume was
deployed and independently verified on 2026-10-03. A separate controlled upload placed exactly the
two contracted ingestion inputs in their versioned paths. Remote byte sizes and independently
downloaded SHA-256 identities match the manifest. No ingestion job has run and no table was created.

## Purpose

RailPulse needs a governed file boundary between the ignored local source artifacts and managed
Bronze tables. A Unity Catalog managed Volume provides that boundary without treating CSV files as
tables or placing data in a user-specific workspace directory.

The development object is:

```text
workspace.railpulse_bronze.source
```

Its runtime root is:

```text
/Volumes/workspace/railpulse_bronze/source
```

Databricks CLI file commands require the same path with the `dbfs:` scheme. Spark and Python jobs
will use the `/Volumes/...` form.

## Versioned source layout

Source artifacts are isolated below the manifest-backed dataset version rather than overwritten in
one mutable directory:

```text
/Volumes/workspace/railpulse_bronze/source/
└── metropt3/
    └── uci-791-aab991a970e5/
        ├── telemetry/
        │   └── MetroPT3(AirCompressor).csv
        └── reference/
            └── metropt3_failure_events.csv
```

The telemetry filename preserves the official archive member name. The failure reference is the
reviewed four-row transcription tracked in `data/reference/`. The official PDF is provenance for
that transcription but is not a runtime ingestion input.

The expected dataset version, filenames, sizes, and SHA-256 values remain authoritative in
`docs/dataset_manifest.json`. Uploading a file does not make it trusted: the first managed Bronze
job must verify its identity before reading or writing any table.

## Code contract

`DatabricksSourceLanding` combines a validated `CatalogNamespace`, a lowercase dataset version, and
a validated Volume name. It produces the qualified Unity Catalog object name, the Volume root, both
source paths, and the CLI form of a Volume path. Unsafe identifiers, traversal-like dataset
versions, and non-Volume CLI paths fail before a job can use them.

The default Volume name is configurable through the bundle variable `source_volume`; the physical
path remains environment-specific while the layout below it is versioned and deterministic.

## Local source preflight

On 2026-10-03, both local runtime inputs were read without modification and compared with
`docs/dataset_manifest.json`:

| Artifact | Actual bytes | Size contract | SHA-256 contract |
| --- | ---: | --- | --- |
| Official telemetry CSV | 218,300,507 | Match | Match |
| Reviewed failure reference | 556 | Match | Match |

The failure-reference byte count is explicit in the manifest so the same identity can be reconciled
after upload. Raw telemetry remains ignored, and no machine-local path is recorded in versioned
evidence.

## Deployment evidence

The bundle manages exactly one source Volume resource with deletion protection:

```text
workspace.railpulse_bronze.source
```

Before deployment, strict validation returned `Validation OK!`. The reviewed plan reported one
Volume creation, two existing job updates, and no deletion. Deployment then reported the same
resource changes: one created, two changed, and none deleted.

An independent Catalog API read confirmed:

| Property | Verified value |
| --- | --- |
| Catalog | `workspace` |
| Schema | `railpulse_bronze` |
| Volume | `source` |
| Type | `MANAGED` |
| Comment | `Governed landing storage for checksum-verified RailPulse source files.` |

A file listing of `dbfs:/Volumes/workspace/railpulse_bronze/source` returned an empty result. The
workspace files uploaded by bundle deployment contain packaged code and configuration under the
bundle workspace root; they are not dataset files in this Volume.

The post-deployment plan reports the Volume unchanged with no creation or deletion pending. It
continues to show updates for the two jobs because the bundle builds a dynamically versioned wheel;
no second deployment was performed.

Required upload access is `USE CATALOG`, `USE SCHEMA`, and `WRITE VOLUME`. Credentials and OAuth
profiles remain machine-local. Local source paths, workspace URLs, user identities, and deployment
state must not be committed.

## Controlled source-upload evidence

On 2026-10-03, the two locally verified inputs were uploaded separately from bundle deployment and
without enabling overwrite. Each contracted directory then contained exactly one regular file:

| Artifact | Remote bytes | Directory file count | Downloaded SHA-256 |
| --- | ---: | ---: | --- |
| Official telemetry CSV | 218,300,507 | 1 | Manifest match |
| Reviewed failure reference | 556 | 1 | Manifest match |

Each remote file was downloaded to an ignored temporary directory and hashed independently. Both
downloaded byte counts and SHA-256 values matched the manifest, after which the temporary copies
were removed. This proves the transferred contents rather than relying only on CLI success output.
No job ran and no Bronze table was created during the upload checkpoint.

## Deferred ingestion boundary

This contract does not create Bronze tables or choose batch versus streaming ingestion. The next
job increment will:

1. accept the two contracted Volume paths as managed parameters;
2. verify file size and SHA-256 against the dataset manifest;
3. preserve raw tokens and source metadata;
4. write fully qualified Bronze tables through the shared catalog namespace;
5. reconcile the first write and an identical rerun.

The 218 MB telemetry file does not require distributed computing. Spark remains appropriate here as
a production-pattern implementation that will share semantics with later incremental replay.

## References

- [Databricks Unity Catalog Volumes](https://docs.databricks.com/aws/en/volumes/)
- [Work with files in Volumes](https://docs.databricks.com/aws/en/volumes/volume-files)
- [Declarative Automation Bundles resources](https://docs.databricks.com/aws/en/dev-tools/bundles/resources)
