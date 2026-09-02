"""Ingest source-aligned MetroPT-3 records into idempotent Bronze Delta tables."""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from railpulse.config import RailPulseConfig, load_config
from railpulse.ingestion.schemas import (
    CORRUPT_RECORD_FIELD,
    FAILURE_RAW_FIELDS,
    TELEMETRY_RAW_FIELDS,
    failure_csv_schema,
    telemetry_csv_schema,
)
from railpulse.spark import create_local_spark_session

if TYPE_CHECKING:
    from collections.abc import Sequence

TELEMETRY_TABLE = "telemetry_raw"
FAILURE_TABLE = "failure_reports_raw"
RECORD_ID_FIELD = "record_id"


class BronzeIngestionError(RuntimeError):
    """Raised when Bronze cannot preserve and reconcile a source artifact safely."""


@dataclass(frozen=True)
class DatasetArtifacts:
    """Checksums and version identifiers required by Bronze ingestion."""

    dataset_version: str
    telemetry_sha256: str
    failure_transcription_sha256: str
    failure_source_document_sha256: str


@dataclass(frozen=True)
class BronzeIngestionResult:
    """Count reconciliation and lineage for one Bronze table write."""

    table_name: str
    target_path: str
    source_sha256: str
    ingestion_batch_id: str
    source_row_count: int
    inserted_row_count: int
    matched_target_row_count: int
    target_row_count_before: int
    target_row_count_after: int


def file_sha256(path: str | Path) -> str:
    """Return the SHA-256 digest of a file without loading it into memory."""

    source_path = Path(path).expanduser().resolve()
    digest = hashlib.sha256()
    with source_path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_file_sha256(path: str | Path, expected_sha256: str) -> str:
    """Verify one source file and return its normalized digest."""

    normalized_expected = expected_sha256.strip().lower()
    if len(normalized_expected) != 64 or any(
        character not in "0123456789abcdef" for character in normalized_expected
    ):
        raise BronzeIngestionError(f"Invalid expected SHA-256 value: {expected_sha256!r}")

    actual = file_sha256(path)
    if actual != normalized_expected:
        raise BronzeIngestionError(
            f"SHA-256 mismatch for {Path(path)}: expected {normalized_expected}, received {actual}"
        )
    return actual


def load_dataset_artifacts(manifest_path: str | Path) -> DatasetArtifacts:
    """Load the exact source identities required by the Bronze boundary."""

    path = Path(manifest_path).expanduser().resolve()
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
        members = {member["name"]: member for member in manifest["archive"]["members"]}
        failure_reference = manifest["failure_reference"]
        return DatasetArtifacts(
            dataset_version=manifest["dataset"]["version_id"],
            telemetry_sha256=members["MetroPT3(AirCompressor).csv"]["sha256"],
            failure_transcription_sha256=failure_reference["transcription_sha256"],
            failure_source_document_sha256=members["Data Description_Metro.pdf"]["sha256"],
        )
    except (OSError, KeyError, TypeError, json.JSONDecodeError) as error:
        raise BronzeIngestionError(
            f"Unable to load Bronze source identities from {path}"
        ) from error


def deterministic_batch_id(dataset_version: str, table_name: str, source_sha256: str) -> str:
    """Return a stable batch identifier for one logical table/source combination."""

    material = f"{dataset_version}|{table_name}|{source_sha256.lower()}".encode()
    return hashlib.sha256(material).hexdigest()


def bronze_table_path(config: RailPulseConfig, table_name: str) -> Path:
    """Resolve a local path corresponding to a logical Bronze table."""

    return config.paths.delta / config.schemas.bronze / table_name


def _read_raw_csv(
    spark: SparkSession,
    source_path: Path,
    *,
    schema: StructType,
    raw_field_names: tuple[str, ...],
) -> DataFrame:
    source = (
        spark.read.format("csv")
        .schema(schema)
        .option("header", "true")
        .option("enforceSchema", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_FIELD)
        .option("multiLine", "false")
        .option("encoding", "UTF-8")
        .load(str(source_path))
    )
    with_metadata = source.select("*", F.col("_metadata").alias("_source_metadata"))
    return with_metadata.toDF(
        *raw_field_names,
        CORRUPT_RECORD_FIELD,
        "_source_metadata",
    )


def _lineage_columns(
    *,
    table_name: str,
    raw_key_field: str,
    source_sha256: str,
    dataset_version: str,
    ingested_at: datetime,
) -> list[Column]:
    batch_id = deterministic_batch_id(dataset_version, table_name, source_sha256)
    if ingested_at.tzinfo is None:
        raise BronzeIngestionError("ingested_at must include an explicit timezone")
    ingestion_time_utc = ingested_at.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")
    source_key = F.coalesce(
        F.col(raw_key_field),
        F.sha2(F.coalesce(F.col(CORRUPT_RECORD_FIELD), F.lit("")), 256),
    )
    return [
        F.sha2(
            F.concat_ws("|", F.lit(table_name), F.lit(source_sha256), source_key),
            256,
        ).alias(RECORD_ID_FIELD),
        F.col("_source_metadata.file_name").alias("source_filename"),
        F.lit(source_sha256).alias("source_sha256"),
        F.col("_source_metadata.file_modification_time").alias("source_modified_at"),
        F.to_timestamp(
            F.lit(ingestion_time_utc),
            "yyyy-MM-dd HH:mm:ss.SSSSSS",
        ).alias("ingested_at"),
        F.lit(batch_id).alias("ingestion_batch_id"),
        F.lit(dataset_version).alias("dataset_version"),
    ]


def read_telemetry_bronze(
    spark: SparkSession,
    source_path: str | Path,
    *,
    source_sha256: str,
    dataset_version: str,
    ingested_at: datetime | None = None,
) -> DataFrame:
    """Read telemetry as raw strings and append traceable Bronze metadata."""

    path = Path(source_path).expanduser().resolve()
    ingestion_time = ingested_at or datetime.now(UTC)
    raw = _read_raw_csv(
        spark,
        path,
        schema=telemetry_csv_schema(),
        raw_field_names=TELEMETRY_RAW_FIELDS,
    )
    return raw.select(
        *TELEMETRY_RAW_FIELDS,
        *_lineage_columns(
            table_name=TELEMETRY_TABLE,
            raw_key_field="source_index_raw",
            source_sha256=source_sha256,
            dataset_version=dataset_version,
            ingested_at=ingestion_time,
        ),
        CORRUPT_RECORD_FIELD,
    )


def read_failure_reports_bronze(
    spark: SparkSession,
    source_path: str | Path,
    *,
    source_sha256: str,
    source_document_sha256: str,
    dataset_version: str,
    ingested_at: datetime | None = None,
) -> DataFrame:
    """Read the separate failure transcription and retain its source-document identity."""

    path = Path(source_path).expanduser().resolve()
    ingestion_time = ingested_at or datetime.now(UTC)
    raw = _read_raw_csv(
        spark,
        path,
        schema=failure_csv_schema(),
        raw_field_names=FAILURE_RAW_FIELDS,
    )
    return raw.select(
        *FAILURE_RAW_FIELDS,
        *_lineage_columns(
            table_name=FAILURE_TABLE,
            raw_key_field="source_row_raw",
            source_sha256=source_sha256,
            dataset_version=dataset_version,
            ingested_at=ingestion_time,
        ),
        F.lit(source_document_sha256).alias("source_document_sha256"),
        CORRUPT_RECORD_FIELD,
    )


def _assert_source_keys_are_safe(frame: DataFrame, table_name: str) -> None:
    missing_id = frame.where(
        F.col(RECORD_ID_FIELD).isNull() | (F.length(F.col(RECORD_ID_FIELD)) == 0)
    ).limit(1)
    if missing_id.count():
        raise BronzeIngestionError(f"{table_name} contains a missing Bronze record ID")

    duplicate = (
        frame.groupBy(RECORD_ID_FIELD)
        .count()
        .where(F.col("count") > 1)
        .select(RECORD_ID_FIELD, "count")
        .limit(1)
        .collect()
    )
    if duplicate:
        record = duplicate[0]
        raise BronzeIngestionError(
            f"{table_name} contains duplicate source key {record[RECORD_ID_FIELD]} "
            f"({record['count']} records)"
        )


def merge_bronze_records(
    frame: DataFrame,
    target_path: str | Path,
    *,
    table_name: str,
    source_sha256: str,
    dataset_version: str,
) -> BronzeIngestionResult:
    """Insert unseen records into a path-backed Delta table and reconcile counts."""

    path = Path(target_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path_string = str(path)
    spark = frame.sparkSession
    cached = frame.persist(StorageLevel.DISK_ONLY)

    try:
        source_count = cached.count()
        if source_count == 0:
            raise BronzeIngestionError(f"{table_name} source contains no data rows")
        _assert_source_keys_are_safe(cached, table_name)

        target_exists = DeltaTable.isDeltaTable(spark, path_string)
        before_count = spark.read.format("delta").load(path_string).count() if target_exists else 0

        if target_exists:
            (
                DeltaTable.forPath(spark, path_string)
                .alias("target")
                .merge(
                    cached.alias("source"),
                    f"target.{RECORD_ID_FIELD} = source.{RECORD_ID_FIELD}",
                )
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            cached.write.format("delta").mode("errorifexists").save(path_string)

        target = spark.read.format("delta").load(path_string)
        after_count = target.count()
        inserted_count = after_count - before_count
        unmatched_count = (
            cached.select(RECORD_ID_FIELD)
            .join(
                target.select(RECORD_ID_FIELD),
                on=RECORD_ID_FIELD,
                how="left_anti",
            )
            .limit(1)
            .count()
        )

        if inserted_count < 0 or inserted_count > source_count or unmatched_count:
            raise BronzeIngestionError(
                f"{table_name} reconciliation failed: source={source_count}, "
                f"unmatched={unmatched_count}, before={before_count}, after={after_count}"
            )

        return BronzeIngestionResult(
            table_name=f"bronze.{table_name}",
            target_path=path_string,
            source_sha256=source_sha256,
            ingestion_batch_id=deterministic_batch_id(
                dataset_version,
                table_name,
                source_sha256,
            ),
            source_row_count=source_count,
            inserted_row_count=inserted_count,
            matched_target_row_count=source_count,
            target_row_count_before=before_count,
            target_row_count_after=after_count,
        )
    finally:
        cached.unpersist()


def ingest_telemetry(
    spark: SparkSession,
    config: RailPulseConfig,
    source_path: str | Path,
    *,
    expected_sha256: str,
    ingested_at: datetime | None = None,
) -> BronzeIngestionResult:
    """Verify and ingest the telemetry artifact into ``bronze.telemetry_raw``."""

    source_sha256 = verify_file_sha256(source_path, expected_sha256)
    frame = read_telemetry_bronze(
        spark,
        source_path,
        source_sha256=source_sha256,
        dataset_version=config.dataset_version,
        ingested_at=ingested_at,
    )
    return merge_bronze_records(
        frame,
        bronze_table_path(config, TELEMETRY_TABLE),
        table_name=TELEMETRY_TABLE,
        source_sha256=source_sha256,
        dataset_version=config.dataset_version,
    )


def ingest_failure_reports(
    spark: SparkSession,
    config: RailPulseConfig,
    source_path: str | Path,
    *,
    expected_sha256: str,
    source_document_sha256: str,
    ingested_at: datetime | None = None,
) -> BronzeIngestionResult:
    """Verify and ingest the separate failure reference into its Bronze table."""

    source_sha256 = verify_file_sha256(source_path, expected_sha256)
    frame = read_failure_reports_bronze(
        spark,
        source_path,
        source_sha256=source_sha256,
        source_document_sha256=source_document_sha256,
        dataset_version=config.dataset_version,
        ingested_at=ingested_at,
    )
    return merge_bronze_records(
        frame,
        bronze_table_path(config, FAILURE_TABLE),
        table_name=FAILURE_TABLE,
        source_sha256=source_sha256,
        dataset_version=config.dataset_version,
    )


def _default_telemetry_path(config: RailPulseConfig) -> Path:
    return config.paths.raw_data / "source" / "metropt3-uci-791" / "MetroPT3(AirCompressor).csv"


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/default.toml"))
    parser.add_argument("--manifest", type=Path, default=Path("docs/dataset_manifest.json"))
    parser.add_argument("--telemetry", type=Path)
    parser.add_argument("--failures", type=Path)
    parser.add_argument("--master", default="local[2]")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the verified telemetry and failure-reference Bronze ingestion."""

    args = _parse_args(argv)
    config = load_config(args.config)
    artifacts = load_dataset_artifacts(args.manifest)
    if config.dataset_version != artifacts.dataset_version:
        raise BronzeIngestionError(
            "Configured dataset version does not match the verified dataset manifest"
        )

    telemetry_path = args.telemetry or _default_telemetry_path(config)
    failure_path = args.failures or (
        config.project_root / "data" / "reference" / "metropt3_failure_events.csv"
    )
    spark = create_local_spark_session(
        "railpulse-bronze-ingestion",
        master=args.master,
        warehouse_dir=config.project_root / "spark-warehouse",
    )
    try:
        results = [
            ingest_telemetry(
                spark,
                config,
                telemetry_path,
                expected_sha256=artifacts.telemetry_sha256,
            ),
            ingest_failure_reports(
                spark,
                config,
                failure_path,
                expected_sha256=artifacts.failure_transcription_sha256,
                source_document_sha256=artifacts.failure_source_document_sha256,
            ),
        ]
        print(json.dumps([asdict(result) for result in results], indent=2, sort_keys=True))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
