"""Read-only verification of manifest-backed files in the managed source Volume."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Protocol

from railpulse.catalog import CatalogNamespace
from railpulse.landing import (
    TELEMETRY_SOURCE_FILENAME,
    DatabricksSourceLanding,
)

SOURCE_PREFLIGHT_CONTRACT_VERSION = "databricks-source-preflight-v1"


class SourcePreflightError(RuntimeError):
    """Raised when a landed source cannot be reconciled without writing data."""


@dataclass(frozen=True)
class SourceContracts:
    """Manifest identities required by the managed source boundary."""

    dataset_version: str
    telemetry_size_bytes: int
    telemetry_sha256: str
    failure_reference_size_bytes: int
    failure_reference_sha256: str


@dataclass(frozen=True)
class SourceFileEvidence:
    """Verified identity for one landed source file."""

    artifact: str
    volume_path: str
    size_bytes: int
    sha256: str


@dataclass(frozen=True)
class SourcePreflightResult:
    """Versioned, non-writing reconciliation evidence for the source Volume."""

    contract_version: str
    landing_contract_version: str
    dataset_version: str
    qualified_volume: str
    files: tuple[SourceFileEvidence, ...]


class SourceFileReader(Protocol):
    """Minimal read-only filesystem boundary used by source reconciliation."""

    def size_bytes(self, path: str) -> int: ...

    def sha256(self, path: str) -> str: ...


class VolumeFileReader:
    """Read file metadata and bytes through a mounted Unity Catalog Volume."""

    def size_bytes(self, path: str) -> int:
        source = Path(path)
        if not source.is_file():
            raise SourcePreflightError(f"Expected landed source file is unavailable: {path}")
        return source.stat().st_size

    def sha256(self, path: str) -> str:
        source = Path(path)
        digest = hashlib.sha256()
        try:
            with source.open("rb") as stream:
                for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
                    digest.update(chunk)
        except OSError as error:
            raise SourcePreflightError(f"Unable to read landed source file: {path}") from error
        return digest.hexdigest()


def _required_size(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise SourcePreflightError(f"Manifest field {field_name} must be a positive integer.")
    return value


def _required_sha256(value: object, *, field_name: str) -> str:
    normalized = str(value).strip().lower() if value is not None else ""
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise SourcePreflightError(f"Manifest field {field_name} must be a SHA-256 value.")
    return normalized


def load_source_contracts(manifest_path: str | Path) -> SourceContracts:
    """Load only the two runtime source identities from the committed manifest."""

    try:
        manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
        dataset_version_value = manifest["dataset"]["version_id"]
        members = manifest["archive"]["members"]
        telemetry_matches = [
            member
            for member in members
            if isinstance(member, dict) and member.get("name") == TELEMETRY_SOURCE_FILENAME
        ]
        if len(telemetry_matches) != 1:
            raise SourcePreflightError(
                "Manifest must contain exactly one contracted telemetry member."
            )
        telemetry = telemetry_matches[0]
        failures = manifest["failure_reference"]
    except (OSError, KeyError, TypeError, json.JSONDecodeError) as error:
        raise SourcePreflightError("Unable to load the managed source manifest.") from error

    if not isinstance(dataset_version_value, str) or not dataset_version_value.strip():
        raise SourcePreflightError("Manifest dataset version must not be empty.")
    if not isinstance(failures, dict):
        raise SourcePreflightError("Manifest failure reference must be an object.")
    return SourceContracts(
        dataset_version=dataset_version_value.strip(),
        telemetry_size_bytes=_required_size(
            telemetry.get("size_bytes"), field_name="archive.members[].size_bytes"
        ),
        telemetry_sha256=_required_sha256(
            telemetry.get("sha256"), field_name="archive.members[].sha256"
        ),
        failure_reference_size_bytes=_required_size(
            failures.get("size_bytes"), field_name="failure_reference.size_bytes"
        ),
        failure_reference_sha256=_required_sha256(
            failures.get("transcription_sha256"),
            field_name="failure_reference.transcription_sha256",
        ),
    )


def reconcile_landed_sources(
    landing: DatabricksSourceLanding,
    contracts: SourceContracts,
    reader: SourceFileReader,
) -> SourcePreflightResult:
    """Verify both landed files against exact manifest identities without writing data."""

    if landing.dataset_version != contracts.dataset_version:
        raise SourcePreflightError(
            "Source landing dataset version does not match the managed source manifest."
        )

    requested = (
        (
            "telemetry",
            landing.telemetry_path,
            contracts.telemetry_size_bytes,
            contracts.telemetry_sha256,
        ),
        (
            "failure_reference",
            landing.failure_reference_path,
            contracts.failure_reference_size_bytes,
            contracts.failure_reference_sha256,
        ),
    )
    evidence: list[SourceFileEvidence] = []
    for artifact, volume_path, expected_size, expected_sha256 in requested:
        try:
            actual_size = reader.size_bytes(volume_path)
        except OSError as error:
            raise SourcePreflightError(
                f"Unable to inspect landed source file: {volume_path}"
            ) from error
        if actual_size != expected_size:
            raise SourcePreflightError(
                f"Size mismatch for {artifact}: expected {expected_size}, received {actual_size}."
            )
        try:
            actual_sha256 = reader.sha256(volume_path).strip().lower()
        except OSError as error:
            raise SourcePreflightError(
                f"Unable to hash landed source file: {volume_path}"
            ) from error
        if actual_sha256 != expected_sha256:
            raise SourcePreflightError(f"SHA-256 mismatch for {artifact}.")
        evidence.append(
            SourceFileEvidence(
                artifact=artifact,
                volume_path=volume_path,
                size_bytes=actual_size,
                sha256=actual_sha256,
            )
        )

    return SourcePreflightResult(
        contract_version=SOURCE_PREFLIGHT_CONTRACT_VERSION,
        landing_contract_version=landing.contract_version,
        dataset_version=contracts.dataset_version,
        qualified_volume=landing.qualified_volume,
        files=tuple(evidence),
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--bronze_schema", default="railpulse_bronze")
    parser.add_argument("--silver_schema", default="railpulse_silver")
    parser.add_argument("--gold_schema", default="railpulse_gold")
    parser.add_argument("--source_volume", default="source")
    parser.add_argument("--manifest_path", default="docs/dataset_manifest.json")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run managed source reconciliation and print one machine-readable result."""

    args = _parse_args(argv)
    namespace = CatalogNamespace(
        catalog=args.catalog,
        bronze_schema=args.bronze_schema,
        silver_schema=args.silver_schema,
        gold_schema=args.gold_schema,
    )
    contracts = load_source_contracts(args.manifest_path)
    landing = DatabricksSourceLanding(
        namespace=namespace,
        dataset_version=contracts.dataset_version,
        volume_name=args.source_volume,
    )
    result = reconcile_landed_sources(landing, contracts, VolumeFileReader())
    print(json.dumps(asdict(result), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
