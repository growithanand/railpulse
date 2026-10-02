"""Governed source-landing paths for Databricks ingestion."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import PurePosixPath

from railpulse.catalog import CatalogNamespace

SOURCE_LANDING_CONTRACT_VERSION = "databricks-source-landing-v1"
DEFAULT_SOURCE_VOLUME = "source"
TELEMETRY_SOURCE_FILENAME = "MetroPT3(AirCompressor).csv"
FAILURE_REFERENCE_FILENAME = "metropt3_failure_events.csv"

_DATASET_VERSION_PATTERN = re.compile(r"^[a-z0-9]+(?:[._-][a-z0-9]+)*$")


class SourceLandingError(ValueError):
    """Raised when a governed source-landing path would be unsafe or ambiguous."""


@dataclass(frozen=True)
class DatabricksSourceLanding:
    """Versioned paths below one managed Unity Catalog Volume."""

    namespace: CatalogNamespace
    dataset_version: str
    volume_name: str = DEFAULT_SOURCE_VOLUME

    def __post_init__(self) -> None:
        self.namespace.volume("bronze", self.volume_name)
        if not isinstance(self.dataset_version, str) or not _DATASET_VERSION_PATTERN.fullmatch(
            self.dataset_version
        ):
            raise SourceLandingError(
                "dataset_version must contain lowercase letters, digits, and single '.', '_', "
                "or '-' separators"
            )

    @property
    def contract_version(self) -> str:
        """Return the immutable source-landing contract version."""

        return SOURCE_LANDING_CONTRACT_VERSION

    @property
    def qualified_volume(self) -> str:
        """Return the three-level Unity Catalog name of the managed Volume."""

        return self.namespace.volume("bronze", self.volume_name)

    @property
    def volume_root(self) -> str:
        """Return the runtime filesystem root for the managed Volume."""

        return str(
            PurePosixPath("/Volumes")
            / self.namespace.catalog
            / self.namespace.bronze_schema
            / self.volume_name
        )

    @property
    def dataset_root(self) -> str:
        """Return the immutable directory reserved for this source version."""

        return str(PurePosixPath(self.volume_root) / "metropt3" / self.dataset_version)

    @property
    def telemetry_path(self) -> str:
        """Return the expected official telemetry CSV path."""

        return str(PurePosixPath(self.dataset_root) / "telemetry" / TELEMETRY_SOURCE_FILENAME)

    @property
    def failure_reference_path(self) -> str:
        """Return the expected versioned failure-reference CSV path."""

        return str(PurePosixPath(self.dataset_root) / "reference" / FAILURE_REFERENCE_FILENAME)

    def cli_uri(self, volume_path: str) -> str:
        """Return the `dbfs:` URI required by Databricks CLI file commands."""

        is_configured_root = volume_path == self.volume_root
        is_below_configured_root = isinstance(volume_path, str) and volume_path.startswith(
            f"{self.volume_root}/"
        )
        if not is_configured_root and not is_below_configured_root:
            raise SourceLandingError("volume_path must be inside the configured Volume root")
        return f"dbfs:{volume_path}"
