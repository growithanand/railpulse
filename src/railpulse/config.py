"""Load and validate RailPulse's environment-independent project configuration."""

from __future__ import annotations

import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


class ConfigurationError(ValueError):
    """Raised when a RailPulse configuration file is missing a required value."""


@dataclass(frozen=True)
class StoragePaths:
    """Absolute local paths used by pipeline components."""

    raw_data: Path
    processed_data: Path
    delta: Path
    checkpoints: Path
    artifacts: Path


@dataclass(frozen=True)
class SchemaNames:
    """Logical catalog and medallion-layer schema names."""

    catalog: str
    bronze: str
    silver: str
    gold: str


@dataclass(frozen=True)
class RailPulseConfig:
    """Validated settings shared by local and Databricks-compatible components."""

    name: str
    environment: str
    dataset_version: str
    project_root: Path
    paths: StoragePaths
    schemas: SchemaNames


def _default_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _required_section(document: Mapping[str, object], name: str) -> Mapping[str, object]:
    section = document.get(name)
    if not isinstance(section, dict):
        raise ConfigurationError(f"Missing or invalid [{name}] section")
    return section


def _required_string(section: Mapping[str, object], key: str, section_name: str) -> str:
    value = section.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(f"Missing or invalid [{section_name}].{key}")
    return value.strip()


def _project_path(value: str, root: Path) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (root / path).resolve()


def load_config(
    config_path: str | Path | None = None,
    *,
    project_root: str | Path | None = None,
) -> RailPulseConfig:
    """Load required settings without creating data or artifact directories.

    Args:
        config_path: TOML file. Relative paths are interpreted from ``project_root``.
        project_root: Root used to resolve repository-relative storage paths.

    Returns:
        An immutable, validated configuration object.

    Raises:
        ConfigurationError: If the file is unavailable, invalid TOML, or incomplete.
    """

    root = Path(project_root).expanduser().resolve() if project_root else _default_project_root()
    path = Path(config_path) if config_path else Path("configs/default.toml")
    path = path.resolve() if path.is_absolute() else (root / path).resolve()

    try:
        with path.open("rb") as config_file:
            document = tomllib.load(config_file)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ConfigurationError(f"Unable to load configuration from {path}: {error}") from error

    project = _required_section(document, "project")
    paths = _required_section(document, "paths")
    schemas = _required_section(document, "schemas")

    return RailPulseConfig(
        name=_required_string(project, "name", "project"),
        environment=_required_string(project, "environment", "project"),
        dataset_version=_required_string(project, "dataset_version", "project"),
        project_root=root,
        paths=StoragePaths(
            raw_data=_project_path(_required_string(paths, "raw_data", "paths"), root),
            processed_data=_project_path(_required_string(paths, "processed_data", "paths"), root),
            delta=_project_path(_required_string(paths, "delta", "paths"), root),
            checkpoints=_project_path(_required_string(paths, "checkpoints", "paths"), root),
            artifacts=_project_path(_required_string(paths, "artifacts", "paths"), root),
        ),
        schemas=SchemaNames(
            catalog=_required_string(schemas, "catalog", "schemas"),
            bronze=_required_string(schemas, "bronze", "schemas"),
            silver=_required_string(schemas, "silver", "schemas"),
            gold=_required_string(schemas, "gold", "schemas"),
        ),
    )
