from pathlib import Path

import pytest

from railpulse.config import ConfigurationError, load_config


def test_default_config_has_expected_namespaces() -> None:
    config = load_config()

    assert config.name == "railpulse"
    assert config.environment == "local"
    assert config.dataset_version == "uci-791-aab991a970e5"
    assert config.schemas.catalog == "railpulse"
    assert (config.schemas.bronze, config.schemas.silver, config.schemas.gold) == (
        "bronze",
        "silver",
        "gold",
    )


def test_default_storage_paths_are_absolute_and_project_relative() -> None:
    config = load_config()

    for path in (
        config.paths.raw_data,
        config.paths.processed_data,
        config.paths.delta,
        config.paths.checkpoints,
        config.paths.artifacts,
    ):
        assert path.is_absolute()
        assert path.is_relative_to(config.project_root)


def test_default_config_does_not_depend_on_working_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    config = load_config()

    assert (config.project_root / "configs" / "default.toml").is_file()
    assert config.paths.raw_data == config.project_root / "data" / "raw"


def test_explicit_project_root_controls_relative_paths(tmp_path: Path) -> None:
    config_file = tmp_path / "settings.toml"
    config_file.write_text(
        """
[project]
name = "railpulse-test"
environment = "test"
dataset_version = "fixture-v1"

[paths]
raw_data = "inputs"
processed_data = "outputs"
delta = "delta"
checkpoints = "checkpoints"
artifacts = "artifacts"

[schemas]
catalog = "test_catalog"
bronze = "bronze"
silver = "silver"
gold = "gold"
""".strip(),
        encoding="utf-8",
    )

    config = load_config(config_file, project_root=tmp_path)

    assert config.project_root == tmp_path.resolve()
    assert config.paths.raw_data == (tmp_path / "inputs").resolve()
    assert config.dataset_version == "fixture-v1"


def test_missing_required_value_is_rejected(tmp_path: Path) -> None:
    config_file = tmp_path / "incomplete.toml"
    config_file.write_text(
        """
[project]
name = "railpulse"
environment = "test"

[paths]
raw_data = "data/raw"
processed_data = "data/processed"
delta = "data/delta"
checkpoints = "data/checkpoints"
artifacts = "artifacts"

[schemas]
catalog = "railpulse"
bronze = "bronze"
silver = "silver"
gold = "gold"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ConfigurationError, match=r"\[project\]\.dataset_version"):
        load_config(config_file, project_root=tmp_path)
