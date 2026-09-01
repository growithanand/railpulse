import railpulse


def test_package_version() -> None:
    assert railpulse.__version__ == "0.1.0"


def test_package_has_expected_public_api() -> None:
    assert railpulse.__all__ == ["__version__"]
