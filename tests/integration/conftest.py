from collections.abc import Iterator

import pytest

pyspark = pytest.importorskip("pyspark")
pytest.importorskip("delta")

from pyspark.sql import SparkSession  # noqa: E402

from railpulse.spark import create_local_spark_session  # noqa: E402


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    session = create_local_spark_session(
        "railpulse-bronze-tests",
        warehouse_dir=tmp_path_factory.mktemp("spark-warehouse"),
    )
    yield session
    session.stop()
