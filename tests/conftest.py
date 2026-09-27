import uuid

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    warehouse_dir = tmp_path_factory.mktemp("spark-warehouse")
    builder = (
        SparkSession.builder.appName("cdc-demo-tests")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def unique_table_name():
    def _make(prefix: str) -> str:
        return f"{prefix}_{uuid.uuid4().hex[:8]}"

    return _make
