import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("tests")
         .config("spark.sql.shuffle.partitions", "2").config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.ansi.enabled", "false").config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()
