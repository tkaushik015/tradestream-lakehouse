import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# Make repo root importable as "app"
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def spark():
    os.environ["PYSPARK_PYTHON"] = "/opt/anaconda3/bin/python"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "/opt/anaconda3/bin/python"

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("tradestream-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()
