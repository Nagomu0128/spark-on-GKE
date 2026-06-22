import sys
from pathlib import Path

import pytest

# Make jobs/ importable the same way the image does (spark-submit puts the app
# file's dir on sys.path, so aggregate.py can `import schema`).
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[*]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


# Shared fixture: e1 is duplicated to exercise dedup. After dedup -> books:{e1,e2},
# music:{e3,e4} => books cnt=2 total=15.5, music cnt=2 total=10.0.
SAMPLE_ROWS = [
    ("e1", "books", 10.0),
    ("e2", "books", 5.5),
    ("e3", "music", 3.0),
    ("e1", "books", 10.0),
    ("e4", "music", 7.0),
]


@pytest.fixture
def sample_events(spark):
    from schema import EVENTS_SCHEMA

    return spark.createDataFrame(SAMPLE_ROWS, schema=EVENTS_SCHEMA)
