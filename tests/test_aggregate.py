"""Unit tests for the aggregate transform (TT.1, TT.3).

Run in an environment with pyspark==3.5.6 and Java 17:
    pip install -r tests/requirements-dev.txt
    pytest tests/
"""
import aggregate
from schema import EVENTS_SCHEMA


def _sample(spark):
    rows = [
        ("e1", "books", 10.0),
        ("e2", "books", 5.5),
        ("e3", "music", 3.0),
        ("e1", "books", 10.0),  # duplicate business key -> must be dropped
        ("e4", "music", 7.0),
    ]
    return spark.createDataFrame(rows, schema=EVENTS_SCHEMA)


def test_transform_dedups_and_aggregates(spark):
    out = {
        r["category"]: r
        for r in aggregate.transform(_sample(spark), "2026-06-01").collect()
    }
    assert out["books"]["cnt"] == 2
    assert abs(out["books"]["total"] - 15.5) < 1e-9
    assert out["music"]["cnt"] == 2
    assert abs(out["music"]["total"] - 10.0) < 1e-9
    assert all(r["run_date"] == "2026-06-01" for r in out.values())


def test_transform_is_idempotent(spark):
    first = sorted(tuple(r) for r in aggregate.transform(_sample(spark), "2026-06-01").collect())
    second = sorted(tuple(r) for r in aggregate.transform(_sample(spark), "2026-06-01").collect())
    assert first == second
