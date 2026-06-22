"""Unit tests for the DQ gate (TT.2)."""
import aggregate
import pytest
import validate_dq


def test_dq_passes_on_good_data(sample_events):
    agg = aggregate.transform(sample_events, "2026-06-01")
    result = validate_dq.run_checks(agg, max_null_rate=0.0)
    assert result["rows"] == 2
    assert result["null_rate"] == 0.0


def test_dq_fails_on_zero_rows(sample_events):
    empty = aggregate.transform(sample_events, "2026-06-01").filter("cnt < 0")
    with pytest.raises(ValueError, match="zero rows"):
        validate_dq.run_checks(empty)


def test_dq_fails_on_null_key(spark):
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    # Explicit schema: an all-None column can't be type-inferred by createDataFrame.
    schema = StructType(
        [
            StructField("category", StringType()),
            StructField("cnt", LongType()),
            StructField("total", DoubleType()),
            StructField("run_date", StringType()),
        ]
    )
    df = spark.createDataFrame([(None, 1, 1.0, "2026-06-01")], schema)
    with pytest.raises(ValueError, match="null rate"):
        validate_dq.run_checks(df, max_null_rate=0.0)
