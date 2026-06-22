"""Idempotency test (TT.3, Design §8): writing the same run_date twice yields one
unique result — overwrite, not append. Uses a local parquet dir (needs Java 17)."""
import aggregate


def test_overwrite_write_is_idempotent(spark, sample_events, tmp_path):
    out = str(tmp_path / "agg")
    for _ in range(2):
        (
            aggregate.transform(sample_events, "2026-06-01")
            .coalesce(1)
            .write.mode("overwrite")
            .partitionBy("run_date")
            .parquet(out)
        )

    back = spark.read.parquet(out)
    rows = sorted(
        (r["category"], r["cnt"], round(float(r["total"]), 3), r["run_date"])
        for r in back.collect()
    )
    assert rows == [
        ("books", 2, 15.5, "2026-06-01"),
        ("music", 2, 10.0, "2026-06-01"),
    ]
    # Second write overwrote the partition rather than appending.
    assert back.count() == 2
