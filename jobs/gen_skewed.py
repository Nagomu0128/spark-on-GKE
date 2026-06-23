"""Generate a category-skewed events dataset into raw/ to reproduce shuffle skew
(Phase 5, Design §9).

One "hot" category receives the bulk of the rows, so the groupBy("category")
shuffle in aggregate.py concentrates almost all data on a single reducer task —
the classic data-skew symptom. The salted aggregate (aggregate.py --salt N)
spreads that key across N sub-groups and reassembles, balancing the shuffle.

Writes header CSV to raw/events/dt=<run_date>/ matching jobs/schema.py so the
existing aggregate job can read it unchanged.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402


def raw_path(lake: str, run_date: str) -> str:
    return f"{lake}/raw/events/dt={run_date}/"


def build(spark: SparkSession, rows: int, hot_fraction: float, num_cold: int) -> DataFrame:
    """`rows` total: a `hot_fraction` share go to category 'hot', the rest spread
    over `num_cold` cold keys. event_id is unique so dedup keeps every row."""
    return (
        spark.range(rows)
        .withColumn("event_id", F.concat(F.lit("e"), F.col("id").cast("string")))
        .withColumn(
            "category",
            F.when(F.rand(seed=42) < F.lit(hot_fraction), F.lit("hot")).otherwise(
                F.concat(F.lit("cold_"), (F.rand(seed=7) * num_cold).cast("int").cast("string"))
            ),
        )
        .withColumn("amount", F.round(F.rand(seed=13) * 100, 2))
        .select("event_id", "category", "amount")
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-date", required=True, help="logical date YYYY-MM-DD")
    ap.add_argument("--lake", default=os.environ.get("LAKE"), help="gs://<bucket> (or env LAKE)")
    ap.add_argument("--rows", type=int, default=2_000_000)
    ap.add_argument("--hot-fraction", type=float, default=0.9)
    ap.add_argument("--num-cold", type=int, default=200)
    args = ap.parse_args()
    if not args.lake:
        ap.error("--lake or env LAKE is required")

    spark = SparkSession.builder.appName("gen-skewed").getOrCreate()
    try:
        df = build(spark, args.rows, args.hot_fraction, args.num_cold)
        (
            df.write.mode("overwrite")
            .option("header", True)
            .csv(raw_path(args.lake, args.run_date))
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
