"""Aggregate raw events for one logical date into staging Parquet.

Design invariants:
- `run_date` is the logical date of the data, not the run date (§6.1, §8):
  read only raw/events/dt=<run_date>/.
- Output goes to staging/; promotion to curated happens in the publish step (§7, §8).
- Idempotent: dedup on the business key, overwrite the run_date partition (§8).
"""
import argparse
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from schema import BUSINESS_KEY, EVENTS_SCHEMA


def raw_path(lake: str, run_date: str) -> str:
    return f"{lake}/raw/events/dt={run_date}/"


def staging_path(lake: str) -> str:
    return f"{lake}/staging/agg_by_category/"


def transform(df: DataFrame, run_date: str) -> DataFrame:
    """Pure transform (unit-tested): dedup -> group -> tag with run_date."""
    return (
        df.dropDuplicates([BUSINESS_KEY])
        .groupBy("category")
        .agg(
            F.count(F.lit(1)).alias("cnt"),
            F.sum(F.col("amount")).alias("total"),
        )
        .withColumn("run_date", F.lit(run_date))
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-date", required=True, help="logical date YYYY-MM-DD")
    ap.add_argument(
        "--lake", default=os.environ.get("LAKE"), help="gs://<bucket> (or env LAKE)"
    )
    args = ap.parse_args()
    if not args.lake:
        ap.error("--lake or env LAKE is required")

    spark = SparkSession.builder.appName("kaggle-agg").getOrCreate()
    try:
        df = (
            spark.read.schema(EVENTS_SCHEMA)
            .option("header", True)
            .csv(raw_path(args.lake, args.run_date))
        )
        agg = transform(df, args.run_date)
        # Aggregated output is small -> few files. partitionOverwriteMode=dynamic
        # (sparkConf) overwrites only the target run_date partition.
        (
            agg.coalesce(1)
            .write.mode("overwrite")
            .partitionBy("run_date")
            .parquet(staging_path(args.lake))
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
