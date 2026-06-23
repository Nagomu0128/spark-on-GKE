"""Aggregate raw events for one logical date into staging Parquet.

Design invariants:
- `run_date` is the logical date of the data, not the run date (§6.1, §8):
  read only raw/events/dt=<run_date>/.
- Output goes to staging/; promotion to curated happens in the publish step (§7, §8).
- Idempotent: dedup on the business key, overwrite the run_date partition (§8).
"""
import argparse
import os
import sys

# Make sibling modules importable whether run by spark-submit or imported in tests.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from schema import BUSINESS_KEY, EVENTS_SCHEMA  # noqa: E402


def raw_path(lake: str, run_date: str) -> str:
    return f"{lake}/raw/events/dt={run_date}/"


def staging_path(lake: str) -> str:
    return f"{lake}/staging/agg_by_category/"


def transform(df: DataFrame, run_date: str, salt: int = 1) -> DataFrame:
    """Pure transform (unit-tested): dedup -> group -> tag with run_date.

    salt > 1 enables a two-stage salted aggregation (Design §9): the group key is
    split into `salt` random sub-groups to balance a skewed shuffle, then the
    partials are summed back per category. The result is identical to salt=1.
    """
    deduped = df.dropDuplicates([BUSINESS_KEY])
    if salt > 1:
        agg = (
            deduped.withColumn("_salt", (F.rand() * salt).cast("int"))
            .groupBy("category", "_salt")
            .agg(
                F.count(F.lit(1)).alias("_cnt"),
                F.sum(F.col("amount")).alias("_total"),
            )
            .groupBy("category")
            .agg(
                F.sum("_cnt").alias("cnt"),
                F.sum("_total").alias("total"),
            )
        )
    else:
        agg = deduped.groupBy("category").agg(
            F.count(F.lit(1)).alias("cnt"),
            F.sum(F.col("amount")).alias("total"),
        )
    return agg.withColumn("run_date", F.lit(run_date))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-date", required=True, help="logical date YYYY-MM-DD")
    ap.add_argument(
        "--lake", default=os.environ.get("LAKE"), help="gs://<bucket> (or env LAKE)"
    )
    ap.add_argument(
        "--salt",
        type=int,
        default=1,
        help="salt buckets for skew mitigation; 1 = off (Design §9)",
    )
    args = ap.parse_args()
    if not args.lake:
        ap.error("--lake or env LAKE is required")

    spark = SparkSession.builder.appName("kaggle-agg").getOrCreate()
    try:
        df = (
            spark.read.schema(EVENTS_SCHEMA)
            .option("header", True)
            # Validate the CSV header against the schema (fail fast on column drift)
            # instead of the default positional mapping.
            .option("enforceSchema", False)
            .csv(raw_path(args.lake, args.run_date))
        )
        agg = transform(df, args.run_date, args.salt)
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
