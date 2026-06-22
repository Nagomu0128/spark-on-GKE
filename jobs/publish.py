"""Promote a validated run_date partition from staging to curated (P3.2, Design §8).

Idempotent: dynamic partition overwrite replaces only this run_date, then a
`_SUCCESS` sentinel marks the partition safe to read.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from io_util import mark_success  # noqa: E402

STAGING = "staging/agg_by_category"
CURATED = "curated/agg_by_category"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-date", required=True, help="logical date YYYY-MM-DD")
    ap.add_argument("--lake", default=os.environ.get("LAKE"))
    args = ap.parse_args()
    if not args.lake:
        ap.error("--lake or env LAKE is required")

    spark = SparkSession.builder.appName("publish").getOrCreate()
    try:
        staging = f"{args.lake}/{STAGING}/"
        curated = f"{args.lake}/{CURATED}/"
        df = spark.read.parquet(staging).filter(F.col("run_date") == args.run_date)
        # partitionOverwriteMode=dynamic (sparkConf) -> replace only this run_date.
        df.write.mode("overwrite").partitionBy("run_date").parquet(curated)
        mark_success(spark, f"{curated}run_date={args.run_date}")
        print(f"Published run_date={args.run_date} -> {curated}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
