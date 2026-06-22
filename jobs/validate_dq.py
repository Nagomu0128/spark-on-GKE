"""Data-quality gate on the staging output (P3.1, Design §7).

Reads the run_date partition from staging and raises (non-zero exit) if a check
breaches its threshold, so the downstream publish step does not run.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

STAGING = "staging/agg_by_category"
EXPECTED_COLUMNS = {"category", "cnt", "total", "run_date"}


def staging_path(lake: str) -> str:
    return f"{lake}/{STAGING}/"


def run_checks(df: DataFrame, max_null_rate: float = 0.0) -> dict:
    """Validate the partition. Raises ValueError on any breach; returns metrics."""
    missing = EXPECTED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"DQ failed: missing columns {sorted(missing)}")
    total = df.count()
    if total == 0:
        raise ValueError("DQ failed: zero rows")
    nulls = df.filter(F.col("category").isNull()).count()
    null_rate = nulls / total
    if null_rate > max_null_rate:
        raise ValueError(
            f"DQ failed: category null rate {null_rate:.3f} > {max_null_rate}"
        )
    return {"rows": total, "null_rate": null_rate}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-date", required=True, help="logical date YYYY-MM-DD")
    ap.add_argument("--lake", default=os.environ.get("LAKE"))
    ap.add_argument("--max-null-rate", type=float, default=0.0)
    args = ap.parse_args()
    if not args.lake:
        ap.error("--lake or env LAKE is required")

    spark = SparkSession.builder.appName("validate-dq").getOrCreate()
    try:
        df = spark.read.parquet(staging_path(args.lake)).filter(
            F.col("run_date") == args.run_date
        )
        result = run_checks(df, args.max_null_rate)
        print(f"DQ passed for run_date={args.run_date}: {result}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
