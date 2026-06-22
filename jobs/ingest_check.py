"""Fail the run if the raw slice for the logical date is missing or empty
(P4 ingest_check). Runs driver-side via the GCS connector (spark KSA / WI)."""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession  # noqa: E402


def raw_prefix(lake: str, run_date: str) -> str:
    return f"{lake}/raw/events/dt={run_date}/"


def has_input(spark, prefix: str) -> bool:
    """True if the prefix exists and holds at least one non-hidden file."""
    jvm = spark._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(prefix)
    fs = hpath.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(hpath):
        return False
    return any(
        not st.getPath().getName().startswith("_")
        and not st.getPath().getName().startswith(".")
        for st in fs.listStatus(hpath)
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-date", required=True, help="logical date YYYY-MM-DD")
    ap.add_argument("--lake", default=os.environ.get("LAKE"))
    args = ap.parse_args()
    if not args.lake:
        ap.error("--lake or env LAKE is required")

    spark = SparkSession.builder.appName("ingest-check").getOrCreate()
    try:
        prefix = raw_prefix(args.lake, args.run_date)
        if not has_input(spark, prefix):
            raise SystemExit(f"ingest_check failed: no input at {prefix}")
        print(f"ingest_check OK: {prefix}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
