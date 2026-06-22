"""Explicit schema for the raw events dataset.

Explicit (not inferSchema) for determinism and idempotency (Design §8, §12):
inferred types can drift between days and break reproducibility.
"""
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# Raw events CSV columns. The logical date `dt` lives in the path
# (raw/events/dt=YYYY-MM-DD/), not as a column.
EVENTS_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("category", StringType(), nullable=True),
        StructField("amount", DoubleType(), nullable=True),
    ]
)

# Business key for dedup (idempotency, Design §8).
BUSINESS_KEY = "event_id"
