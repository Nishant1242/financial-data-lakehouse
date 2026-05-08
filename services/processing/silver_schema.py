"""
silver_schema.py

Purpose: defines the explicit schema for Silver layer trade data.

Why explicit schema instead of schema inference?
1. Speed — Spark doesn't need to scan all data to guess types
2. Safety — bad records are caught and rejected, not silently corrupted
3. Consistency — schema never changes unexpectedly between runs
4. Documentation — schema IS the data contract between Bronze and Silver

This file is imported by the batch pipeline and streaming pipeline.
Single source of truth for what a clean trade record looks like.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    LongType
)


# ── Silver layer trade schema ────────────────────────────
# This is what every record looks like AFTER cleaning
SILVER_TRADE_SCHEMA = StructType([
    StructField("trade_id",    StringType(),    nullable=False),
    StructField("symbol",      StringType(),    nullable=False),
    StructField("price",       DoubleType(),    nullable=False),
    StructField("quantity",    DoubleType(),    nullable=False),
    StructField("notional",    DoubleType(),    nullable=False),
    StructField("exchange",    StringType(),    nullable=True),
    StructField("trade_type",  StringType(),    nullable=False),
    StructField("timestamp",   TimestampType(), nullable=False),
    StructField("ingested_at", TimestampType(), nullable=False),
    StructField("source",      StringType(),    nullable=False),
    StructField("conditions",  StringType(),    nullable=True),
    StructField("trade_date",  StringType(),    nullable=False),
    StructField("trade_hour",  StringType(),    nullable=False),
])


# ── Bronze layer raw schema ──────────────────────────────
# Used when reading raw JSON from MinIO Bronze
# Everything comes in as strings from JSON files
BRONZE_TRADE_SCHEMA = StructType([
    StructField("trade_id",    StringType(), nullable=True),
    StructField("symbol",      StringType(), nullable=True),
    StructField("price",       StringType(), nullable=True),
    StructField("quantity",    StringType(), nullable=True),
    StructField("notional",    StringType(), nullable=True),
    StructField("exchange",    StringType(), nullable=True),
    StructField("trade_type",  StringType(), nullable=True),
    StructField("timestamp",   StringType(), nullable=True),
    StructField("ingested_at", StringType(), nullable=True),
    StructField("source",      StringType(), nullable=True),
    StructField("conditions",  StringType(), nullable=True),
])


# ── Data quality rules ───────────────────────────────────
# These rules are applied during Silver transformation
# Records failing these rules are rejected to a dead letter file
QUALITY_RULES = {
    "price_positive":     "price > 0",
    "quantity_positive":  "quantity > 0",
    "notional_positive":  "notional > 0",
    "symbol_not_empty":   "symbol IS NOT NULL AND symbol != ''",
    "trade_id_not_empty": "trade_id IS NOT NULL AND trade_id != ''",
}