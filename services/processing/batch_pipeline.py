
"""
batch_pipeline.py

Purpose: reads raw trade JSON files from MinIO Bronze layer,
cleans and validates them, and writes clean Parquet files
to MinIO Silver layer.

This is a BATCH pipeline — it runs on a schedule (e.g. every hour)
and processes all new Bronze files since the last run.

The Bronze → Silver transformation does 6 things:
1. Read raw JSONL files from MinIO Bronze
2. Enforce schema — cast strings to correct types
3. Remove duplicates — same trade_id appearing twice
4. Handle nulls — fill or reject missing values
5. Add derived columns — trade_date, trade_hour for partitioning
6. Write clean Parquet to MinIO Silver partitioned by date/hour

Why Parquet instead of JSON?
JSON stores data row by row — to read price column you read everything.
Parquet stores data column by column — to read price you only read
the price column. For analytics (avg price, total volume) this is
10-100x faster than JSON.
"""

import os
import sys
import logging
from datetime import datetime, timezone

# Fix HADOOP_HOME for Windows
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType

from dotenv import load_dotenv

# Add project root to path so we can import silver_schema
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from services.processing.silver_schema import QUALITY_RULES

# ── Load environment variables ──────────────────────────
load_dotenv()

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BRONZE_BUCKET    = os.getenv("MINIO_BUCKET_BRONZE")
SILVER_BUCKET    = os.getenv("MINIO_BUCKET_SILVER")

# ── Set up logging ───────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# ── Create SparkSession ──────────────────────────────────
def create_spark_session():
    """
    Creates and returns a SparkSession configured to
    talk to MinIO using the S3A protocol.

    S3A is the Hadoop connector for S3-compatible storage.
    MinIO speaks the same language as S3, so S3A works
    perfectly with MinIO.
    """
    logger.info("Creating SparkSession...")

    spark = SparkSession.builder \
        .appName("BronzeToSilverBatch") \
        .master("local[*]") \
        .config("spark.hadoop.fs.s3a.endpoint",
                f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key",
                MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key",
                MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access",
                "true") \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled",
                "true") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer") \
        .config("spark.hadoop.fs.s3a.multipart.size", "104857600") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"SparkSession created | Version: {spark.version}")
    return spark


# ── Read Bronze data ─────────────────────────────────────
def read_bronze(spark):
    """
    Reads all JSONL files from MinIO Bronze bucket.
    Returns a raw DataFrame with everything as strings.
    """
    bronze_path = f"s3a://{BRONZE_BUCKET}/trades/"
    logger.info(f"Reading Bronze data from: {bronze_path}")

    df = spark.read \
        .option("multiline", "false") \
        .json(bronze_path)

    record_count = df.count()
    logger.info(f"Bronze records read: {record_count}")
    logger.info("Bronze schema:")
    df.printSchema()

    return df


# ── Clean and transform ──────────────────────────────────
def transform_bronze_to_silver(df):
    """
    Applies all cleaning and transformation steps.
    Returns a clean DataFrame ready for Silver layer.

    Steps:
    1. Cast types — convert strings to numbers and timestamps
    2. Add derived columns — trade_date and trade_hour
    3. Remove duplicates — deduplicate on trade_id
    4. Handle nulls — fill exchange and conditions
    5. Round notional — fix floating point precision
    6. Filter quality — reject records failing quality rules
    """
    logger.info("Starting Bronze → Silver transformation...")

    # ── Step 1: Cast types ───────────────────────────────
    # Your Bronze data has price as "80816.6" (string)
    # Silver needs price as 80816.6 (actual number)
    # cast() converts between types
    logger.info("Step 1: Casting types...")
    df = df \
        .withColumn("price",
            F.col("price").cast(DoubleType())) \
        .withColumn("quantity",
            F.col("quantity").cast(DoubleType())) \
        .withColumn("notional",
            F.col("notional").cast(DoubleType())) \
        .withColumn("timestamp",
            F.to_timestamp(F.col("timestamp"))) \
        .withColumn("ingested_at",
            F.to_timestamp(F.col("ingested_at")))

    # ── Step 2: Add derived columns ──────────────────────
    # Extract date and hour from timestamp
    # These become partition columns in Silver Parquet
    logger.info("Step 2: Adding derived columns...")
    df = df \
        .withColumn("trade_date",
            F.date_format(F.col("timestamp"), "yyyy-MM-dd")) \
        .withColumn("trade_hour",
            F.date_format(F.col("timestamp"), "HH"))

    # ── Step 3: Remove duplicates ────────────────────────
    # Same trade can appear twice if producer retried
    # Keep the first occurrence, drop subsequent ones
    logger.info("Step 3: Removing duplicates...")
    count_before = df.count()
    df = df.dropDuplicates(["trade_id"])
    count_after = df.count()
    duplicates_removed = count_before - count_after
    logger.info(
        f"Duplicates removed: {duplicates_removed} "
        f"({count_before} → {count_after} records)"
    )

    # ── Step 4: Handle nulls ─────────────────────────────
    # exchange and conditions are legitimately null
    # Replace None with "UNKNOWN" so downstream
    # SQL queries don't need to handle nulls
    logger.info("Step 4: Handling nulls...")
    df = df \
        .fillna("UNKNOWN", subset=["exchange"]) \
        .fillna("NONE",    subset=["conditions"])

    # ── Step 5: Round notional ───────────────────────────
    # Raw notional: 23.9217136 (floating point mess)
    # Clean notional: 23.92 (2 decimal places)
    # round() keeps numbers clean for reporting
    logger.info("Step 5: Rounding notional values...")
    df = df.withColumn("notional",
        F.round(F.col("notional"), 2))

    # ── Step 6: Filter quality ───────────────────────────
    # Separate good records from bad records
    # Good records go to Silver
    # Bad records go to a dead letter file for investigation
    logger.info("Step 6: Applying quality filters...")

    # Build one combined filter from all quality rules
    quality_filter = " AND ".join(QUALITY_RULES.values())
    good_df = df.filter(quality_filter)
    bad_df  = df.filter(f"NOT ({quality_filter})")

    good_count = good_df.count()
    bad_count  = bad_df.count()
    logger.info(f"Quality check — passed: {good_count} | failed: {bad_count}")

    if bad_count > 0:
        logger.warning(
            f"{bad_count} records failed quality checks "
            f"— writing to dead letter"
        )

    return good_df, bad_df


# ── Write Silver data ────────────────────────────────────
def write_silver(df, spark):
    """
    Writes clean DataFrame as Parquet to MinIO Silver bucket.
    Partitioned by trade_date and trade_hour.

    Why partitionBy(trade_date, trade_hour)?
    When analysts query "show me all BTC trades today",
    Spark reads ONLY today's folder — skipping all other days.
    This is called partition pruning — dramatic performance boost.
    """
    silver_path = f"s3a://{SILVER_BUCKET}/trades/"
    logger.info(f"Writing Silver data to: {silver_path}")

    df.write \
        .mode("append") \
        .partitionBy("trade_date", "trade_hour") \
        .parquet(silver_path)

    logger.info("Silver write complete")


# ── Write dead letter ────────────────────────────────────
def write_dead_letter(df):
    """
    Writes rejected records to a separate location
    for investigation. Uses the Bronze bucket so
    data engineers can inspect what failed and why.
    """
    dead_letter_path = (
        f"s3a://{BRONZE_BUCKET}/dead_letter/"
        f"trades_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )

    df.write \
        .mode("overwrite") \
        .json(dead_letter_path)

    logger.info(f"Dead letter records written to: {dead_letter_path}")


# ── Show Silver sample ───────────────────────────────────
def show_silver_sample(spark):
    """
    Reads back the Silver data and shows a sample.
    Proves the pipeline worked correctly.
    """
    silver_path = f"s3a://{SILVER_BUCKET}/trades/"
    logger.info("Reading back Silver data to verify...")

    df = spark.read.parquet(silver_path)
    total = df.count()

    logger.info(f"Total Silver records: {total}")
    logger.info("Silver schema:")
    df.printSchema()

    logger.info("Sample Silver records:")
    df.select(
        "trade_id", "symbol", "price",
        "quantity", "notional", "trade_date",
        "trade_hour", "exchange"
    ).show(5, truncate=False)

    logger.info("Symbol distribution:")
    df.groupBy("symbol") \
      .agg(
          F.count("*").alias("trade_count"),
          F.avg("price").alias("avg_price"),
          F.sum("notional").alias("total_notional")
      ) \
      .orderBy("symbol") \
      .show(truncate=False)


# ── Main entrypoint ──────────────────────────────────────
if __name__ == "__main__":
    logger.info("=" * 55)
    logger.info("  Financial Data Lakehouse — Batch Pipeline")
    logger.info("  Bronze → Silver Transformation")
    logger.info("=" * 55)

    start_time = datetime.now(timezone.utc)
    spark = create_spark_session()

    try:
        # Read
        bronze_df = read_bronze(spark)

        # Transform
        silver_df, dead_letter_df = transform_bronze_to_silver(bronze_df)

        # Write Silver
        write_silver(silver_df, spark)

        # Write dead letter if any bad records
        if dead_letter_df.count() > 0:
            write_dead_letter(dead_letter_df)

        # Verify
        show_silver_sample(spark)

        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).seconds
        logger.info(f"Pipeline complete | Duration: {duration}s")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

    finally:
        spark.stop()
        logger.info("SparkSession stopped")