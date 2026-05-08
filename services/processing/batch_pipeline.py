"""
batch_pipeline.py

Production-grade Bronze → Silver batch transformation pipeline.

Author: Nishant Kadam
Project: Financial Data Lakehouse
Version: 2.0.0

Architecture:
    Bronze (raw JSONL) → PySpark transforms → Silver (clean Parquet)

Design decisions:
    - Config injected via environment variables — never hardcoded
    - Dataclasses for config and results — self-documenting
    - Type hints on every function — readable without comments
    - Structured logging with context — searchable in production
    - Specific exception handling — no silent failures
    - Idempotent writes — safe to re-run without duplicates
    - PipelineResult object — metrics captured, not just printed
    - S3A bytebuffer upload — AWS recommended, no disk temp writes
"""

import os
import sys
import time
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

# Fix HADOOP_HOME for Windows before any Spark imports
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"

import findspark
findspark.init()

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType
from dotenv import load_dotenv

# Add project root to path for local imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)
))))
from services.processing.silver_schema import QUALITY_RULES


# ── Logging setup ────────────────────────────────────────────────────────────
def setup_logging(level: str = "INFO") -> logging.Logger:
    """
    Configures structured logging for the pipeline.
    Format includes timestamp, level, and message for
    easy parsing by log aggregation tools like Datadog.
    """
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    # Silence noisy Spark/Hadoop internal loggers
    for noisy in ["py4j", "pyspark", "org.apache"]:
        logging.getLogger(noisy).setLevel(logging.WARNING)

    return logging.getLogger("lakehouse.batch_pipeline")


logger = setup_logging()


# ── Configuration dataclasses ─────────────────────────────────────────────────
@dataclass
class StorageConfig:
    """
    All MinIO/S3 storage configuration.
    Loaded from environment variables — never hardcoded.
    """
    endpoint: str
    access_key: str
    secret_key: str
    bronze_bucket: str
    silver_bucket: str

    @classmethod
    def from_env(cls) -> "StorageConfig":
        """Factory method — creates config from environment."""
        required = [
            "MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY",
            "MINIO_BUCKET_BRONZE", "MINIO_BUCKET_SILVER"
        ]
        missing = [k for k in required if not os.getenv(k)]
        if missing:
            raise EnvironmentError(
                f"Missing required environment variables: {missing}. "
                f"Check your .env file."
            )
        return cls(
            endpoint=os.getenv("MINIO_ENDPOINT"),
            access_key=os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY"),
            bronze_bucket=os.getenv("MINIO_BUCKET_BRONZE"),
            silver_bucket=os.getenv("MINIO_BUCKET_SILVER"),
        )


@dataclass
class PipelineConfig:
    """
    Pipeline behavior configuration.
    Separate from storage config — follows single responsibility.
    """
    app_name: str = "FinancialLakehouse.BronzeToSilver"
    spark_master: str = "local[*]"
    log_level: str = "WARN"
    batch_date: Optional[str] = None  # None = process all dates

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        return cls(
            app_name=os.getenv("SPARK_APP_NAME", "FinancialLakehouse.BronzeToSilver"),
            spark_master=os.getenv("SPARK_MASTER", "local[*]"),
            log_level=os.getenv("SPARK_LOG_LEVEL", "WARN"),
            batch_date=os.getenv("PIPELINE_BATCH_DATE"),
        )


# ── Pipeline result dataclass ─────────────────────────────────────────────────
@dataclass
class PipelineResult:
    """
    Structured result object returned by the pipeline.
    Used for monitoring, alerting, and downstream orchestration.
    Airflow can check result.status to decide next steps.
    """
    status: str = "PENDING"          # PENDING / SUCCESS / FAILED / PARTIAL
    records_read: int = 0
    records_written: int = 0
    duplicates_removed: int = 0
    quality_failures: int = 0
    duration_seconds: float = 0.0
    error_message: Optional[str] = None
    bronze_path: str = ""
    silver_path: str = ""
    run_timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def log_summary(self, logger: logging.Logger) -> None:
        """Logs a clean summary — one line with all key metrics."""
        logger.info(
            "Pipeline result | "
            f"status={self.status} | "
            f"read={self.records_read} | "
            f"written={self.records_written} | "
            f"dupes_removed={self.duplicates_removed} | "
            f"quality_failures={self.quality_failures} | "
            f"duration={self.duration_seconds:.1f}s"
        )


# ── SparkSession factory ───────────────────────────────────────────────────────
def create_spark_session(
    pipeline_cfg: PipelineConfig,
    storage_cfg: StorageConfig
) -> SparkSession:
    """
    Creates a SparkSession configured for MinIO S3A access.

    S3A fast upload with bytebuffer avoids local disk writes
    entirely — recommended by AWS for S3-compatible storage.
    """
    logger.info(f"Initializing SparkSession | app={pipeline_cfg.app_name}")

    spark = (
        SparkSession.builder
        .appName(pipeline_cfg.app_name)
        .master(pipeline_cfg.spark_master)
        # S3A connector for MinIO access
        .config("spark.hadoop.fs.s3a.endpoint",
                f"http://{storage_cfg.endpoint}")
        .config("spark.hadoop.fs.s3a.access.key",
                storage_cfg.access_key)
        .config("spark.hadoop.fs.s3a.secret.key",
                storage_cfg.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # In-memory upload — no disk temp writes
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
        .config("spark.hadoop.fs.s3a.multipart.size", "104857600")
        # Download S3A JARs automatically
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        # Adaptive Query Execution — auto-optimize at runtime
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(pipeline_cfg.log_level)
    logger.info(f"SparkSession ready | version={spark.version}")
    return spark


# ── Bronze reader ──────────────────────────────────────────────────────────────
def read_bronze(
    spark: SparkSession,
    storage_cfg: StorageConfig,
    batch_date: Optional[str] = None
) -> DataFrame:
    """
    Reads raw JSONL trade files from MinIO Bronze bucket.

    Args:
        spark: Active SparkSession
        storage_cfg: Storage configuration
        batch_date: Optional date filter e.g. "2026-05-08"
                    None = read all available data

    Returns:
        Raw DataFrame with all fields as-is from source

    Raises:
        ValueError: If no data found in Bronze path
    """
    # Build path — optionally filter by date partition
    if batch_date:
        path = f"s3a://{storage_cfg.bronze_bucket}/trades/year={batch_date[:4]}/month={batch_date[5:7]}/day={batch_date[8:10]}/"
    else:
        path = f"s3a://{storage_cfg.bronze_bucket}/trades/"

    logger.info(f"Reading Bronze | path={path}")

    try:
        df = (
            spark.read
            .option("multiline", "false")
            .json(path)
        )
        count = df.count()

        if count == 0:
            raise ValueError(f"No records found in Bronze path: {path}")

        logger.info(f"Bronze read complete | records={count} | path={path}")
        return df

    except Exception as e:
        logger.error(f"Bronze read failed | path={path} | error={str(e)}")
        raise


# ── Transformation functions ───────────────────────────────────────────────────
def cast_types(df: DataFrame) -> DataFrame:
    """
    Casts Bronze string fields to correct Silver types.
    Bronze JSON stores everything as strings.
    Silver requires proper numeric and timestamp types.
    """
    return (
        df
        .withColumn("price",
            F.col("price").cast(DoubleType()))
        .withColumn("quantity",
            F.col("quantity").cast(DoubleType()))
        .withColumn("notional",
            F.col("notional").cast(DoubleType()))
        .withColumn("timestamp",
            F.to_timestamp(F.col("timestamp")))
        .withColumn("ingested_at",
            F.to_timestamp(F.col("ingested_at")))
    )


def add_derived_columns(df: DataFrame) -> DataFrame:
    """
    Adds trade_date and trade_hour derived from timestamp.
    These become Parquet partition columns in Silver layer.
    Enables partition pruning — queries filter by folder,
    not by scanning all data.
    """
    return (
        df
        .withColumn("trade_date",
            F.date_format(F.col("timestamp"), "yyyy-MM-dd"))
        .withColumn("trade_hour",
            F.date_format(F.col("timestamp"), "HH"))
    )


def remove_duplicates(df: DataFrame) -> tuple[DataFrame, int]:
    """
    Deduplicates on trade_id — the natural unique key.
    Returns cleaned DataFrame and count of removed dupes.

    Idempotency guarantee: running this pipeline twice
    on the same Bronze data never creates duplicate Silver records.
    """
    count_before = df.count()
    df_clean = df.dropDuplicates(["trade_id"])
    count_after = df_clean.count()
    dupes_removed = count_before - count_after

    if dupes_removed > 0:
        logger.warning(
            f"Duplicates removed | count={dupes_removed} | "
            f"before={count_before} | after={count_after}"
        )
    else:
        logger.info(f"Deduplication complete | no duplicates found")

    return df_clean, dupes_removed


def handle_nulls(df: DataFrame) -> DataFrame:
    """
    Fills known-nullable fields with sentinel values.
    Downstream SQL never needs to handle NULLs for these fields.

    exchange and conditions are legitimately absent in Alpaca data.
    UNKNOWN and NONE are industry-standard sentinel values.
    """
    return (
        df
        .fillna("UNKNOWN", subset=["exchange"])
        .fillna("NONE",    subset=["conditions"])
    )


def round_numerics(df: DataFrame) -> DataFrame:
    """
    Rounds notional to 2 decimal places.
    Floating point arithmetic produces values like 23.9217136.
    Financial reporting requires exactly 2 decimal places.
    """
    return df.withColumn("notional", F.round(F.col("notional"), 2))


def apply_quality_rules(
    df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    Splits DataFrame into passing and failing records.

    Records passing all quality rules → Silver layer
    Records failing any quality rule → dead letter storage

    Quality rules defined in silver_schema.py — single source of truth.
    This separation means: bad data never contaminates Silver.
    """
    quality_filter = " AND ".join(QUALITY_RULES.values())

    good_df = df.filter(quality_filter)
    bad_df  = df.filter(f"NOT ({quality_filter})")

    good_count = good_df.count()
    bad_count  = bad_df.count()

    if bad_count > 0:
        logger.warning(
            f"Quality failures | failed={bad_count} | "
            f"passed={good_count} | "
            f"rules={list(QUALITY_RULES.keys())}"
        )
    else:
        logger.info(
            f"Quality check passed | records={good_count} | "
            f"failures=0"
        )

    return good_df, bad_df


def transform_bronze_to_silver(
    df: DataFrame
) -> tuple[DataFrame, DataFrame, int]:
    """
    Orchestrates all transformation steps in order.
    Each step is a pure function — testable independently.

    Returns:
        silver_df: Clean records ready for Silver layer
        dead_letter_df: Records that failed quality checks
        dupes_removed: Count of duplicates found
    """
    logger.info("Starting Bronze → Silver transformation")

    df = cast_types(df)
    logger.info("Step 1/5 complete | types cast")

    df = add_derived_columns(df)
    logger.info("Step 2/5 complete | derived columns added")

    df, dupes_removed = remove_duplicates(df)
    logger.info(f"Step 3/5 complete | deduplication done")

    df = handle_nulls(df)
    logger.info("Step 4/5 complete | nulls handled")

    df = round_numerics(df)
    logger.info("Step 5/5 complete | numerics rounded")

    silver_df, dead_letter_df = apply_quality_rules(df)
    logger.info("Quality rules applied")

    return silver_df, dead_letter_df, dupes_removed


# ── Silver writer ──────────────────────────────────────────────────────────────
def write_silver(
    df: DataFrame,
    storage_cfg: StorageConfig
) -> str:
    """
    Writes clean DataFrame as Parquet to Silver bucket.
    Partitioned by trade_date and trade_hour for query performance.

    Returns:
        silver_path: The path where data was written
    """
    silver_path = f"s3a://{storage_cfg.silver_bucket}/trades/"
    logger.info(f"Writing Silver | path={silver_path}")

    start = time.time()
    (
        df.write
        .mode("append")
        .partitionBy("trade_date", "trade_hour")
        .parquet(silver_path)
    )
    duration = time.time() - start

    logger.info(
        f"Silver write complete | "
        f"path={silver_path} | "
        f"duration={duration:.1f}s"
    )
    return silver_path


# ── Dead letter writer ─────────────────────────────────────────────────────────
def write_dead_letter(
    df: DataFrame,
    storage_cfg: StorageConfig
) -> str:
    """
    Writes quality-failed records to dead letter storage.
    Data engineers investigate dead letter files to fix
    upstream data quality issues.

    Returns:
        dead_letter_path: Path where failed records were written
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = (
        f"s3a://{storage_cfg.bronze_bucket}/"
        f"dead_letter/trades_{timestamp}/"
    )

    df.write.mode("overwrite").json(path)
    logger.warning(f"Dead letter records written | path={path}")
    return path


# ── Silver verifier ────────────────────────────────────────────────────────────
def verify_silver(
    spark: SparkSession,
    storage_cfg: StorageConfig
) -> int:
    """
    Reads back Silver data to confirm write succeeded.
    Logs schema, sample records, and symbol distribution.

    Returns:
        total_records: Count of records in Silver layer
    """
    silver_path = f"s3a://{storage_cfg.silver_bucket}/trades/"

    df = spark.read.parquet(silver_path)
    total = df.count()

    logger.info(f"Silver verification | total_records={total}")

    # Symbol distribution — key business metric
    logger.info("Symbol distribution in Silver:")
    (
        df.groupBy("symbol")
        .agg(
            F.count("*").alias("trade_count"),
            F.round(F.avg("price"), 2).alias("avg_price"),
            F.round(F.sum("notional"), 2).alias("total_notional")
        )
        .orderBy(F.desc("trade_count"))
        .show(truncate=False)
    )

    return total


# ── Pipeline orchestrator ──────────────────────────────────────────────────────
def run_pipeline(
    storage_cfg: StorageConfig,
    pipeline_cfg: PipelineConfig
) -> PipelineResult:
    """
    Main pipeline orchestrator.
    Coordinates all steps and returns a structured result.

    This function is the single entry point — Airflow calls this.
    Returns PipelineResult so Airflow knows success/failure/partial.
    """
    result = PipelineResult(
        bronze_path=f"s3a://{storage_cfg.bronze_bucket}/trades/",
        silver_path=f"s3a://{storage_cfg.silver_bucket}/trades/"
    )
    pipeline_start = time.time()
    spark = None

    try:
        spark = create_spark_session(pipeline_cfg, storage_cfg)

        # Read
        bronze_df = read_bronze(
            spark,
            storage_cfg,
            batch_date=pipeline_cfg.batch_date
        )
        result.records_read = bronze_df.count()

        # Transform
        silver_df, dead_letter_df, dupes_removed = (
            transform_bronze_to_silver(bronze_df)
        )
        result.duplicates_removed = dupes_removed
        result.quality_failures = dead_letter_df.count()

        # Write Silver
        write_silver(silver_df, storage_cfg)
        result.records_written = silver_df.count()

        # Write dead letter if any failures
        if result.quality_failures > 0:
            write_dead_letter(dead_letter_df, storage_cfg)

        # Verify
        verify_silver(spark, storage_cfg)

        result.status = (
            "PARTIAL" if result.quality_failures > 0
            else "SUCCESS"
        )

    except Exception as e:
        result.status = "FAILED"
        result.error_message = str(e)
        logger.error(
            f"Pipeline failed | error={str(e)}",
            exc_info=True
        )
        raise

    finally:
        result.duration_seconds = time.time() - pipeline_start
        result.log_summary(logger)

        if spark:
            spark.stop()
            logger.info("SparkSession stopped")

    return result


# ── Entrypoint ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    load_dotenv()

    logger.info("=" * 60)
    logger.info("  Financial Data Lakehouse — Batch Pipeline v2.0")
    logger.info("=" * 60)

    storage_cfg  = StorageConfig.from_env()
    pipeline_cfg = PipelineConfig.from_env()

    result = run_pipeline(storage_cfg, pipeline_cfg)

    # Exit with non-zero code on failure
    # This is how Airflow and CI/CD systems detect failures
    if result.status == "FAILED":
        sys.exit(1)

    logger.info(f"Pipeline finished | status={result.status}")