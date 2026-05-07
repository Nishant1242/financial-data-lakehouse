"""
bronze_consumer.py

Purpose: reads trade events from the Kafka trades topic
and writes them as raw JSON files into MinIO Bronze layer.

What it does step by step:
1. Connects to Kafka as a consumer
2. Connects to MinIO Bronze bucket
3. Reads trade messages in batches
4. Writes each batch as a timestamped JSON file to MinIO
5. Runs forever until you press Ctrl+C

File path structure in MinIO Bronze:
bronze/trades/year=2026/month=05/day=07/hour=04/trades_20260507_040000.json

Why this structure?
Partitioning by date/hour means PySpark can read
only the files it needs instead of scanning everything.
Called "partition pruning" — a critical performance pattern.
"""

import os
import json
import logging
from datetime import datetime, timezone
from io import BytesIO
from dotenv import load_dotenv
from kafka import KafkaConsumer
from minio import Minio

# ── Load environment variables ──────────────────────────
load_dotenv()

KAFKA_SERVERS     = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC       = os.getenv("KAFKA_TOPIC_TRADES")
MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY  = os.getenv("MINIO_SECRET_KEY")
BRONZE_BUCKET     = os.getenv("MINIO_BUCKET_BRONZE")

# How many trades to collect before writing to MinIO
# 10 trades per file keeps files small and frequent
# In production this would be 10,000+ per file
BATCH_SIZE = 10

# ── Set up logging ───────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# ── Create MinIO client ──────────────────────────────────
def create_minio_client():
    """
    Creates and returns a MinIO client.
    Also ensures the Bronze bucket exists.
    """
    logger.info("Connecting to MinIO...")
    client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    # Create Bronze bucket if it doesn't exist
    if not client.bucket_exists(BRONZE_BUCKET):
        client.make_bucket(BRONZE_BUCKET)
        logger.info(f"Created bucket: {BRONZE_BUCKET}")
    else:
        logger.info(f"Bucket exists: {BRONZE_BUCKET}")

    return client


# ── Create Kafka Consumer ────────────────────────────────
def create_kafka_consumer():
    """
    Creates and returns a Kafka consumer.
    Reads from the trades topic from the beginning.
    """
    logger.info("Connecting to Kafka consumer...")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        group_id="bronze_layer_consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        max_poll_records=BATCH_SIZE
    )
    logger.info(f"Connected to Kafka | Topic: {KAFKA_TOPIC}")
    return consumer


# ── Build MinIO file path ────────────────────────────────
def build_file_path(batch_time):
    """
    Builds a date-partitioned file path for MinIO.

    Example output:
    trades/year=2026/month=05/day=07/hour=04/trades_20260507_040000.json

    Why partition by date/hour?
    When PySpark reads Bronze data, it can skip entire
    folders based on date filters. Reading 1 hour of data
    instead of all data = dramatically faster processing.
    """
    return (
        f"trades/"
        f"year={batch_time.strftime('%Y')}/"
        f"month={batch_time.strftime('%m')}/"
        f"day={batch_time.strftime('%d')}/"
        f"hour={batch_time.strftime('%H')}/"
        f"trades_{batch_time.strftime('%Y%m%d_%H%M%S')}.json"
    )


# ── Write batch to MinIO ─────────────────────────────────
def write_batch_to_bronze(minio_client, trades, batch_num):
    """
    Writes a batch of trades as a JSON file to MinIO Bronze.

    Each file contains one trade per line (JSONL format).
    JSONL = JSON Lines — each line is a valid JSON object.
    This format is ideal for big data processing because
    Spark can read each line independently in parallel.
    """
    if not trades:
        return

    batch_time = datetime.now(timezone.utc)
    file_path  = build_file_path(batch_time)

    # Convert list of trades to JSONL format
    # Each trade becomes one line of JSON
    jsonl_content = "\n".join(json.dumps(trade) for trade in trades)
    jsonl_bytes   = jsonl_content.encode("utf-8")
    jsonl_stream  = BytesIO(jsonl_bytes)

    # Upload to MinIO
    minio_client.put_object(
        bucket_name=BRONZE_BUCKET,
        object_name=file_path,
        data=jsonl_stream,
        length=len(jsonl_bytes),
        content_type="application/x-ndjson"
    )

    logger.info(
        f"Batch {batch_num} written to Bronze | "
        f"Trades: {len(trades)} | "
        f"Path: {BRONZE_BUCKET}/{file_path} | "
        f"Size: {len(jsonl_bytes)} bytes"
    )


# ── Main consumer loop ───────────────────────────────────
if __name__ == "__main__":
    logger.info("=" * 55)
    logger.info("  Financial Data Lakehouse — Bronze Consumer")
    logger.info("=" * 55)

    minio_client  = create_minio_client()
    kafka_consumer = create_kafka_consumer()

    batch        = []
    batch_num    = 0
    total_trades = 0

    logger.info(f"Listening for trades | Batch size: {BATCH_SIZE}")
    logger.info("Press Ctrl+C to stop\n")

    try:
        for message in kafka_consumer:
            trade = message.value
            batch.append(trade)

            # When batch is full — write to MinIO
            if len(batch) >= BATCH_SIZE:
                batch_num += 1
                total_trades += len(batch)
                write_batch_to_bronze(minio_client, batch, batch_num)
                batch = []  # Reset batch for next round

    except KeyboardInterrupt:
        # Write any remaining trades before shutting down
        if batch:
            batch_num += 1
            total_trades += len(batch)
            write_batch_to_bronze(minio_client, batch, batch_num)

        logger.info(f"\nConsumer stopped.")
        logger.info(f"Total batches written: {batch_num}")
        logger.info(f"Total trades landed:   {total_trades}")
        kafka_consumer.close()
