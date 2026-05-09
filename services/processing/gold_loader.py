"""
gold_loader.py

Production-grade Silver → Gold loader.
Reads clean Parquet from MinIO Silver, loads into PostgreSQL star schema.

Author: Nishant Kadam
Version: 1.0.0

Flow:
    Silver Parquet → pandas DataFrame → PostgreSQL fact_trades + dim_time

Design decisions:
    - pandas for Gold loading (smaller dataset, simpler than Spark)
    - Upsert pattern (INSERT ON CONFLICT) — idempotent, safe to re-run
    - dim_time populated from actual trade dates in data
    - Composite indexes pre-created in DDL — no runtime cost
    - PipelineResult returned — Airflow can check status
"""

import os
import sys
import time
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Tuple

import pandas as pd
from minio import Minio
from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row
import pyarrow.parquet as pq
import pyarrow as pa
from io import BytesIO


# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("lakehouse.gold_loader")


# ── Config dataclasses ─────────────────────────────────────────────────────────
@dataclass
class StorageConfig:
    endpoint: str
    access_key: str
    secret_key: str
    silver_bucket: str

    @classmethod
    def from_env(cls) -> "StorageConfig":
        return cls(
            endpoint=os.getenv("MINIO_ENDPOINT"),
            access_key=os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY"),
            silver_bucket=os.getenv("MINIO_BUCKET_SILVER"),
        )


@dataclass
class PostgresConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> "PostgresConfig":
        return cls(
            host=os.getenv("POSTGRES_HOST"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )

    @property
    def connection_string(self) -> str:
        return (
            f"host={self.host} port={self.port} "
            f"dbname={self.dbname} user={self.user} "
            f"password={self.password}"
        )


@dataclass
class LoaderResult:
    status: str = "PENDING"
    files_read: int = 0
    records_read: int = 0
    dim_time_upserted: int = 0
    fact_trades_inserted: int = 0
    fact_trades_skipped: int = 0
    duration_seconds: float = 0.0
    error_message: Optional[str] = None
    run_timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def log_summary(self, logger: logging.Logger) -> None:
        logger.info(
            f"Loader result | status={self.status} | "
            f"files={self.files_read} | "
            f"records_read={self.records_read} | "
            f"dim_time_upserted={self.dim_time_upserted} | "
            f"fact_trades_inserted={self.fact_trades_inserted} | "
            f"skipped={self.fact_trades_skipped} | "
            f"duration={self.duration_seconds:.1f}s"
        )


# ── MinIO reader ───────────────────────────────────────────────────────────────
def read_silver_to_dataframe(
    storage_cfg: StorageConfig
) -> Tuple[pd.DataFrame, int]:
    """
    Reads all Silver Parquet files from MinIO into a pandas DataFrame.
    Adds partition columns trade_date and trade_hour from the MinIO file path.
    """
    logger.info("Connecting to MinIO Silver bucket...")

    client = Minio(
        endpoint=storage_cfg.endpoint,
        access_key=storage_cfg.access_key,
        secret_key=storage_cfg.secret_key,
        secure=False
    )

    objects = list(client.list_objects(
        storage_cfg.silver_bucket,
        prefix="trades/",
        recursive=True
    ))

    parquet_files = [
        obj.object_name for obj in objects
        if obj.object_name.endswith(".parquet")
        and not obj.object_name.endswith(".crc")
    ]

    if not parquet_files:
        raise ValueError(
            f"No Parquet files found in "
            f"{storage_cfg.silver_bucket}/trades/"
        )

    logger.info(f"Found {len(parquet_files)} Parquet files in Silver")

    dfs = []

    for file_path in parquet_files:
        response = None

        try:
            response = client.get_object(
                storage_cfg.silver_bucket,
                file_path
            )

            data = response.read()
            table = pq.read_table(BytesIO(data))
            df_part = table.to_pandas()

            # Extract partition values from path:
            # trades/trade_date=2026-05-08/trade_hour=10/file.parquet
            path_parts = file_path.replace("\\", "/").split("/")

            trade_date = None
            trade_hour = None

            for part in path_parts:
                if part.startswith("trade_date="):
                    trade_date = part.split("=", 1)[1]
                elif part.startswith("trade_hour="):
                    trade_hour = part.split("=", 1)[1]

            if trade_date is None:
                raise ValueError(f"Missing trade_date partition in path: {file_path}")

            if trade_hour is None:
                raise ValueError(f"Missing trade_hour partition in path: {file_path}")

            df_part["trade_date"] = trade_date
            df_part["trade_hour"] = int(trade_hour)

            dfs.append(df_part)

            logger.info(f"Read: {file_path}")

        finally:
            if response is not None:
                response.close()
                response.release_conn()

    df = pd.concat(dfs, ignore_index=True)

    required_columns = [
        "trade_id", "symbol", "trade_date", "trade_hour",
        "price", "quantity", "notional", "trade_type",
        "source", "timestamp", "ingested_at"
    ]

    missing_columns = [
        col for col in required_columns
        if col not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required columns after Silver read: {missing_columns}. "
            f"Available columns: {df.columns.tolist()}"
        )

    df = df.drop_duplicates(subset=["trade_id"])

    logger.info(
        f"Silver read complete | "
        f"files={len(parquet_files)} | "
        f"records={len(df)} | "
        f"symbols={df['symbol'].unique().tolist()} | "
        f"columns={df.columns.tolist()}"
    )

    return df, len(parquet_files)


# ── dim_time loader ────────────────────────────────────────────────────────────
def upsert_dim_time(
    conn: psycopg.Connection,
    df: pd.DataFrame
) -> int:
    """
    Populates dim_time with all unique trade dates in the data.
    Uses UPSERT (INSERT ON CONFLICT DO NOTHING) — idempotent.

    Extracts date components from trade_date column.
    day_of_week_num: 1=Monday, 7=Sunday (ISO standard).

    Returns:
        count: Number of rows upserted
    """
    # Get unique dates from data
    unique_dates = df["trade_date"].unique()
    logger.info(f"Upserting dim_time | dates={len(unique_dates)}")

    count = 0
    with conn.cursor() as cur:
        for date_val in unique_dates:
            # Handle both string and datetime date types
            if isinstance(date_val, str):
                date_obj = pd.to_datetime(date_val).date()
            else:
                date_obj = pd.Timestamp(date_val).date()

            cur.execute("""
                INSERT INTO dim_time (
                    trade_date, year, month, day, quarter,
                    day_of_week, day_of_week_num,
                    is_weekend, month_name
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                ON CONFLICT (trade_date) DO NOTHING
            """, (
                date_obj,
                date_obj.year,
                date_obj.month,
                date_obj.day,
                (date_obj.month - 1) // 3 + 1,
                date_obj.strftime("%A"),
                date_obj.isoweekday(),
                date_obj.isoweekday() in (6, 7),
                date_obj.strftime("%B")
            ))
            count += cur.rowcount

    conn.commit()
    logger.info(f"dim_time upsert complete | rows={count}")
    return count


# ── fact_trades loader ─────────────────────────────────────────────────────────
def insert_fact_trades(
    conn: psycopg.Connection,
    df: pd.DataFrame
) -> Tuple[int, int]:
    """
    Inserts trade records into fact_trades.
    Uses ON CONFLICT DO NOTHING — skips already-loaded trades.

    Why ON CONFLICT DO NOTHING instead of UPDATE?
    Trades are immutable — a trade that happened cannot be edited.
    If the same trade_id appears again, we skip it silently.
    This is correct financial data behavior.

    Returns:
        inserted: Number of new rows inserted
        skipped: Number of rows that already existed
    """
    logger.info(f"Inserting fact_trades | records={len(df)}")

    # Columns to insert — matches DDL exactly
    columns = [
        "trade_id", "symbol", "trade_date", "trade_hour",
        "price", "quantity", "notional", "trade_type",
        "exchange", "source", "timestamp", "ingested_at"
    ]

    inserted = 0
    skipped  = 0

    with conn.cursor() as cur:
        for _, row in df.iterrows():
            try:
                # Handle timestamp conversion
                timestamp = pd.Timestamp(row["timestamp"])
                if timestamp.tzinfo is None:
                    timestamp = timestamp.tz_localize("UTC")

                ingested_at = pd.Timestamp(row["ingested_at"])
                if ingested_at.tzinfo is None:
                    ingested_at = ingested_at.tz_localize("UTC")

                # Handle trade_date
                if isinstance(row["trade_date"], str):
                    trade_date = pd.to_datetime(row["trade_date"]).date()
                else:
                    trade_date = pd.Timestamp(row["trade_date"]).date()

                cur.execute("""
                    INSERT INTO fact_trades (
                        trade_id, symbol, trade_date, trade_hour,
                        price, quantity, notional, trade_type,
                        exchange, source, timestamp, ingested_at
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                    ON CONFLICT (trade_id) DO NOTHING
                """, (
                    str(row["trade_id"]),
                    str(row["symbol"]),
                    trade_date,
                    int(str(row["trade_hour"])),
                    float(row["price"]),
                    float(row["quantity"]),
                    float(row["notional"]),
                    str(row["trade_type"]),
                    str(row.get("exchange", "UNKNOWN")),
                    str(row["source"]),
                    timestamp.to_pydatetime(),
                    ingested_at.to_pydatetime()
                ))

                if cur.rowcount == 1:
                    inserted += 1
                else:
                    skipped += 1

            except Exception as e:
                logger.warning(
                    f"Row insert failed | "
                    f"trade_id={row.get('trade_id', 'unknown')} | "
                    f"error={str(e)}"
                )
                skipped += 1

    conn.commit()
    logger.info(
        f"fact_trades insert complete | "
        f"inserted={inserted} | skipped={skipped}"
    )
    return inserted, skipped


# ── Verification queries ───────────────────────────────────────────────────────
def verify_gold(conn: psycopg.Connection) -> None:
    """
    Runs verification queries to confirm data loaded correctly.
    Shows the same business metrics an analyst would look at.
    """
    logger.info("Running Gold layer verification queries...")

    with conn.cursor(row_factory=dict_row) as cur:

        # Total trade count
        cur.execute("SELECT COUNT(*) as total FROM fact_trades")
        total = cur.fetchone()["total"]
        logger.info(f"Total trades in Gold: {total}")

        # Symbol distribution
        cur.execute("""
            SELECT
                i.symbol,
                i.asset_class,
                COUNT(f.trade_id)          AS trade_count,
                ROUND(AVG(f.price)::numeric, 2) AS avg_price,
                ROUND(SUM(f.notional)::numeric, 2) AS total_notional
            FROM fact_trades f
            JOIN dim_instrument i ON f.symbol = i.symbol
            GROUP BY i.symbol, i.asset_class
            ORDER BY trade_count DESC
        """)
        rows = cur.fetchall()

        logger.info("Symbol distribution in Gold:")
        logger.info(f"{'Symbol':<12} {'Count':<8} {'Avg Price':<15} {'Total Notional'}")
        logger.info("-" * 55)
        for row in rows:
            logger.info(
                f"{row['symbol']:<12} "
                f"{row['trade_count']:<8} "
                f"${row['avg_price']:<14} "
                f"${row['total_notional']}"
            )

        # Date coverage
        cur.execute("""
            SELECT
                t.trade_date,
                t.day_of_week,
                COUNT(f.trade_id) AS trade_count
            FROM fact_trades f
            JOIN dim_time t ON f.trade_date = t.trade_date
            GROUP BY t.trade_date, t.day_of_week
            ORDER BY t.trade_date
        """)
        dates = cur.fetchall()
        logger.info(f"\nDate coverage: {len(dates)} trading day(s)")
        for d in dates:
            logger.info(
                f"  {d['trade_date']} ({d['day_of_week']}) — "
                f"{d['trade_count']} trades"
            )


# ── Main orchestrator ──────────────────────────────────────────────────────────
def run_gold_loader(
    storage_cfg: StorageConfig,
    postgres_cfg: PostgresConfig
) -> LoaderResult:
    """
    Main orchestrator — reads Silver, loads Gold.
    Returns structured LoaderResult for Airflow integration.
    """
    result = LoaderResult()
    start  = time.time()

    try:
        # Read Silver
        df, file_count = read_silver_to_dataframe(storage_cfg)
        result.files_read   = file_count
        result.records_read = len(df)

        # Connect to PostgreSQL
        logger.info(
            f"Connecting to PostgreSQL | "
            f"host={postgres_cfg.host}:{postgres_cfg.port} | "
            f"db={postgres_cfg.dbname}"
        )
        conn = psycopg.connect(postgres_cfg.connection_string)
        logger.info("PostgreSQL connection established")

        # Load dimensions first — fact table references them
        result.dim_time_upserted = upsert_dim_time(conn, df)

        # Load facts
        result.fact_trades_inserted, result.fact_trades_skipped = (
            insert_fact_trades(conn, df)
        )

        # Verify
        verify_gold(conn)

        conn.close()
        result.status = "SUCCESS"

    except Exception as e:
        result.status        = "FAILED"
        result.error_message = str(e)
        logger.error(f"Gold loader failed | error={str(e)}", exc_info=True)
        raise

    finally:
        result.duration_seconds = time.time() - start
        result.log_summary(logger)

    return result


# ── Entrypoint ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    load_dotenv()

    logger.info("=" * 60)
    logger.info("  Financial Data Lakehouse — Gold Loader v1.0")
    logger.info("  Silver Parquet → PostgreSQL Star Schema")
    logger.info("=" * 60)

    storage_cfg  = StorageConfig.from_env()
    postgres_cfg = PostgresConfig.from_env()

    result = run_gold_loader(storage_cfg, postgres_cfg)

    if result.status == "FAILED":
        sys.exit(1)

    logger.info(f"Gold loader finished | status={result.status}")