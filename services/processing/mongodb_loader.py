"""
mongodb_loader.py

Loads Gold layer trade data into MongoDB as client portfolio documents.
Each document represents one symbol's complete trading profile —
aggregated stats + recent trades bundled together.

Why MongoDB here?
- No joins needed — all data in one document
- Flexible schema — add fields without migrations
- Fast single-document reads — ideal for API responses
- Natural fit for portfolio/summary documents

Author: Nishant Kadam
Version: 1.0.0
"""

import os
import sys
import time
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import psycopg
from psycopg.rows import dict_row
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError
from dotenv import load_dotenv


# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("lakehouse.mongodb_loader")


# ── Config ─────────────────────────────────────────────────────────────────────
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
class MongoConfig:
    uri: str
    database: str

    @classmethod
    def from_env(cls) -> "MongoConfig":
        return cls(
            uri=os.getenv("MONGO_URI"),
            database=os.getenv("MONGO_DB"),
        )


@dataclass
class LoaderResult:
    status: str = "PENDING"
    documents_written: int = 0
    documents_updated: int = 0
    duration_seconds: float = 0.0
    error_message: Optional[str] = None

    def log_summary(self, logger: logging.Logger) -> None:
        logger.info(
            f"MongoDB loader result | status={self.status} | "
            f"written={self.documents_written} | "
            f"updated={self.documents_updated} | "
            f"duration={self.duration_seconds:.1f}s"
        )


# ── Data fetcher ───────────────────────────────────────────────────────────────
def fetch_symbol_summary(
    pg_conn: psycopg.Connection
) -> list[dict]:
    """
    Fetches symbol performance summary from PostgreSQL analytics schema.
    This is the aggregated data from your dbt mart_symbol_performance model.
    """
    with pg_conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                symbol,
                display_name,
                asset_class,
                is_major_coin,
                total_trades,
                total_notional_usd,
                avg_notional_per_trade,
                all_time_low,
                all_time_high,
                avg_price,
                price_stddev,
                trading_days,
                first_seen::text,
                last_seen::text,
                last_updated::text,
                volume_rank,
                activity_rank
            FROM analytics.mart_symbol_performance
            ORDER BY volume_rank
        """)
        return cur.fetchall()


def fetch_recent_trades(
    pg_conn: psycopg.Connection,
    symbol: str,
    limit: int = 10
) -> list[dict]:
    """
    Fetches most recent trades for a symbol from Gold fact table.
    Embedded in the MongoDB document for instant access.
    """
    with pg_conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                trade_id,
                price,
                quantity,
                notional,
                trade_type,
                exchange,
                timestamp::text,
                ingested_at::text
            FROM fact_trades
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT %s
        """, (symbol, limit))
        return cur.fetchall()


def fetch_daily_breakdown(
    pg_conn: psycopg.Connection,
    symbol: str
) -> list[dict]:
    """
    Fetches daily OHLC-style summary per symbol.
    Comes from your dbt mart_daily_summary model.
    """
    with pg_conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                trade_date::text,
                day_of_week,
                is_weekend,
                trade_count,
                total_notional_usd,
                low_price,
                high_price,
                avg_price,
                open_price,
                close_price
            FROM analytics.mart_daily_summary
            WHERE symbol = %s
            ORDER BY trade_date DESC
        """, (symbol,))
        return cur.fetchall()


# ── Document builder ───────────────────────────────────────────────────────────
def build_portfolio_document(
    summary: dict,
    recent_trades: list[dict],
    daily_breakdown: list[dict]
) -> dict:
    """
    Builds a complete portfolio document for one symbol.

    Document structure:
    {
        _id: "BTC/USD",              ← use symbol as MongoDB _id
        symbol: "BTC/USD",
        display_name: "BTC / USD",
        performance: { ... },        ← aggregated stats
        recent_trades: [ ... ],      ← last 10 trades
        daily_breakdown: [ ... ],    ← per-day OHLC
        metadata: { ... }            ← pipeline info
    }

    Why embed recent_trades and daily_breakdown?
    A client portfolio API call fetches ONE document and returns
    everything — no joins, no multiple queries, sub-millisecond
    response time. This is the key advantage of document databases.
    """
    return {
        "_id": summary["symbol"],
        "symbol": summary["symbol"],
        "display_name": summary["display_name"],
        "asset_class": summary["asset_class"],
        "is_major_coin": summary["is_major_coin"],

        "performance": {
            "total_trades":           summary["total_trades"],
            "total_notional_usd":     float(summary["total_notional_usd"]),
            "avg_notional_per_trade": float(summary["avg_notional_per_trade"]),
            "all_time_low":           float(summary["all_time_low"]),
            "all_time_high":          float(summary["all_time_high"]),
            "avg_price":              float(summary["avg_price"]),
            "price_stddev":           float(summary["price_stddev"]),
            "trading_days":           summary["trading_days"],
            "first_seen":             summary["first_seen"],
            "last_seen":              summary["last_seen"],
            "volume_rank":            summary["volume_rank"],
            "activity_rank":          summary["activity_rank"],
        },

        "recent_trades": [
            {
                "trade_id":   t["trade_id"],
                "price":      float(t["price"]),
                "quantity":   float(t["quantity"]),
                "notional":   float(t["notional"]),
                "trade_type": t["trade_type"],
                "exchange":   t["exchange"],
                "timestamp":  t["timestamp"],
            }
            for t in recent_trades
        ],

        "daily_breakdown": [
            {
                "date":               d["trade_date"],
                "day_of_week":        d["day_of_week"],
                "is_weekend":         d["is_weekend"],
                "trade_count":        d["trade_count"],
                "total_notional_usd": float(d["total_notional_usd"]),
                "ohlc": {
                    "open":  float(d["open_price"]),
                    "high":  float(d["high_price"]),
                    "low":   float(d["low_price"]),
                    "close": float(d["close_price"]),
                    "avg":   float(d["avg_price"]),
                }
            }
            for d in daily_breakdown
        ],

        "metadata": {
            "source":       "financial_data_lakehouse",
            "pipeline":     "gold_to_mongodb",
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "version":      "1.0.0"
        }
    }


# ── MongoDB writer ─────────────────────────────────────────────────────────────
def setup_mongodb_indexes(collection) -> None:
    """
    Creates indexes on the portfolio collection.
    Indexes make queries fast — without them MongoDB
    scans every document for every query.
    """
    collection.create_index([("symbol", ASCENDING)], unique=True)
    collection.create_index([("asset_class", ASCENDING)])
    collection.create_index([("performance.volume_rank", ASCENDING)])
    collection.create_index([("is_major_coin", ASCENDING)])
    logger.info("MongoDB indexes created")


def upsert_portfolio_documents(
    collection,
    documents: list[dict]
) -> tuple[int, int]:
    """
    Upserts portfolio documents into MongoDB.
    Uses replace_one with upsert=True — creates if not exists,
    replaces if exists. Idempotent — safe to re-run.

    Returns:
        inserted: new documents created
        updated: existing documents replaced
    """
    inserted = 0
    updated  = 0

    for doc in documents:
        result = collection.replace_one(
            {"_id": doc["_id"]},
            doc,
            upsert=True
        )
        if result.upserted_id:
            inserted += 1
        else:
            updated += 1

    return inserted, updated


# ── Main orchestrator ──────────────────────────────────────────────────────────
def run_mongodb_loader(
    postgres_cfg: PostgresConfig,
    mongo_cfg: MongoConfig
) -> LoaderResult:
    """
    Main orchestrator — reads from PostgreSQL Gold, writes to MongoDB.
    """
    result = LoaderResult()
    start  = time.time()

    try:
        # Connect to PostgreSQL
        logger.info("Connecting to PostgreSQL...")
        pg_conn = psycopg.connect(postgres_cfg.connection_string)
        logger.info("PostgreSQL connected")

        # Connect to MongoDB
        logger.info(f"Connecting to MongoDB | uri={mongo_cfg.uri}")
        mongo_client = MongoClient(mongo_cfg.uri)
        db           = mongo_client[mongo_cfg.database]
        collection   = db["portfolio_summary"]
        logger.info(f"MongoDB connected | db={mongo_cfg.database}")

        # Setup indexes
        setup_mongodb_indexes(collection)

        # Fetch symbol summaries from PostgreSQL
        summaries = fetch_symbol_summary(pg_conn)
        logger.info(f"Fetched {len(summaries)} symbol summaries")

        # Build and upsert one document per symbol
        documents = []
        for summary in summaries:
            symbol = summary["symbol"]
            logger.info(f"Building portfolio document | symbol={symbol}")

            recent_trades   = fetch_recent_trades(pg_conn, symbol)
            daily_breakdown = fetch_daily_breakdown(pg_conn, symbol)

            doc = build_portfolio_document(
                summary,
                recent_trades,
                daily_breakdown
            )
            documents.append(doc)
            logger.info(
                f"Document built | symbol={symbol} | "
                f"recent_trades={len(recent_trades)} | "
                f"daily_days={len(daily_breakdown)}"
            )

        # Write to MongoDB
        result.documents_written, result.documents_updated = (
            upsert_portfolio_documents(collection, documents)
        )

        # Verify
        total_docs = collection.count_documents({})
        logger.info(f"MongoDB verification | total_documents={total_docs}")

        # Show sample document structure
        sample = collection.find_one({"symbol": "BTC/USD"})
        if sample:
            logger.info(
                f"Sample BTC/USD document | "
                f"performance.total_trades="
                f"{sample['performance']['total_trades']} | "
                f"recent_trades_count={len(sample['recent_trades'])} | "
                f"daily_breakdown_days="
                f"{len(sample['daily_breakdown'])}"
            )

        pg_conn.close()
        mongo_client.close()
        result.status = "SUCCESS"

    except Exception as e:
        result.status        = "FAILED"
        result.error_message = str(e)
        logger.error(
            f"MongoDB loader failed | error={str(e)}",
            exc_info=True
        )
        raise

    finally:
        result.duration_seconds = time.time() - start
        result.log_summary(logger)

    return result


# ── Entrypoint ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    load_dotenv()

    logger.info("=" * 60)
    logger.info("  Financial Data Lakehouse — MongoDB Loader v1.0")
    logger.info("  PostgreSQL Gold → MongoDB Portfolio Documents")
    logger.info("=" * 60)

    postgres_cfg = PostgresConfig.from_env()
    mongo_cfg    = MongoConfig.from_env()

    result = run_mongodb_loader(postgres_cfg, mongo_cfg)

    if result.status == "FAILED":
        sys.exit(1)

    logger.info(f"MongoDB loader finished | status={result.status}")