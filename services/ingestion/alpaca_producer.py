"""
alpaca_producer.py

Purpose: connects to Alpaca Markets WebSocket and streams
real-time US stock trade events into the Kafka trades topic.

Symbols streamed: AAPL, TSLA, MSFT, AMZN, GOOGL
These are the 5 most traded US stocks — high volume,
real trades happening every second during market hours.

What it does step by step:
1. Opens a WebSocket connection to Alpaca
2. Authenticates with your API key
3. Subscribes to trade events for 5 major stocks
4. For every trade received — publishes it to Kafka
5. Runs forever until you press Ctrl+C
"""

import os
import json
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaProducer
from alpaca.data.live import CryptoDataStream

# ── Load environment variables ──────────────────────────
load_dotenv()

ALPACA_API_KEY    = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
KAFKA_SERVERS     = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC       = os.getenv("KAFKA_TOPIC_TRADES")

# Stocks to stream — 5 highest volume US equities
SYMBOLS = ["BTC/USD", "ETH/USD", "SOL/USD"]

# ── Set up logging ───────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ── Trade counter ────────────────────────────────────────
trade_count = 0


# ── Create Kafka Producer ────────────────────────────────
def create_kafka_producer():
    """
    Creates and returns a Kafka producer.
    Converts Python dicts to JSON bytes before sending.
    """
    logger.info("Connecting to Kafka...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        retry_backoff_ms=500
    )
    logger.info(f"Connected to Kafka at {KAFKA_SERVERS}")
    return producer


# ── Trade handler ────────────────────────────────────────
async def handle_trade(trade, producer):
    """
    Receives a trade event from Alpaca WebSocket.
    Transforms it into our standard format.
    Publishes it to the Kafka trades topic.
    """
    global trade_count

    try:
        # Transform Alpaca trade into our standard format
        trade_event = {
            "trade_id":    str(trade.id) if trade.id else "",
            "symbol":      trade.symbol,
            "price":       float(trade.price),
            "quantity":    float(trade.size),
            "notional":    float(trade.price) * float(trade.size),
            "exchange":    trade.exchange if hasattr(trade, "exchange") else "",
            "trade_type":  "CRYPTO",
            "timestamp":   trade.timestamp.isoformat() if trade.timestamp else "",
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "source":      "alpaca.markets",
            "conditions":  trade.conditions if hasattr(trade, "conditions") else []
        }

        # Publish to Kafka — key by symbol for partition ordering
        producer.send(
            topic=KAFKA_TOPIC,
            key=trade_event["symbol"],
            value=trade_event
        )

        trade_count += 1

        # Log every 10 trades to avoid flooding terminal
        if trade_count % 10 == 0:
            logger.info(
                f"Trades published: {trade_count} | "
                f"Latest: {trade_event['symbol']} @ "
                f"${trade_event['price']:,.2f} | "
                f"Qty: {trade_event['quantity']} | "
                f"Notional: ${trade_event['notional']:,.2f}"
            )

    except Exception as e:
        logger.error(f"Error processing trade: {e}")


# ── Main entrypoint ──────────────────────────────────────
if __name__ == "__main__":
    logger.info("=" * 55)
    logger.info("  Financial Data Lakehouse — Alpaca Producer")
    logger.info("=" * 55)
    logger.info(f"Streaming symbols: {', '.join(SYMBOLS)}")
    logger.info("Press Ctrl+C to stop\n")

    # Create Kafka producer
    producer = create_kafka_producer()

    # Create Alpaca WebSocket stream
    stream = CryptoDataStream(
        api_key=ALPACA_API_KEY,
        secret_key=ALPACA_SECRET_KEY
    )

    # Register trade handler for each symbol
    # lambda wraps async handler to pass producer along
    async def trade_handler(trade):
     await handle_trade(trade, producer)

    stream.subscribe_trades(
     trade_handler,
     *SYMBOLS
    )
   

    logger.info("  Financial Data Lakehouse — Alpaca Crypto Producer")
    logger.info(f"Subscribed to: {', '.join(SYMBOLS)}")

    try:
        stream.run()
    except KeyboardInterrupt:
        logger.info("\nShutting down producer...")
        producer.flush()
        producer.close()
        logger.info(f"Producer stopped. Total trades: {trade_count}")
