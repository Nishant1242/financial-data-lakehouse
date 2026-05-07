"""
polygon_producer.py

Purpose: connects to Polygon.io WebSocket and streams
live crypto trade events into the Kafka trades topic.

What it does step by step:
1. Opens a WebSocket connection to Polygon.io
2. Authenticates with your API key
3. Subscribes to BTC and ETH trade events
4. For every trade received — publishes it to Kafka
5. Runs forever until you press Ctrl+C
"""

import os
import json
import time
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaProducer
import websocket

# ── Load environment variables ──────────────────────────
load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_TRADES")

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
    Creates and returns a Kafka producer client.
    The producer is the object that publishes messages to Kafka topics.
    """
    logger.info("Connecting to Kafka...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        retry_backoff_ms=500
    )
    logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
    return producer


# ── Process incoming WebSocket message ──────────────────
def process_message(producer, raw_message):
    """
    Receives a raw message from Polygon.io,
    parses it, enriches it, and sends it to Kafka.
    """
    global trade_count

    try:
        messages = json.loads(raw_message)

        for msg in messages:
            event_type = msg.get("ev")

            # Handle authentication response
            if event_type == "status":
                status = msg.get("status")
                message = msg.get("message", "")
                logger.info(f"Polygon status: {status} — {message}")

                if status == "auth_success":
                    logger.info("Authentication successful!")
                elif status == "auth_failed":
                    logger.error("Authentication failed — check your API key")

            # Handle real trade events
            elif event_type == "XT":
                trade = {
                    "trade_id":       msg.get("i", ""),
                    "symbol":         msg.get("pair", ""),
                    "price":          float(msg.get("p", 0)),
                    "quantity":       float(msg.get("s", 0)),
                    "notional":       float(msg.get("p", 0)) * float(msg.get("s", 0)),
                    "exchange_id":    msg.get("x", ""),
                    "trade_type":     "CRYPTO",
                    "timestamp":      msg.get("t", 0),
                    "ingested_at":    datetime.now(timezone.utc).isoformat(),
                    "source":         "polygon.io"
                }

                # Use symbol as Kafka message key
                # This ensures all trades for same symbol
                # go to the same partition — ordering guaranteed
                producer.send(
                    topic=KAFKA_TOPIC,
                    key=trade["symbol"],
                    value=trade
                )

                trade_count += 1

                if trade_count % 10 == 0:
                    logger.info(
                        f"Trades published: {trade_count} | "
                        f"Latest: {trade['symbol']} @ "
                        f"${trade['price']:,.2f} | "
                        f"Qty: {trade['quantity']}"
                    )

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse message: {e}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


# ── WebSocket event handlers ─────────────────────────────
def on_open(ws, producer):
    """Called when WebSocket connection is established."""
    logger.info("WebSocket connection opened")

    # Step 1: authenticate
    auth_message = json.dumps({"action": "auth", "params": POLYGON_API_KEY})
    ws.send(auth_message)
    logger.info("Authentication request sent")

    # Step 2: subscribe to BTC and ETH trades
    subscribe_message = json.dumps({
        "action": "subscribe",
        "params": "XT.BTC-USD,XT.ETH-USD"
    })
    ws.send(subscribe_message)
    logger.info("Subscribed to BTC-USD and ETH-USD trades")


def on_message(ws, message, producer):
    """Called every time a new message arrives from Polygon.io."""
    process_message(producer, message)


def on_error(ws, error):
    """Called when a WebSocket error occurs."""
    logger.error(f"WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    """Called when the WebSocket connection closes."""
    logger.info(f"WebSocket closed | Code: {close_status_code} | {close_msg}")
    logger.info(f"Total trades published: {trade_count}")


# ── Main entrypoint ──────────────────────────────────────
if __name__ == "__main__":
    logger.info("=" * 55)
    logger.info("  Financial Data Lakehouse — Polygon.io Producer")
    logger.info("=" * 55)

    # Create Kafka producer
    producer = create_kafka_producer()

    # Create WebSocket connection with handler functions
    ws = websocket.WebSocketApp(
        url="wss://socket.polygon.io/crypto",
        on_open=lambda ws: on_open(ws, producer),
        on_message=lambda ws, msg: on_message(ws, msg, producer),
        on_error=on_error,
        on_close=on_close
    )

    logger.info("Connecting to Polygon.io WebSocket...")
    logger.info("Press Ctrl+C to stop\n")

    try:
        # run_forever keeps the WebSocket connection alive
        # It automatically reconnects if the connection drops
        ws.run_forever(ping_interval=30, ping_timeout=10)
    except KeyboardInterrupt:
        logger.info("\nShutting down producer...")
        producer.flush()
        producer.close()
        logger.info(f"Producer stopped. Total trades published: {trade_count}")
