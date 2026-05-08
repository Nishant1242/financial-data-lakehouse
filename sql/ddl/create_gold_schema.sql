-- =============================================================================
-- create_gold_schema.sql
--
-- Creates the Gold layer star schema in PostgreSQL.
-- Run this once to initialize the database structure.
--
-- Tables created:
--   dim_instrument  — what was traded (BTC, ETH, SOL)
--   dim_time        — when it was traded (date dimensions)
--   fact_trades     — the core trade events (facts)
--
-- Design principles:
--   - Composite indexes on common query patterns
--   - NUMERIC(18,8) for prices — financial precision required
--   - Timestamps in UTC — always store in UTC, display in local
--   - created_at/updated_at on every table — audit trail
-- =============================================================================

-- ── Extensions ────────────────────────────────────────────────────────────────
-- Enable uuid generation for future use
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ── Dimension: Instrument ──────────────────────────────────────────────────────
-- Describes what was traded
-- One row per trading symbol (BTC/USD, ETH/USD, SOL/USD)
CREATE TABLE IF NOT EXISTS dim_instrument (
    symbol          VARCHAR(20)  PRIMARY KEY,
    base_currency   VARCHAR(10)  NOT NULL,   -- BTC, ETH, SOL
    quote_currency  VARCHAR(10)  NOT NULL,   -- USD
    asset_class     VARCHAR(20)  NOT NULL,   -- CRYPTO, EQUITY, FX
    exchange        VARCHAR(50),
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE dim_instrument IS
    'Instrument reference data. One row per trading symbol.';
COMMENT ON COLUMN dim_instrument.symbol IS
    'Trading symbol e.g. BTC/USD. Primary key.';
COMMENT ON COLUMN dim_instrument.base_currency IS
    'The asset being traded e.g. BTC in BTC/USD.';


-- ── Dimension: Time ────────────────────────────────────────────────────────────
-- Date dimension for time-based analysis
-- Pre-populated with dates — analysts filter by day_of_week, is_weekend etc.
CREATE TABLE IF NOT EXISTS dim_time (
    trade_date      DATE         PRIMARY KEY,
    year            SMALLINT     NOT NULL,
    month           SMALLINT     NOT NULL,
    day             SMALLINT     NOT NULL,
    quarter         SMALLINT     NOT NULL,
    day_of_week     VARCHAR(10)  NOT NULL,   -- Monday, Tuesday etc.
    day_of_week_num SMALLINT     NOT NULL,   -- 1=Monday, 7=Sunday
    is_weekend      BOOLEAN      NOT NULL,
    month_name      VARCHAR(10)  NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE dim_time IS
    'Date dimension. One row per calendar date.';
COMMENT ON COLUMN dim_time.is_weekend IS
    'TRUE for Saturday and Sunday. Crypto trades 24/7 but useful for equity analysis.';


-- ── Fact: Trades ──────────────────────────────────────────────────────────────
-- Core trade events. Every row is one trade.
-- References dim_instrument and dim_time via foreign keys.
CREATE TABLE IF NOT EXISTS fact_trades (
    trade_id        VARCHAR(50)      PRIMARY KEY,
    symbol          VARCHAR(20)      NOT NULL
                    REFERENCES dim_instrument(symbol),
    trade_date      DATE             NOT NULL
                    REFERENCES dim_time(trade_date),
    trade_hour      SMALLINT         NOT NULL,
    price           NUMERIC(18, 8)   NOT NULL,   -- 8 decimal places for crypto
    quantity        NUMERIC(18, 8)   NOT NULL,
    notional        NUMERIC(18, 2)   NOT NULL,   -- 2 decimal for dollar amounts
    trade_type      VARCHAR(20)      NOT NULL,   -- CRYPTO, EQUITY
    exchange        VARCHAR(50),
    source          VARCHAR(50)      NOT NULL,   -- alpaca.markets, polygon.io
    timestamp       TIMESTAMPTZ      NOT NULL,   -- exact trade time in UTC
    ingested_at     TIMESTAMPTZ      NOT NULL,   -- when our system received it
    created_at      TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE fact_trades IS
    'Core trade events. One row per trade. Heart of the star schema.';
COMMENT ON COLUMN fact_trades.notional IS
    'price * quantity. Pre-computed for query performance.';
COMMENT ON COLUMN fact_trades.timestamp IS
    'Exact trade timestamp from exchange, stored in UTC.';
COMMENT ON COLUMN fact_trades.ingested_at IS
    'When our pipeline received and processed this trade.';


-- ── Indexes ───────────────────────────────────────────────────────────────────
-- Composite indexes on the most common query patterns
-- Each index trades write speed for read speed
-- These specific patterns come from common risk/analytics queries

-- Most common: filter by symbol AND date range
-- "Show me all BTC trades this week"
CREATE INDEX IF NOT EXISTS idx_fact_trades_symbol_date
    ON fact_trades(symbol, trade_date);

-- Second most common: filter by date only
-- "Show me all trades today"
CREATE INDEX IF NOT EXISTS idx_fact_trades_date
    ON fact_trades(trade_date);

-- For time-series analysis within a day
-- "Show me BTC trades between 9am and 10am"
CREATE INDEX IF NOT EXISTS idx_fact_trades_timestamp
    ON fact_trades(timestamp);

-- For source system queries
-- "Show me all Alpaca trades"
CREATE INDEX IF NOT EXISTS idx_fact_trades_source
    ON fact_trades(source);


-- ── Seed dim_instrument ───────────────────────────────────────────────────────
-- Insert the three instruments we stream from Alpaca
-- ON CONFLICT DO NOTHING = safe to run multiple times (idempotent)
INSERT INTO dim_instrument
    (symbol, base_currency, quote_currency, asset_class, exchange)
VALUES
    ('BTC/USD', 'BTC', 'USD', 'CRYPTO', 'ALPACA'),
    ('ETH/USD', 'ETH', 'USD', 'CRYPTO', 'ALPACA'),
    ('SOL/USD', 'SOL', 'USD', 'CRYPTO', 'ALPACA')
ON CONFLICT (symbol) DO NOTHING;