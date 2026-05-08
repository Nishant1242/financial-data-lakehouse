-- mart_daily_summary.sql
--
-- Daily trading summary by symbol.
-- This is the primary mart analysts query for performance reports.
-- Materialized as TABLE — pre-computed for fast query response.
--
-- Answers questions like:
--   "What was BTC's total volume on May 8th?"
--   "Which day had the highest SOL trading activity?"

WITH trades AS (
    SELECT * FROM {{ ref('stg_trades') }}
),

time_dim AS (
    SELECT * FROM {{ source('gold', 'dim_time') }}
),

daily_agg AS (
    SELECT
        t.trade_date,
        td.day_of_week,
        td.is_weekend,
        t.symbol,

        -- Volume metrics
        COUNT(*)                              AS trade_count,
        SUM(t.quantity)                       AS total_quantity,
        ROUND(SUM(t.notional)::NUMERIC, 2)    AS total_notional_usd,

        -- Price metrics
        ROUND(MIN(t.price)::NUMERIC, 2)       AS low_price,
        ROUND(MAX(t.price)::NUMERIC, 2)       AS high_price,
        ROUND(AVG(t.price)::NUMERIC, 2)       AS avg_price,

        -- First and last trade of the day (OHLC style)
        ROUND(FIRST_VALUE(t.price) OVER (
            PARTITION BY t.trade_date, t.symbol
            ORDER BY t.traded_at
        )::NUMERIC, 2)                         AS open_price,

        ROUND(LAST_VALUE(t.price) OVER (
            PARTITION BY t.trade_date, t.symbol
            ORDER BY t.traded_at
            ROWS BETWEEN UNBOUNDED PRECEDING
                     AND UNBOUNDED FOLLOWING
        )::NUMERIC, 2)                         AS close_price,

        -- Pipeline metadata
        MIN(t.ingested_at)                    AS first_ingested_at,
        MAX(t.ingested_at)                    AS last_ingested_at

    FROM trades t
    JOIN time_dim td ON t.trade_date = td.trade_date
    GROUP BY
        t.trade_date, td.day_of_week, td.is_weekend,
        t.symbol, t.traded_at, t.price
)

SELECT DISTINCT
    trade_date,
    day_of_week,
    is_weekend,
    symbol,
    trade_count,
    total_quantity,
    total_notional_usd,
    low_price,
    high_price,
    avg_price,
    open_price,
    close_price,
    first_ingested_at,
    last_ingested_at
FROM daily_agg
ORDER BY trade_date, symbol