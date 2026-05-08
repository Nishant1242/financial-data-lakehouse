-- mart_symbol_performance.sql
--
-- Overall symbol performance across all trading days.
-- Answers: "Which crypto performed best? Which had most volume?"
-- Used for executive dashboards and portfolio reports.

WITH trades AS (
    SELECT * FROM {{ ref('stg_trades') }}
),

instruments AS (
    SELECT * FROM {{ ref('stg_instruments') }}
),

performance AS (
    SELECT
        t.symbol,
        i.display_name,
        i.asset_class,
        i.is_major_coin,

        -- Volume
        COUNT(*)                                AS total_trades,
        ROUND(SUM(t.notional)::NUMERIC, 2)      AS total_notional_usd,
        ROUND(AVG(t.notional)::NUMERIC, 2)      AS avg_notional_per_trade,

        -- Price range across all time
        ROUND(MIN(t.price)::NUMERIC, 2)         AS all_time_low,
        ROUND(MAX(t.price)::NUMERIC, 2)         AS all_time_high,
        ROUND(AVG(t.price)::NUMERIC, 2)         AS avg_price,

        -- Price volatility (standard deviation)
        ROUND(STDDEV(t.price)::NUMERIC, 2)      AS price_stddev,

        -- Trading days covered
        COUNT(DISTINCT t.trade_date)            AS trading_days,
        MIN(t.trade_date)                       AS first_seen,
        MAX(t.trade_date)                       AS last_seen,

        -- Pipeline metadata
        MAX(t.ingested_at)                      AS last_updated

    FROM trades t
    JOIN instruments i ON t.symbol = i.symbol
    GROUP BY
        t.symbol,
        i.display_name,
        i.asset_class,
        i.is_major_coin
)

SELECT
    *,
    -- Rank by total volume
    RANK() OVER (ORDER BY total_notional_usd DESC)  AS volume_rank,
    -- Rank by trade count
    RANK() OVER (ORDER BY total_trades DESC)         AS activity_rank
FROM performance
ORDER BY total_notional_usd DESC