-- stg_trades.sql
--
-- Staging model for fact_trades.
-- Sits directly on top of the raw Gold table.
-- Standardizes column names and adds light transforms.
-- Materialized as VIEW — always fresh, no storage cost.

WITH source AS (
    SELECT * FROM {{ source('gold', 'fact_trades') }}
),

renamed AS (
    SELECT
        -- Identifiers
        trade_id,
        symbol,

        -- Time dimensions
        trade_date,
        trade_hour,
        timestamp         AS traded_at,
        ingested_at,

        -- Financials
        price,
        quantity,
        notional,

        -- Metadata
        trade_type,
        exchange,
        source            AS data_source,

        -- Derived
        CASE
            WHEN price > 50000  THEN 'large_cap'
            WHEN price > 1000   THEN 'mid_cap'
            ELSE                     'small_cap'
        END               AS price_tier

    FROM source
)

SELECT * FROM renamed