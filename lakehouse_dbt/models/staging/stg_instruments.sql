-- stg_instruments.sql
--
-- Staging model for dim_instrument.
-- Adds derived fields useful for analysis.

WITH source AS (
    SELECT * FROM {{ source('gold', 'dim_instrument') }}
),

enriched AS (
    SELECT
        symbol,
        base_currency,
        quote_currency,
        asset_class,
        exchange,
        is_active,

        -- Derived: readable display name
        base_currency || ' / ' || quote_currency  AS display_name,

        -- Derived: is this a major coin?
        CASE
            WHEN base_currency IN ('BTC', 'ETH')  THEN TRUE
            ELSE                                        FALSE
        END  AS is_major_coin

    FROM source
    WHERE is_active = TRUE
)

SELECT * FROM enriched