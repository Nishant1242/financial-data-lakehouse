"""
db.py — Shared database connection and query functions.
All pages import from here — single source of truth.
"""

import os
import streamlit as st
import psycopg
from psycopg.rows import dict_row
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def query(sql: str, params=None) -> pd.DataFrame:
    """Run a SQL query and return a DataFrame — fresh connection each time."""
    conn = psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "lakehouse"),
        user=os.getenv("POSTGRES_USER", "lakehouse_user"),
        password=os.getenv("POSTGRES_PASSWORD", "lakehouse_pass"),
        row_factory=dict_row
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            rows = cur.fetchall()
        return pd.DataFrame(rows)
    finally:
        conn.close()



@st.cache_data(ttl=60)
def get_symbol_performance() -> pd.DataFrame:
    return query("""
        SELECT symbol, display_name, asset_class, is_major_coin,
               total_trades, total_notional_usd, avg_notional_per_trade,
               all_time_low, all_time_high, avg_price, price_stddev,
               trading_days, first_seen::text, last_seen::text, volume_rank
        FROM analytics.mart_symbol_performance
        ORDER BY volume_rank
    """)


@st.cache_data(ttl=60)
def get_daily_summary(symbol=None, start_date=None, end_date=None) -> pd.DataFrame:
    sql = """
        SELECT DISTINCT trade_date, day_of_week, is_weekend, symbol,
               trade_count, total_notional_usd,
               low_price, high_price, avg_price, open_price, close_price
        FROM analytics.mart_daily_summary
        WHERE 1=1
    """
    params = []
    if symbol and symbol != "All":
        sql += " AND symbol = %s"
        params.append(symbol)
    if start_date:
        sql += " AND trade_date >= %s"
        params.append(start_date)
    if end_date:
        sql += " AND trade_date <= %s"
        params.append(end_date)
    sql += " ORDER BY trade_date, symbol"
    return query(sql, params)


@st.cache_data(ttl=60)
def get_recent_trades(symbol=None, limit=50) -> pd.DataFrame:
    sql = """
        SELECT symbol, price, quantity, notional,
               timestamp, trade_date, trade_hour, source
        FROM fact_trades
        WHERE 1=1
    """
    params = []
    if symbol and symbol != "All":
        sql += " AND symbol = %s"
        params.append(symbol)
    sql += " ORDER BY timestamp DESC LIMIT %s"
    params.append(limit)
    return query(sql, params)


@st.cache_data(ttl=60)
def get_pipeline_stats() -> pd.DataFrame:
    return query("""
        SELECT
            COUNT(*) as total_trades,
            COUNT(DISTINCT trade_date) as trading_days,
            COUNT(DISTINCT symbol) as symbols,
            MIN(ingested_at)::text as first_ingested,
            MAX(ingested_at)::text as last_ingested,
            ROUND(SUM(notional)::numeric, 2) as total_notional,
            ROUND(AVG(notional)::numeric, 2) as avg_notional,
            COUNT(CASE WHEN exchange = 'UNKNOWN' THEN 1 END) as null_exchange_count
        FROM fact_trades
    """)


@st.cache_data(ttl=60)
def get_hourly_volume() -> pd.DataFrame:
    return query("""
        SELECT trade_hour, symbol, COUNT(*) as trade_count,
               ROUND(SUM(notional)::numeric, 2) as total_notional
        FROM fact_trades
        GROUP BY trade_hour, symbol
        ORDER BY trade_hour, symbol
    """)


@st.cache_data(ttl=60)
def get_all_trades() -> pd.DataFrame:
    return query("""
        SELECT f.trade_id, f.symbol, f.price, f.quantity, f.notional,
               f.trade_type, f.exchange, f.source,
               f.timestamp::text as timestamp,
               f.trade_date::text as trade_date,
               f.trade_hour, f.ingested_at::text as ingested_at,
               i.asset_class, i.base_currency, i.quote_currency
        FROM fact_trades f
        JOIN dim_instrument i ON f.symbol = i.symbol
        ORDER BY f.timestamp DESC
    """)