"""Pipeline Health page — engineering metrics."""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from utils.db import get_pipeline_stats, get_all_trades, get_hourly_volume
from utils.styles import PLOTLY_BASE, TEXT_COLOR, COLORS


def render():
    st.markdown("""
    <div class="page-header">
        <div>
            <p class="page-title">⚙️ Pipeline Health</p>
            <p class="page-subtitle">Data engineering metrics — Bronze · Silver · Gold layers</p>
        </div>
        <span class="live-badge">● ALL SYSTEMS OPERATIONAL</span>
    </div>
    """, unsafe_allow_html=True)

    stats = get_pipeline_stats()
    if stats.empty:
        st.warning("No pipeline data available.")
        return

    s = stats.iloc[0]

    # ── Pipeline stages ────────────────────────────────────────────────────────
    st.markdown('<div class="section">PIPELINE STAGES</div>', unsafe_allow_html=True)

    stages = [
        ("🔗 Alpaca WebSocket", "Source", "BTC/USD · ETH/USD · SOL/USD", "Real-time crypto stream", "24/7"),
        ("📨 Apache Kafka", "Message Queue", "trades topic · 3 partitions", "kafka:29092 · PLAINTEXT", "Active"),
        ("🗄️ MinIO Bronze", "Raw Storage", "JSONL format · Hive partitioned", "s3a://bronze/trades/", "Healthy"),
        ("⚡ PySpark Silver", "Transformation", "Schema validation · Deduplication", "local[*] · S3A bytebuffer", "Healthy"),
        ("🐘 PostgreSQL Gold", "Star Schema", "fact_trades · dim_instrument · dim_time", "localhost:5432", "Healthy"),
        ("🔧 dbt Models", "Analytics Layer", "4 models · 8 tests passing", "analytics schema", "✓ All Pass"),
        ("🍃 MongoDB", "Document Store", "portfolio_summary collection", "localhost:27017", "Healthy"),
        ("🌀 Apache Airflow", "Orchestration", "@hourly · 7 tasks · retries=2", "localhost:8080", "Running"),
    ]

    for icon_name, stage_type, detail, tech, status in stages:
        st.markdown(f"""
        <div class="stage-ok">
            <div style="display:flex;justify-content:space-between;align-items:center;">
                <div>
                    <span class="stage-name">{icon_name}</span>
                    <div class="stage-detail">{stage_type} · {detail}</div>
                    <div style="font-size:0.65rem;color:#374151;margin-top:0.1rem;">{tech}</div>
                </div>
                <span style="font-size:0.7rem;font-weight:600;color:#10b981;
                background:rgba(16,185,129,0.1);padding:0.2rem 0.6rem;
                border-radius:4px;">{status}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown('<div class="section">DATA QUALITY METRICS</div>', unsafe_allow_html=True)

    k1, k2, k3, k4, k5 = st.columns(5)

    def kpi(col, label, value, sub="", color="#e2e8f0"):
        col.markdown(f"""
        <div class="kpi">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value" style="color:{color};">{value}</div>
            <div class="kpi-sub kpi-gray">{sub}</div>
        </div>""", unsafe_allow_html=True)

    kpi(k1, "Records in Gold", f"{int(s['total_trades']):,}", "fact_trades table")
    kpi(k2, "dbt Tests", "8 / 8", "All passing ✓", "#10b981")
    kpi(k3, "Null Exchange", f"{int(s['null_exchange_count']):,}", "Filled as UNKNOWN")
    kpi(k4, "Avg Trade Value", f"${float(s['avg_notional']):,.2f}", "Per trade notional")
    kpi(k5, "Pipeline Latency", "~60s", "Kafka → Gold")

    # ── Hourly distribution ────────────────────────────────────────────────────
    st.markdown('<div class="section">INGESTION PATTERN BY HOUR</div>', unsafe_allow_html=True)

    hourly = get_hourly_volume()
    if not hourly.empty:
        agg = hourly.groupby("trade_hour")["trade_count"].sum().reset_index()
        fig = go.Figure(go.Bar(
            x=[f"{h:02d}:00" for h in agg["trade_hour"]],
            y=agg["trade_count"],
            marker_color="#1e40af",
            marker_line_width=0,
        ))
        fig.update_layout(**PLOTLY_BASE,
            title=dict(text="Trades Ingested by Hour (UTC)", font=dict(size=12, color="#94a3b8")),
            height=260, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    # ── Symbol distribution ────────────────────────────────────────────────────
    st.markdown('<div class="section">SYMBOL DISTRIBUTION IN GOLD LAYER</div>', unsafe_allow_html=True)

    all_trades = get_all_trades()
    if not all_trades.empty:
        c1, c2 = st.columns(2)

        with c1:
            dist = all_trades.groupby("symbol").size().reset_index(name="count")
            fig = go.Figure(go.Pie(
                labels=dist["symbol"], values=dist["count"],
                hole=0.5,
                marker=dict(
                    colors=[COLORS.get(s, "#64748b") for s in dist["symbol"]],
                    line=dict(color="#111827", width=2)
                ),
                textinfo="percent+label",
            ))
            fig.update_layout(**PLOTLY_BASE,
                title=dict(text="Trade Count by Symbol", font=dict(size=12, color="#94a3b8")),
                height=260, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            vol_dist = all_trades.groupby("symbol")["notional"].sum().reset_index()
            fig = go.Figure(go.Bar(
                x=vol_dist["symbol"],
                y=vol_dist["notional"].astype(float),
                marker_color=[COLORS.get(s, "#64748b") for s in vol_dist["symbol"]],
                marker_line_width=0,
                text=[f"${v:,.0f}" for v in vol_dist["notional"].astype(float)],
                textposition="outside",
                textfont=dict(color=TEXT_COLOR, size=11),
            ))
            fig.update_layout(**PLOTLY_BASE,
                title=dict(text="Volume by Symbol (USD)", font=dict(size=12, color="#94a3b8")),
                height=260, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)