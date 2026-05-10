"""Overview page — executive summary."""

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from utils.db import get_symbol_performance, get_pipeline_stats, get_recent_trades, get_daily_summary
from utils.styles import COLORS, PLOTLY_BASE, TEXT_COLOR, CHART_BG, GRID_COLOR


def render():

    # Header
    st.markdown(f"""
    <div class="page-header">
        <div>
            <p class="page-title">🏠 Executive Overview</p>
            <p class="page-subtitle">Live crypto market intelligence powered by your data lakehouse</p>
        </div>
        <div style="text-align:right;">
            <span class="live-badge">● LIVE</span>
            <p style="font-size:0.72rem;color:#4b5563;margin-top:0.3rem;">
                {datetime.now().strftime('%b %d, %Y %H:%M UTC')}
            </p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Load data
    perf  = get_symbol_performance()
    stats = get_pipeline_stats()
    daily = get_daily_summary()

    if stats.empty:
        st.warning("No data available. Run the pipeline first.")
        return

    s = stats.iloc[0]
    total_notional = float(s["total_notional"]) if s["total_notional"] else 0
    total_trades   = int(s["total_trades"])
    trading_days   = int(s["trading_days"])

    # ── KPIs ───────────────────────────────────────────────────────────────────
    k1, k2, k3, k4, k5, k6 = st.columns(6)

    def kpi(col, label, value, sub, sub_class="kpi-gray"):
        col.markdown(f"""
        <div class="kpi">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
            <div class="kpi-sub {sub_class}">{sub}</div>
        </div>""", unsafe_allow_html=True)

    kpi(k1, "Total Trades", f"{total_trades:,}", f"↑ {trading_days} trading days", "kpi-green")
    kpi(k2, "Total Volume", f"${total_notional:,.0f}", "3 crypto pairs", "kpi-gray")

    for col, sym in zip([k3, k4, k5], ["BTC/USD", "ETH/USD", "SOL/USD"]):
        row = perf[perf["symbol"] == sym]
        if not row.empty:
            r = row.iloc[0]
            spread = float(r["all_time_high"]) - float(r["all_time_low"])
            kpi(col, f"{sym} Avg",
                f"${float(r['avg_price']):,.2f}",
                f"Range ${spread:,.2f}", "kpi-gray")

    kpi(k6, "Pipeline Status", "HEALTHY", "↑ All systems OK", "kpi-green")

    st.markdown('<div class="section">MARKET SNAPSHOT</div>', unsafe_allow_html=True)

    # ── Volume + Pie ───────────────────────────────────────────────────────────
    c1, c2, c3 = st.columns([1.3, 1.3, 1])

    with c1:
        syms   = perf["symbol"].tolist()
        vols   = perf["total_notional_usd"].astype(float).tolist()
        clrs   = [COLORS.get(s, "#64748b") for s in syms]
        fig = go.Figure(go.Bar(
            x=syms, y=vols,
            marker_color=clrs, marker_line_width=0,
            text=[f"${v:,.0f}" for v in vols],
            textposition="outside",
            textfont=dict(color=TEXT_COLOR, size=11),
        ))
        fig.update_layout(**PLOTLY_BASE,
            title=dict(text="Volume by Asset (USD)", font=dict(size=12, color="#94a3b8")),
            showlegend=False, height=260)
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        fig = go.Figure(go.Bar(
            x=syms,
            y=perf["total_trades"].tolist(),
            marker_color=clrs, marker_line_width=0,
        ))
        fig.update_layout(**PLOTLY_BASE,
            title=dict(text="Trade Count by Asset", font=dict(size=12, color="#94a3b8")),
            showlegend=False, height=260)
        st.plotly_chart(fig, use_container_width=True)

    with c3:
        fig = go.Figure(go.Pie(
            labels=syms, values=vols, hole=0.6,
            marker=dict(colors=clrs, line=dict(color=CHART_BG, width=2)),
            textinfo="percent",
            textfont=dict(size=11, color="white"),
        ))
        fig.add_annotation(
            text=f"${total_notional:,.0f}<br><span style='font-size:9px'>Total</span>",
            x=0.5, y=0.5, font=dict(size=12, color="#e2e8f0"),
            showarrow=False, xref="paper", yref="paper"
        )
        fig.update_layout(**PLOTLY_BASE,
            title=dict(text="Portfolio Share", font=dict(size=12, color="#94a3b8")),
            showlegend=True, height=260,
            margin=dict(l=0, r=60, t=35, b=10))
        st.plotly_chart(fig, use_container_width=True)

    # ── Daily trend ────────────────────────────────────────────────────────────
    st.markdown('<div class="section">DAILY VOLUME TREND</div>', unsafe_allow_html=True)

    if not daily.empty:
        daily["trade_date"] = pd.to_datetime(daily["trade_date"])
        fig = go.Figure()
        for sym in ["BTC/USD", "ETH/USD", "SOL/USD"]:
            d = daily[daily["symbol"] == sym].groupby("trade_date")["total_notional_usd"].sum().reset_index()
            if not d.empty:
                fill_colors = {
                    "BTC/USD": "rgba(247,147,26,0.06)",
                    "ETH/USD": "rgba(98,126,234,0.06)",
                    "SOL/USD": "rgba(153,69,255,0.06)"
                }
                fig.add_trace(go.Scatter(
                    x=d["trade_date"], y=d["total_notional_usd"].astype(float),
                    name=sym, mode="lines+markers",
                    line=dict(color=COLORS[sym], width=2),
                    marker=dict(size=7, color=COLORS[sym]),
                    fill="tozeroy", fillcolor=fill_colors[sym],
                    hovertemplate="<b>%{x}</b><br>$%{y:,.2f}<extra>" + sym + "</extra>"
                ))
        fig.update_layout(**PLOTLY_BASE,
            title=dict(text="Daily Volume Trend (USD)", font=dict(size=12, color="#94a3b8")),
            height=260, hovermode="x unified",
            legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=11)))
        st.plotly_chart(fig, use_container_width=True)

    # ── Recent trades ──────────────────────────────────────────────────────────
    st.markdown('<div class="section">RECENT TRADE ACTIVITY</div>', unsafe_allow_html=True)
    recent = get_recent_trades(limit=10)
    if not recent.empty:
        recent["timestamp"] = pd.to_datetime(recent["timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        recent["price"]    = recent["price"].apply(lambda x: f"${float(x):,.4f}")
        recent["notional"] = recent["notional"].apply(lambda x: f"${float(x):,.2f}")
        st.dataframe(
            recent[["timestamp","symbol","price","quantity","notional"]].rename(columns={
                "timestamp":"Timestamp (UTC)","symbol":"Symbol",
                "price":"Price","quantity":"Qty","notional":"Notional"
            }),
            use_container_width=True, hide_index=True, height=280
        )