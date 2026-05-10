"""Asset Deep Dive page — per-symbol analysis."""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from utils.db import get_symbol_performance, get_recent_trades, get_daily_summary
from utils.styles import COLORS, PLOTLY_BASE, TEXT_COLOR


def render():
    st.markdown("""
    <div class="page-header">
        <div>
            <p class="page-title">💎 Asset Deep Dive</p>
            <p class="page-subtitle">Per-asset performance, volatility and trade distribution</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Sidebar
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 💎 Asset Selection")
    selected = st.sidebar.radio("Select Asset", ["BTC/USD", "ETH/USD", "SOL/USD"])

    perf  = get_symbol_performance()
    daily = get_daily_summary(symbol=selected)
    recent = get_recent_trades(symbol=selected, limit=50)

    row = perf[perf["symbol"] == selected]
    if row.empty:
        st.warning("No data for this asset.")
        return

    r = row.iloc[0]
    color = COLORS.get(selected, "#64748b")
    stddev = float(r["price_stddev"]) if r["price_stddev"] else 0
    volatility = "LOW" if stddev < 100 else "MEDIUM" if stddev < 500 else "HIGH"
    vol_color  = "#10b981" if volatility == "LOW" else "#f59e0b" if volatility == "MEDIUM" else "#ef4444"

    # Asset header card
    st.markdown(f"""
    <div class="asset" style="border-left:4px solid {color}; margin-bottom:1rem;">
        <div style="display:flex;justify-content:space-between;align-items:center;">
            <div>
                <span style="font-size:1.4rem;font-weight:700;color:#e2e8f0;">{selected}</span>
                &nbsp;<span style="font-size:0.72rem;color:#4b5563;">{'★ Major Asset' if r['is_major_coin'] else 'Alternative Asset'}</span>
                <div style="font-size:2rem;font-weight:700;color:{color};margin:0.3rem 0;">
                    ${float(r['avg_price']):,.2f}
                </div>
                <div style="font-size:0.8rem;color:#6b7280;">
                    All-time Low: ${float(r['all_time_low']):,.2f} &nbsp;·&nbsp;
                    All-time High: ${float(r['all_time_high']):,.2f} &nbsp;·&nbsp;
                    σ: ${stddev:,.2f}
                </div>
            </div>
            <div style="text-align:right;">
                <div style="font-size:1.2rem;font-weight:700;color:#e2e8f0;">
                    ${float(r['total_notional_usd']):,.2f}
                </div>
                <div style="font-size:0.75rem;color:#6b7280;">Total Volume</div>
                <div style="font-size:0.78rem;font-weight:600;color:{vol_color};margin-top:0.4rem;">
                    {volatility} VOLATILITY
                </div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # KPIs
    k1, k2, k3, k4, k5 = st.columns(5)
    def kpi(col, label, value, sub=""):
        col.markdown(f"""
        <div class="kpi">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value" style="font-size:1.2rem;">{value}</div>
            <div class="kpi-sub kpi-gray">{sub}</div>
        </div>""", unsafe_allow_html=True)

    kpi(k1, "Total Trades", f"{int(r['total_trades']):,}")
    kpi(k2, "Avg Trade Size", f"${float(r['avg_notional_per_trade']):,.2f}")
    kpi(k3, "Trading Days", f"{int(r['trading_days'])}")
    kpi(k4, "Price Range", f"${float(r['all_time_high'])-float(r['all_time_low']):,.2f}")
    kpi(k5, "Std Deviation", f"${stddev:,.2f}")

    st.markdown('<div class="section">PRICE & VOLUME HISTORY</div>', unsafe_allow_html=True)

    if not daily.empty:
        daily["trade_date"] = pd.to_datetime(daily["trade_date"])
        c1, c2 = st.columns(2)

        with c1:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=daily["trade_date"],
                y=daily["avg_price"].astype(float),
                name="Avg Price",
                line=dict(color=color, width=2.5),
                fill="tozeroy",
                fillcolor=f"rgba({','.join(str(int(color.lstrip('#')[i:i+2],16)) for i in (0,2,4))},0.08)",
                mode="lines+markers", marker=dict(size=7)
            ))
            fig.update_layout(**PLOTLY_BASE,
                title=dict(text=f"{selected} Average Price", font=dict(size=12, color="#94a3b8")),
                height=280, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            fig = go.Figure(go.Bar(
                x=daily["trade_date"],
                y=daily["total_notional_usd"].astype(float),
                marker_color=color, marker_opacity=0.8, marker_line_width=0,
                name="Volume"
            ))
            fig.update_layout(**PLOTLY_BASE,
                title=dict(text=f"{selected} Daily Volume", font=dict(size=12, color="#94a3b8")),
                height=280, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        # OHLC candlestick style
        st.markdown('<div class="section">OHLC PRICE ACTION</div>', unsafe_allow_html=True)
        fig = go.Figure(go.Candlestick(
            x=daily["trade_date"],
            open=daily["open_price"].astype(float),
            high=daily["high_price"].astype(float),
            low=daily["low_price"].astype(float),
            close=daily["close_price"].astype(float),
            increasing_line_color="#10b981",
            decreasing_line_color="#ef4444",
            name=selected
        ))
        fig.update_layout(**PLOTLY_BASE,
            title=dict(text=f"{selected} Open / High / Low / Close", font=dict(size=12, color="#94a3b8")),
            height=320, xaxis_rangeslider_visible=False)
        st.plotly_chart(fig, use_container_width=True)

    # Recent trades
    st.markdown('<div class="section">RECENT TRADES</div>', unsafe_allow_html=True)
    if not recent.empty:
        recent["timestamp"] = pd.to_datetime(recent["timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        recent["price"]    = recent["price"].apply(lambda x: f"${float(x):,.4f}")
        recent["notional"] = recent["notional"].apply(lambda x: f"${float(x):,.2f}")
        st.dataframe(
            recent[["timestamp","price","quantity","notional","trade_hour"]].rename(columns={
                "timestamp":"Timestamp","price":"Price",
                "quantity":"Qty","notional":"Notional","trade_hour":"Hour"
            }),
            use_container_width=True, hide_index=True, height=350
        )