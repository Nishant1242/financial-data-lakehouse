"""Market Analysis page — filters, trends, price analysis."""

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime, date, timedelta
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from utils.db import get_daily_summary, get_recent_trades, get_hourly_volume
from utils.styles import COLORS, PLOTLY_BASE, TEXT_COLOR, CHART_BG


def render():
    st.markdown("""
    <div class="page-header">
        <div>
            <p class="page-title">📈 Market Analysis</p>
            <p class="page-subtitle">Price trends, volume analysis and market microstructure</p>
        </div>
        <span class="live-badge">● LIVE DATA</span>
    </div>
    """, unsafe_allow_html=True)

    # ── Sidebar filters ────────────────────────────────────────────────────────
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🔍 Filters")

    symbol_filter = st.sidebar.selectbox(
        "Symbol", ["All", "BTC/USD", "ETH/USD", "SOL/USD"]
    )

    daily_all = get_daily_summary()
    if not daily_all.empty:
        min_date = pd.to_datetime(daily_all["trade_date"]).min().date()
        max_date = pd.to_datetime(daily_all["trade_date"]).max().date()
    else:
        min_date = max_date = date.today()

    date_range = st.sidebar.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    start_date = date_range[0] if len(date_range) > 0 else min_date
    end_date   = date_range[1] if len(date_range) > 1 else max_date

    st.sidebar.markdown("---")
    show_volume  = st.sidebar.checkbox("Show Volume", value=True)
    show_price   = st.sidebar.checkbox("Show Price Range", value=True)
    show_hourly  = st.sidebar.checkbox("Show Hourly Heatmap", value=True)
    trade_limit  = st.sidebar.slider("Recent Trades", 10, 100, 30)

    # ── Load filtered data ─────────────────────────────────────────────────────
    sym   = symbol_filter if symbol_filter != "All" else None
    daily = get_daily_summary(symbol=sym, start_date=start_date, end_date=end_date)
    recent = get_recent_trades(symbol=sym, limit=trade_limit)

    if daily.empty:
        st.info("No data for selected filters. Try adjusting the date range or symbol.")
        return

    daily["trade_date"] = pd.to_datetime(daily["trade_date"])

    # ── Summary KPIs for filtered range ───────────────────────────────────────
    k1, k2, k3, k4 = st.columns(4)

    total_vol    = daily["total_notional_usd"].astype(float).sum()
    total_trades = daily["trade_count"].sum()
    avg_price    = daily["avg_price"].astype(float).mean()
    days_covered = daily["trade_date"].nunique()

    def kpi(col, label, value, sub):
        col.markdown(f"""
        <div class="kpi">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
            <div class="kpi-sub kpi-gray">{sub}</div>
        </div>""", unsafe_allow_html=True)

    kpi(k1, "Volume (Filtered)", f"${total_vol:,.0f}", f"{days_covered} days")
    kpi(k2, "Trades (Filtered)", f"{int(total_trades):,}", f"Avg ${total_vol/max(total_trades,1):,.2f}/trade")
    kpi(k3, "Avg Price", f"${avg_price:,.2f}", f"{symbol_filter}")
    kpi(k4, "Date Range", f"{days_covered} days", f"{start_date} → {end_date}")

    # ── Volume trend ───────────────────────────────────────────────────────────
    if show_volume:
        st.markdown('<div class="section">VOLUME TREND</div>', unsafe_allow_html=True)
        fig = go.Figure()

        symbols_to_plot = ["BTC/USD", "ETH/USD", "SOL/USD"] if symbol_filter == "All" else [symbol_filter]
        fill_colors = {
            "BTC/USD": "rgba(247,147,26,0.06)",
            "ETH/USD": "rgba(98,126,234,0.06)",
            "SOL/USD": "rgba(153,69,255,0.06)"
        }

        for sym_plot in symbols_to_plot:
            d = daily[daily["symbol"] == sym_plot] if symbol_filter == "All" else daily
            d = d.groupby("trade_date")["total_notional_usd"].sum().reset_index()
            if not d.empty:
                fig.add_trace(go.Scatter(
                    x=d["trade_date"],
                    y=d["total_notional_usd"].astype(float),
                    name=sym_plot, mode="lines+markers",
                    line=dict(color=COLORS.get(sym_plot, "#64748b"), width=2.5),
                    marker=dict(size=8),
                    fill="tozeroy",
                    fillcolor=fill_colors.get(sym_plot, "rgba(100,100,100,0.05)"),
                ))

        fig.update_layout(**PLOTLY_BASE,
            title=dict(text="Daily Trading Volume (USD)", font=dict(size=12, color="#94a3b8")),
            height=300, hovermode="x unified",
            legend=dict(bgcolor="rgba(0,0,0,0)"))
        st.plotly_chart(fig, use_container_width=True)

    # ── Price range OHLC style ─────────────────────────────────────────────────
    if show_price:
        st.markdown('<div class="section">PRICE ANALYSIS</div>', unsafe_allow_html=True)
        c1, c2 = st.columns(2)

        with c1:
            # Daily avg price line
            fig = go.Figure()
            symbols_to_plot = ["BTC/USD","ETH/USD","SOL/USD"] if symbol_filter == "All" else [symbol_filter]
            for sym_plot in symbols_to_plot:
                d = daily[daily["symbol"] == sym_plot] if symbol_filter == "All" else daily
                if not d.empty:
                    fig.add_trace(go.Scatter(
                        x=d["trade_date"],
                        y=d["avg_price"].astype(float),
                        name=sym_plot,
                        mode="lines+markers",
                        line=dict(color=COLORS.get(sym_plot, "#64748b"), width=2),
                        marker=dict(size=6),
                    ))
            fig.update_layout(**PLOTLY_BASE,
                title=dict(text="Average Price Over Time", font=dict(size=12, color="#94a3b8")),
                height=280, hovermode="x unified",
                legend=dict(bgcolor="rgba(0,0,0,0)"))
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            # High-Low range
            fig = go.Figure()
            for sym_plot in (["BTC/USD","ETH/USD","SOL/USD"] if symbol_filter == "All" else [symbol_filter]):
                d = daily[daily["symbol"] == sym_plot] if symbol_filter == "All" else daily
                if not d.empty:
                    fig.add_trace(go.Scatter(
                        x=list(d["trade_date"]) + list(d["trade_date"])[::-1],
                        y=list(d["high_price"].astype(float)) + list(d["low_price"].astype(float))[::-1],
                        fill="toself",
                        name=f"{sym_plot} Range",
                        fillcolor=f"rgba({','.join(str(int(COLORS.get(sym_plot,'#64748b').lstrip('#')[i:i+2],16)) for i in (0,2,4))},0.15)",
                        line=dict(color=COLORS.get(sym_plot, "#64748b"), width=0.5),
                    ))
            fig.update_layout(**PLOTLY_BASE,
                title=dict(text="Daily High/Low Range", font=dict(size=12, color="#94a3b8")),
                height=280, hovermode="x unified",
                legend=dict(bgcolor="rgba(0,0,0,0)"))
            st.plotly_chart(fig, use_container_width=True)

    # ── Hourly heatmap ─────────────────────────────────────────────────────────
    if show_hourly:
        st.markdown('<div class="section">HOURLY ACTIVITY HEATMAP</div>', unsafe_allow_html=True)
        hourly = get_hourly_volume()
        if not hourly.empty:
            pivot = hourly.pivot_table(
                index="symbol", columns="trade_hour",
                values="trade_count", aggfunc="sum", fill_value=0
            )
            fig = go.Figure(go.Heatmap(
                z=pivot.values,
                x=[f"{h:02d}:00" for h in pivot.columns],
                y=pivot.index.tolist(),
                colorscale=[[0,"#111827"],[0.5,"#1e40af"],[1,"#f7931a"]],
                text=pivot.values,
                texttemplate="%{text}",
                textfont=dict(size=10),
                hovertemplate="Hour: %{x}<br>Symbol: %{y}<br>Trades: %{z}<extra></extra>"
            ))
            fig.update_layout(**PLOTLY_BASE,
                title=dict(text="Trades by Hour (UTC)", font=dict(size=12, color="#94a3b8")),
                height=220)
            st.plotly_chart(fig, use_container_width=True)

    # ── Recent trades table ────────────────────────────────────────────────────
    st.markdown('<div class="section">TRADE LOG</div>', unsafe_allow_html=True)
    if not recent.empty:
        recent["timestamp"] = pd.to_datetime(recent["timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        recent["price"]    = recent["price"].apply(lambda x: f"${float(x):,.4f}")
        recent["notional"] = recent["notional"].apply(lambda x: f"${float(x):,.2f}")
        st.dataframe(
            recent[["timestamp","symbol","price","quantity","notional","trade_hour"]].rename(columns={
                "timestamp":"Timestamp","symbol":"Symbol","price":"Price",
                "quantity":"Qty","notional":"Notional","trade_hour":"Hour"
            }),
            use_container_width=True, hide_index=True, height=350
        )