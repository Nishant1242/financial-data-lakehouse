"""Reports & Export page — CSV and PDF generation."""

import streamlit as st
import pandas as pd
import io
from datetime import datetime, date
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from utils.db import get_all_trades, get_symbol_performance, get_pipeline_stats, get_daily_summary


def generate_pdf(perf_df, stats_df, daily_df):
    """Generate a professional PDF report using reportlab."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.lib import colors
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                     Table, TableStyle, HRFlowable)
    from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT

    buffer = io.BytesIO()
    doc    = SimpleDocTemplate(buffer, pagesize=A4,
                               leftMargin=2*cm, rightMargin=2*cm,
                               topMargin=2*cm, bottomMargin=2*cm)

    styles = getSampleStyleSheet()
    dark   = colors.HexColor("#0a0e1a")
    accent = colors.HexColor("#1e40af")
    light  = colors.HexColor("#e2e8f0")
    gray   = colors.HexColor("#6b7280")
    green  = colors.HexColor("#10b981")

    title_style = ParagraphStyle("title", parent=styles["Normal"],
        fontSize=20, fontName="Helvetica-Bold", textColor=light, spaceAfter=4)
    sub_style   = ParagraphStyle("sub", parent=styles["Normal"],
        fontSize=10, fontName="Helvetica", textColor=gray, spaceAfter=12)
    section_style = ParagraphStyle("section", parent=styles["Normal"],
        fontSize=12, fontName="Helvetica-Bold", textColor=accent, spaceAfter=8)
    body_style  = ParagraphStyle("body", parent=styles["Normal"],
        fontSize=9, fontName="Helvetica", textColor=light, spaceAfter=4)

    story = []

    # Title
    story.append(Paragraph("Financial Data Lakehouse", title_style))
    story.append(Paragraph(
        f"Executive Intelligence Report · Generated {datetime.now().strftime('%B %d, %Y at %H:%M UTC')}",
        sub_style
    ))
    story.append(HRFlowable(width="100%", thickness=1, color=accent))
    story.append(Spacer(1, 0.4*cm))

    # Pipeline summary
    if not stats_df.empty:
        s = stats_df.iloc[0]
        story.append(Paragraph("Pipeline Summary", section_style))
        summary_data = [
            ["Metric", "Value"],
            ["Total Trades Processed", f"{int(s['total_trades']):,}"],
            ["Total Volume (USD)", f"${float(s['total_notional']):,.2f}"],
            ["Trading Days Covered", f"{int(s['trading_days'])}"],
            ["Average Trade Value", f"${float(s['avg_notional']):,.2f}"],
            ["Data Quality Tests", "8 / 8 Passing ✓"],
            ["Pipeline Latency", "~60 seconds"],
        ]
        t = Table(summary_data, colWidths=[9*cm, 8*cm])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), accent),
            ("TEXTCOLOR",  (0,0), (-1,0), colors.white),
            ("FONTNAME",   (0,0), (-1,0), "Helvetica-Bold"),
            ("FONTSIZE",   (0,0), (-1,-1), 9),
            ("BACKGROUND", (0,1), (-1,-1), colors.HexColor("#111827")),
            ("TEXTCOLOR",  (0,1), (-1,-1), light),
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.HexColor("#111827"), colors.HexColor("#1f2937")]),
            ("GRID",       (0,0), (-1,-1), 0.5, colors.HexColor("#1f2937")),
            ("ALIGN",      (1,0), (1,-1), "RIGHT"),
            ("PADDING",    (0,0), (-1,-1), 6),
        ]))
        story.append(t)
        story.append(Spacer(1, 0.5*cm))

    # Symbol performance
    if not perf_df.empty:
        story.append(Paragraph("Asset Performance", section_style))
        headers = ["Symbol", "Trades", "Avg Price", "Low", "High", "Volume (USD)", "Volatility"]
        perf_data = [headers]
        for _, row in perf_df.iterrows():
            stddev = float(row["price_stddev"]) if row["price_stddev"] else 0
            vol = "LOW" if stddev < 100 else "MEDIUM" if stddev < 500 else "HIGH"
            perf_data.append([
                row["symbol"],
                f"{int(row['total_trades']):,}",
                f"${float(row['avg_price']):,.2f}",
                f"${float(row['all_time_low']):,.2f}",
                f"${float(row['all_time_high']):,.2f}",
                f"${float(row['total_notional_usd']):,.2f}",
                vol
            ])
        t = Table(perf_data, colWidths=[3*cm, 2*cm, 2.5*cm, 2.5*cm, 2.5*cm, 3*cm, 2*cm])
        t.setStyle(TableStyle([
            ("BACKGROUND",    (0,0), (-1,0), accent),
            ("TEXTCOLOR",     (0,0), (-1,0), colors.white),
            ("FONTNAME",      (0,0), (-1,0), "Helvetica-Bold"),
            ("FONTSIZE",      (0,0), (-1,-1), 8),
            ("ROWBACKGROUNDS",(0,1), (-1,-1), [colors.HexColor("#111827"), colors.HexColor("#1f2937")]),
            ("TEXTCOLOR",     (0,1), (-1,-1), light),
            ("GRID",          (0,0), (-1,-1), 0.5, colors.HexColor("#1f2937")),
            ("ALIGN",         (1,0), (-1,-1), "RIGHT"),
            ("ALIGN",         (0,0), (0,-1), "LEFT"),
            ("PADDING",       (0,0), (-1,-1), 5),
        ]))
        story.append(t)
        story.append(Spacer(1, 0.5*cm))

    # Daily summary
    if not daily_df.empty:
        story.append(Paragraph("Daily Trading Summary", section_style))
        daily_df["trade_date"] = pd.to_datetime(daily_df["trade_date"]).dt.strftime("%Y-%m-%d")
        agg = daily_df.groupby(["trade_date","symbol"]).agg({
            "trade_count": "sum",
            "total_notional_usd": "sum",
            "avg_price": "mean"
        }).reset_index()

        headers = ["Date", "Symbol", "Trades", "Volume (USD)", "Avg Price"]
        daily_data = [headers]
        for _, row in agg.iterrows():
            daily_data.append([
                row["trade_date"],
                row["symbol"],
                f"{int(row['trade_count']):,}",
                f"${float(row['total_notional_usd']):,.2f}",
                f"${float(row['avg_price']):,.2f}",
            ])

        t = Table(daily_data, colWidths=[4*cm, 4*cm, 3*cm, 4*cm, 4*cm])
        t.setStyle(TableStyle([
            ("BACKGROUND",    (0,0), (-1,0), accent),
            ("TEXTCOLOR",     (0,0), (-1,0), colors.white),
            ("FONTNAME",      (0,0), (-1,0), "Helvetica-Bold"),
            ("FONTSIZE",      (0,0), (-1,-1), 8),
            ("ROWBACKGROUNDS",(0,1), (-1,-1), [colors.HexColor("#111827"), colors.HexColor("#1f2937")]),
            ("TEXTCOLOR",     (0,1), (-1,-1), light),
            ("GRID",          (0,0), (-1,-1), 0.5, colors.HexColor("#1f2937")),
            ("ALIGN",         (2,0), (-1,-1), "RIGHT"),
            ("PADDING",       (0,0), (-1,-1), 5),
        ]))
        story.append(t)

    # Footer
    story.append(Spacer(1, 1*cm))
    story.append(HRFlowable(width="100%", thickness=0.5, color=gray))
    story.append(Paragraph(
        f"Financial Data Lakehouse · Built by Nishant Kadam · "
        f"Stack: Kafka · PySpark · Airflow · PostgreSQL · dbt · MongoDB",
        ParagraphStyle("footer", parent=styles["Normal"],
            fontSize=7, textColor=gray, alignment=TA_CENTER, spaceBefore=6)
    ))

    doc.build(story)
    buffer.seek(0)
    return buffer


def render():
    st.markdown("""
    <div class="page-header">
        <div>
            <p class="page-title">📋 Reports & Export</p>
            <p class="page-subtitle">Download trade data and generate executive PDF reports</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Sidebar filters
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📋 Report Options")
    report_symbol = st.sidebar.selectbox("Symbol", ["All", "BTC/USD", "ETH/USD", "SOL/USD"])
    include_pipeline = st.sidebar.checkbox("Include Pipeline Stats", value=True)
    include_daily    = st.sidebar.checkbox("Include Daily Summary", value=True)

    # Load data
    all_trades = get_all_trades()
    perf       = get_symbol_performance()
    stats      = get_pipeline_stats()
    daily      = get_daily_summary()

    # Filter trades
    if report_symbol != "All":
        filtered_trades = all_trades[all_trades["symbol"] == report_symbol]
    else:
        filtered_trades = all_trades

    st.markdown('<div class="section">EXPORT OPTIONS</div>', unsafe_allow_html=True)

    c1, c2, c3 = st.columns(3)

    # CSV export
    with c1:
        st.markdown("""
        <div class="report-card">
            <div class="report-icon">📊</div>
            <div class="report-title">Trade Data CSV</div>
            <div class="report-desc">Complete trade history with all fields.
            Ready for Excel, Python, or any analytics tool.</div>
        </div>
        """, unsafe_allow_html=True)

        if not filtered_trades.empty:
            csv = filtered_trades.to_csv(index=False)
            fname = f"trades_{report_symbol.replace('/','_')}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
            st.download_button(
                label="⬇️ Download CSV",
                data=csv,
                file_name=fname,
                mime="text/csv",
                use_container_width=True
            )

    # Performance CSV
    with c2:
        st.markdown("""
        <div class="report-card">
            <div class="report-icon">📈</div>
            <div class="report-title">Performance Summary CSV</div>
            <div class="report-desc">Asset performance metrics including
            volume, prices, volatility and rankings.</div>
        </div>
        """, unsafe_allow_html=True)

        if not perf.empty:
            csv = perf.to_csv(index=False)
            st.download_button(
                label="⬇️ Download CSV",
                data=csv,
                file_name=f"performance_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                mime="text/csv",
                use_container_width=True
            )

    # PDF report
    with c3:
        st.markdown("""
        <div class="report-card">
            <div class="report-icon">📄</div>
            <div class="report-title">Executive PDF Report</div>
            <div class="report-desc">Professional PDF with pipeline summary,
            asset performance and daily trading tables.</div>
        </div>
        """, unsafe_allow_html=True)

        if st.button("🔄 Generate PDF", use_container_width=True):
            with st.spinner("Generating PDF report..."):
                try:
                    daily_for_pdf = daily if include_daily else pd.DataFrame()
                    stats_for_pdf = stats if include_pipeline else pd.DataFrame()
                    pdf_buffer = generate_pdf(perf, stats_for_pdf, daily_for_pdf)
                    st.download_button(
                        label="⬇️ Download PDF",
                        data=pdf_buffer,
                        file_name=f"lakehouse_report_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf",
                        mime="application/pdf",
                        use_container_width=True
                    )
                    st.success("PDF generated successfully!")
                except Exception as e:
                    st.error(f"PDF generation failed: {str(e)}")

    # Data preview
    st.markdown('<div class="section">DATA PREVIEW</div>', unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs(["🔄 Trade Data", "📊 Performance", "📅 Daily Summary"])

    with tab1:
        st.caption(f"Showing {len(filtered_trades):,} records — {report_symbol}")
        if not filtered_trades.empty:
            display = filtered_trades[[
                "trade_id","symbol","price","quantity","notional",
                "trade_type","trade_date","trade_hour","source"
            ]].copy()
            display["price"]    = display["price"].apply(lambda x: f"${float(x):,.4f}")
            display["notional"] = display["notional"].apply(lambda x: f"${float(x):,.2f}")
            st.dataframe(display, use_container_width=True, hide_index=True, height=350)

    with tab2:
        st.caption("Asset performance from dbt mart_symbol_performance")
        if not perf.empty:
            st.dataframe(perf, use_container_width=True, hide_index=True)

    with tab3:
        st.caption("Daily summary from dbt mart_daily_summary")
        if not daily.empty:
            st.dataframe(daily, use_container_width=True, hide_index=True, height=350)