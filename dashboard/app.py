"""
app.py — Financial Data Lakehouse Dashboard
Multi-page executive intelligence platform.
"""

import streamlit as st

st.set_page_config(
    page_title="Financial Data Lakehouse",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Import shared styles
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
from utils.styles import CSS

st.markdown(CSS, unsafe_allow_html=True)

# ── Sidebar navigation ─────────────────────────────────────────────────────────
st.sidebar.markdown("""
<div style="padding:1rem 0 0.5rem;">
    <p style="font-size:1.1rem;font-weight:700;color:#e2e8f0;margin:0;">📊 FDL</p>
    <p style="font-size:0.7rem;color:#4b5563;margin:0;">Financial Data Lakehouse</p>
</div>
""", unsafe_allow_html=True)

st.sidebar.markdown("---")
st.sidebar.markdown("""
<p style="font-size:0.68rem;font-weight:600;color:#475569;text-transform:uppercase;
letter-spacing:0.08em;margin-bottom:0.5rem;">Navigation</p>
""", unsafe_allow_html=True)

page = st.sidebar.radio(
    label="Navigation",
    options=[
        "🏠 Overview",
        "📈 Market Analysis",
        "💎 Asset Deep Dive",
        "⚙️ Pipeline Health",
        "📋 Reports & Export"
    ],
    label_visibility="collapsed"
)

st.sidebar.markdown("---")
st.sidebar.markdown(f"""
<div style="font-size:0.68rem;color:#374151;padding:0.5rem 0;">
    <p style="margin:0;">Stack</p>
    <p style="margin:0;color:#4b5563;">
        Kafka · PySpark · Airflow<br>
        PostgreSQL · dbt · MongoDB
    </p>
    <p style="margin:0.5rem 0 0;color:#374151;">Built by Nishant Kadam</p>
</div>
""", unsafe_allow_html=True)

# ── Route to views ─────────────────────────────────────────────────────────────
if page == "🏠 Overview":
    from views import overview
    overview.render()
elif page == "📈 Market Analysis":
    from views import market
    market.render()
elif page == "💎 Asset Deep Dive":
    from views import assets
    assets.render()
elif page == "⚙️ Pipeline Health":
    from views import pipeline
    pipeline.render()
elif page == "📋 Reports & Export":
    from views import reports
    reports.render()