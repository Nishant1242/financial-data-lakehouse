"""
styles.py — Shared CSS and theme constants for all dashboard pages.
"""

# ── Color palette ──────────────────────────────────────────────────────────────
COLORS = {
    "BTC/USD": "#f7931a",
    "ETH/USD": "#627eea",
    "SOL/USD": "#9945ff",
}

CHART_BG   = "#111827"
GRID_COLOR = "#1f2937"
TEXT_COLOR = "#94a3b8"
BG_COLOR   = "#0a0e1a"
FONT       = "Inter, -apple-system, sans-serif"

PLOTLY_BASE = dict(
    paper_bgcolor=CHART_BG,
    plot_bgcolor=CHART_BG,
    font=dict(family=FONT, color=TEXT_COLOR, size=11),
    xaxis=dict(gridcolor=GRID_COLOR, showgrid=True, zeroline=False),
    yaxis=dict(gridcolor=GRID_COLOR, showgrid=True, zeroline=False),
)


# ── Shared CSS ─────────────────────────────────────────────────────────────────
CSS = """
<style>
    .stApp { background-color: #0a0e1a; }
    .block-container { padding: 1.2rem 2rem; }

    /* Hide streamlit chrome */
    #MainMenu, footer, header { visibility: hidden; }
    .stDeployButton { display: none; }

    /* Page header */
    .page-header {
        background: linear-gradient(135deg, #1a1f35 0%, #0d1117 100%);
        border: 1px solid #1e3a5f;
        border-radius: 12px;
        padding: 1.2rem 1.8rem;
        margin-bottom: 1.2rem;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }
    .page-title {
        font-size: 1.5rem;
        font-weight: 700;
        color: #e2e8f0;
        margin: 0;
    }
    .page-subtitle {
        font-size: 0.8rem;
        color: #64748b;
        margin-top: 0.2rem;
    }
    .live-badge {
        display: inline-flex;
        align-items: center;
        gap: 5px;
        background: rgba(16,185,129,0.1);
        border: 1px solid rgba(16,185,129,0.3);
        color: #10b981;
        font-size: 0.72rem;
        font-weight: 600;
        padding: 0.25rem 0.7rem;
        border-radius: 20px;
    }

    /* KPI cards */
    .kpi {
        background: #111827;
        border: 1px solid #1f2937;
        border-radius: 10px;
        padding: 1.1rem 1.3rem;
    }
    .kpi-label {
        font-size: 0.68rem;
        font-weight: 600;
        color: #6b7280;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-bottom: 0.3rem;
    }
    .kpi-value {
        font-size: 1.5rem;
        font-weight: 700;
        color: #f1f5f9;
        line-height: 1.1;
    }
    .kpi-sub { font-size: 0.72rem; margin-top: 0.25rem; }
    .kpi-green { color: #10b981; }
    .kpi-red   { color: #ef4444; }
    .kpi-gray  { color: #6b7280; }

    /* Section dividers */
    .section {
        font-size: 0.72rem;
        font-weight: 600;
        color: #475569;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        padding-bottom: 0.5rem;
        border-bottom: 1px solid #1f2937;
        margin: 1rem 0 0.8rem;
    }

    /* Filter sidebar */
    .stSidebar { background-color: #0d1117; }
    .stSidebar .block-container { padding: 1rem; }

    /* Pipeline stage */
    .stage-ok {
        background: rgba(16,185,129,0.07);
        border: 1px solid rgba(16,185,129,0.2);
        border-radius: 8px;
        padding: 0.75rem 1rem;
        margin-bottom: 0.5rem;
    }
    .stage-name {
        font-size: 0.8rem;
        font-weight: 600;
        color: #10b981;
    }
    .stage-detail {
        font-size: 0.7rem;
        color: #4b5563;
        margin-top: 0.1rem;
    }

    /* Asset card */
    .asset {
        background: #111827;
        border: 1px solid #1f2937;
        border-radius: 10px;
        padding: 1.1rem 1.2rem;
        margin-bottom: 0.7rem;
    }

    /* Report card */
    .report-card {
        background: #111827;
        border: 1px solid #1f2937;
        border-radius: 10px;
        padding: 1.5rem;
        text-align: center;
        margin-bottom: 1rem;
    }
    .report-icon { font-size: 2.5rem; margin-bottom: 0.5rem; }
    .report-title {
        font-size: 1rem;
        font-weight: 600;
        color: #e2e8f0;
        margin-bottom: 0.3rem;
    }
    .report-desc {
        font-size: 0.78rem;
        color: #6b7280;
        margin-bottom: 1rem;
    }
</style>
"""