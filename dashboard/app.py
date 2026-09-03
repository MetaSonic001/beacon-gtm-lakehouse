# =============================================================================
# Beacon GTM Lakehouse — Streamlit dashboard
# =============================================================================
# One page that explains the WHOLE project AND shows its results:
#   • What the project is / the data it uses / the stack
#   • Every pipeline process, what it does and WHY
#   • The results (Gold tables) as KPIs + interactive charts
#   • Where every result is saved
#   • What you can actually DO with those results
#
# Run it after the pipeline has produced data:
#     python scripts/run_pipeline.py            # run everything first
#     streamlit run dashboard/app.py            # then view this
#
# It reads the Gold Parquet files directly (data/gold/*) so it works with
# NO Docker / Postgres required. If Postgres is reachable it also reads the
# data_quality_log table; otherwise it falls back to the local CSV that
# run_pipeline.py appends to.
# =============================================================================

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Page config + paths
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Beacon GTM Lakehouse",
    page_icon="🏔️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

ROOT = Path(__file__).resolve().parent.parent
GOLD = ROOT / "data" / "gold"
QUALITY_CSV = ROOT / "data" / "quality" / "quality_log.csv"

# ---------------------------------------------------------------------------
# Theme-aware styling
# ---------------------------------------------------------------------------
IS_DARK = st.get_option("theme.base") == "dark"

TXT   = "#e6edf3" if IS_DARK else "#1f2933"
MUT   = "#9aa7b4" if IS_DARK else "#6b7a8a"
CARD  = "rgba(38,46,58,0.75)" if IS_DARK else "rgba(255,255,255,0.85)"
BORDER= "rgba(255,255,255,0.10)" if IS_DARK else "rgba(15,25,35,0.10)"
ACCENT= "#38bdf8"
GOOD  = "#34d399"
WARN  = "#fbbf24"

st.markdown(f"""
<style>
    :root {{
        --txt: {TXT};
        --mut: {MUT};
        --card: {CARD};
        --border: {BORDER};
        --accent: {ACCENT};
        --good: {GOOD};
        --warn: {WARN};
    }}
    .block-container {{ padding-top: 1.4rem; padding-bottom: 3rem; max-width: 1200px; }}
    .hero {{
        background: linear-gradient(135deg, rgba(56,189,248,0.16), rgba(52,211,153,0.10));
        border: 1px solid var(--border);
        border-radius: 16px;
        padding: 22px 26px;
        margin-bottom: 18px;
    }}
    .hero h1 {{ margin: 0; font-size: 1.9rem; letter-spacing: -0.01em; color: var(--txt); }}
    .hero p  {{ margin: 6px 0 0; color: var(--mut); font-size: 1.02rem; }}
    .chip {{
        display: inline-block; padding: 2px 10px; margin: 0 6px 6px 0;
        background: rgba(56,189,248,0.14); color: var(--txt);
        border: 1px solid rgba(56,189,248,0.35);
        border-radius: 999px; font-size: 0.8rem; white-space: nowrap;
    }}
    .card {{
        background: var(--card); border: 1px solid var(--border);
        border-radius: 14px; padding: 16px 18px; margin-bottom: 12px;
    }}
    .card h3 {{ margin: 0 0 6px; font-size: 1.02rem; color: var(--txt); }}
    .card p  {{ margin: 0; color: var(--mut); font-size: 0.92rem; line-height: 1.5; }}
    .step {{ border-left: 3px solid var(--accent); padding: 2px 0 2px 14px; margin-bottom: 14px; }}
    .step .st-title {{ color: var(--txt); }}
    .flow {{ display:flex; flex-wrap:wrap; align-items:center; gap:10px; margin-top:8px; }}
    .node {{
        background: rgba(56,189,248,0.10); border: 1px solid rgba(56,189,248,0.4);
        border-radius: 10px; padding: 10px 14px; min-width: 150px; text-align:center;
    }}
    .node b {{ color: var(--txt); }}
    .node span {{ display:block; color: var(--mut); font-size: 0.78rem; }}
    .arrow {{ color: var(--accent); font-weight: 700; font-size: 1.2rem; }}
    .kpi-note {{ color: var(--mut); font-size: 0.85rem; }}
    section[data-testid="stSidebar"] {{ background: {CARD}; }}
    .empty {{ background: var(--card); border: 1px dashed var(--border); border-radius: 14px; padding: 28px; text-align:center; color: var(--mut);}}
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Data loading (cached; throws cleanly when a table is missing)
# ---------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def load_gold(table: str) -> pd.DataFrame:
    """Read one Gold table (parquet at data/gold/<table>/)."""
    p = GOLD / table
    if not p.exists() or not any(p.glob("*.parquet")):
        raise FileNotFoundError(table)
    return pd.read_parquet(p)


@st.cache_data(show_spinner=False)
def load_quality() -> pd.DataFrame:
    """Data-quality trend: local CSV first, Postgres fallback."""
    if QUALITY_CSV.exists():
        df = pd.read_csv(QUALITY_CSV, parse_dates=["run_timestamp"])
        if not df.empty:
            return df[["run_timestamp", "input_rows", "duplicates_removed",
                       "passed_validation", "quarantined", "elapsed_seconds"]]
    # Fallback: read gold.data_quality_log from Postgres if it's reachable.
    pg = None
    try:
        import psycopg2
        pg = psycopg2.connect(
            host=os.getenv("PG_HOST", "localhost"), port=os.getenv("PG_PORT", "5432"),
            dbname=os.getenv("PG_DB", "beacon"), user=os.getenv("PG_USER", "beacon"),
            password=os.getenv("PG_PASSWORD", "beacon"), connect_timeout=2,
        )
        df = pd.read_sql(
            "SELECT run_timestamp::timestamp, input_rows, duplicates_removed, "
            "passed_validation, quarantined, elapsed_seconds "
            "FROM gold.data_quality_log ORDER BY run_timestamp", pg)
        return df
    except Exception:
        return pd.DataFrame()
    finally:
        if pg is not None:
            pg.close()


def has_gold(table: str) -> bool:
    p = GOLD / table
    return p.exists() and any(p.glob("*.parquet"))


# ---------------------------------------------------------------------------
# Revenue-based segment (the Maven dataset has no segment field, so we derive
# one from annual revenue, which is expressed in millions of USD). Buckets are
# chosen to split the ~85 accounts into roughly even thirds.
# ---------------------------------------------------------------------------
def _segment(row):
    rev = row.get("revenue", 0) or 0
    if rev >= 2000:      # >= $2B
        return "Enterprise"
    elif rev >= 500:     # $500M – $2B
        return "Mid-Market"
    return "SMB"         # < $500M


# ---------------------------------------------------------------------------
# Plotly light/dark theming
# ---------------------------------------------------------------------------
def px_theme():
    return dict(
        template="plotly_dark" if IS_DARK else "plotly_white",
        color_discrete_sequence=["#38bdf8", "#34d399", "#fbbf24",
                                 "#f472b6", "#a78bfa", "#fb923c", "#2dd4bf"],
    )


def style_fig(fig, height=380):
    fig.update_layout(legend=dict(orientation="h", y=1.12, x=0),
                      margin=dict(l=10, r=10, t=46, b=10),
                      height=height,
                      font=dict(size=12, color=TXT))
    fig.update_xaxes(gridcolor=BORDER, showgrid=False)
    fig.update_yaxes(gridcolor=BORDER)
    return fig


PLOT_ARGS = dict(use_container_width=True)


def chart(fig, **kw):
    """st.plotly_chart wrapper tolerant of the use_container_width deprecation."""
    try:
        st.plotly_chart(fig, use_container_width=True, **kw)
    except TypeError:
        st.plotly_chart(fig, **kw)


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.markdown("""
<div class="hero">
  <h1>🏔️ Beacon GTM Lakehouse</h1>
  <p>An end-to-end data-engineering portfolio project: raw CRM sales data →
  a <b>Bronze → Silver → Gold</b> medallion lakehouse → analytics-ready tables →
  BI &amp; this dashboard. Everything runs locally.</p>
  <div style="margin-top:10px">
    <span class="chip">PySpark 3.5</span>
    <span class="chip">MinIO (S3)</span>
    <span class="chip">PostgreSQL 16</span>
    <span class="chip">Redpanda (Kafka)</span>
    <span class="chip">Airflow-ready DAG</span>
    <span class="chip">Maven CRM dataset</span>
  </div>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Sidebar filters — derive the option lists from the live Gold tables, so the
# filters always match the data (and degrade gracefully if there is none yet).
# ---------------------------------------------------------------------------
def _filter_options():
    """Return (sectors, segments, offices) or empty lists if no Gold yet."""
    try:
        a = load_gold("account_performance")
        a["segment"] = a.apply(_segment, axis=1)
        r = load_gold("sales_rep_performance")
        return (sorted(a["sector"].dropna().unique()),
                sorted(a["segment"].unique()),
                sorted(r["regional_office"].dropna().unique()))
    except Exception:
        return [], [], []

with st.sidebar:
    st.markdown("### 🔎 Filters")
    st.caption("Slices the Results tab by sector, account segment, and office.")
    _sectors, _segments, _offices = _filter_options()
    sel_sectors = st.multiselect("Sector", _sectors, default=_sectors)
    sel_segments = st.multiselect("Account segment", _segments, default=_segments)
    sel_offices = st.multiselect("Regional office", _offices, default=_offices)
    st.markdown("---")
    st.caption("Beacon GTM Lakehouse · runs on your laptop")

TAB_ABOUT, TAB_PIPELINE, TAB_RESULTS, TAB_WHERE, TAB_NEXT = st.tabs([
    "1 · About the project",
    "2 · Pipeline & why",
    "3 · Results / analytics",
    "4 · Where results live",
    "5 · What you can do with results",
])

# ===========================================================================
# TAB 1 — ABOUT / OVERVIEW
# ===========================================================================
with TAB_ABOUT:
    st.markdown("""
<div class="card">
  <h3>What is this project?</h3>
  <p>
    <b>Beacon GTM Lakehouse</b> models the data platform a real B2B SaaS company needs
    to understand its sales pipeline. It ingests four raw CRM CSV tables
    (<i>accounts</i>, <i>products</i>, <i>sales_teams</i>, <i>sales_pipeline</i>),
    runs them through a three-layer <b>Medallion architecture</b>, and serves clean,
    pre-aggregated analytics tables. It's a hands-on portfolio showcase of the whole
    modern data-engineering skill set — <b>all on your laptop, no cloud account needed</b>.
  </p>
</div>
<div class=”card”>
  <h3>The data</h3>
  <p>
    The source is the <b>Actual Maven Analytics “CRM Sales Opportunities”</b> dataset for a
    fictitious computer-hardware company — <b>85 accounts, ~8,800 pipeline records, 2016-2017</b>.
    Four relational tables sharing foreign keys — the exact shape of a real CRM export.
    The dataset contains <b>natural, authentic data-quality issues</b> (missing account FKs on ~1,400
    opportunities, open deals with no close_date/close_value, a product name typo “GTXPro” vs “GTX Pro”,
    a sector typo “technolgy”, and deal stages limited to Won/Lost/Engaging/Prospecting).
    The pipeline adapts this schema to the Medallion model and handles these issues via
    validation with quarantine — no synthetic bad data needed.
  </p>
</div>
<div class="card">
  <h3>Why does it exist?</h3>
  <p>
    To demonstrate that real skills are exercised end-to-end, not just “load a CSV →
    draw a chart”: a Bronze → Silver → Gold lakehouse, schema enforcement,
    a quarantine pattern for rejected rows, a quality log you can trend, a Spark join
    that was benchmarked and optimized, a Kafka-API streaming path, and Airflow
    orchestration — all reproducible from this repo in one command.
  </p>
</div>
""", unsafe_allow_html=True)

    st.markdown("**The architecture at a glance**")
    st.markdown("""
<div class="flow">
  <div class="node"><b>📥 Raw CSVs</b><span>data/bronze/maven</span></div>
  <span class="arrow">→</span>
  <div class="node"><b>🥉 Bronze</b><span>as-is landing zone</span></div>
  <span class="arrow">→</span>
  <div class="node"><b>🥈 Silver</b><span>clean · validate · dedupe</span></div>
  <span class="arrow">→</span>
  <div class="node"><b>🥇 Gold</b><span>aggregated for BI</span></div>
  <span class="arrow">→</span>
  <div class="node"><b>🗄️ Postgres</b><span>gold.* serving layer</span></div>
  <span class="arrow">→</span>
  <div class="node"><b>📊 This dashboard</b><span>see the results</span></div>
</div>
""", unsafe_allow_html=True)

    st.markdown("")
    st.markdown("""<div class="card">
  <h3>Tech stack — and why each piece</h3>
  <p>
    <b>PySpark 3.5</b> runs every Bronze→Silver→Gold transformation locally.
    <b>MinIO</b> is an S3-compatible object store standing in for the data lake, and
    <b>Redpanda</b> is a Kafka-API broker — so the code written against
    <code>s3a://</code> and <code>kafka-python</code> ports to real AWS S3 / MSK
    without changes. <b>PostgreSQL</b> is the serving layer a BI tool queries with SQL.
    <b>Airflow</b> (optional) wires the whole batch pipeline into a scheduled DAG.
    <b>Streamlit + Plotly</b> power this page.
  </p>
</div>""", unsafe_allow_html=True)


# ===========================================================================
# TAB 2 — PIPELINE & WHY
# ===========================================================================
with TAB_PIPELINE:
    st.markdown("""
The pipeline is a **Medallion (multi-hop) architecture** — a de-facto standard for
organizing a data lake into layers of increasing quality and structure. Each hop is a
new Parquet dataset with a stricter guarantee than the last.
""")

    steps = [
        ("1 · Land the raw data — Bronze",
         "`scripts/ingest_actual_data.py`",
         "Ingests the actual Maven CRM dataset from `actual_maven_data/` into "
         "`data/bronze/maven/` with minimal normalization (trim strings, cast numbers, "
         "parse dates, fix known typos). **Why:** keep the landing zone as an immutable "
         "copy of what actually arrived — you can always re-process from here."),
        ("2 · (Optional) Stream events — Redpanda",
         "`ingestion/kafka_producer.py` → `spark_jobs/streaming_events.py`",
         "A producer pushes deal events to a Kafka-API topic; Spark Structured Streaming "
         "consumes them. **Why:** demonstrates real-time intake alongside batch — the same "
         "code targets managed Kafka later. Skipped by default."),
        ("3 · Clean & validate — Bronze → Silver",
         "`spark_jobs/bronze_to_silver.py`",
         "Enforces schema, deduplicates on `opportunity_id`, validates rows against rules "
         "(valid FKs, non-negative prices, stage in funnel, dates sane), writes clean "
         "Parquet to `data/silver/` and **rejected rows to `data/quarantine/`**. "
         "**Why:** analytics must not be poisoned by broken records — bad rows are parked, "
         "not silently dropped."),
        ("4 · Build business aggregates — Silver → Gold",
         "`spark_jobs/silver_to_gold.py`",
         "Joins the fact table to dimensions and computes pre-aggregated, analytics-ready "
         "tables: `account_performance`, `sales_rep_performance`, `product_performance`, "
         "`pipeline_funnel`. **Why:** BI users query simple aggregates — metrics are "
         "computed once, consistently, at build time."),
        ("5 · Serve to BI — Gold → Postgres",
         "`spark_jobs/load_gold_to_postgres.py` → `gold.*` schema",
         "Loads the four Gold tables into PostgreSQL over JDBC. **Why:** a real serving "
         "layer — analysts and BI tools query `gold.*` with plain SQL, or use this "
         "dashboard."),
    ]
    for title, cmd, why in steps:
        st.markdown(f"""
<div class="step">
  <h3 style="margin:0;color:{TXT}">{title}</h3>
  <p style="margin:2px 0;color:{ACCENT};font-family:monospace;font-size:0.85rem">{cmd}</p>
  <p style="margin:4px 0 0;color:{MUT}">{why}</p>
</div>
""", unsafe_allow_html=True)

    st.markdown("""
<div class="card">
  <h3>Data quality &amp; quarantine — the part that matters</h3>
  <p>
    Every row that fails validation is parked in <code>data/quarantine/fact_opportunity_rejected/</code>
    <b>with reason flags</b> instead of being discarded. The Bronze→Silver step prints a
    summary (input rows, duplicates removed, passed, quarantined) which <code>run_pipeline.py</code>
    also appends to <code>data/quality/quality_log.csv</code> — so you can <b>trend quality
    over time</b> and watch the quarantine rate drop as the source improves.
  </p>
</div>""", unsafe_allow_html=True)


# ===========================================================================
# TAB 3 — RESULTS / ANALYTICS
# ===========================================================================
with TAB_RESULTS:
    if not any(has_gold(t) for t in
               ["account_performance", "sales_rep_performance",
                "product_performance", "pipeline_funnel"]):
        st.markdown("""
<div class="empty">
  <h3 style="margin:0 0 8px;color:{TXT}">No results yet</h3>
  Run the pipeline first — it takes a few minutes and streams + saves a log:
  <br><br><code>python scripts/run_pipeline.py</code><br><br>
  then refresh this page. The dashboard reads the Gold tables it produces under
  <code>data/gold/</code>.
</div>""".replace("{TXT}", TXT), unsafe_allow_html=True)
    else:
        acc = load_gold("account_performance")
        rep = load_gold("sales_rep_performance")
        prod = load_gold("product_performance")
        funnel = load_gold("pipeline_funnel")

        acc["segment"] = acc.apply(_segment, axis=1)

        # ---- Apply sidebar filters ----
        sel_acc = acc[acc["sector"].isin(sel_sectors) & acc["segment"].isin(sel_segments)]
        sel_rep = rep[rep["regional_office"].isin(sel_offices)]
        filtered = len(sel_acc) < len(acc) or len(sel_rep) < len(rep)

        # ---- KPI row (respects the active filters) ----
        total_won   = sel_acc["won_value"].sum()
        total_deals = sel_rep["num_opportunities"].sum()
        total_won_n = sel_rep["num_won"].sum()
        win_rate    = total_won_n / total_deals if total_deals else 0
        avg_deal    = sel_acc[sel_acc["won_value"] > 0]["avg_deal_size"].mean()
        avg_days    = sel_acc[sel_acc["won_value"] > 0]["avg_days_to_close"].mean()

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("💰 Total won revenue", f"${total_won:,.0f}")
        k2.metric("🏆 Win rate", f"{win_rate*100:.1f}%")
        k3.metric("📈 Opportunities", f"{total_deals:,.0f}")
        k4.metric("🏢 Accounts", f"{len(sel_acc):,.0f}")
        k5, k6, k7, k8 = st.columns(4)
        k5.metric("💵 Avg won deal size", f"${avg_deal:,.0f}" if avg_deal == avg_deal else "—")
        k6.metric("⏱️ Avg days to close", f"{avg_days:,.0f}" if avg_days == avg_days else "—")
        k7.metric("🥇 Won deals", f"{total_won_n:,.0f}")
        k8.metric("🧹 Data-quality rows captured",
                  f"{len(load_quality()):,} runs" if not load_quality().empty else "0")
        if filtered:
            st.caption("Showing filtered subset — use the sidebar to clear sector / segment / office.")
        else:
            st.caption("Gold tables are pre-aggregated per account / rep / product / funnel stage.")

        # ---- Revenue over time (new gold table: monthly_revenue) ----
        st.markdown("")
        st.markdown("#### 📈 Won revenue over time (by close month)")
        if has_gold("monthly_revenue"):
            m = load_gold("monthly_revenue").sort_values(["close_year", "close_month"])
            m["month"] = pd.to_datetime(
                m["close_year"].astype(str) + "-" + m["close_month"].astype(str).str.zfill(2) + "-01")
            fig = px.area(m, x="month", y="won_value", markers=True, **px_theme())
            fig.update_layout(xaxis_title="", yaxis_title="Revenue recognized ($)", showlegend=False)
            fig.update_traces(line=dict(color="#38bdf8", width=3), fillcolor="rgba(56,189,248,0.15)")
            chart(style_fig(fig, 380))
            with st.expander("Monthly revenue table"):
                st.dataframe(m[["month", "num_won", "won_value"]], use_container_width=True)
        else:
            st.info("Run the pipeline again to produce `monthly_revenue` (won revenue by month).")

        st.markdown("")
        # ---- Funnel (whole pipeline, not filterable by account/office) ----
        st.markdown("#### 🪜 Sales funnel — where deals drop off")
        if not funnel.empty:
            funnel_sorted = funnel.sort_values("stage_index")
            f_funnel = go.Figure(go.Funnel(
                x=[f"{r.opportunity_count:,.0f}" for _, r in funnel_sorted.iterrows()],
                y=funnel_sorted["deal_stage"],
                textposition="inside", textinfo="value",
                marker=dict(color="#38bdf8"),
            ))
            f_funnel = style_fig(f_funnel, 360)
            f_funnel.update_traces(opacity=0.9)
            f_funnel.update_layout(yaxis=dict(autorange="reversed"))
            chart(f_funnel)
            with st.expander("Funnel table"):
                st.dataframe(funnel_sorted[["stage_index", "deal_stage",
                                            "opportunity_count", "total_value",
                                            "conversion_ratio"]], use_container_width=True)

        st.markdown("")
        # ---- Revenue by sector & segment ----
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("#### 🏭 Won revenue by sector")
            by_sec = (sel_acc.groupby("sector", as_index=False)["won_value"].sum()
                      .sort_values("won_value", ascending=False).head(8))
            fig = px.bar(by_sec, x="won_value", y="sector", orientation="h", **px_theme())
            fig.update_layout(xaxis_title="Won revenue ($)", yaxis_title="")
            fig.update_yaxes(autorange="reversed")
            chart(style_fig(fig, 380))
        with c2:
            st.markdown("#### 📦 Won revenue by segment")
            by_seg = (sel_acc.groupby("segment", as_index=False)["won_value"].sum()
                      .sort_values("won_value", ascending=False))
            fig = px.bar(by_seg, x="segment", y="won_value", color="segment", **px_theme())
            fig.update_layout(xaxis_title="", yaxis_title="Won revenue ($)", showlegend=False)
            chart(style_fig(fig, 380))

        # ---- Office / manager treemap (new) ----
        st.markdown("#### 🗺️ Won revenue by office & manager")
        if not sel_rep.empty:
            tree = px.treemap(sel_rep, path=[px.Constant("All"), "manager", "regional_office"],
                              values="won_value", color="won_value",
                              color_continuous_scale="Blues", **px_theme())
            tree.update_traces(textinfo="label+value")
            tree.update_layout(margin=dict(l=10, r=10, t=30, b=10))
            chart(style_fig(tree, 420))
        else:
            st.info("No reps match the current office filter.")

        st.markdown("")
        # ---- Top reps & products ----
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("#### 👤 Top 10 sales reps by won revenue")
            top_rep = sel_rep.sort_values("won_value", ascending=False).head(10)
            fig = px.bar(top_rep, x="won_value", y="sales_agent", orientation="h",
                         color="won_value", color_continuous_scale="Blues", **px_theme())
            fig.update_yaxes(autorange="reversed")
            fig.update_layout(xaxis_title="Won revenue ($)", yaxis_title="", showlegend=False)
            chart(style_fig(fig, 420))
            st.caption("Win rate shown per rep in the full table below.")
        with c2:
            st.markdown("#### 📟 Top 10 products by won revenue")
            top_prod = prod.sort_values("won_value", ascending=False).head(10)
            fig = px.bar(top_prod, x="won_value", y="product", orientation="h",
                         color="series", **px_theme())
            fig.update_yaxes(autorange="reversed")
            fig.update_layout(xaxis_title="Won revenue ($)", yaxis_title="", legend_title="Series")
            chart(style_fig(fig, 420))

        st.markdown("")
        # ---- Win rate vs volume scatter ----
        st.markdown("#### 🎯 Win rate vs. deal volume (rep efficiency)")
        if not sel_rep.empty:
            fig = px.scatter(sel_rep, x="num_opportunities", y="win_rate",
                             size="won_value", color="regional_office",
                             hover_name="sales_agent", **px_theme())
            fig.update_layout(xaxis_title="Opportunities", yaxis_title="Win rate")
            fig.update_yaxes(tickformat=".0%")
            chart(style_fig(fig, 420))

        st.markdown("")
        # ---- Compare two reps (new) ----
        st.markdown("#### ⚖️ Compare two sales reps")
        if len(sel_rep) >= 1:
            agents = sel_rep["sales_agent"].tolist()
            cA, cB = st.columns(2)
            with cA:
                rep_a = st.selectbox("Rep A", agents, index=0, key="rep_a")
            with cB:
                rep_b = st.selectbox("Rep B", agents,
                                     index=min(1, len(agents) - 1), key="rep_b")
            if rep_a == rep_b:
                st.info("Pick two different reps to compare.")
            else:
                def _rep_row(name):
                    r = sel_rep[sel_rep["sales_agent"] == name].iloc[0]
                    return {
                        "Won revenue": f"${r['won_value']:,.0f}",
                        "Opportunities": f"{r['num_opportunities']:,.0f}",
                        "Won deals": f"{r['num_won']:,.0f}",
                        "Win rate": f"{r['win_rate']*100:.1f}%",
                        "Avg deal size": f"${r['avg_deal_size']:,.0f}",
                        "Avg days to close": f"{r['avg_days_to_close']:,.1f}",
                        "Office": str(r["regional_office"]),
                        "Manager": str(r["manager"]),
                    }
                cmp_df = pd.DataFrame({rep_a: _rep_row(rep_a), rep_b: _rep_row(rep_b)})
                st.dataframe(cmp_df, use_container_width=True)
                # Side-by-side numeric comparison bars
                num = sel_rep[sel_rep["sales_agent"].isin([rep_a, rep_b])]
                num = num.melt(id_vars=["sales_agent"],
                               value_vars=["won_value", "num_opportunities", "num_won", "win_rate"])
                fig = px.bar(num, x="variable", y="value", color="sales_agent",
                             barmode="group", **px_theme())
                fig.update_layout(xaxis_title="", yaxis_title="", legend_title="")
                chart(style_fig(fig, 360))
        else:
            st.info("No reps match the current office filter.")

        st.markdown("")
        # ---- Data quality trend ----
        st.markdown("#### 🧹 Data-quality trend across runs")
        q = load_quality()
        if q.empty:
            st.info("No quality runs recorded yet. Each `python scripts/run_pipeline.py` "
                    "appends a row to `data/quality/quality_log.csv`.")
        else:
            q = q.sort_values("run_timestamp")
            q["quarantine_rate"] = q["quarantined"] / q["input_rows"].replace(0, pd.NA)
            fig = go.Figure()
            fig.add_trace(go.Bar(name="Input rows", x=q["run_timestamp"],
                                 y=q["input_rows"], marker_color="#38bdf8"))
            fig.add_trace(go.Bar(name="Passed validation", x=q["run_timestamp"],
                                 y=q["passed_validation"], marker_color="#34d399"))
            fig.add_trace(go.Bar(name="Quarantined", x=q["run_timestamp"],
                                 y=q["quarantined"], marker_color="#fbbf24"))
            fig.add_trace(go.Scatter(name="Quarantine rate %", x=q["run_timestamp"],
                                     y=q["quarantine_rate"] * 100, yaxis="y2",
                                     mode="lines+markers", line=dict(color="#f472b6", width=3)))
            fig.update_layout(barmode="group",
                              yaxis=dict(title="Rows"),
                              yaxis2=dict(title="Quarantine rate %",
                                          overlaying="y", side="right",
                                          range=[0, max(20, q["quarantine_rate"].max() * 120)]))
            chart(style_fig(fig, 400))

        st.markdown("")
        st.markdown("### Full Gold tables (query-ready)")
        t1, t2 = st.tabs([
            "Account performance",
            "Sales rep performance"])
        with t1:
            st.dataframe(sel_acc.head(100), use_container_width=True)
        with t2:
            st.dataframe(sel_rep.head(100), use_container_width=True)
        t3, t4 = st.tabs(["Product performance", "Funnel"])
        with t3:
            st.dataframe(prod.head(100), use_container_width=True)
        with t4:
            st.dataframe(funnel, use_container_width=True)


# ===========================================================================
# TAB 4 — WHERE RESULTS LIVE
# ===========================================================================
with TAB_WHERE:
    st.markdown("""
After `python scripts/run_pipeline.py`, every artifact is saved **in the repo itself**
so you can inspect it — nothing is locked inside a cloud account.
""")
    rows = [
        ("Raw landing — Bronze", "data/bronze/maven/*.csv",
         "The 4 original Maven CRM CSVs."),
        ("Clean layer — Silver", "data/silver/{dim_account, dim_product, dim_sales_team, fact_opportunity}",
         "Validated, deduplicated Parquet; the fact table is partitioned by year & month."),
        ("Analytics layer — Gold", "data/gold/{account_performance, product_performance, sales_rep_performance, pipeline_funnel, monthly_revenue}",
         "Pre-aggregated, BI-ready Parquet tables — this dashboard reads these."),
        ("Quarantine", "data/quarantine/fact_opportunity_rejected",
         "Rejected rows with reason flags, kept instead of deleted."),
        ("Serving layer — Postgres", "schema gold.* (db beacon)",
         "Same 4 Gold tables, queryable with plain SQL. SQL: docker compose exec postgres psql -U beacon -d beacon"),
        ("Data lake — MinIO/S3", "buckets beacon-lakehouse · beacon-raw · beacon-curated · beacon-gold",
         "Console: http://localhost:9001 — S3 API equivalents of the data zones."),
        ("Run log — New", "logs/pipeline_<timestamp>.log",
         "Every line of the last pipeline run, persisted by run_pipeline.py."),
        ("Quality trend — New", "data/quality/quality_log.csv",
         "One row per run: input rows, duplicates removed, passed, quarantined, elapsed."),
    ]
    for label, path, desc in rows:
        st.markdown(f"""
<div class="card">
  <h3 style="display:inline">{label}</h3>&nbsp;&nbsp;<code style="color:{ACCENT}">{path}</code>
  <p>{desc}</p>
</div>
""", unsafe_allow_html=True)

    st.divider()
    st.markdown("**Also check the live consoles** (start with `docker compose up -d`)")
    cons = [
        ("MinIO console", "http://localhost:9001"),
        ("Redpanda console", "http://localhost:8080"),
    ]
    cols = st.columns(len(cons))
    for col, (name, url) in zip(cols, cons):
        with col:
            st.markdown(f"**{name}**  \n[{url}]({url})")


# ===========================================================================
# TAB 5 — WHAT YOU CAN DO WITH THE RESULTS
# ===========================================================================
with TAB_NEXT:
    st.markdown("""
These aren't just charts — the Gold tables are the same tables an analyst would query.
Here's how to actually read and act on them:
""")
    cards = [
        ("🎯 Pipeline & win rate",
         "`pipeline_funnel` shows where deals stall. If Prospecting → Qualification "
         "converts poorly, reps are chasing the wrong accounts; if Negotiation → Won is "
         "low, pricing / procurement is the bottleneck. Focus coaching there."),
        ("🏭 Where the money comes from",
         "`account_performance` sliced by **sector & segment** shows which markets "
         "deliver the most won revenue and best win rate. Tells you which segments to "
         "double down on vs. deprioritize."),
        ("👤 Rep & office effectiveness",
         "`sales_rep_performance` ranks reps by revenue and win rate, across "
         "`regional_office` and `manager`. Find your top performers, their fastest "
         "closures (avg days to close), and the offices that under-perform so you can "
         "investigate."),
        ("📟 Product portfolio",
         "`product_performance` shows which products win fastest and at what margin — "
         "guides the roadmap (push the winners, fix the slow movers)."),
        ("🧹 Data quality monitoring",
         "`data_quality_log` / `data/quality/quality_log.csv` trends duplicates and "
         "quarantine rates over time — a concrete signal that the pipeline is healthy "
         "and the source is improving."),
    ]
    for title, body in cards:
        st.markdown(f"""
<div class="card">
  <h3>{title}</h3>
  <p>{body}</p>
</div>
""", unsafe_allow_html=True)

    st.markdown("""
<div class="card">
  <h3>Ready-made SQL analyses</h3>
  <p>
    <code>sql/analytics_queries.sql</code> ships eight analyst queries you can run against
    Postgres — rep leaderboard, win rate by office, revenue by sector × segment, product
    performance, funnel, manager leaderboard, a window-function quartile analysis, and the
    quality trend:
    <br><br>
    <code>docker compose exec postgres psql -U beacon -d beacon -f sql/analytics_queries.sql</code>
  </p>
</div>
""", unsafe_allow_html=True)

    st.markdown("")
    st.markdown(f"""<div style="color:{MUT}; font-size:0.85rem">
  Built as a portfolio project. Run everything with
  <code>python scripts/run_pipeline.py</code> (saves a log under <code>logs/</code>),
  then revisit this page.
</div>""", unsafe_allow_html=True)