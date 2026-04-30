import streamlit as st
import pandas as pd
import plotly.express as px
from zoneinfo import ZoneInfo
from datetime import datetime
import psycopg2


def show_dashboard_v2(conn, cur):

    try:
        conn.rollback()
    except:
        pass

    st.title("🏭 Factory Intelligence Command Center")

    # ===================== STRONG CSS =====================
    st.markdown("""
    <style>
    .kpi-box{
        border:1.5px solid #d0d0d0;
        border-radius:12px;
        padding:14px;
        text-align:center;
        background:#ffffff;
        min-height:95px;
        box-shadow:0 1px 4px rgba(0,0,0,0.04);
    }
    .kpi-title{
        font-size:12px;
        color:#666;
        margin-bottom:8px;
        font-weight:600;
    }
    .kpi-value{
        font-size:26px;
        font-weight:700;
        color:#111;
    }
    .panel-box{
        border:1.5px solid #d0d0d0;
        border-radius:12px;
        padding:16px;
        background:#ffffff;
        min-height:300px;
        box-shadow:0 1px 4px rgba(0,0,0,0.04);
    }
    .bottom-box{
        border:1.5px solid #d0d0d0;
        border-radius:12px;
        padding:16px;
        background:#ffffff;
        min-height:430px;
        box-shadow:0 1px 4px rgba(0,0,0,0.04);
    }
    </style>
    """, unsafe_allow_html=True)

    stage_order = [
        "Measurement",
        "Cutting List",
        "Production",
        "Pre Assembly",
        "Polishing",
        "Final Assembly",
        "Dispatch"
    ]

    # ===================== MASTER TOTAL HOUSES =====================
    total_houses = pd.read_sql_query("SELECT COUNT(DISTINCT house_no) as cnt FROM houses", conn)["cnt"][0]

    # ===================== LIGHT LIVE QUERY =====================
    live_query = """
    WITH latest_tracking AS (
        SELECT DISTINCT ON (product_instance_id)
            product_instance_id,
            stage_id,
            timestamp
        FROM tracking_log
        ORDER BY product_instance_id, timestamp DESC
    )

    SELECT
        h.house_no,
        h.project_name,
        h.unit_name,
        p.product_instance_id,
        COALESCE(s.stage_name, 'Not Started') AS current_stage,
        lt.timestamp
    FROM products p
    JOIN houses h ON p.house_id = h.house_id
    LEFT JOIN latest_tracking lt ON p.product_instance_id = lt.product_instance_id
    LEFT JOIN stages s ON lt.stage_id = s.stage_id
    """

    live_df = pd.read_sql_query(live_query, conn)

    if live_df.empty:
        st.warning("No production data available.")
        return

    live_df["timestamp"] = pd.to_datetime(live_df["timestamp"], errors="coerce")

    # ===================== CORE KPI ENGINE =====================
    total_products = len(live_df)

    dispatch_products = len(live_df[live_df["current_stage"] == "Dispatch"])
    completion_pct = round((dispatch_products / total_products) * 100, 2) if total_products else 0

    backlog_counts = {}
    impacted_house_counts = {}

    for stg in stage_order:

        if stg == "Measurement":
            df = live_df[live_df["current_stage"].isin(["Not Started", "Measurement"])]
        else:
            df = live_df[live_df["current_stage"] == stg]

        backlog_counts[stg] = len(df)
        impacted_house_counts[stg] = df["house_no"].nunique()

    bottleneck_stage = max(backlog_counts, key=backlog_counts.get)
    highest_pending = backlog_counts[bottleneck_stage]

    # ===================== CLOSED / ACTIVE / CRITICAL HOUSE ENGINE =====================
    house_group = live_df.groupby("house_no")["current_stage"].apply(list)

    closed_houses = 0
    active_houses = 0

    for house, vals in house_group.items():

        if all(v == "Dispatch" for v in vals):
            closed_houses += 1
        else:
            active_houses += 1

    critical_houses = active_houses

    # ===================== OPERATIONAL LOAD RANK =====================
    active_df = live_df[live_df["current_stage"] != "Dispatch"]

    unit_rank = active_df.groupby("unit_name").size().sort_values(ascending=False)
    project_rank = active_df.groupby("project_name").size().sort_values(ascending=False)

    top_unit = unit_rank.index[0] if not unit_rank.empty else "N/A"
    top_project = project_rank.index[0] if not project_rank.empty else "N/A"

    measurement_backlog = backlog_counts["Measurement"]
    production_backlog = backlog_counts["Production"]
    dispatch_backlog = backlog_counts["Dispatch"]

    # ===================== KPI ROW =====================
    c1,c2,c3,c4,c5,c6,c7 = st.columns(7)

    kpi_data = [
        ("Live Products", total_products),
        ("Active Houses", active_houses),
        ("Completion %", f"{completion_pct}%"),
        ("Bottleneck", bottleneck_stage),
        ("Highest Pending", highest_pending),
        ("Closed Houses", closed_houses),
        ("Critical Houses", critical_houses)
    ]

    for col, (title, value) in zip([c1,c2,c3,c4,c5,c6,c7], kpi_data):
        with col:
            st.markdown(f"""
            <div class="kpi-box">
                <div class="kpi-title">{title}</div>
                <div class="kpi-value">{value}</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ===================== ROW 2 =====================
    r1,r2,r3 = st.columns(3)

    with r1:
        st.markdown('<div class="panel-box">', unsafe_allow_html=True)
        st.subheader("📦 Stage Backlog Snapshot")
        for stg in stage_order:
            st.write(f"**{stg}** : {backlog_counts[stg]} pending products")
        st.markdown('</div>', unsafe_allow_html=True)

    with r2:
        st.markdown('<div class="panel-box">', unsafe_allow_html=True)
        st.subheader("🏠 House Impact Snapshot")
        for stg in stage_order:
            st.write(f"**{stg}** : {impacted_house_counts[stg]} impacted houses")
        st.markdown('</div>', unsafe_allow_html=True)

    with r3:
        st.markdown('<div class="panel-box">', unsafe_allow_html=True)
        st.subheader("🧠 Smart Manager Insights")
        st.info(f"🔴 Major bottleneck at {bottleneck_stage}")
        st.info(f"🟠 {measurement_backlog} products waiting from measurement")
        st.info(f"🟡 {dispatch_backlog} products pending dispatch closure")
        st.info(f"🏗 Highest operational unit : {top_unit}")
        st.info(f"📍 Highest running project : {top_project}")
        st.info(f"⚠ {critical_houses} houses still under execution")
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ===================== ROW 3 =====================
    b1,b2 = st.columns([1.8,1])

    with b1:
        st.markdown('<div class="bottom-box">', unsafe_allow_html=True)
        st.subheader("📈 Daily Workflow Pending Trend")

        trend_df = live_df.dropna(subset=["timestamp"]).copy()

        if not trend_df.empty:
            trend_df["Date"] = trend_df["timestamp"].dt.date
            daily = trend_df.groupby(["Date","current_stage"]).size().reset_index(name="Count")

            if not daily.empty:
                fig = px.line(
                    daily,
                    x="Date",
                    y="Count",
                    color="current_stage",
                    markers=True,
                    height=350
                )
                fig.update_layout(
                    margin=dict(l=5,r=5,t=5,b=5),
                    plot_bgcolor="white",
                    paper_bgcolor="white"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No trend history available.")
        else:
            st.warning("No timestamp data available.")
        st.markdown('</div>', unsafe_allow_html=True)

    with b2:
        st.markdown('<div class="bottom-box">', unsafe_allow_html=True)
        st.subheader("🚨 SLA Breach Audit + Recommendations")

        measurement_pct = round((measurement_backlog/total_products)*100,2)
        production_pct = round((production_backlog/total_products)*100,2)
        dispatch_pct = round((dispatch_backlog/total_products)*100,2)

        if measurement_pct > 30:
            st.error(f"Measurement backlog critical : {measurement_pct}%")
        else:
            st.success(f"Measurement stable : {measurement_pct}%")

        if production_pct > 20:
            st.error(f"Production queue overloaded : {production_pct}%")
        else:
            st.success(f"Production manageable : {production_pct}%")

        if dispatch_pct > 5:
            st.error(f"Dispatch closure weak : {dispatch_pct}%")
        else:
            st.success(f"Dispatch healthy : {dispatch_pct}%")

        if completion_pct < 20:
            st.error(f"Factory throughput critically low : {completion_pct}%")
        else:
            st.success(f"Factory throughput acceptable : {completion_pct}%")

        st.markdown("### 📌 Recommended Actions")
        st.write(f"• Push manpower immediately to {bottleneck_stage}")
        st.write("• Conduct supervisor review on all active houses")
        st.write("• Run dispatch closure monitoring")
        st.write(f"• Management focus required in {top_unit}")
        st.write("• Clear unfinished products stage by stage")
        st.markdown('</div>', unsafe_allow_html=True)
