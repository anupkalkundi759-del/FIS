import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
from datetime import datetime
from zoneinfo import ZoneInfo


def show_dashboard_v2(conn, cur):

    try:
        conn.rollback()
    except:
        pass

    st.title("🏭 Factory Intelligence Command Center")

    stage_order = [
        "Measurement",
        "Cutting List",
        "Production",
        "Pre Assembly",
        "Polishing",
        "Final Assembly",
        "Dispatch"
    ]

    # ===================== MASTER HOUSE COUNT =====================
    total_houses_query = "SELECT COUNT(DISTINCT house_id) as cnt FROM houses"
    total_houses = pd.read_sql_query(total_houses_query, conn)["cnt"][0]

    # ===================== LIGHT LIVE QUERY WITH TRUE KEYS =====================
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
        h.house_id,
        h.house_no,
        h.project_name,
        h.unit_name,
        p.product_instance_id,
        COALESCE(s.stage_name, 'Not Started') AS current_stage,
        lt.timestamp
    FROM products p
    JOIN houses h ON p.house_id = h.house_id
    LEFT JOIN latest_tracking lt
        ON p.product_instance_id = lt.product_instance_id
    LEFT JOIN stages s
        ON lt.stage_id = s.stage_id
    """

    live_df = pd.read_sql_query(live_query, conn)

    if live_df.empty:
        st.warning("No production data available.")
        return

    live_df["timestamp"] = pd.to_datetime(live_df["timestamp"], errors="coerce")

    # ===================== PRODUCT KPI ENGINE =====================
    total_products = len(live_df)

    dispatch_products = len(live_df[live_df["current_stage"] == "Dispatch"])
    completion_pct = round((dispatch_products / total_products) * 100, 2) if total_products > 0 else 0

    backlog_counts = {}
    impacted_house_counts = {}

    for stg in stage_order:

        if stg == "Measurement":
            temp_df = live_df[live_df["current_stage"].isin(["Not Started", "Measurement"])]
        else:
            temp_df = live_df[live_df["current_stage"] == stg]

        backlog_counts[stg] = len(temp_df)
        impacted_house_counts[stg] = temp_df["house_id"].nunique()

    bottleneck_stage = max(backlog_counts, key=backlog_counts.get)
    highest_pending = backlog_counts[bottleneck_stage]

    # ===================== TRUE CLOSED HOUSE CALC =====================
    house_stage = live_df.groupby("house_id")["current_stage"].apply(list)

    closed_houses = 0

    for hid, vals in house_stage.items():
        if all(v == "Dispatch" for v in vals):
            closed_houses += 1

    active_houses = total_houses - closed_houses
    critical_houses = active_houses

    # ===================== OPERATIONAL LOAD =====================
    active_df = live_df[live_df["current_stage"] != "Dispatch"]

    unit_rank = active_df.dropna(subset=["unit_name"]).groupby("unit_name").size().sort_values(ascending=False)
    project_rank = active_df.dropna(subset=["project_name"]).groupby("project_name").size().sort_values(ascending=False)

    top_unit = unit_rank.index[0] if not unit_rank.empty else "N/A"
    top_project = project_rank.index[0] if not project_rank.empty else "N/A"

    measurement_backlog = backlog_counts["Measurement"]
    production_backlog = backlog_counts["Production"]
    dispatch_backlog = backlog_counts["Dispatch"]

    # ===================== KPI ROW =====================
    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)

    c1.metric("Live Products", total_products)
    c2.metric("Active Houses", active_houses)
    c3.metric("Completion %", f"{completion_pct}%")
    c4.metric("Bottleneck", bottleneck_stage)
    c5.metric("Highest Pending", highest_pending)
    c6.metric("Closed Houses", closed_houses)
    c7.metric("Critical Houses", critical_houses)

    st.markdown("---")

    # ===================== ROW 2 NATIVE BOXES =====================
    r1, r2, r3 = st.columns(3)

    with r1:
        with st.container(border=True):
            st.subheader("📦 Stage Backlog Snapshot")
            for stg in stage_order:
                st.markdown(f"**{stg}** : {backlog_counts[stg]} pending products")

    with r2:
        with st.container(border=True):
            st.subheader("🏠 House Impact Snapshot")
            for stg in stage_order:
                st.markdown(f"**{stg}** : {impacted_house_counts[stg]} impacted houses")

    with r3:
        with st.container(border=True):
            st.subheader("🧠 Smart Manager Insights")
            st.info(f"🔴 Major bottleneck at {bottleneck_stage}")
            st.info(f"🟠 {measurement_backlog} products waiting from measurement")
            st.info(f"🟡 {dispatch_backlog} products pending dispatch closure")
            st.info(f"🏗 Highest operational unit : {top_unit}")
            st.info(f"📍 Highest running project : {top_project}")
            st.info(f"⚠ {critical_houses} houses still under execution")

    st.markdown("---")

    # ===================== ROW 3 START =====================
    b1, b2 = st.columns([1.8, 1])

    with b1:
        with st.container(border=True):
            st.subheader("📈 Daily Workflow Pending Trend")

            trend_df = live_df.dropna(subset=["timestamp"]).copy()

            if not trend_df.empty:
                trend_df["Date"] = trend_df["timestamp"].dt.date

                daily = trend_df.groupby(["Date", "current_stage"]).size().reset_index(name="Count")

                if not daily.empty:
                    fig = px.line(
                        daily,
                        x="Date",
                        y="Count",
                        color="current_stage",
                        markers=True,
                        height=420
                    )

                    fig.update_layout(
                        margin=dict(l=5, r=5, t=5, b=5),
                        plot_bgcolor="white",
                        paper_bgcolor="white"
                    )

                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("No daily movement records available.")
            else:
                st.warning("No timestamp trend data available.")

    with b2:
        with st.container(border=True):
            st.subheader("🚨 SLA Breach Audit + Recommendations")

            measurement_pct = round((measurement_backlog / total_products) * 100, 2) if total_products else 0
            production_pct = round((production_backlog / total_products) * 100, 2) if total_products else 0
            dispatch_pct = round((dispatch_backlog / total_products) * 100, 2) if total_products else 0

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
                st.success(f"Dispatch closure healthy : {dispatch_pct}%")

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
