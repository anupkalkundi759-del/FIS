import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
from zoneinfo import ZoneInfo


def show_dashboard_v2(conn, cur):

    st.title("🏭 Factory Intelligence Command Center")

    tz = ZoneInfo("Asia/Kolkata")
    now = datetime.now(tz)

    stage_order = [
        "Measurement",
        "Cutting List",
        "Production",
        "Pre Assembly",
        "Polishing",
        "Final Assembly",
        "Dispatch"
    ]

    stage_rank = {
        "Not Started": 0,
        "Measurement": 1,
        "Cutting List": 2,
        "Production": 3,
        "Pre Assembly": 4,
        "Polishing": 5,
        "Final Assembly": 6,
        "Dispatch": 7
    }

    # ================= LIVE DATA =================
    live_query = """
    WITH latest_tracking AS (
        SELECT DISTINCT ON (t.product_instance_id)
            t.product_instance_id,
            t.stage_id,
            t.timestamp
        FROM tracking_log t
        ORDER BY t.product_instance_id, t.timestamp DESC
    )

    SELECT
        h.project_name,
        h.unit_name,
        h.house_no,
        pm.product_code,
        COALESCE(s.stage_name, 'Not Started') AS current_stage,
        lt.timestamp,
        p.product_instance_id,
        h.house_id
    FROM products p
    JOIN houses h ON p.house_id = h.house_id
    JOIN products_master pm ON p.product_id = pm.product_id
    LEFT JOIN latest_tracking lt
        ON p.product_instance_id = lt.product_instance_id
    LEFT JOIN stages s
        ON lt.stage_id = s.stage_id
    """

    live_df = pd.read_sql(live_query, conn)

    if live_df.empty:
        st.warning("No production data available.")
        return

    live_df["timestamp"] = pd.to_datetime(live_df["timestamp"], errors="coerce")
    live_df["StageRank"] = live_df["current_stage"].map(stage_rank)

    total_products = len(live_df)
    active_houses = live_df["house_no"].nunique()

    dispatch_products = len(live_df[live_df["current_stage"] == "Dispatch"])
    completion_pct = round((dispatch_products / total_products) * 100, 2) if total_products > 0 else 0

    backlog_counts = {}
    for stg in stage_order:
        if stg == "Measurement":
            backlog_counts[stg] = len(live_df[live_df["current_stage"].isin(["Not Started", "Measurement"])])
        else:
            backlog_counts[stg] = len(live_df[live_df["current_stage"] == stg])

    bottleneck_stage = max(backlog_counts, key=backlog_counts.get)
    highest_pending = backlog_counts[bottleneck_stage]

    house_group = live_df.groupby("house_no")["current_stage"].apply(list)

    closed_houses = 0
    critical_houses = 0

    for h, vals in house_group.items():
        if all(v == "Dispatch" for v in vals):
            closed_houses += 1
        else:
            critical_houses += 1

    unit_rank = live_df.dropna(subset=["unit_name"]).groupby("unit_name").size().sort_values(ascending=False)
    project_rank = live_df.dropna(subset=["project_name"]).groupby("project_name").size().sort_values(ascending=False)

    top_unit = unit_rank.index[0] if not unit_rank.empty else "N/A"
    top_project = project_rank.index[0] if not project_rank.empty else "N/A"

    measurement_backlog = backlog_counts["Measurement"]
    production_backlog = backlog_counts["Production"]
    dispatch_backlog = backlog_counts["Dispatch"]

    # ================= KPI ROW =================
    k1, k2, k3, k4, k5, k6, k7 = st.columns(7)

    k1.metric("Live Products", total_products)
    k2.metric("Active Houses", active_houses)
    k3.metric("Completion %", f"{completion_pct}%")
    k4.metric("Bottleneck", bottleneck_stage)
    k5.metric("Highest Pending", highest_pending)
    k6.metric("Closed Houses", closed_houses)
    k7.metric("Critical Houses", critical_houses)

    st.markdown("---")

    # ================= ROW 2 =================
    a1, a2, a3 = st.columns([1, 1, 1])

    with a1:
        st.subheader("📦 Stage Backlog Snapshot")
        for stg in stage_order:
            st.write(f"**{stg}** : {backlog_counts[stg]} pending products")

    with a2:
        st.subheader("🏠 House Impact Snapshot")
        for stg in stage_order:
            if stg == "Measurement":
                hc = live_df[live_df["current_stage"].isin(["Not Started", "Measurement"])]["house_no"].nunique()
            else:
                hc = live_df[live_df["current_stage"] == stg]["house_no"].nunique()
            st.write(f"**{stg}** : {hc} impacted houses")

    with a3:
        st.subheader("🧠 Smart Manager Insights")

        st.info(f"🔴 Worst bottleneck at {bottleneck_stage}")
        st.info(f"🟠 {measurement_backlog} products waiting from measurement")
        st.info(f"🟡 {dispatch_backlog} products pending for dispatch closure")
        st.info(f"🏗 Highest supervisory load in Unit {top_unit}")
        st.info(f"📍 Highest running volume in Project {top_project}")
        st.info(f"⚠ {critical_houses} houses still under execution")

    st.markdown("---")

    # ================= ROW 3 =================
    b1, b2 = st.columns([1.7, 1])

    with b1:
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
                    height=430
                )
                fig.update_layout(
                    margin=dict(l=5, r=5, t=5, b=5)
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No daily movement records available.")
        else:
            st.warning("No timestamp trend data available.")

    with b2:
        st.subheader("🚨 SLA Breach Audit + Recommendations")

        measurement_pct = round((measurement_backlog / total_products) * 100, 2) if total_products > 0 else 0
        production_pct = round((production_backlog / total_products) * 100, 2) if total_products > 0 else 0
        dispatch_pct = round((dispatch_backlog / total_products) * 100, 2) if total_products > 0 else 0

        if measurement_pct > 30:
            st.error(f"Measurement backlog critical : {measurement_pct}%")
        else:
            st.success(f"Measurement flow stable : {measurement_pct}%")

        if production_pct > 20:
            st.error(f"Production queue overloaded : {production_pct}%")
        else:
            st.success(f"Production queue manageable : {production_pct}%")

        if dispatch_pct > 5:
            st.error(f"Dispatch closure weak : {dispatch_pct}%")
        else:
            st.success(f"Dispatch closure healthy : {dispatch_pct}%")

        if completion_pct < 20:
            st.error(f"Factory throughput critically low : {completion_pct}%")
        else:
            st.success(f"Factory throughput acceptable : {completion_pct}%")

        st.markdown("### 📌 Recommended Actions")
        st.write(f"• Deploy manpower immediately to {bottleneck_stage}")
        st.write("• Run supervisor review on all critical houses")
        st.write("• Conduct dispatch closure meeting")
        st.write(f"• Management intervention required in Unit {top_unit}")
        st.write("• Push unfinished houses aggressively to next stage")
