import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
from zoneinfo import ZoneInfo


def show_dashboard_v2(conn, cur):

    st.title("🏭 Factory Intelligence Command Center")

    tz = ZoneInfo("Asia/Kolkata")
    now = datetime.now(tz)

    # ========================= MASTER STAGE ORDER =========================
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

    # ========================= LIVE SNAPSHOT QUERY (FINAL CORRECTED) =========================
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
        st.warning("No live production data found.")
        return

    live_df["timestamp"] = pd.to_datetime(live_df["timestamp"], errors="coerce")
    live_df["StageRank"] = live_df["current_stage"].map(stage_rank)

    # ========================= FILTERS =========================
    st.subheader("📌 Drilldown Filters")

    f1, f2, f3 = st.columns(3)

    with f1:
        selected_project = st.selectbox("Select Project", ["All"] + sorted(live_df["project_name"].dropna().unique().tolist()))

    if selected_project != "All":
        live_df = live_df[live_df["project_name"] == selected_project]

    with f2:
        selected_unit = st.selectbox("Select Unit", ["All"] + sorted(live_df["unit_name"].dropna().unique().tolist()))

    if selected_unit != "All":
        live_df = live_df[live_df["unit_name"] == selected_unit]

    with f3:
        selected_houses = st.multiselect("Select Houses (Optional)", sorted(live_df["house_no"].astype(str).unique().tolist()))

    if selected_houses:
        live_df = live_df[live_df["house_no"].astype(str).isin(selected_houses)]

    # ========================= KPI ENGINE =========================
    total_products = len(live_df)
    total_houses = live_df["house_no"].nunique()

    dispatch_products = len(live_df[live_df["current_stage"] == "Dispatch"])
    overall_completion = round((dispatch_products / total_products) * 100, 2) if total_products > 0 else 0

    backlog_counts = {}
    for stg in stage_order:
        if stg == "Measurement":
            backlog_counts[stg] = len(live_df[live_df["current_stage"].isin(["Not Started", "Measurement"])])
        else:
            backlog_counts[stg] = len(live_df[live_df["current_stage"] == stg])

    bottleneck_stage = max(backlog_counts, key=backlog_counts.get)
    bottleneck_pending = backlog_counts[bottleneck_stage]

    house_group = live_df.groupby("house_no")["current_stage"].apply(list)

    closed_houses = 0
    critical_houses = 0

    for house, stages in house_group.items():
        if all(str(x) == "Dispatch" for x in stages):
            closed_houses += 1
        else:
            critical_houses += 1

    # ========================= ROW 1 KPI CARDS =========================
    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)

    c1.metric("Live Products", total_products)
    c2.metric("Active Houses", total_houses)
    c3.metric("Completion %", f"{overall_completion}%")
    c4.metric("Bottleneck", bottleneck_stage)
    c5.metric("Highest Pending", bottleneck_pending)
    c6.metric("Closed Houses", closed_houses)
    c7.metric("Critical Houses", critical_houses)

    st.markdown("---")

    # ========================= ROW 2 =========================
    r2c1, r2c2, r2c3 = st.columns([1.15, 1.15, 1])

    # BOX 1
    with r2c1:
        st.subheader("📦 Stage Backlog Products")

        stage_prod_df = pd.DataFrame({
            "Stage": list(backlog_counts.keys()),
            "Pending Products": list(backlog_counts.values())
        })

        fig1 = px.bar(stage_prod_df, x="Stage", y="Pending Products", text="Pending Products", height=330)
        fig1.update_layout(margin=dict(l=5, r=5, t=10, b=5))
        st.plotly_chart(fig1, use_container_width=True)

    # BOX 2
    with r2c2:
        st.subheader("🏠 House Impact by Stage")

        house_stage_counts = []
        for stg in stage_order:
            if stg == "Measurement":
                cnt = live_df[live_df["current_stage"].isin(["Not Started", "Measurement"])]["house_no"].nunique()
            else:
                cnt = live_df[live_df["current_stage"] == stg]["house_no"].nunique()
            house_stage_counts.append([stg, cnt])

        house_stage_df = pd.DataFrame(house_stage_counts, columns=["Stage", "Impacted Houses"])

        fig2 = px.bar(house_stage_df, x="Stage", y="Impacted Houses", text="Impacted Houses", height=330)
        fig2.update_layout(margin=dict(l=5, r=5, t=10, b=5))
        st.plotly_chart(fig2, use_container_width=True)

    # BOX 3
    with r2c3:
        st.subheader("🧠 Smart Manager Insights")

        top_unit = live_df.groupby("unit_name").size().sort_values(ascending=False).index[0]
        top_project = live_df.groupby("project_name").size().sort_values(ascending=False).index[0]

        measurement_backlog = backlog_counts["Measurement"]
        dispatch_backlog = backlog_counts["Dispatch"]

        st.info(f"🔴 Highest congestion at {bottleneck_stage} with {bottleneck_pending} products")
        st.info(f"🟠 Measurement pending workflow : {measurement_backlog}")
        st.info(f"🟡 Dispatch pending closure : {dispatch_backlog}")
        st.info(f"🏗️ Highest operational load in Unit : {top_unit}")
        st.info(f"📍 Highest running volume in Project : {top_project}")
        st.info(f"⚠️ {critical_houses} houses still under production")

    st.markdown("---")

    # ========================= ROW 3 =========================
    r3c1, r3c2 = st.columns([1.55, 1])

    with r3c1:
        st.subheader("📈 Daily Workflow Movement Trend")

        trend_df = live_df.dropna(subset=["timestamp"]).copy()

        if not trend_df.empty:
            trend_df["Date"] = trend_df["timestamp"].dt.date
            daily = trend_df.groupby(["Date", "current_stage"]).size().reset_index(name="Count")

            fig3 = px.line(daily, x="Date", y="Count", color="current_stage", markers=True, height=360)
            fig3.update_layout(margin=dict(l=5, r=5, t=10, b=5))
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.warning("No historical timestamps available.")

    with r3c2:
        st.subheader("🚨 SLA Breach Audit")

        measurement_pct = round((measurement_backlog / total_products) * 100, 2)
        production_pct = round((backlog_counts["Production"] / total_products) * 100, 2)
        dispatch_pct = round((dispatch_backlog / total_products) * 100, 2)

        if measurement_pct > 40:
            st.error(f"Measurement SLA Breach : {measurement_pct}%")
        else:
            st.success(f"Measurement SLA Healthy : {measurement_pct}%")

        if production_pct > 20:
            st.error(f"Production Queue Heavy : {production_pct}%")
        else:
            st.success(f"Production Queue Stable : {production_pct}%")

        if dispatch_pct > 5:
            st.error(f"Dispatch Closure Weak : {dispatch_pct}%")
        else:
            st.success(f"Dispatch Closure Healthy : {dispatch_pct}%")

        if overall_completion < 20:
            st.error(f"Factory Throughput Critically Low : {overall_completion}%")
        else:
            st.success(f"Factory Throughput Acceptable : {overall_completion}%")

        st.markdown("### 📌 Recommended Actions")
        st.write(f"• Push manpower toward {bottleneck_stage}")
        st.write("• Conduct immediate review on critical houses")
        st.write("• Accelerate dispatch clearance meeting")
        st.write(f"• Focus supervisory intervention in {top_unit}")
