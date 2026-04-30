import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from zoneinfo import ZoneInfo


def show_dashboard(conn, cur):

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

    # ========================= LIVE SNAPSHOT QUERY (CORRECTED) =========================
    live_query = """
    WITH latest_tracking AS (
        SELECT
            t.house_id,
            t.product_id,
            t.stage_id,
            t.timestamp,
            ROW_NUMBER() OVER (
                PARTITION BY t.house_id, t.product_id
                ORDER BY t.timestamp DESC
            ) AS rn
        FROM tracking_log t
    )

    SELECT
        pr.project_name,
        u.unit_name,
        h.house_no,
        pm.product_code,
        COALESCE(s.stage_name, 'Not Started') AS current_stage,
        lt.timestamp
    FROM products p
    JOIN houses h ON p.house_id = h.house_id
    JOIN units u ON h.unit_id = u.unit_id
    JOIN projects pr ON u.project_id = pr.project_id
    JOIN products_master pm ON p.product_id = pm.product_id
    LEFT JOIN latest_tracking lt
        ON p.house_id = lt.house_id
        AND p.product_id = lt.product_id
        AND lt.rn = 1
    LEFT JOIN stages s ON lt.stage_id = s.stage_id
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

    backlog_counts = live_df["current_stage"].value_counts().to_dict()

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

    # ========================= ROW 2 THREE BOXES =========================
    r2c1, r2c2, r2c3 = st.columns([1.15, 1.15, 1])

    # -------- BOX 1 STAGE BACKLOG PRODUCT CHART --------
    with r2c1:
        st.subheader("📦 Stage Backlog Products")

        stage_product_counts = []
        for stg in stage_order:
            if stg == "Measurement":
                cnt = len(live_df[live_df["current_stage"].isin(["Not Started", "Measurement"])])
            else:
                cnt = len(live_df[live_df["current_stage"] == stg])
            stage_product_counts.append([stg, cnt])

        stage_prod_df = pd.DataFrame(stage_product_counts, columns=["Stage", "Pending Products"])

        fig1 = px.bar(
            stage_prod_df,
            x="Stage",
            y="Pending Products",
            text="Pending Products",
            height=330
        )
        fig1.update_layout(margin=dict(l=5, r=5, t=10, b=5))
        st.plotly_chart(fig1, use_container_width=True)

    # -------- BOX 2 HOUSE IMPACTED CHART --------
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

        fig2 = px.pie(
            house_stage_df,
            names="Stage",
            values="Impacted Houses",
            hole=0.45,
            height=330
        )
        fig2.update_layout(margin=dict(l=5, r=5, t=10, b=5))
        st.plotly_chart(fig2, use_container_width=True)

    # -------- BOX 3 SMART INSIGHTS --------
    with r2c3:
        st.subheader("🧠 Smart Manager Insights")

        top_unit = live_df.groupby("unit_name").size().sort_values(ascending=False).index[0]
        top_project = live_df.groupby("project_name").size().sort_values(ascending=False).index[0]

        measurement_backlog = stage_prod_df.iloc[0]["Pending Products"]
        dispatch_backlog = stage_prod_df[stage_prod_df["Stage"] == "Dispatch"]["Pending Products"].iloc[0]

        st.info(f"🔴 Highest factory congestion at {bottleneck_stage} with {bottleneck_pending} products.")
        st.info(f"🟠 Measurement side still carries {measurement_backlog} pending workflow.")
        st.info(f"🟡 Dispatch still awaiting closure for {dispatch_backlog} products.")
        st.info(f"🏗️ Highest operational load concentrated in Unit: {top_unit}")
        st.info(f"📍 Highest running product volume under Project: {top_project}")
        st.info(f"⚠️ {critical_houses} houses still need production attention.")

    st.markdown("---")

    # ========================= ROW 3 TWO BOXES =========================
    r3c1, r3c2 = st.columns([1.55, 1])

    # -------- BOTTOM LEFT TREND GRAPH --------
    with r3c1:
        st.subheader("📈 Daily Workflow Movement Trend")

        trend_df = live_df.dropna(subset=["timestamp"]).copy()

        if not trend_df.empty:
            trend_df["Date"] = trend_df["timestamp"].dt.date
            daily = trend_df.groupby(["Date", "current_stage"]).size().reset_index(name="Count")

            fig3 = px.line(
                daily,
                x="Date",
                y="Count",
                color="current_stage",
                markers=True,
                height=360
            )
            fig3.update_layout(margin=dict(l=5, r=5, t=10, b=5))
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.warning("No historical timestamps available for trend.")

    # -------- BOTTOM RIGHT SLA PANEL --------
    with r3c2:
        st.subheader("🚨 SLA Breach Audit")

        measurement_pct = round((measurement_backlog / total_products) * 100, 2)
        production_pct = round((len(live_df[live_df["current_stage"] == "Production"]) / total_products) * 100, 2)
        dispatch_pct = round((dispatch_backlog / total_products) * 100, 2)

        if measurement_pct > 40:
            st.error(f"Measurement SLA Breach : {measurement_pct}% backlog")
        else:
            st.success(f"Measurement SLA Healthy : {measurement_pct}%")

        if production_pct > 20:
            st.error(f"Production Queue Heavy : {production_pct}%")
        else:
            st.success(f"Production Queue Stable : {production_pct}%")

        if dispatch_pct > 5:
            st.error(f"Dispatch Closure Weak : {dispatch_pct}% pending")
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
