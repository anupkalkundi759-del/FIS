import streamlit as st
import pandas as pd
import plotly.express as px


def show_dashboard_v2(conn, cur):

    st.markdown("## 📊 Factory Intelligence Dashboard")

    # ================= MASTER COUNTS =================
    cur.execute("SELECT COUNT(*) FROM projects")
    total_projects = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM units")
    total_units = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM houses")
    total_houses = cur.fetchone()[0]

    # ================= TRACKING =================
    df = pd.read_sql("""
        SELECT product_instance_id, stage_id, status, timestamp
        FROM tracking_log
    """, conn)

    if df.empty:
        st.warning("No tracking data available")
        return

    # ================= LATEST STATE =================
    df = df.sort_values("timestamp", ascending=False)
    latest_df = df.drop_duplicates(subset=["product_instance_id"], keep="first")

    total_products = latest_df["product_instance_id"].nunique()

    completed = len(latest_df[latest_df["status"] == "Completed"])
    dispatched = len(latest_df[latest_df["status"] == "Dispatched"])
    in_progress = len(latest_df[latest_df["status"] == "In Progress"])
    pending = total_products - completed

    # ================= KPI ROW =================
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Projects", total_projects)
    k2.metric("Units", total_units)
    k3.metric("Houses", total_houses)
    k4.metric("Products", total_products)

    k5, k6, k7, k8 = st.columns(4)
    k5.metric("Completed", completed)
    k6.metric("Dispatched", dispatched)
    k7.metric("In Progress", in_progress)
    k8.metric("Pending", pending)

    st.markdown("---")

    # ================= STAGE DATA =================
    stage_df = pd.read_sql("""
        SELECT s.stage_name, COUNT(*) as count
        FROM tracking_log t
        JOIN stages s ON t.stage_id = s.stage_id
        GROUP BY s.stage_name
        ORDER BY count DESC
    """, conn)

    # ================= MAIN LAYOUT =================
    left, right = st.columns([2, 1])

    # -------- LEFT (CHART) --------
    with left:
        fig = px.bar(stage_df, x="stage_name", y="count",
                     title="Stage Load (Where work is stuck)")
        st.plotly_chart(fig, use_container_width=True)

    # -------- RIGHT (INSIGHTS) --------
    with right:
        st.markdown("### 🚨 Bottleneck")
        if not stage_df.empty:
            bottleneck = stage_df.iloc[0]["stage_name"]
            st.error(f"{bottleneck}")

        st.markdown("### 📊 Completion Health")

        completion_rate = (completed / total_products) * 100 if total_products else 0

        st.progress(int(completion_rate))

        st.write(f"Completion Rate: **{completion_rate:.1f}%**")

        st.markdown("### ⚠ Alerts")

        if completion_rate < 20:
            st.warning("Very low completion rate")

        if in_progress > completed:
            st.warning("High WIP → risk of delay")

        if dispatched == 0:
            st.warning("Nothing dispatched yet")

    st.markdown("---")

    # ================= SUMMARY =================
    st.markdown("### 📋 Stage Summary")
    st.dataframe(stage_df, use_container_width=True)
