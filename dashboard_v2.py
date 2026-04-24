import streamlit as st
import pandas as pd
import plotly.express as px


def show_dashboard_v2(conn, cur):

    st.markdown("## 📊 Factory Intelligence Dashboard")

    # ================= MASTER KPIs =================
    cur.execute("SELECT COUNT(*) FROM projects")
    total_projects = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM units")
    total_units = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM houses")
    total_houses = cur.fetchone()[0]

    # ================= TRACKING DATA =================
    df = pd.read_sql("""
        SELECT product_instance_id, stage_id, status, timestamp
        FROM tracking_log
    """, conn)

    if df.empty:
        st.warning("No tracking data available")
        return

    # ================= FIX: LATEST STAGE PER PRODUCT =================
    df = df.sort_values("timestamp", ascending=False)
    latest_df = df.drop_duplicates(subset=["product_instance_id"], keep="first")

    total_products = latest_df["product_instance_id"].nunique()

    completed = len(latest_df[latest_df["status"] == "Completed"])
    dispatched = len(latest_df[latest_df["status"] == "Dispatched"])
    in_progress = len(latest_df[latest_df["status"] == "In Progress"])
    pending = total_products - completed

    # ================= KPI ROW 1 =================
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Projects", total_projects)
    k2.metric("Units", total_units)
    k3.metric("Houses", total_houses)
    k4.metric("Products", total_products)

    # ================= KPI ROW 2 =================
    k5, k6, k7, k8 = st.columns(4)
    k5.metric("Completed", completed)
    k6.metric("Dispatched", dispatched)
    k7.metric("In Progress", in_progress)
    k8.metric("Pending", pending)

    st.markdown("---")

    # ================= STAGE DISTRIBUTION =================
    stage_df = pd.read_sql("""
        SELECT s.stage_name, COUNT(*) as count
        FROM tracking_log t
        JOIN stages s ON t.stage_id = s.stage_id
        GROUP BY s.stage_name
        ORDER BY count DESC
    """, conn)

    # ================= SINGLE CHART (NO DUPLICATE PIE) =================
    fig = px.bar(stage_df, x="stage_name", y="count", title="Stage Distribution")
    st.plotly_chart(fig, use_container_width=True)

    # ================= BOTTLENECK =================
    if not stage_df.empty:
        bottleneck = stage_df.iloc[0]["stage_name"]
        st.error(f"🚨 Bottleneck Stage: {bottleneck}")

    # ================= SUMMARY TABLE =================
    st.markdown("### Stage Summary")
    st.dataframe(stage_df, use_container_width=True)
