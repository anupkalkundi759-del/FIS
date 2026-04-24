import streamlit as st
import pandas as pd
import plotly.express as px


def show_dashboard_v2(conn, cur):

    st.markdown("## 📊 Factory Intelligence Dashboard")

    # ================= DATA =================
    df = pd.read_sql("""
        SELECT product_instance_id, stage_id, status, timestamp
        FROM tracking_log
    """, conn)

    if df.empty:
        st.warning("No tracking data available")
        return

    df = df.sort_values("timestamp", ascending=False)
    latest = df.drop_duplicates("product_instance_id")

    total = latest["product_instance_id"].nunique()
    completed = len(latest[latest["status"] == "Completed"])
    dispatched = len(latest[latest["status"] == "Dispatched"])
    in_progress = len(latest[latest["status"] == "In Progress"])
    pending = total - completed

    # ================= KPI STRIP =================
    k1, k2, k3, k4 = st.columns(4)

    k1.metric("Total Products", total)
    k2.metric("Completed", completed)
    k3.metric("In Progress", in_progress)
    k4.metric("Pending", pending)

    # ================= HEALTH =================
    completion_rate = (completed / total) * 100 if total else 0

    if completion_rate > 70:
        st.success(f"🟢 Healthy Flow ({completion_rate:.1f}%)")
    elif completion_rate > 40:
        st.warning(f"🟡 Moderate Flow ({completion_rate:.1f}%)")
    else:
        st.error(f"🔴 Critical Flow ({completion_rate:.1f}%)")

    # ================= STAGE LOAD =================
    stage_df = pd.read_sql("""
        SELECT s.stage_name, COUNT(*) as count
        FROM tracking_log t
        JOIN stages s ON t.stage_id = s.stage_id
        GROUP BY s.stage_name
        ORDER BY count DESC
    """, conn)

    # ================= BOTTLENECK =================
    bottleneck = stage_df.iloc[0]["stage_name"]
    st.error(f"🚨 Bottleneck: {bottleneck}")

    # ================= MAIN VIEW =================
    col1, col2 = st.columns([2, 1])

    with col1:
        fig = px.bar(stage_df, x="stage_name", y="count",
                     title="Where Work is Stuck")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("### ⚠ Key Signals")

        if in_progress > completed:
            st.warning("Too much WIP → slow flow")

        if dispatched == 0:
            st.warning("No dispatch happening")

        if bottleneck == "Design & Engineering":
            st.error("Upstream blocking entire pipeline")

        if completion_rate < 30:
            st.error("System underperforming")

    # ================= QUICK SUMMARY =================
    st.markdown("### 📋 Quick Summary")

    st.write(f"""
    - Total Products: **{total}**
    - Completed: **{completed}**
    - In Progress: **{in_progress}**
    - Pending: **{pending}**
    - Bottleneck Stage: **{bottleneck}**
    """)

    # ================= STAGE TABLE =================
    st.dataframe(stage_df, use_container_width=True)
