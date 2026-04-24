import streamlit as st
import pandas as pd
import plotly.express as px


def show_dashboard_v2(conn, cur):

    st.markdown("## 📊 Factory Intelligence Dashboard")

    # ================= KPI DATA =================

    cur.execute("SELECT COUNT(*) FROM projects")
    total_projects = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM units")
    total_units = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM houses")
    total_houses = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM products")
    total_products = cur.fetchone()[0]

    cur.execute("""
        SELECT 
            COUNT(*) FILTER (WHERE status = 'Completed'),
            COUNT(*) FILTER (WHERE status = 'Dispatched'),
            COUNT(*) FILTER (WHERE status = 'In Progress')
        FROM tracking_log
    """)
    completed, dispatched, in_progress = cur.fetchone()

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

    # ================= STAGE DATA =================
    df = pd.read_sql("""
        SELECT s.stage_name, COUNT(*) as count
        FROM tracking_log t
        JOIN stages s ON t.stage_id = s.stage_id
        GROUP BY s.stage_name
        ORDER BY count DESC
    """, conn)

    # ================= MAIN VISUAL ROW =================
    col1, col2 = st.columns(2)

    with col1:
        fig = px.bar(df, x="stage_name", y="count", title="Stage Distribution")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig2 = px.pie(df, names="stage_name", values="count", title="Work Share")
        st.plotly_chart(fig2, use_container_width=True)

    # ================= BOTTLENECK =================
    bottleneck = df.iloc[0]["stage_name"]
    st.error(f"🚨 Bottleneck Stage: {bottleneck}")

    # ================= SUMMARY TABLE =================
    st.markdown("### Stage Summary")
    st.dataframe(df, use_container_width=True)
