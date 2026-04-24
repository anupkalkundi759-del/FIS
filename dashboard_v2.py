import streamlit as st
import pandas as pd
import plotly.express as px


def show_dashboard_v2(conn, cur):

    st.markdown("## 📊 Factory Intelligence Dashboard")

    # ================= KPI DATA =================
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE status = 'In Progress') as in_progress,
            COUNT(*) FILTER (WHERE status = 'Completed') as completed
        FROM tracking_log
    """)
    total, in_progress, completed = cur.fetchone()
    pending = total - completed

    # ================= KPI ROW =================
    c1, c2, c3, c4 = st.columns(4)

    c1.metric("Total", total)
    c2.metric("In Progress", in_progress)
    c3.metric("Completed", completed)
    c4.metric("Pending", pending)

    st.markdown("---")

    # ================= STAGE DATA =================
    df = pd.read_sql("""
        SELECT s.stage_name, COUNT(*) as count
        FROM tracking_log t
        JOIN stages s ON t.stage_id = s.stage_id
        GROUP BY s.stage_name
        ORDER BY count DESC
    """, conn)

    # ================= GRID ROW 1 =================
    col1, col2, col3 = st.columns(3)

    with col1:
        fig1 = px.bar(df, x="stage_name", y="count", title="Stage Distribution")
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        fig2 = px.pie(df, names="stage_name", values="count", title="Stage Share")
        st.plotly_chart(fig2, use_container_width=True)

    with col3:
        fig3 = px.bar(df.head(5), x="stage_name", y="count", title="Top WIP")
        st.plotly_chart(fig3, use_container_width=True)

    # ================= GRID ROW 2 =================
    col4, col5, col6 = st.columns(3)

    with col4:
        fig4 = px.bar(df, x="count", y="stage_name", orientation='h', title="WIP by Stage")
        st.plotly_chart(fig4, use_container_width=True)

    with col5:
        fig5 = px.line(df, x="stage_name", y="count", title="Flow Trend")
        st.plotly_chart(fig5, use_container_width=True)

    with col6:
        bottleneck = df.iloc[0]["stage_name"]
        st.error(f"🚨 Bottleneck Stage: {bottleneck}")
