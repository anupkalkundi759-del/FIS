import streamlit as st
import pandas as pd
import plotly.express as px


def show_dashboard_v2(conn, cur):

    st.markdown("# 📊 Factory Intelligence Dashboard")

    # ================= DATA =================
    df = pd.read_sql("""
        SELECT 
            p.product_instance_id,
            pr.project_name,
            u.unit_name,
            h.house_name,
            s.stage_name,
            t.status,
            t.timestamp
        FROM tracking_log t
        JOIN products p ON t.product_instance_id = p.product_instance_id
        JOIN houses h ON p.house_id = h.house_id
        JOIN units u ON h.unit_id = u.unit_id
        JOIN projects pr ON u.project_id = pr.project_id
        JOIN stages s ON t.stage_id = s.stage_id
    """, conn)

    if df.empty:
        st.warning("No data")
        return

    # ================= LATEST =================
    df = df.sort_values("timestamp", ascending=False)
    latest = df.drop_duplicates("product_instance_id")

    total = len(latest)
    completed = len(latest[latest["status"] == "Completed"])
    dispatched = len(latest[latest["status"] == "Dispatched"])
    pending = total - completed

    # ================= KPI =================
    c1, c2, c3, c4, c5 = st.columns(5)

    c1.metric("Projects", latest["project_name"].nunique())
    c2.metric("Products", total)
    c3.metric("Completed %", f"{(completed/total)*100:.1f}%")
    c4.metric("Pending %", f"{(pending/total)*100:.1f}%")
    c5.metric("Dispatch %", f"{(dispatched/total)*100:.1f}%")

    # ================= DONUT =================
    donut_df = pd.DataFrame({
        "status": ["Completed", "Pending"],
        "count": [completed, pending]
    })

    fig1 = px.pie(
        donut_df,
        names="status",
        values="count",
        hole=0.6,
        title="Overall Progress"
    )

    # ================= HEATMAP =================
    heat = latest.groupby(
        ["project_name", "unit_name"]
    ).agg(
        total=("product_instance_id", "count"),
        completed=("status", lambda x: (x == "Completed").sum())
    ).reset_index()

    heat["pending_ratio"] = (heat["total"] - heat["completed"]) / heat["total"]

    fig2 = px.density_heatmap(
        heat,
        x="project_name",
        y="unit_name",
        z="pending_ratio",
        title="Project Risk Heatmap"
    )

    # ================= BOTTLENECK =================
    stage = latest["stage_name"].value_counts().reset_index()
    stage.columns = ["stage", "count"]

    fig3 = px.bar(
        stage,
        x="stage",
        y="count",
        title="Stage Load (Bottleneck View)"
    )

    bottleneck = stage.iloc[0]["stage"]

    # ================= TOP RISK =================
    proj = latest.groupby("project_name").agg(
        total=("product_instance_id", "count"),
        completed=("status", lambda x: (x == "Completed").sum())
    ).reset_index()

    proj["pending"] = proj["total"] - proj["completed"]

    top = proj.sort_values("pending", ascending=False).head(5)

    fig4 = px.bar(
        top,
        x="pending",
        y="project_name",
        orientation="h",
        title="Top Risk Projects"
    )

    # ================= LAYOUT =================
    r1, r2 = st.columns(2)
    r1.plotly_chart(fig1, use_container_width=True)
    r2.plotly_chart(fig2, use_container_width=True)

    r3, r4 = st.columns(2)
    r3.plotly_chart(fig3, use_container_width=True)
    r4.plotly_chart(fig4, use_container_width=True)

    # ================= ALERT =================
    st.error(f"🚨 Bottleneck Stage: {bottleneck}")
