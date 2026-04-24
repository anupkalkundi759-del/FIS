import streamlit as st
import pandas as pd
import plotly.express as px


def show_dashboard_v2(conn, cur):

    st.markdown("# 🏭 Factory Intelligence Dashboard")

    # ================= DATA =================
    df = pd.read_sql("""
        SELECT 
            p.product_instance_id,
            pr.project_name,
            u.unit_name,
            h.house_no,
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
        st.warning("No data available")
        return

    # ================= LATEST STATUS =================
    df = df.sort_values("timestamp", ascending=False)
    latest = df.drop_duplicates("product_instance_id")

    # ================= KPI =================
    total_products = latest["product_instance_id"].nunique()
    completed = (latest["status"] == "Completed").sum()
    dispatched = (latest["status"] == "Dispatched").sum()
    pending = total_products - completed

    total_projects = latest["project_name"].nunique()
    total_units = latest["unit_name"].nunique()
    total_houses = latest["house_no"].nunique()

    # KPI ROW 1
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Projects", total_projects)
    k2.metric("Units", total_units)
    k3.metric("Houses", total_houses)
    k4.metric("Products", total_products)

    # KPI ROW 2
    k5, k6, k7 = st.columns(3)
    k5.metric("Completed %", f"{(completed/total_products)*100:.1f}%")
    k6.metric("Pending %", f"{(pending/total_products)*100:.1f}%")
    k7.metric("Dispatch %", f"{(dispatched/total_products)*100:.1f}%")

    st.markdown("---")

    # ================= DONUT (ONLY ONE OVERVIEW) =================
    donut_df = pd.DataFrame({
        "Status": ["Completed", "Pending"],
        "Count": [completed, pending]
    })

    fig1 = px.pie(
        donut_df,
        names="Status",
        values="Count",
        hole=0.6,
        title="Overall Completion Status"
    )

    # ================= PROJECT RISK =================
    proj = latest.groupby(
        ["project_name", "unit_name"]
    ).agg(
        total=("product_instance_id", "count"),
        completed=("status", lambda x: (x == "Completed").sum())
    ).reset_index()

    proj["pending"] = proj["total"] - proj["completed"]

    # ================= HEATMAP =================
    proj["pending_ratio"] = proj["pending"] / proj["total"]

    fig2 = px.density_heatmap(
        proj,
        x="project_name",
        y="unit_name",
        z="pending_ratio",
        title="Project Risk Heatmap (Pending %)"
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

    # ================= TOP RISK PROJECTS =================
    top = proj.sort_values("pending", ascending=False).head(5)

    fig4 = px.bar(
        top,
        x="pending",
        y="project_name",
        orientation="h",
        title="Top Risk Projects (Pending Load)"
    )

    # ================= LAYOUT (NO SCROLL DESIGN) =================
    r1, r2 = st.columns(2)
    r1.plotly_chart(fig1, use_container_width=True)
    r2.plotly_chart(fig2, use_container_width=True)

    r3, r4 = st.columns(2)
    r3.plotly_chart(fig3, use_container_width=True)
    r4.plotly_chart(fig4, use_container_width=True)

    # ================= ALERT =================
    st.error(f"🚨 Bottleneck Stage: {bottleneck}")

    # ================= DECISION INSIGHTS =================
    st.markdown("### ⚠ Decision Insights")

    if pending > completed:
        st.warning("High pending workload → risk of delay")

    if dispatched == 0:
        st.warning("No dispatch → pipeline blockage")

    if proj["pending"].max() > (proj["total"].max() * 0.7):
        st.error("Critical project overload detected")
