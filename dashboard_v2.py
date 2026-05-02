def show_dashboard_v2(conn, cur):
    import streamlit as st
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    st.title("📊 MFG Analytics Dashboard")

    try:
        conn.rollback()
    except:
        pass

    # =========================================================
    # LIVE MASTER QUERY
    # =========================================================
    live_query = """
    SELECT
        p.project_name,
        u.unit_name,
        h.house_no,
        pr.product_instance_id,
        COALESCE(pm.product_code,'NO PRODUCT') AS product_code,
        COALESCE(pcs.stage_name,'Not Started') AS current_stage,
        COALESCE(pcs.status,'Not Started') AS live_status,
        pcs.updated_at
    FROM houses h
    JOIN units u ON h.unit_id = u.unit_id
    JOIN projects p ON u.project_id = p.project_id
    LEFT JOIN products pr ON h.house_id = pr.house_id
    LEFT JOIN products_master pm ON pr.product_id = pm.product_id
    LEFT JOIN product_current_stage pcs
        ON pr.product_instance_id = pcs.product_instance_id
    """

    cur.execute(live_query)
    rows = cur.fetchall()

    live_df = pd.DataFrame(rows, columns=[
        "Project", "Unit", "House", "ProductInstance", "Product",
        "Stage", "Status", "UpdatedAt"
    ])

    if live_df.empty:
        st.warning("No dashboard data available.")
        return

    live_df = live_df[live_df["Product"] != "NO PRODUCT"].copy()

    # =========================================================
    # STAGE ORDER
    # =========================================================
    stage_order = [
        "Not Started",
        "Cutting List",
        "Production",
        "Pre Assembly",
        "Polishing",
        "Final Assembly",
        "Dispatch"
    ]

    stage_rank = {s: i for i, s in enumerate(stage_order)}
    live_df["StageRank"] = live_df["Stage"].map(stage_rank).fillna(0)

    # =========================================================
    # KPI COMPUTATION
    # =========================================================
    total_products = len(live_df)
    total_houses = live_df["House"].nunique()

    pending_products = len(live_df[live_df["Stage"] == "Not Started"])
    running_products = len(live_df[(live_df["StageRank"] > 0) & (live_df["Stage"] != "Dispatch")])
    dispatched_products = len(live_df[live_df["Stage"] == "Dispatch"])

    overall_completion = round((dispatched_products / total_products) * 100, 2) if total_products > 0 else 0

    house_grp = live_df.groupby("House").agg(
        total=("ProductInstance", "count"),
        dispatch=("Stage", lambda x: (x == "Dispatch").sum())
    ).reset_index()

    completed_houses = len(house_grp[house_grp["total"] == house_grp["dispatch"]])

    # =========================================================
    # KPI ROW
    # =========================================================
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Total Products", total_products)
    k2.metric("Total Houses", total_houses)
    k3.metric("Pending Products", pending_products)
    k4.metric("Running Products", running_products)
    k5.metric("Dispatched Products", dispatched_products)
    k6.metric("Overall Completion", f"{overall_completion}%")

    st.markdown("---")

    # =========================================================
    # ROW 2 - FACTORY FLOW + STATUS
    # =========================================================
    r1, r2 = st.columns(2)

    stage_counts = live_df.groupby("Stage").size().reindex(stage_order).fillna(0)
    stage_completion = []

    for stg in stage_order:
        rk = stage_rank[stg]

        if stg == "Not Started":
            comp = round(((total_products - stage_counts[stg]) / total_products) * 100, 2)

        elif stg == "Dispatch":
            comp = round((stage_counts[stg] / total_products) * 100, 2)

        else:
            pending = len(live_df[live_df["StageRank"] <= rk])
            comp = round(((total_products - pending) / total_products) * 100, 2)

        stage_completion.append(comp)

    with r1:
        fig1 = make_subplots(specs=[[{"secondary_y": True}]])
        fig1.add_trace(
            go.Bar(x=stage_order, y=stage_counts.values, name="Pending Distribution"),
            secondary_y=False
        )
        fig1.add_trace(
            go.Scatter(x=stage_order, y=stage_completion, mode="lines+markers", name="Completion %"),
            secondary_y=True
        )
        fig1.update_layout(title="Factory Stage Flow Trend", height=380, margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig1, use_container_width=True)

    live_df["StatusBand"] = live_df.apply(
        lambda x: "Not Started" if x["Stage"] == "Not Started"
        else ("Completed" if x["Stage"] == "Dispatch" or x["Status"] == "Completed" else "In Progress"),
        axis=1
    )

    stack_df = live_df.groupby(["Stage", "StatusBand"]).size().reset_index(name="Count")

    with r2:
        fig2 = px.bar(
            stack_df,
            x="Stage",
            y="Count",
            color="StatusBand",
            barmode="stack",
            category_orders={"Stage": stage_order},
            title="Product Status by Stage"
        )
        fig2.update_layout(height=380, margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("---")

    # =========================================================
    # ROW 3 - UNIT PERFORMANCE + HOUSE COMPLETION
    # =========================================================
    r3, r4 = st.columns(2)

    unit_perf = live_df.groupby("Unit").agg(
        Total=("ProductInstance", "count"),
        Pending=("Stage", lambda x: (x == "Not Started").sum()),
        Running=("StageRank", lambda x: ((x > 0) & (x < 6)).sum()),
        Dispatch=("Stage", lambda x: (x == "Dispatch").sum())
    ).reset_index()

    unit_perf["PressureScore"] = unit_perf["Pending"] + unit_perf["Running"] - unit_perf["Dispatch"]
    top_units = unit_perf.sort_values("PressureScore", ascending=False).head(8)

    with r3:
        fig3 = px.bar(
            top_units,
            x="PressureScore",
            y="Unit",
            orientation="h",
            title="Unit Performance Comparison"
        )
        fig3.update_layout(height=380, margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig3, use_container_width=True)

    house_grp["pct"] = (house_grp["dispatch"] / house_grp["total"]) * 100

    def get_band(x):
        if x == 100:
            return "100%"
        elif x >= 75:
            return "75-99%"
        elif x >= 50:
            return "50-75%"
        elif x >= 25:
            return "25-50%"
        else:
            return "0-25%"

    house_grp["Band"] = house_grp["pct"].apply(get_band)

    band_df = house_grp["Band"].value_counts().reindex(
        ["0-25%", "25-50%", "50-75%", "75-99%", "100%"]
    ).fillna(0)

    with r4:
        fig4 = px.bar(
            x=band_df.index,
            y=band_df.values,
            title="House Completion Band"
        )
        fig4.update_layout(height=380, margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig4, use_container_width=True)

    st.markdown("---")

    # =========================================================
    # ROW 4 - DELAY RISK + BOTTLENECK
    # =========================================================
    r5, r6 = st.columns(2)

    live_df["UpdatedAt"] = pd.to_datetime(live_df["UpdatedAt"], errors="coerce")
    today = pd.Timestamp.today()

    live_df["AgeDays"] = (today - live_df["UpdatedAt"]).dt.days
    live_df.loc[live_df["UpdatedAt"].isna(), "AgeDays"] = 30

    def risk_band(x):
        if x >= 10:
            return "Critical Delay"
        elif x >= 5:
            return "Moderate Delay"
        else:
            return "Healthy"

    live_df["RiskBand"] = live_df["AgeDays"].apply(risk_band)

    risk_df = live_df["RiskBand"].value_counts().reindex(
        ["Healthy", "Moderate Delay", "Critical Delay"]
    ).fillna(0)

    with r5:
        fig5 = px.bar(
            x=risk_df.index,
            y=risk_df.values,
            title="Schedule Risk / Delay Trend"
        )
        fig5.update_layout(height=380, margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig5, use_container_width=True)

    bottleneck_labels = ["Measurement", "Cutting List", "Production", "Pre Assembly", "Polishing", "Final Assembly", "Dispatch"]
    bottleneck_vals = [
        len(live_df[live_df["Stage"] == "Not Started"]),
        len(live_df[live_df["Stage"] == "Cutting List"]),
        len(live_df[live_df["Stage"] == "Production"]),
        len(live_df[live_df["Stage"] == "Pre Assembly"]),
        len(live_df[live_df["Stage"] == "Polishing"]),
        len(live_df[live_df["Stage"] == "Final Assembly"]),
        len(live_df[live_df["Stage"] == "Dispatch"])
    ]

    with r6:
        fig6 = go.Figure(go.Funnel(y=bottleneck_labels, x=bottleneck_vals))
        fig6.update_layout(title="Stage Bottleneck Waterfall", height=380, margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig6, use_container_width=True)

    st.markdown("---")

    # =========================================================
    # ROW 5 - DISPATCH READINESS + TOP STUCK UNITS
    # =========================================================
    r7, r8 = st.columns(2)

    near_dispatch = live_df[live_df["Stage"].isin(["Polishing", "Final Assembly"])]

    if not near_dispatch.empty:
        dispatch_ready = near_dispatch.groupby("House").size().reset_index(name="NearDispatchProducts")
        dispatch_ready = dispatch_ready.sort_values("NearDispatchProducts", ascending=False).head(10)

        with r7:
            fig7 = px.bar(
                dispatch_ready,
                x="House",
                y="NearDispatchProducts",
                title="Near Dispatch Readiness Houses"
            )
            fig7.update_layout(height=360, margin=dict(l=10, r=10, t=40, b=10))
            st.plotly_chart(fig7, use_container_width=True)
    else:
        with r7:
            st.info("No near dispatch houses.")

    stuck_units = (
        live_df[live_df["RiskBand"] == "Critical Delay"]
        .groupby("Unit")
        .size()
        .reset_index(name="CriticalProducts")
        .sort_values("CriticalProducts", ascending=False)
        .head(10)
    )

    if not stuck_units.empty:
        with r8:
            fig8 = px.bar(
                stuck_units,
                x="CriticalProducts",
                y="Unit",
                orientation="h",
                title="Top Stuck Units"
            )
            fig8.update_layout(height=360, margin=dict(l=10, r=10, t=40, b=10))
            st.plotly_chart(fig8, use_container_width=True)
    else:
        with r8:
            st.info("No critical stuck units.")

    st.markdown("---")

    # =========================================================
    # BOTTOM KPI STRIP
    # =========================================================
    b1, b2, b3, b4 = st.columns(4)

    highest_stage = stage_counts.idxmax()
    highest_stage_count = int(stage_counts.max())

    high_risk_products = len(live_df[live_df["RiskBand"] == "Critical Delay"])
    near_dispatch_count = len(near_dispatch)
    closure_houses = completed_houses

    b1.metric("Highest Pressure Stage", f"{highest_stage} ({highest_stage_count})")
    b2.metric("Critical Delayed Products", high_risk_products)
    b3.metric("Near Dispatch Products", near_dispatch_count)
    b4.metric("Fully Closed Houses", closure_houses)
