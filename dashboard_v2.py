def show_dashboard_v2(conn, cur):
    import streamlit as st
    import pandas as pd
    from datetime import datetime, timedelta

    st.title("📌 Executive Factory Dashboard")

    # ====================== MASTER LIVE QUERY ======================
    query = """
    WITH latest_tracking AS (
        SELECT
            t.product_instance_id,
            s.stage_name,
            t.status,
            t.timestamp,
            ROW_NUMBER() OVER (
                PARTITION BY t.product_instance_id
                ORDER BY t.timestamp DESC
            ) AS rn
        FROM tracking_log t
        JOIN stages s ON t.stage_id = s.stage_id
    )

    SELECT
        pr.project_name,
        u.unit_name,
        h.house_no,
        p.product_instance_id,
        pm.product_code,

        CASE
            WHEN lt.stage_name = 'Dispatch' AND lt.status = 'Completed' THEN 'Completed'
            WHEN lt.stage_name IS NULL THEN 'Not Started'
            ELSE lt.stage_name
        END AS current_stage,

        CASE
            WHEN lt.stage_name IS NULL THEN 'Not Started'
            ELSE lt.status
        END AS current_status,

        lt.timestamp

    FROM products p
    JOIN products_master pm ON p.product_id = pm.product_id
    JOIN houses h ON p.house_id = h.house_id
    JOIN units u ON h.unit_id = u.unit_id
    JOIN projects pr ON u.project_id = pr.project_id
    LEFT JOIN latest_tracking lt
        ON lt.product_instance_id = p.product_instance_id
        AND lt.rn = 1
    ORDER BY pr.project_name, u.unit_name, h.house_no
    """

    cur.execute(query)
    rows = cur.fetchall()

    if not rows:
        st.warning("No production data found")
        return

    df = pd.DataFrame(rows, columns=[
        "Project", "Unit", "House", "ProductInstance", "Product",
        "Stage", "Status", "Timestamp"
    ])

    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")

    # ====================== KPI ROW 1 HOUSE STATUS ======================
    total_houses = df["House"].astype(str).nunique()

    house_summary = df.groupby("House")["Stage"].apply(list)

    completed_houses = 0
    wip_houses = 0
    pending_houses = 0
    yet_start_houses = 0

    for house, stages in house_summary.items():

        if all(str(x) == "Completed" for x in stages):
            completed_houses += 1

        elif all(str(x) == "Not Started" for x in stages):
            yet_start_houses += 1

        elif "Not Started" in stages and len(set(stages)) == 1:
            pending_houses += 1

        else:
            wip_houses += 1

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("🏠 Total Houses", total_houses)
    c2.metric("✅ Completed", completed_houses)
    c3.metric("🟡 WIP", wip_houses)
    c4.metric("🟠 Pending", pending_houses)
    c5.metric("🔴 Yet Start", yet_start_houses)

    st.markdown("---")

    # ====================== KPI ROW 2 DISPATCH MOVEMENT ======================
    now = pd.Timestamp.now()

    today_dispatch = len(df[
        (df["Stage"] == "Completed") &
        (df["Timestamp"].dt.date == now.date())
    ]["House"].unique())

    week_dispatch = len(df[
        (df["Stage"] == "Completed") &
        (df["Timestamp"] >= now - pd.Timedelta(days=7))
    ]["House"].unique())

    month_dispatch = len(df[
        (df["Stage"] == "Completed") &
        (df["Timestamp"] >= now - pd.Timedelta(days=30))
    ]["House"].unique())

    d1, d2, d3 = st.columns(3)
    d1.metric("🚚 Dispatch Today", today_dispatch)
    d2.metric("🚚 Dispatch Week", week_dispatch)
    d3.metric("🚚 Dispatch Month", month_dispatch)

    st.markdown("---")

    # ====================== KPI ROW 3 PRODUCT STATUS ======================
    total_products = len(df)
    active_products = len(df[df["Stage"] != "Completed"])
    pending_products = len(df[df["Stage"] == "Not Started"])

    p1, p2, p3 = st.columns(3)
    p1.metric("📦 Total Products", total_products)
    p2.metric("🏭 Active Products", active_products)
    p3.metric("⌛ Pending Products", pending_products)

    st.markdown("---")

    # ====================== STAGE WISE BOTTLENECK ======================
    st.subheader("🏭 Stage Wise Bottleneck")

    stage_order = [
        "Not Started",
        "Cutting List",
        "Production",
        "Pre Assembly",
        "Polishing",
        "Final Assembly",
        "Dispatch",
        "Completed"
    ]

    stage_counts = df["Stage"].value_counts().reindex(stage_order, fill_value=0)

    bottleneck_df = pd.DataFrame({
        "Stage": stage_counts.index,
        "Products": stage_counts.values
    })

    st.bar_chart(bottleneck_df.set_index("Stage"))

    st.markdown("---")

    # ====================== UNIT STATUS KPI ======================
    total_units = df["Unit"].nunique()

    started_units = df[df["Stage"] != "Not Started"]["Unit"].nunique()
    pending_units = df[df["Stage"] == "Not Started"]["Unit"].nunique()

    completed_units = 0

    for unit, grp in df.groupby("Unit"):
        if all(grp["Stage"] == "Completed"):
            completed_units += 1

    u1, u2, u3, u4 = st.columns(4)
    u1.metric("🏗 Total Units", total_units)
    u2.metric("▶ Started Units", started_units)
    u3.metric("⌛ Pending Units", pending_units)
    u4.metric("✅ Completed Units", completed_units)

    st.markdown("---")

    # ====================== ACTIVE PROJECT SUMMARY ======================
    st.subheader("📋 Active Project Summary")

    project_rows = []

    for project, grp in df.groupby("Project"):
        proj_houses = grp["House"].nunique()
        proj_wip = grp[grp["Stage"] != "Completed"]["House"].nunique()
        proj_pending_products = len(grp[grp["Stage"] == "Not Started"])
        proj_dispatch = round((len(grp[grp["Stage"] == "Completed"]) / len(grp)) * 100, 2)

        project_rows.append([
            project,
            proj_houses,
            proj_wip,
            proj_pending_products,
            f"{proj_dispatch}%"
        ])

    proj_df = pd.DataFrame(project_rows, columns=[
        "Project", "Total Houses", "WIP Houses", "Pending Products", "Dispatch %"
    ])

    st.dataframe(proj_df, use_container_width=True, height=250)

    st.markdown("---")

    # ====================== CRITICAL ALERT PANEL ======================
    st.subheader("⚠ Critical Alerts")

    highest_pending_stage = stage_counts.drop("Completed").idxmax()
    highest_pending_count = stage_counts.drop("Completed").max()

    max_pending_project = proj_df.sort_values("Pending Products", ascending=False).iloc[0]["Project"]

    st.error(f"⚠ Highest Pending Stage : {highest_pending_stage} ({highest_pending_count} products)")
    st.error(f"⚠ Yet To Start Houses : {yet_start_houses}")
    st.error(f"⚠ Max Pending Project : {max_pending_project}")
    st.error(f"⚠ Dispatch Performance This Week : {week_dispatch} Houses")
    b1.metric("Highest Pressure Stage", f"{highest_stage} ({highest_stage_count})")
    b2.metric("Critical Delayed Products", high_risk_products)
    b3.metric("Near Dispatch Products", near_dispatch_count)
    b4.metric("Fully Closed Houses", closure_houses)
