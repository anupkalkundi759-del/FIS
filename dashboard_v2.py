def show_dashboard_v2(conn, cur):
    import streamlit as st
    import pandas as pd

    st.title("📌 Executive Factory Dashboard")

    # ================= MASTER HOUSE COUNT =================
    cur.execute("""
        SELECT pr.project_name, u.unit_name, h.house_no, h.house_id
        FROM houses h
        JOIN units u ON h.unit_id = u.unit_id
        JOIN projects pr ON u.project_id = pr.project_id
        ORDER BY pr.project_name, u.unit_name, h.house_no
    """)
    house_master_rows = cur.fetchall()

    house_master_df = pd.DataFrame(house_master_rows, columns=[
        "Project", "Unit", "House", "HouseID"
    ])

    total_houses = len(house_master_df)

    # ================= MASTER LIVE PRODUCT QUERY =================
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
        h.house_id,
        p.product_instance_id,
        COALESCE(pm.product_code,'NO PRODUCT') AS product_code,

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

    FROM houses h
    JOIN units u ON h.unit_id = u.unit_id
    JOIN projects pr ON u.project_id = pr.project_id
    LEFT JOIN products p ON h.house_id = p.house_id
    LEFT JOIN products_master pm ON p.product_id = pm.product_id
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
        "Project", "Unit", "House", "HouseID", "ProductInstance",
        "Product", "Stage", "Status", "Timestamp"
    ])

    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")

    # remove fake NO PRODUCT from product totals where needed
    real_product_df = df[df["Product"] != "NO PRODUCT"].copy()

    # ================= HOUSE KPI CLASSIFICATION =================
    completed_houses = 0
    wip_houses = 0
    pending_houses = 0
    yet_start_houses = 0

    dispatch_today = 0
    dispatch_week = 0
    dispatch_month = 0

    now = pd.Timestamp.now()

    for house, grp in df.groupby("House"):

        actual_grp = grp[grp["Product"] != "NO PRODUCT"]

        total_house_products = len(actual_grp)

        if total_house_products == 0:
            yet_start_houses += 1
            continue

        completed_products = len(actual_grp[actual_grp["Stage"] == "Completed"])
        not_started_products = len(actual_grp[actual_grp["Stage"] == "Not Started"])
        active_products = len(actual_grp[
            (actual_grp["Stage"] != "Completed") &
            (actual_grp["Stage"] != "Not Started")
        ])

        if completed_products == total_house_products:
            completed_houses += 1

            latest_close = actual_grp["Timestamp"].max()

            if pd.notna(latest_close):
                if latest_close.date() == now.date():
                    dispatch_today += 1
                if latest_close >= now - pd.Timedelta(days=7):
                    dispatch_week += 1
                if latest_close >= now - pd.Timedelta(days=30):
                    dispatch_month += 1

        elif not_started_products == total_house_products:
            yet_start_houses += 1

        elif not_started_products > 0 and (completed_products > 0 or active_products > 0):
            pending_houses += 1

        else:
            wip_houses += 1

    # ================= KPI ROW 1 =================
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("🏠 Total Houses", total_houses)
    c2.metric("✅ Completed", completed_houses)
    c3.metric("🟡 WIP", wip_houses)
    c4.metric("🟠 Pending", pending_houses)
    c5.metric("🔴 Yet Start", yet_start_houses)

    st.markdown("---")

    # ================= KPI ROW 2 =================
    d1, d2, d3 = st.columns(3)
    d1.metric("🚚 Dispatch Today", dispatch_today)
    d2.metric("🚚 Dispatch Week", dispatch_week)
    d3.metric("🚚 Dispatch Month", dispatch_month)

    st.markdown("---")

    # ================= KPI ROW 3 PRODUCT =================
    total_products = len(real_product_df)
    active_products_total = len(real_product_df[
        (real_product_df["Stage"] != "Completed") &
        (real_product_df["Stage"] != "Not Started")
    ])
    pending_products_total = len(real_product_df[real_product_df["Stage"] == "Not Started"])

    p1, p2, p3 = st.columns(3)
    p1.metric("📦 Total Products", total_products)
    p2.metric("🏭 Active Products", active_products_total)
    p3.metric("⌛ Pending Products", pending_products_total)

    st.markdown("---")

    # ================= STAGE WISE BOTTLENECK =================
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

    stage_counts = real_product_df["Stage"].value_counts().reindex(stage_order, fill_value=0)

    bottleneck_df = pd.DataFrame({
        "Stage": stage_counts.index,
        "Products": stage_counts.values
    })

    st.bar_chart(bottleneck_df.set_index("Stage"))

    st.markdown("---")

    # ================= UNIT STATUS KPI =================
    total_units = house_master_df["Unit"].nunique()
    started_units = 0
    pending_units = 0
    completed_units = 0

    for unit, grp in df.groupby("Unit"):

        houses_in_unit = grp["House"].nunique()
        completed_in_unit = 0
        notstart_in_unit = 0

        for house, hgrp in grp.groupby("House"):
            actual_h = hgrp[hgrp["Product"] != "NO PRODUCT"]

            if len(actual_h) == 0:
                notstart_in_unit += 1
                continue

            if all(actual_h["Stage"] == "Completed"):
                completed_in_unit += 1
            elif all(actual_h["Stage"] == "Not Started"):
                notstart_in_unit += 1

        if completed_in_unit == houses_in_unit:
            completed_units += 1
        elif notstart_in_unit == houses_in_unit:
            pending_units += 1
        else:
            started_units += 1

    u1, u2, u3, u4 = st.columns(4)
    u1.metric("🏗 Total Units", total_units)
    u2.metric("▶ Started Units", started_units)
    u3.metric("⌛ Pending Units", pending_units)
    u4.metric("✅ Completed Units", completed_units)

    st.markdown("---")

    # ================= ACTIVE PROJECT SUMMARY =================
    st.subheader("📋 Active Project Summary")

    project_rows = []

    for project, grp in df.groupby("Project"):

        proj_total_houses = grp["House"].nunique()
        proj_completed_houses = 0
        proj_pending_products = len(grp[(grp["Product"] != "NO PRODUCT") & (grp["Stage"] == "Not Started")])

        for house, hgrp in grp.groupby("House"):
            actual_h = hgrp[hgrp["Product"] != "NO PRODUCT"]
            if len(actual_h) > 0 and all(actual_h["Stage"] == "Completed"):
                proj_completed_houses += 1

        proj_wip = proj_total_houses - proj_completed_houses
        proj_dispatch_pct = round((proj_completed_houses / proj_total_houses) * 100, 2) if proj_total_houses > 0 else 0

        project_rows.append([
            project,
            proj_total_houses,
            proj_wip,
            proj_pending_products,
            f"{proj_dispatch_pct}%"
        ])

    proj_df = pd.DataFrame(project_rows, columns=[
        "Project", "Total Houses", "WIP Houses", "Pending Products", "Dispatch %"
    ])

    st.dataframe(proj_df, use_container_width=True, height=250)

    st.markdown("---")

    # ================= CRITICAL ALERTS =================
    st.subheader("⚠ Critical Alerts")

    highest_pending_stage = stage_counts.drop("Completed").idxmax()
    highest_pending_count = stage_counts.drop("Completed").max()

    max_pending_project = proj_df.sort_values("Pending Products", ascending=False).iloc[0]["Project"]

    near_dispatch_products = len(real_product_df[real_product_df["Stage"] == "Dispatch"])

    a1, a2, a3, a4 = st.columns(4)
    a1.metric("Highest Pressure Stage", f"{highest_pending_stage} ({highest_pending_count})")
    a2.metric("Yet To Start Houses", yet_start_houses)
    a3.metric("Max Pending Project", max_pending_project)
    a4.metric("Near Dispatch Products", near_dispatch_products)
