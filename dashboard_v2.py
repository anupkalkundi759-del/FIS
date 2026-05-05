def show_dashboard_v2(conn, cur):
    import streamlit as st
    import pandas as pd
    from datetime import datetime

    st.title("📌 Executive Factory Dashboard")

    # ================= MASTER HOUSE DATA =================
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

    house_master_df["UnitKey"] = house_master_df["Project"].astype(str) + "_" + house_master_df["Unit"].astype(str)

    total_houses = len(house_master_df)

    # ================= MASTER LIVE PRODUCT QUERY =================
    query = """
    WITH latest_tracking AS (
        SELECT
            t.product_instance_id,
            s.stage_name,
            t.status,
            t.timestamp,
            DATE(t.timestamp) as track_date,
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

        lt.timestamp,
        lt.track_date

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
        "Product", "Stage", "Status", "Timestamp", "TrackDate"
    ])

    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    df["UnitKey"] = df["Project"].astype(str) + "_" + df["Unit"].astype(str)

    real_product_df = df[df["Product"] != "NO PRODUCT"].copy()

    # ================= HOUSE KPI CLASSIFICATION =================
    completed_houses = 0
    wip_houses = 0
    yet_start_houses = 0

    for house_id, grp in df.groupby("HouseID"):

        actual_grp = grp[grp["Product"] != "NO PRODUCT"]
        total_house_products = len(actual_grp)

        if total_house_products == 0:
            yet_start_houses += 1
            continue

        completed_products = len(actual_grp[actual_grp["Stage"] == "Completed"])
        not_started_products = len(actual_grp[actual_grp["Stage"] == "Not Started"])

        if completed_products == total_house_products:
            completed_houses += 1

        elif not_started_products == total_house_products:
            yet_start_houses += 1

        else:
            wip_houses += 1

    # ================= KPI ROW 1 HOUSE STATUS =================
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🏠 Total Houses", total_houses)
    c2.metric("✅ Completed", completed_houses)
    c3.metric("🟡 WIP", wip_houses)
    c4.metric("🔴 Yet Start", yet_start_houses)

    st.markdown("---")

    # ================= KPI ROW 2 DISPATCH =================
    today_date = datetime.now().date()

    dispatch_completed_df = real_product_df[real_product_df["Stage"] == "Completed"].copy()

    total_dispatch = len(dispatch_completed_df)

    dispatch_today = len(dispatch_completed_df[
        pd.to_datetime(dispatch_completed_df["TrackDate"], errors="coerce").dt.date == today_date
    ])

    d1, d2 = st.columns(2)
    d1.metric("🚚 Total Dispatch", total_dispatch)
    d2.metric("🚚 Dispatch Today", dispatch_today)

    st.markdown("---")

    # ================= KPI ROW 3 PRODUCT STATUS =================
    total_products = len(real_product_df)

    active_products_total = len(real_product_df[
        (real_product_df["Stage"] != "Completed") &
        (real_product_df["Stage"] != "Not Started")
    ])

    pending_products_total = len(real_product_df[
        real_product_df["Stage"] == "Not Started"
    ])

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
    total_units = house_master_df["UnitKey"].nunique()
    active_units = 0
    yetstart_units = 0
    completed_units = 0

    for unitkey, grp in df.groupby("UnitKey"):

        houses_in_unit = grp["HouseID"].nunique()
        completed_in_unit = 0
        yetstart_in_unit = 0

        for house_id, hgrp in grp.groupby("HouseID"):
            actual_h = hgrp[hgrp["Product"] != "NO PRODUCT"]

            if len(actual_h) == 0:
                yetstart_in_unit += 1
                continue

            if all(actual_h["Stage"] == "Completed"):
                completed_in_unit += 1
            elif all(actual_h["Stage"] == "Not Started"):
                yetstart_in_unit += 1

        if completed_in_unit == houses_in_unit:
            completed_units += 1
        elif yetstart_in_unit == houses_in_unit:
            yetstart_units += 1
        else:
            active_units += 1

    u1, u2, u3, u4 = st.columns(4)
    u1.metric("🏗 Total Units", total_units)
    u2.metric("▶ Active Units", active_units)
    u3.metric("⌛ Yet Start Units", yetstart_units)
    u4.metric("✅ Completed Units", completed_units)

    st.markdown("---")

    # ================= ACTIVE PROJECT SUMMARY =================
    st.subheader("📋 Active Project Summary")

    project_rows = []

    for project, grp in df.groupby("Project"):

        proj_total_houses = grp["HouseID"].nunique()
        proj_started_houses = 0
        proj_yetstart_houses = 0

        for house_id, hgrp in grp.groupby("HouseID"):
            actual_h = hgrp[hgrp["Product"] != "NO PRODUCT"]

            if len(actual_h) == 0:
                proj_yetstart_houses += 1
                continue

            if all(actual_h["Stage"] == "Not Started"):
                proj_yetstart_houses += 1
            else:
                proj_started_houses += 1

        proj_total_products = len(grp[grp["Product"] != "NO PRODUCT"])
        proj_completed_products = len(grp[
            (grp["Product"] != "NO PRODUCT") &
            (grp["Stage"] == "Completed")
        ])

        proj_pending_products = proj_total_products - proj_completed_products

        house_component = ((proj_total_houses - proj_yetstart_houses) / proj_total_houses) * 50 if proj_total_houses > 0 else 0
        product_component = (proj_completed_products / proj_total_products) * 50 if proj_total_products > 0 else 0
        proj_overall_completion = round(house_component + product_component, 2)

        project_rows.append([
            project,
            proj_total_houses,
            proj_started_houses,
            proj_yetstart_houses,
            proj_pending_products,
            f"{proj_overall_completion}%"
        ])

    proj_df = pd.DataFrame(project_rows, columns=[
        "Project", "Total Houses", "Started Houses", "Yet Start Houses", "Pending Products", "Overall Completion %"
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
