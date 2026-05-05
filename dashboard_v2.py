def show_dashboard_v2(conn, cur):
    import streamlit as st
    import pandas as pd
    import plotly.express as px

    st.title("📌 Executive Factory Dashboard")

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

    query = """
    WITH latest_tracking AS (
        SELECT
            t.product_instance_id,
            s.stage_name,
            t.status,
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
            WHEN lt.stage_name IS NULL THEN 'Yet To Start'
            ELSE lt.stage_name
        END AS current_stage

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
        "Project", "Unit", "House", "HouseID", "ProductInstance", "Product", "Stage"
    ])

    real_product_df = df[df["Product"] != "NO PRODUCT"].copy()

    # ================= HOUSE SUMMARY =================
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
        yts_products = len(actual_grp[actual_grp["Stage"] == "Yet To Start"])

        if completed_products == total_house_products:
            completed_houses += 1
        elif yts_products == total_house_products:
            yet_start_houses += 1
        else:
            wip_houses += 1

    # ================= PRODUCT SUMMARY =================
    total_products = len(real_product_df)
    active_products_total = len(real_product_df[
        ~real_product_df["Stage"].isin(["Yet To Start", "Completed"])
    ])
    pending_products_total = len(real_product_df[real_product_df["Stage"] == "Yet To Start"])
    ready_for_dispatch = len(real_product_df[real_product_df["Stage"] == "Dispatch"])
    total_dispatched_products = len(real_product_df[real_product_df["Stage"] == "Completed"])

    # ================= SUMMARY OF TOTAL UNITS =================
    st.markdown("### Summary Of Total Units")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🏠 Total Houses", total_houses)
    c2.metric("✅ Completed", completed_houses)
    c3.metric("🟡 WIP", wip_houses)
    c4.metric("🔴 Yet Start", yet_start_houses)

    st.markdown("")

    # ================= SUMMARY OF TOTAL PRODUCTS =================
    st.markdown("### Summary Of Total Products")
    p1, p2, p3, p4, p5 = st.columns(5)
    p1.metric("📦 Total Products", total_products)
    p2.metric("🏭 Running Products", active_products_total)
    p3.metric("⌛ Pending Products", pending_products_total)
    p4.metric("🚚 Ready For Dispatch", ready_for_dispatch)
    p5.metric("✅ Total Dispatched Products", total_dispatched_products)

    st.markdown("---")

    # ================= STAGE WISE BOTTLENECK =================
    st.markdown("### Stage wise bottleneck")

    stage_order = [
        "Yet To Start",
        "Measurement",
        "Cutting List",
        "Production",
        "Pre Assembly",
        "Polishing",
        "Final Assembly",
        "Dispatch",
        "Completed"
    ]

    stage_counts = real_product_df["Stage"].value_counts().reindex(stage_order, fill_value=0)

    chart_df = pd.DataFrame({
        "Stage": stage_counts.index,
        "Products": stage_counts.values
    })

    fig = px.bar(chart_df, x="Stage", y="Products", text="Products", height=360)
    fig.update_traces(textposition='outside')
    fig.update_layout(
        margin=dict(l=10, r=10, t=20, b=20),
        xaxis_title="",
        yaxis_title="Products"
    )
    st.plotly_chart(fig, use_container_width=True)

    # ================= ACTIVE PROJECT SUMMARY =================
    st.markdown("### 📋 Active Project Summary")

    project_rows = []

    stage_score = {
        "Yet To Start": 0,
        "Measurement": 1,
        "Cutting List": 2,
        "Production": 3,
        "Pre Assembly": 4,
        "Polishing": 5,
        "Final Assembly": 6,
        "Dispatch": 7,
        "Completed": 8
    }

    for project, grp in df.groupby("Project"):

        proj_total_houses = grp["HouseID"].nunique()
        proj_started_houses = 0
        proj_yetstart_houses = 0

        for house_id, hgrp in grp.groupby("HouseID"):
            actual_h = hgrp[hgrp["Product"] != "NO PRODUCT"]

            if len(actual_h) == 0:
                proj_yetstart_houses += 1
                continue

            if all(actual_h["Stage"] == "Yet To Start"):
                proj_yetstart_houses += 1
            else:
                proj_started_houses += 1

        proj_total_products = len(grp[grp["Product"] != "NO PRODUCT"])
        proj_pending_products = len(grp[(grp["Product"] != "NO PRODUCT") & (grp["Stage"] != "Completed")])
        proj_dispatched_products = len(grp[(grp["Product"] != "NO PRODUCT") & (grp["Stage"] == "Completed")])

        achieved = grp[grp["Product"] != "NO PRODUCT"]["Stage"].map(stage_score).fillna(0).sum()
        total_possible = proj_total_products * 8

        proj_overall_completion = round((achieved / total_possible) * 100, 2) if total_possible > 0 else 0

        project_rows.append([
            project,
            proj_total_houses,
            proj_started_houses,
            proj_yetstart_houses,
            proj_pending_products,
            proj_dispatched_products,
            f"{proj_overall_completion}%"
        ])

    proj_df = pd.DataFrame(project_rows, columns=[
        "Project", "Total Houses", "Started Houses", "Yet Start Houses",
        "Pending Products", "Total Dispatched Products", "Overall Completion %"
    ])

    st.dataframe(proj_df, use_container_width=True, height=220)

    st.markdown("---")

    # ================= CRITICAL ALERTS =================
    st.markdown("### ⚠ Critical Alerts")

    highest_pending_stage = stage_counts.drop("Completed").idxmax()
    highest_pending_count = stage_counts.drop("Completed").max()
    max_pending_project = proj_df.sort_values("Pending Products", ascending=False).iloc[0]["Project"]

    a1, a2, a3, a4 = st.columns(4)
    a1.metric("Highest Pressure Stage", f"{highest_pending_stage} ({highest_pending_count})")
    a2.metric("Yet To Start Houses", yet_start_houses)
    a3.metric("Max Pending Project", max_pending_project)
    a4.metric("Ready For Dispatch", ready_for_dispatch)
