def show_dashboard_v2(conn, cur):
    import streamlit as st
    import pandas as pd
    import plotly.express as px

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

    # ================= STATUS AWARE LATEST TRACKING =================
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
            WHEN lt.stage_name IS NULL THEN 'Yet To Start'
            WHEN lt.stage_name = 'Dispatch' AND lt.status = 'Completed' THEN 'Completed'
            ELSE lt.stage_name
        END AS current_stage,

        COALESCE(lt.status,'Not Started') AS latest_status

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
        "Project", "Unit", "House", "HouseID",
        "ProductInstance", "Product", "Stage", "Status"
    ])

    real_product_df = df[df["Product"] != "NO PRODUCT"].copy()

    # ================= HOUSE SUMMARY =================
    completed_houses = 0
    wip_houses = 0
    yet_start_houses = 0

    for house_id, grp in df.groupby("HouseID"):
        actual_grp = grp[grp["Product"] != "NO PRODUCT"]

        if len(actual_grp) == 0:
            yet_start_houses += 1
            continue

        if all(actual_grp["Stage"] == "Completed"):
            completed_houses += 1
        elif all(actual_grp["Stage"] == "Yet To Start"):
            yet_start_houses += 1
        else:
            wip_houses += 1

    # ================= PRODUCT SUMMARY =================
    total_products = len(real_product_df)

    running_products_df = real_product_df[
        (real_product_df["Status"] == "In Progress") &
        (~real_product_df["Stage"].isin(["Yet To Start", "Completed"]))
    ]
    active_products_total = len(running_products_df)

    pending_products_total = len(real_product_df[real_product_df["Stage"] == "Yet To Start"])

    ready_for_dispatch = len(real_product_df[
        (real_product_df["Stage"] == "Dispatch") &
        (real_product_df["Status"] == "In Progress")
    ])

    total_dispatched_products = len(real_product_df[real_product_df["Stage"] == "Completed"])

    # ================= SUMMARY OF TOTAL UNITS =================
    st.markdown("### Summary Of Total Units")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🏠 Total Units", total_houses)
    c2.metric("✅ Fully Completed Units", completed_houses)
    c3.metric("🟡 Units At WIP", wip_houses)
    c4.metric("🔴 Units Yet To Start", yet_start_houses)

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

    stage_counts = {
        "Yet To Start": len(real_product_df[real_product_df["Stage"] == "Yet To Start"]),
        "Measurement": len(real_product_df[(real_product_df["Stage"] == "Measurement") & (real_product_df["Status"] == "In Progress")]),
        "Cutting List": len(real_product_df[(real_product_df["Stage"] == "Cutting List") & (real_product_df["Status"] == "In Progress")]),
        "Production": len(real_product_df[(real_product_df["Stage"] == "Production") & (real_product_df["Status"] == "In Progress")]),
        "Pre Assembly": len(real_product_df[(real_product_df["Stage"] == "Pre Assembly") & (real_product_df["Status"] == "In Progress")]),
        "Polishing": len(real_product_df[(real_product_df["Stage"] == "Polishing") & (real_product_df["Status"] == "In Progress")]),
        "Final Assembly": len(real_product_df[(real_product_df["Stage"] == "Final Assembly") & (real_product_df["Status"] == "In Progress")]),
        "Dispatch": len(real_product_df[(real_product_df["Stage"] == "Dispatch") & (real_product_df["Status"] == "In Progress")]),
        "Completed": len(real_product_df[real_product_df["Stage"] == "Completed"])
    }

    chart_df = pd.DataFrame({
        "Stage": list(stage_counts.keys()),
        "Products": list(stage_counts.values())
    })

    fig = px.bar(chart_df, x="Stage", y="Products", text="Products", height=360)
    fig.update_traces(textposition="outside")
    fig.update_layout(
        margin=dict(l=10, r=10, t=20, b=20),
        xaxis_title="",
        yaxis_title="Products"
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("### 📋 Active Project Summary")

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

            if all(actual_h["Stage"] == "Yet To Start"):
                proj_yetstart_houses += 1
            else:
                proj_started_houses += 1

        proj_pending_products = len(grp[(grp["Product"] != "NO PRODUCT") & (grp["Stage"] != "Completed")])
        proj_dispatched_products = len(grp[(grp["Product"] != "NO PRODUCT") & (grp["Stage"] == "Completed")])

        proj_real = grp[grp["Product"] != "NO PRODUCT"]
        achieved = proj_real["Stage"].map(stage_score).fillna(0).sum()
        total_possible = len(proj_real) * 8
        proj_overall_completion = round((achieved / total_possible) * 100, 2) if total_possible > 0 else 0

        running_proj = len(proj_real[
            (proj_real["Status"] == "In Progress") &
            (~proj_real["Stage"].isin(["Yet To Start", "Completed"]))
        ])

        project_rows.append([
            project,
            proj_total_houses,
            proj_started_houses,
            proj_yetstart_houses,
            proj_pending_products,
            proj_dispatched_products,
            running_proj,
            f"{proj_overall_completion}%"
        ])

    proj_df = pd.DataFrame(project_rows, columns=[
        "Project",
        "Total Houses",
        "Started Houses",
        "Yet Start Houses",
        "Pending Products",
        "Total Dispatched Products",
        "Running Products",
        "Overall Completion %"
    ])

    st.dataframe(proj_df, use_container_width=True, height=220)

    st.markdown("---")

    # ================= CRITICAL ALERTS =================
    st.markdown("### ⚠ Critical Alerts")

    highest_pressure_stage = max(stage_counts, key=stage_counts.get)
    highest_pressure_count = stage_counts[highest_pressure_stage]

    max_backlog_project = proj_df.sort_values("Pending Products", ascending=False).iloc[0]["Project"]
    max_running_project = proj_df.sort_values("Running Products", ascending=False).iloc[0]["Project"]

    factory_achieved = real_product_df["Stage"].map(stage_score).fillna(0).sum()
    factory_total_possible = len(real_product_df) * 8
    overall_factory_completion = round((factory_achieved / factory_total_possible) * 100, 2) if factory_total_possible > 0 else 0

    a1, a2, a3, a4 = st.columns(4)
    a1.metric("Highest Pressure Stage", f"{highest_pressure_stage} ({highest_pressure_count})")
    a2.metric("Max Backlog Project", max_backlog_project)
    a3.metric("Max Running Project", max_running_project)
    a4.metric("Overall Factory Completion", f"{overall_factory_completion}%")
