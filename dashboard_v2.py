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
            WHEN lt.stage_name IS NULL THEN 'Not Started'
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

    stage_rank = {
        "Not Started": 0,
        "Cutting List": 1,
        "Production": 2,
        "Pre Assembly": 3,
        "Polishing": 4,
        "Final Assembly": 5,
        "Dispatch": 6,
        "Completed": 7
    }

    real_product_df["StageRank"] = real_product_df["Stage"].map(stage_rank).fillna(0)

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

    total_products = len(real_product_df)
    active_products_total = len(real_product_df[(real_product_df["Stage"] != "Completed") & (real_product_df["Stage"] != "Not Started")])
    pending_products_total = len(real_product_df[real_product_df["Stage"] == "Not Started"])
    ready_for_dispatch = len(real_product_df[real_product_df["Stage"] == "Dispatch"])
    total_dispatched_products = len(real_product_df[real_product_df["Stage"] == "Completed"])

    st.markdown("### Summary Of Total Units")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🏠 Total Houses", total_houses)
    c2.metric("✅ Completed", completed_houses)
    c3.metric("🟡 WIP", wip_houses)
    c4.metric("🔴 Yet Start", yet_start_houses)

    st.markdown("")
    st.markdown("### Summary Of Total Products")
    p1, p2, p3, p4, p5 = st.columns(5)
    p1.metric("📦 Total Products", total_products)
    p2.metric("🏭 Active Products", active_products_total)
    p3.metric("⌛ Pending Products", pending_products_total)
    p4.metric("🚚 Ready For Dispatch", ready_for_dispatch)
    p5.metric("✅ Total Dispatched", total_dispatched_products)

    st.markdown("---")

    st.markdown("### Stage wise bottleneck")

    yet_to_start_count = len(real_product_df[real_product_df["StageRank"] == 0])
    measurement_count = total_products
    cutting_count = len(real_product_df[real_product_df["StageRank"] <= 1])
    production_count = len(real_product_df[real_product_df["StageRank"] <= 2])
    preassembly_count = len(real_product_df[real_product_df["StageRank"] <= 3])
    polishing_count = len(real_product_df[real_product_df["StageRank"] <= 4])
    finalassembly_count = len(real_product_df[real_product_df["StageRank"] <= 5])
    dispatch_count = len(real_product_df[real_product_df["StageRank"] <= 6])
    completed_count = len(real_product_df[real_product_df["StageRank"] == 7])

    chart_df = pd.DataFrame({
        "Stage": [
            "Yet To Start",
            "Measurement",
            "Cutting List",
            "Production",
            "Pre Assembly",
            "Polishing",
            "Final Assembly",
            "Dispatch",
            "Completed"
        ],
        "Products": [
            yet_to_start_count,
            measurement_count,
            cutting_count,
            production_count,
            preassembly_count,
            polishing_count,
            finalassembly_count,
            dispatch_count,
            completed_count
        ]
    })

    fig = px.bar(chart_df, x="Stage", y="Products", text="Products", height=360)
    fig.update_traces(textposition='outside')
    fig.update_layout(
        margin=dict(l=10, r=10, t=20, b=20),
        xaxis_title="",
        yaxis_title="Products",
        uniformtext_minsize=10,
        uniformtext_mode='hide'
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("### 📋 Active Project Summary")

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
        proj_pending_products = len(grp[(grp["Product"] != "NO PRODUCT") & (grp["Stage"] != "Completed")])

        project_total_possible_progress = proj_total_products * 7
        project_achieved_progress = grp[grp["Product"] != "NO PRODUCT"]["Stage"].map(stage_rank).fillna(0).sum()

        proj_overall_completion = round(
            (project_achieved_progress / project_total_possible_progress) * 100, 2
        ) if project_total_possible_progress > 0 else 0

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

    st.dataframe(proj_df, use_container_width=True, height=220)

    st.markdown("---")

    st.markdown("### ⚠ Critical Alerts")

    max_pending_project = proj_df.sort_values("Pending Products", ascending=False).iloc[0]["Project"]

    a1, a2, a3, a4 = st.columns(4)
    a1.metric("Highest Pressure Stage", f"Yet To Start ({yet_to_start_count})")
    a2.metric("Yet To Start Houses", yet_start_houses)
    a3.metric("Max Pending Project", max_pending_project)
    a4.metric("Ready For Dispatch", ready_for_dispatch)
