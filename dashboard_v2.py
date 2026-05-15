def show_dashboard_v2(conn, cur):
    import streamlit as st
    import pandas as pd
    import plotly.express as px

    cur.execute("""
        SELECT DISTINCT quarter
        FROM products
        WHERE quarter IS NOT NULL
        ORDER BY quarter DESC
    """)
    quarters = [q[0] for q in cur.fetchall()]

    if "2026-Q2" not in quarters:
        quarters.insert(0, "2026-Q2")

    quarter_options = ["All"] + quarters

    title_col, quarter_col = st.columns([4, 1])

    with title_col:
        st.title("📌 Production Monitoring Dashboard")

    with quarter_col:
        selected_quarter = st.selectbox(
            "Quarter",
            options=quarter_options,
            index=quarter_options.index("2026-Q2") if "2026-Q2" in quarter_options else 0,
            key="dashboard_quarter"
        )

    if selected_quarter == "All":
        quarter_where = ""
        quarter_params = ()
    else:
        quarter_where = "WHERE p.quarter = %s"
        quarter_params = (selected_quarter,)

    cur.execute(f"""
        SELECT DISTINCT pr.project_name, u.unit_name, h.house_no, h.house_id
        FROM houses h
        JOIN units u ON h.unit_id = u.unit_id
        JOIN projects pr ON u.project_id = pr.project_id
        JOIN products p ON h.house_id = p.house_id
        {quarter_where}
        ORDER BY pr.project_name, u.unit_name, h.house_no
    """, quarter_params)

    house_master_rows = cur.fetchall()

    house_master_df = pd.DataFrame(house_master_rows, columns=[
        "Project", "Unit", "House", "HouseID"
    ])

    total_houses = len(house_master_df)

    cur.execute("SELECT stage_name FROM stages ORDER BY sequence")
    stage_sequence = [x[0] for x in cur.fetchall()]
    last_stage_name = stage_sequence[-1] if stage_sequence else None

    query = f"""
    WITH latest_tracking AS (
        SELECT
            t.product_instance_id,
            s.stage_name,
            t.status,
            ROW_NUMBER() OVER (
                PARTITION BY t.product_instance_id
                ORDER BY t.timestamp DESC, t.ctid DESC
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
        lt.stage_name,
        COALESCE(lt.status,'Not Started') AS latest_status

    FROM houses h
    JOIN units u ON h.unit_id = u.unit_id
    JOIN projects pr ON u.project_id = pr.project_id
    JOIN products p ON h.house_id = p.house_id
    LEFT JOIN products_master pm ON p.product_id = pm.product_id
    LEFT JOIN latest_tracking lt
        ON lt.product_instance_id = p.product_instance_id
        AND lt.rn = 1
    {quarter_where}
    ORDER BY pr.project_name, u.unit_name, h.house_no
    """

    cur.execute(query, quarter_params)
    rows = cur.fetchall()

    if not rows:
        st.warning("No production data found")
        return

    df = pd.DataFrame(rows, columns=[
        "Project", "Unit", "House", "HouseID",
        "ProductInstance", "Product", "RawStage", "Status"
    ])

    df["Stage"] = df["RawStage"].fillna("Yet To Start")
    df["Status"] = df["Status"].fillna("Not Started")

    if last_stage_name:
        df.loc[
            (df["Stage"] == last_stage_name) &
            (df["Status"] == "Completed"),
            "Stage"
        ] = "Completed"

    df["Stage"] = df["Stage"].replace("Not Started", "Yet To Start")

    real_product_df = df[df["Product"] != "NO PRODUCT"].copy()

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

    total_products = len(real_product_df)

    active_products_total = len(real_product_df[
        ~real_product_df["Stage"].isin(["Yet To Start", "Completed"])
    ])

    pending_products_total = len(real_product_df[real_product_df["Stage"] == "Yet To Start"])

    ready_for_dispatch = len(real_product_df[real_product_df["Stage"] == "Dispatch"])

    total_dispatched_products = len(real_product_df[real_product_df["Stage"] == "Completed"])

    st.markdown("### Total Houses Summary")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🏠 Total Houses", total_houses)
    c2.metric("✅ Fully Completed Houses", completed_houses)
    c3.metric("🟡 Units At WIP", wip_houses)
    c4.metric("🔴 Units Yet To Start", yet_start_houses)

    st.markdown("### Summary Of Total Products")
    p1, p2, p3, p4, p5 = st.columns(5)
    p1.metric("📦 Total Products", total_products)
    p2.metric("🏭 Running Products", active_products_total)
    p3.metric("⌛ Pending Products", pending_products_total)
    p4.metric("🚚 Ready For Dispatch", ready_for_dispatch)
    p5.metric("✅ Total Dispatched Products", total_dispatched_products)

    st.markdown("---")

    ordered_stages = [
        "Yet To Start", "Measurement", "Cutting List", "Production",
        "Pre Assembly", "Polishing", "Final Assembly", "Dispatch", "Completed"
    ]

    stage_counts = {}
    for stg in ordered_stages:
        stage_counts[stg] = len(real_product_df[real_product_df["Stage"] == stg])

    chart_df = pd.DataFrame({
        "Stage": list(stage_counts.keys()),
        "Products": list(stage_counts.values())
    })

    cur.execute("""
        SELECT COALESCE(quarter, 'No Quarter') AS quarter, COUNT(*) AS product_count
        FROM products
        GROUP BY COALESCE(quarter, 'No Quarter')
        ORDER BY quarter
    """)

    quarter_rows = cur.fetchall()

    quarter_chart_df = pd.DataFrame(
        quarter_rows,
        columns=["Quarter", "Products"]
    )

    chart_col1, chart_col2 = st.columns(2)

    stage_colors = [
        "#ef4444",
        "#f97316",
        "#f59e0b",
        "#3b82f6",
        "#8b5cf6",
        "#ec4899",
        "#14b8a6",
        "#22c55e",
        "#10b981"
    ]

    quarter_colors = [
        "#2563eb",
        "#16a34a",
        "#f59e0b",
        "#dc2626",
        "#7c3aed",
        "#0891b2"
    ]

    with chart_col1:
        st.markdown("### Stage wise bottleneck")

        fig = px.bar(
            chart_df,
            x="Stage",
            y="Products",
            text="Products",
            height=300,
            color="Stage",
            color_discrete_sequence=stage_colors
        )

        fig.update_traces(
            textposition="inside",
            textfont=dict(
                size=15,
                color="white",
                family="Arial Black"
            ),
            insidetextanchor="end",
            marker_line_width=0
        )

        fig.update_layout(
            margin=dict(l=5, r=5, t=25, b=5),
            xaxis_title="",
            yaxis_title="Products",
            showlegend=False,
            uniformtext_minsize=12,
            uniformtext_mode="show"
        )

        fig.update_xaxes(tickfont=dict(size=10))
        fig.update_yaxes(tickfont=dict(size=10))

        st.plotly_chart(
            fig,
            use_container_width=True,
            config={"displayModeBar": False}
        )

    with chart_col2:
        st.markdown("### Quarter comparison")

        qfig = px.bar(
            quarter_chart_df,
            x="Quarter",
            y="Products",
            text="Products",
            height=300,
            color="Quarter",
            color_discrete_sequence=quarter_colors
        )

        qfig.update_traces(
            textposition="inside",
            textfont=dict(
                size=16,
                color="white",
                family="Arial Black"
            ),
            insidetextanchor="end",
            marker_line_width=0
        )

        qfig.update_layout(
            margin=dict(l=5, r=5, t=25, b=5),
            xaxis_title="",
            yaxis_title="Products",
            showlegend=False,
            uniformtext_minsize=12,
            uniformtext_mode="show"
        )

        qfig.update_xaxes(tickfont=dict(size=10))
        qfig.update_yaxes(tickfont=dict(size=10))

        st.plotly_chart(
            qfig,
            use_container_width=True,
            config={"displayModeBar": False}
        )

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

        proj_pending_products = len(grp[(grp["Product"] != "NO PRODUCT") & (grp["Stage"] == "Yet To Start")])
        proj_dispatched_products = len(grp[(grp["Product"] != "NO PRODUCT") & (grp["Stage"] == "Completed")])
        running_proj = len(grp[(grp["Product"] != "NO PRODUCT") & (~grp["Stage"].isin(["Yet To Start", "Completed"]))])

        proj_real = grp[grp["Product"] != "NO PRODUCT"]
        achieved = proj_real["Stage"].map(stage_score).fillna(0).sum()
        total_possible = len(proj_real) * 8
        proj_overall_completion = round((achieved / total_possible) * 100, 2) if total_possible > 0 else 0

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
        "Project", "Total Houses", "Started Houses", "Yet Start Houses",
        "Pending Products", "Total Dispatched Products", "Running Products", "Overall Completion %"
    ])

    st.dataframe(proj_df, use_container_width=True, height=220)

    st.markdown("---")

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
