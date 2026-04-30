def show_dashboard(conn, cur):
    import streamlit as st
    import pandas as pd

    st.title("📊 Workflow Intelligence Monitor")

    workflow_stages = [
        "Measurement",
        "Cutting List",
        "Production",
        "Pre Assembly",
        "Polishing",
        "Final Assembly",
        "Dispatch"
    ]

    full_stage_order = {
        "Not Started": 0,
        "Measurement": 1,
        "Cutting List": 2,
        "Production": 3,
        "Pre Assembly": 4,
        "Polishing": 5,
        "Final Assembly": 6,
        "Dispatch": 7
    }

    # ================= QUERY CURRENT LATEST STATUS =================
    latest_query = """
    WITH latest_tracking AS (
        SELECT
            t.product_instance_id,
            s.stage_name,
            ROW_NUMBER() OVER (
                PARTITION BY t.product_instance_id
                ORDER BY t.timestamp DESC
            ) AS rn
        FROM tracking_log t
        JOIN stages s ON t.stage_id = s.stage_id
    )

    SELECT
        p.project_name,
        u.unit_name,
        h.house_no,
        pr.product_instance_id,
        COALESCE(pm.product_code,'NO PRODUCT') AS product_code,
        COALESCE(lt.stage_name,'Not Started') AS current_stage

    FROM houses h
    JOIN units u ON h.unit_id = u.unit_id
    JOIN projects p ON u.project_id = p.project_id
    LEFT JOIN products pr ON h.house_id = pr.house_id
    LEFT JOIN products_master pm ON pr.product_id = pm.product_id
    LEFT JOIN latest_tracking lt
        ON pr.product_instance_id = lt.product_instance_id
        AND lt.rn = 1
    """

    cur.execute(latest_query)
    latest_rows = cur.fetchall()

    latest_df = pd.DataFrame(latest_rows, columns=[
        "Project", "Unit", "House", "ProductInstance", "Product", "Current Stage"
    ])

    latest_df["Stage Rank"] = latest_df["Current Stage"].map(full_stage_order)

    # ================= MASTER HOUSE QUERY =================
    house_query = """
    SELECT
        p.project_name,
        u.unit_name,
        h.house_no
    FROM houses h
    JOIN units u ON h.unit_id = u.unit_id
    JOIN projects p ON u.project_id = p.project_id
    """

    cur.execute(house_query)
    house_rows = cur.fetchall()

    master_house_df = pd.DataFrame(house_rows, columns=["Project", "Unit", "House"])

    # ================= FILTERS =================
    st.subheader("📌 Drilldown Filters")

    c1, c2, c3 = st.columns(3)

    with c1:
        selected_project = st.selectbox(
            "Select Project",
            ["All"] + sorted(master_house_df["Project"].dropna().unique().tolist())
        )

    if selected_project != "All":
        latest_df = latest_df[latest_df["Project"] == selected_project]
        master_house_df = master_house_df[master_house_df["Project"] == selected_project]

    with c2:
        selected_unit = st.selectbox(
            "Select Unit",
            ["All"] + sorted(master_house_df["Unit"].dropna().unique().tolist())
        )

    if selected_unit != "All":
        latest_df = latest_df[latest_df["Unit"] == selected_unit]
        master_house_df = master_house_df[master_house_df["Unit"] == selected_unit]

    with c3:
        house_options = sorted(master_house_df["House"].astype(str).dropna().unique().tolist())
        selected_houses = st.multiselect("Select Houses (Optional)", house_options)

    if selected_houses:
        latest_df = latest_df[latest_df["House"].astype(str).isin(selected_houses)]
        master_house_df = master_house_df[master_house_df["House"].astype(str).isin(selected_houses)]

    total_houses = len(master_house_df)
    total_products_scope = len(latest_df[latest_df["Product"] != "NO PRODUCT"])

    # ================= LIVE SUMMARY =================
    st.subheader("📈 Live Workflow Summary")

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Projects", master_house_df["Project"].nunique())
    k2.metric("Units", master_house_df["Unit"].nunique())
    k3.metric("Houses", total_houses)
    k4.metric("Total Products", total_products_scope)

    # ================= TRUE KPI MATRIX =================
    st.subheader("🚦 Stage Completion Performance Matrix")

    kpi_rows = []

    for stage in workflow_stages:

        target_rank = full_stage_order[stage]

        products_pending = len(
            latest_df[
                (latest_df["Product"] != "NO PRODUCT") &
                (latest_df["Stage Rank"] < target_rank)
            ]
        )

        houses_impacted = latest_df[
            (latest_df["Product"] != "NO PRODUCT") &
            (latest_df["Stage Rank"] < target_rank)
        ]["House"].astype(str).nunique()

        completion_pct = round(((total_products_scope - products_pending) / total_products_scope) * 100, 2) if total_products_scope > 0 else 0

        kpi_rows.append([
            stage,
            total_products_scope,
            products_pending,
            houses_impacted,
            f"{completion_pct}%"
        ])

    kpi_df = pd.DataFrame(
        kpi_rows,
        columns=[
            "Stage",
            "Total Products",
            "Pending Products",
            "Houses Impacted",
            "Completion %"
        ]
    )

    kpi_df.index = kpi_df.index + 1
    st.dataframe(kpi_df, use_container_width=True, height=320)

    # ================= HOUSE DETAIL ONLY =================
    if selected_houses:
        for house in selected_houses:
            st.subheader(f"🏠 {house} Detailed Pending Product Status")

            house_df = latest_df[
                (latest_df["House"].astype(str) == str(house)) &
                (latest_df["Product"] != "NO PRODUCT") &
                (latest_df["Current Stage"] != "Dispatch")
            ].copy()

            if house_df.empty:
                st.success("All products dispatched in this house.")
                continue

            detail = house_df[["Product", "Current Stage"]].copy()
            detail.columns = ["Product", "Pending In Stage"]
            detail = detail.reset_index(drop=True)
            detail.index = detail.index + 1

            st.dataframe(detail, use_container_width=True, height=320)
