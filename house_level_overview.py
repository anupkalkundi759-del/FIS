def show_dashboard(conn, cur):
    import streamlit as st
    import pandas as pd

    st.title("📊 Workflow Intelligence Monitor")

    workflow_stages = [
        "Not Started",
        "Cutting List",
        "Production",
        "Pre Assembly",
        "Polishing",
        "Final Assembly",
        "Dispatch"
    ]

    # ================= CURRENT LATEST STATUS QUERY =================
    latest_query = """
    SELECT
        p.project_name,
        u.unit_name,
        h.house_no,
        pr.product_instance_id,
        COALESCE(pm.product_code,'NO PRODUCT') AS product_code,
        COALESCE(pcs.stage_name,'Not Started') AS current_stage

    FROM houses h
    JOIN units u ON h.unit_id = u.unit_id
    JOIN projects p ON u.project_id = p.project_id
    LEFT JOIN products pr ON h.house_id = pr.house_id
    LEFT JOIN products_master pm ON pr.product_id = pm.product_id
    LEFT JOIN product_current_stage pcs
        ON pr.product_instance_id = pcs.product_instance_id
    """

    cur.execute(latest_query)
    latest_rows = cur.fetchall()

    latest_df = pd.DataFrame(latest_rows, columns=[
        "Project", "Unit", "House", "ProductInstance", "Product", "Current Stage"
    ])

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
    product_df = latest_df[latest_df["Product"] != "NO PRODUCT"].copy()
    total_products_scope = len(product_df)

    # ================= LIVE SUMMARY =================
    st.subheader("📈 Live Workflow Summary")

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Projects", master_house_df["Project"].nunique())
    k2.metric("Units", master_house_df["Unit"].nunique())
    k3.metric("Houses", total_houses)
    k4.metric("Total Products", total_products_scope)

    # ================= STAGE COMPLETION MATRIX =================
    st.subheader("🚦 Stage Completion Performance Matrix")

    stage_rank = {
        "Not Started": 0,
        "Cutting List": 1,
        "Production": 2,
        "Pre Assembly": 3,
        "Polishing": 4,
        "Final Assembly": 5,
        "Dispatch": 6
    }

    product_df["StageRank"] = product_df["Current Stage"].map(stage_rank)

    kpi_rows = []

    for stage in workflow_stages:

        current_rank = stage_rank[stage]

        pending_df = product_df[product_df["StageRank"] < current_rank]

        pending_products = len(pending_df)
        houses_impacted = pending_df["House"].astype(str).nunique()

        completion_pct = round(
            ((total_products_scope - pending_products) / total_products_scope) * 100, 2
        ) if total_products_scope > 0 else 0

        stage_label = "Measurement" if stage == "Not Started" else stage

        kpi_rows.append([
            stage_label,
            total_products_scope,
            pending_products,
            houses_impacted,
            f"{completion_pct}%"
        ])

    # ================= OVERALL COMPLETION =================
    house_group = product_df.groupby("House")["Current Stage"].apply(list)

    fully_dispatch_houses = 0
    for house, stages in house_group.items():
        if all(str(x) == "Dispatch" for x in stages):
            fully_dispatch_houses += 1

    overall_houses_impacted = total_houses - fully_dispatch_houses

    dispatch_completed_products = len(product_df[product_df["Current Stage"] == "Dispatch"])
    overall_pending = total_products_scope - dispatch_completed_products
    overall_completion = round(
        (dispatch_completed_products / total_products_scope) * 100, 2
    ) if total_products_scope > 0 else 0

    kpi_rows.append([
        "OVERALL COMPLETION",
        total_products_scope,
        overall_pending,
        overall_houses_impacted,
        f"{overall_completion}%"
    ])

    kpi_df = pd.DataFrame(
        kpi_rows,
        columns=["Stage", "Total Products", "Pending Products", "Houses Impacted", "Completion %"]
    )

    kpi_df.index = kpi_df.index + 1
    st.dataframe(kpi_df, use_container_width=True, height=370)

    # ================= HOUSE DETAILED PENDING =================
    if selected_houses:
        for house in selected_houses:
            st.subheader(f"🏠 {house} Detailed Pending Product Status")

            house_df = product_df[
                (product_df["House"].astype(str) == str(house)) &
                (product_df["Current Stage"] != "Dispatch")
            ].copy()

            if house_df.empty:
                st.success("All products dispatched in this house.")
                continue

            detail = house_df[["Product", "Current Stage"]].copy()
            detail.columns = ["Product", "Pending In Stage"]
            detail = detail.reset_index(drop=True)
            detail.index = detail.index + 1

            st.dataframe(detail, use_container_width=True, height=320)
