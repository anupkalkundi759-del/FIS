def show_dashboard(conn, cur):
    import streamlit as st
    import pandas as pd

    st.title("📊 Workflow Intelligence Monitor")

    workflow_stages = [
        "Not Started",
        "Measurement",
        "Cutting List",
        "Production",
        "Pre Assembly",
        "Polishing",
        "Final Assembly",
        "Dispatch"
    ]
    stage_rank = {s: i for i, s in enumerate(workflow_stages)}

    # ================= MASTER COUNTS =================
    cur.execute("SELECT COUNT(*) FROM projects")
    master_projects = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM units")
    master_units = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM houses")
    master_houses = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM products")
    master_products = cur.fetchone()[0]

    # ================= LIVE QUERY =================
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
        p.project_name,
        u.unit_name,
        h.house_no,
        pr.product_instance_id,
        COALESCE(pm.product_code, 'NO PRODUCT') AS product_code,
        COALESCE(lt.stage_name, 'Not Started') AS current_stage,
        COALESCE(lt.status, 'Pending') AS current_status

    FROM houses h
    JOIN units u ON h.unit_id = u.unit_id
    JOIN projects p ON u.project_id = p.project_id
    LEFT JOIN products pr ON h.house_id = pr.house_id
    LEFT JOIN products_master pm ON pr.product_id = pm.product_id
    LEFT JOIN latest_tracking lt
        ON pr.product_instance_id = lt.product_instance_id
        AND lt.rn = 1
    """

    cur.execute(query)
    rows = cur.fetchall()

    if not rows:
        st.warning("No workflow records found.")
        return

    df = pd.DataFrame(rows, columns=[
        "Project", "Unit", "House", "ProductInstance", "Product", "Current Stage", "Current Status"
    ])

    # ================= FILTERS =================
    st.subheader("📌 Drilldown Filters")

    c1, c2, c3 = st.columns(3)

    with c1:
        selected_project = st.selectbox(
            "Select Project",
            ["All"] + sorted(df["Project"].dropna().unique().tolist())
        )

    temp1 = df.copy()
    if selected_project != "All":
        temp1 = temp1[temp1["Project"] == selected_project]

    with c2:
        selected_unit = st.selectbox(
            "Select Unit",
            ["All"] + sorted(temp1["Unit"].dropna().unique().tolist())
        )

    temp2 = temp1.copy()
    if selected_unit != "All":
        temp2 = temp2[temp2["Unit"] == selected_unit]

    with c3:
        house_options = sorted(temp2["House"].astype(str).dropna().unique().tolist())
        selected_houses = st.multiselect("Select Houses (Optional)", house_options)

    temp3 = temp2.copy()
    if selected_houses:
        temp3 = temp3[temp3["House"].astype(str).isin(selected_houses)]

    # ================= KPI =================
    st.subheader("📈 Live Workflow Summary")

    if selected_project == "All" and selected_unit == "All" and not selected_houses:
        live_projects = master_projects
        live_units = master_units
        live_houses = master_houses
    else:
        live_projects = temp3["Project"].nunique()
        live_units = temp3["Unit"].nunique()
        live_houses = temp3["House"].nunique()

    live_products = len(temp3[temp3["Product"] != "NO PRODUCT"])

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Projects", live_projects)
    k2.metric("Units", live_units)
    k3.metric("Houses", live_houses)
    k4.metric("Total Products", live_products)

    # ================= HOUSE STAGE COMPLETION KPI =================
    st.subheader("🚦 House Stage Completion KPI")

    filtered_houses = sorted(temp3["House"].astype(str).dropna().unique().tolist())
    stage_kpi_rows = []

    for stage in workflow_stages[1:]:
        completed_list = []
        pending_list = []

        for house in filtered_houses:
            house_df = temp3[
                (temp3["House"].astype(str) == str(house)) &
                (temp3["Product"] != "NO PRODUCT")
            ].copy()

            if house_df.empty:
                continue

            total_products = len(house_df)

            completed_products = len(
                house_df[
                    house_df["Current Stage"].map(stage_rank) >= stage_rank[stage]
                ]
            )

            if completed_products == total_products:
                completed_list.append(str(house))
            else:
                pending_list.append(str(house))

        stage_kpi_rows.append({
            "Stage": stage,
            "Houses Completed": len(completed_list),
            "Completed House Nos": ", ".join(completed_list) if completed_list else "-",
            "Houses Pending": len(pending_list),
            "Pending House Nos": ", ".join(pending_list) if pending_list else "-"
        })

    stage_kpi_df = pd.DataFrame(stage_kpi_rows)
    stage_kpi_df.index = stage_kpi_df.index + 1

    st.dataframe(stage_kpi_df, use_container_width=True, height=350)

    # ================= UNIT LEVEL PRODUCT PENDING DISTRIBUTION =================
    if selected_unit != "All":
        st.subheader("🧩 Product Level Pending Distribution In Selected Unit")

        pending_unit_df = temp3[
            (temp3["Product"] != "NO PRODUCT") &
            (temp3["Current Stage"] != "Dispatch")
        ].copy()

        product_stage = pd.pivot_table(
            pending_unit_df,
            index="Product",
            columns="Current Stage",
            values="House",
            aggfunc="count",
            fill_value=0
        ).reset_index()

        for s in workflow_stages:
            if s not in product_stage.columns:
                product_stage[s] = 0

        ordered_cols = ["Product"] + workflow_stages[:-1]
        product_stage = product_stage[ordered_cols]

        rename_cols = {
            "Not Started": "Measurement Pending",
            "Measurement": "Cutting Pending",
            "Cutting List": "Production Pending",
            "Production": "Pre Assembly Pending",
            "Pre Assembly": "Polishing Pending",
            "Polishing": "Final Assembly Pending",
            "Final Assembly": "Dispatch Pending"
        }

        product_stage = product_stage.rename(columns=rename_cols)
        product_stage = product_stage.sort_values(by=product_stage.columns[1:].tolist(), ascending=False)
        product_stage = product_stage.reset_index(drop=True)
        product_stage.index = product_stage.index + 1

        st.dataframe(product_stage, use_container_width=True, height=350)

    # ================= HOUSE LEVEL EXACT PRODUCT DETAIL =================
    if selected_houses:
        for house in selected_houses:
            st.subheader(f"🏠 {house} Detailed Pending Product Status")

            house_df = temp3[
                (temp3["House"].astype(str) == str(house)) &
                (temp3["Product"] != "NO PRODUCT") &
                (temp3["Current Stage"] != "Dispatch")
            ].copy()

            if house_df.empty:
                st.success("All products dispatched in this house.")
                continue

            house_pending_df = house_df[["Product", "Current Stage"]].copy()
            house_pending_df.columns = ["Product", "Pending In Stage"]

            house_pending_df = house_pending_df.sort_values(
                "Pending In Stage",
                key=lambda x: x.map(stage_rank)
            )

            house_pending_df = house_pending_df.reset_index(drop=True)
            house_pending_df.index = house_pending_df.index + 1

            st.dataframe(house_pending_df, use_container_width=True, height=320)
