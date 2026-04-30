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

    # ================= TRUE MASTER HOUSE LIST =================
    master_house_query = """
    SELECT
        p.project_name,
        u.unit_name,
        h.house_no
    FROM houses h
    JOIN units u ON h.unit_id = u.unit_id
    JOIN projects p ON u.project_id = p.project_id
    """

    cur.execute(master_house_query)
    master_house_rows = cur.fetchall()

    master_house_df = pd.DataFrame(master_house_rows, columns=["Project", "Unit", "House"])

    if selected_project != "All":
        master_house_df = master_house_df[master_house_df["Project"] == selected_project]

    if selected_unit != "All":
        master_house_df = master_house_df[master_house_df["Unit"] == selected_unit]

    if selected_houses:
        master_house_df = master_house_df[master_house_df["House"].astype(str).isin(selected_houses)]

    total_houses = len(master_house_df)

    # ================= LIVE KPI =================
    st.subheader("📈 Live Workflow Summary")

    live_projects = master_house_df["Project"].nunique()
    live_units = master_house_df["Unit"].nunique()
    live_houses = total_houses
    live_products = len(temp3[temp3["Product"] != "NO PRODUCT"])

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Projects", live_projects)
    k2.metric("Units", live_units)
    k3.metric("Houses", live_houses)
    k4.metric("Total Products", live_products)

    # ================= EXACT CURRENT STAGE KPI =================
    st.subheader("🚦 House Stage Completion KPI")

    for stage in workflow_stages[1:]:

        stage_house_updated = temp3[
            (temp3["Product"] != "NO PRODUCT") &
            (temp3["Current Stage"] == stage)
        ]["House"].astype(str).nunique()

        stage_house_pending = total_houses - stage_house_updated

        stage_products_pending = len(
            temp3[
                (temp3["Product"] != "NO PRODUCT") &
                (temp3["Current Stage"] == stage)
            ]
        )

        stage_progress_pct = round((stage_house_updated / total_houses) * 100, 2) if total_houses > 0 else 0

        a, b, c, d = st.columns(4)
        a.metric(f"{stage} Houses Updated", stage_house_updated)
        b.metric(f"{stage} Houses Yet To Reach", stage_house_pending)
        c.metric(f"{stage} Products Pending", stage_products_pending)
        d.metric(f"{stage} Progress %", f"{stage_progress_pct}%")

    # ================= PRODUCT LEVEL PENDING DISTRIBUTION =================
    if selected_unit != "All":
        st.subheader("🧩 Product Level Pending Distribution In Selected Unit")

        product_stage = pd.pivot_table(
            temp3[temp3["Product"] != "NO PRODUCT"],
            index="Product",
            columns="Current Stage",
            values="House",
            aggfunc="count",
            fill_value=0
        ).reset_index()

        for s in workflow_stages:
            if s not in product_stage.columns:
                product_stage[s] = 0

        total_qty = temp3[temp3["Product"] != "NO PRODUCT"].groupby("Product")["House"].count().reset_index(name="Total Qty")
        product_stage = product_stage.merge(total_qty, on="Product")

        ordered_cols = ["Product", "Total Qty"] + workflow_stages
        product_stage = product_stage[ordered_cols].sort_values("Total Qty", ascending=False)
        product_stage = product_stage.reset_index(drop=True)
        product_stage.index = product_stage.index + 1

        st.dataframe(product_stage, use_container_width=True, height=420)

    # ================= HOUSE DETAIL =================
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
