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

    # ================= MASTER COUNTS =================
    cur.execute("SELECT COUNT(*) FROM projects")
    master_projects = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM units")
    master_units = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM houses")
    master_houses = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM products")
    master_products = cur.fetchone()[0]

    # ================= QUERY 1 : CURRENT LATEST STATUS =================
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

    # ================= QUERY 2 : HISTORICAL TRACKING TOUCH =================
    history_query = """
    SELECT DISTINCT
        p.project_name,
        u.unit_name,
        h.house_no,
        pr.product_instance_id,
        pm.product_code,
        s.stage_name
    FROM tracking_log t
    JOIN stages s ON t.stage_id = s.stage_id
    JOIN products pr ON t.product_instance_id = pr.product_instance_id
    JOIN products_master pm ON pr.product_id = pm.product_id
    JOIN houses h ON pr.house_id = h.house_id
    JOIN units u ON h.unit_id = u.unit_id
    JOIN projects p ON u.project_id = p.project_id
    """

    cur.execute(history_query)
    history_rows = cur.fetchall()

    history_df = pd.DataFrame(history_rows, columns=[
        "Project", "Unit", "House", "ProductInstance", "Product", "Touched Stage"
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
        history_df = history_df[history_df["Project"] == selected_project]
        master_house_df = master_house_df[master_house_df["Project"] == selected_project]

    with c2:
        selected_unit = st.selectbox(
            "Select Unit",
            ["All"] + sorted(master_house_df["Unit"].dropna().unique().tolist())
        )

    if selected_unit != "All":
        latest_df = latest_df[latest_df["Unit"] == selected_unit]
        history_df = history_df[history_df["Unit"] == selected_unit]
        master_house_df = master_house_df[master_house_df["Unit"] == selected_unit]

    with c3:
        house_options = sorted(master_house_df["House"].astype(str).dropna().unique().tolist())
        selected_houses = st.multiselect("Select Houses (Optional)", house_options)

    if selected_houses:
        latest_df = latest_df[latest_df["House"].astype(str).isin(selected_houses)]
        history_df = history_df[history_df["House"].astype(str).isin(selected_houses)]
        master_house_df = master_house_df[master_house_df["House"].astype(str).isin(selected_houses)]

    total_houses = len(master_house_df)

    # ================= LIVE SUMMARY =================
    st.subheader("📈 Live Workflow Summary")

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Projects", master_house_df["Project"].nunique())
    k2.metric("Units", master_house_df["Unit"].nunique())
    k3.metric("Houses", total_houses)
    k4.metric("Total Products", len(latest_df[latest_df["Product"] != "NO PRODUCT"]))

    # ================= TRUE HISTORICAL KPI =================
    st.subheader("🚦 House Stage Completion KPI")

    for stage in workflow_stages:

        houses_updated = history_df[
            history_df["Touched Stage"] == stage
        ]["House"].astype(str).nunique()

        houses_yet = total_houses - houses_updated

        products_pending = len(
            latest_df[
                (latest_df["Product"] != "NO PRODUCT") &
                (latest_df["Current Stage"] == stage)
            ]
        )

        pct = round((houses_updated / total_houses) * 100, 2) if total_houses > 0 else 0

        a, b, c, d = st.columns(4)
        a.metric(f"{stage} Houses Updated", houses_updated)
        b.metric(f"{stage} Houses Yet To Reach", houses_yet)
        c.metric(f"{stage} Products Pending", products_pending)
        d.metric(f"{stage} Progress %", f"{pct}%")

    # ================= PRODUCT LEVEL CURRENT PENDING =================
    if selected_unit != "All":
        st.subheader("🧩 Product Level Pending Distribution In Selected Unit")

        product_stage = pd.pivot_table(
            latest_df[latest_df["Product"] != "NO PRODUCT"],
            index="Product",
            columns="Current Stage",
            values="House",
            aggfunc="count",
            fill_value=0
        ).reset_index()

        for s in ["Not Started"] + workflow_stages:
            if s not in product_stage.columns:
                product_stage[s] = 0

        total_qty = latest_df[latest_df["Product"] != "NO PRODUCT"].groupby("Product")["House"].count().reset_index(name="Total Qty")
        product_stage = product_stage.merge(total_qty, on="Product")

        ordered_cols = ["Product", "Total Qty", "Not Started"] + workflow_stages
        product_stage = product_stage[ordered_cols].sort_values("Total Qty", ascending=False)
        product_stage = product_stage.reset_index(drop=True)
        product_stage.index = product_stage.index + 1

        st.dataframe(product_stage, use_container_width=True, height=420)

    # ================= HOUSE CURRENT DETAIL =================
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
