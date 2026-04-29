def show_dashboard(conn, cur):
    import streamlit as st
    import pandas as pd

    st.title("📊 Workflow Intelligence Monitor")

    # ================= LIVE PRODUCT STAGE QUERY =================
    query = """
    WITH latest_log AS (
        SELECT
            product_instance_id,
            stage_id,
            status,
            ROW_NUMBER() OVER (
                PARTITION BY product_instance_id
                ORDER BY timestamp DESC
            ) rn
        FROM tracking_log
    )

    SELECT
        p.project_name,
        u.unit_name,
        h.house_no,
        pm.product_code,
        pm.product_category,
        pm.orientation,
        COALESCE(s.stage_name, 'Measurement') as current_stage

    FROM products pr
    JOIN products_master pm ON pr.product_id = pm.product_id
    JOIN houses h ON pr.house_id = h.house_id
    JOIN units u ON h.unit_id = u.unit_id
    JOIN projects p ON u.project_id = p.project_id

    LEFT JOIN latest_log ll
        ON pr.product_instance_id = ll.product_instance_id
        AND ll.rn = 1

    LEFT JOIN stages s
        ON ll.stage_id = s.stage_id

    ORDER BY p.project_name, u.unit_name, h.house_no, pm.product_code
    """

    cur.execute(query)
    rows = cur.fetchall()

    if not rows:
        st.warning("No data available")
        return

    df = pd.DataFrame(rows, columns=[
        "Project", "Unit", "House",
        "Product", "Type", "Orientation", "Current Stage"
    ])

    workflow_stages = [
        "Measurement",
        "Cutting List",
        "Production",
        "Pre Assembly",
        "Polishing",
        "Final Assembly",
        "Dispatch"
    ]

    # ================= FILTERS =================
    st.subheader("📌 Drilldown Filters")

    c1, c2, c3 = st.columns(3)

    with c1:
        selected_project = st.selectbox(
            "Select Project",
            ["All"] + sorted(df["Project"].unique().tolist())
        )

    temp1 = df.copy()
    if selected_project != "All":
        temp1 = temp1[temp1["Project"] == selected_project]

    with c2:
        selected_unit = st.selectbox(
            "Select Unit",
            ["All"] + sorted(temp1["Unit"].unique().tolist())
        )

    temp2 = temp1.copy()
    if selected_unit != "All":
        temp2 = temp2[temp2["Unit"] == selected_unit]

    with c3:
        house_options = sorted(temp2["House"].astype(str).unique().tolist())
        selected_houses = st.multiselect("Select Houses (Optional)", house_options)

    temp3 = temp2.copy()
    if selected_houses:
        temp3 = temp3[temp3["House"].astype(str).isin(selected_houses)]

    # ================= KPI =================
    st.subheader("📈 Live Workflow Summary")

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Projects", temp3["Project"].nunique())
    k2.metric("Units", temp3["Unit"].nunique())
    k3.metric("Houses", temp3["House"].nunique())
    k4.metric("Total Products", len(temp3))

    # ================= MAIN SMART TABLE =================
    if selected_project == "All":

        st.subheader("🏗 Project Workflow Distribution")

        summary = pd.pivot_table(
            temp3,
            index="Project",
            columns="Current Stage",
            values="Product",
            aggfunc="count",
            fill_value=0
        ).reset_index()

        for stage in workflow_stages:
            if stage not in summary.columns:
                summary[stage] = 0

        summary["Total Products"] = summary[workflow_stages].sum(axis=1)

        summary = summary[["Project", "Total Products"] + workflow_stages]

        st.dataframe(summary, use_container_width=True, height=320)

    elif selected_unit == "All":

        st.subheader("🏢 Unit Workflow Distribution")

        summary = pd.pivot_table(
            temp1,
            index="Unit",
            columns="Current Stage",
            values="Product",
            aggfunc="count",
            fill_value=0
        ).reset_index()

        for stage in workflow_stages:
            if stage not in summary.columns:
                summary[stage] = 0

        house_count = temp1.groupby("Unit")["House"].nunique().reset_index(name="Houses")
        total_products = temp1.groupby("Unit")["Product"].count().reset_index(name="Total Products")

        summary = summary.merge(house_count, on="Unit")
        summary = summary.merge(total_products, on="Unit")

        summary = summary[["Unit", "Houses", "Total Products"] + workflow_stages]

        st.dataframe(summary, use_container_width=True, height=320)

    else:

        st.subheader("🧩 Product Workflow Distribution")

        summary = pd.pivot_table(
            temp3,
            index=["Product"],
            columns="Current Stage",
            values="House",
            aggfunc="count",
            fill_value=0
        ).reset_index()

        for stage in workflow_stages:
            if stage not in summary.columns:
                summary[stage] = 0

        total_qty = temp3.groupby("Product")["Product"].count().reset_index(name="Total Qty")
        summary = summary.merge(total_qty, on="Product")

        summary = summary[["Product", "Total Qty"] + workflow_stages]

        summary = summary.sort_values("Total Qty", ascending=False)

        st.dataframe(summary, use_container_width=True, height=450)
