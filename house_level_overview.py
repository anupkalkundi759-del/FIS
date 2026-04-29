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

    stage_rank = {stage: i+1 for i, stage in enumerate(workflow_stages)}

    # ================= LIVE PRODUCT STAGE QUERY =================
    query = """
    WITH latest_log AS (
        SELECT
            product_instance_id,
            stage_id,
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
    """

    cur.execute(query)
    rows = cur.fetchall()

    if not rows:
        st.warning("No workflow data available.")
        return

    df = pd.DataFrame(rows, columns=[
        "Project", "Unit", "House", "Product", "Current Stage"
    ])

    # ================= FILTERS =================
    st.subheader("📌 Drilldown Filters")

    c1, c2, c3 = st.columns(3)

    with c1:
        selected_project = st.selectbox("Select Project", ["All"] + sorted(df["Project"].unique().tolist()))

    temp1 = df.copy()
    if selected_project != "All":
        temp1 = temp1[temp1["Project"] == selected_project]

    with c2:
        selected_unit = st.selectbox("Select Unit", ["All"] + sorted(temp1["Unit"].unique().tolist()))

    temp2 = temp1.copy()
    if selected_unit != "All":
        temp2 = temp2[temp2["Unit"] == selected_unit]

    with c3:
        house_options = sorted(temp2["House"].astype(str).unique().tolist())
        selected_houses = st.multiselect("Select Houses (Optional)", house_options)

    temp3 = temp2.copy()
    if selected_houses:
        temp3 = temp3[temp3["House"].astype(str).isin(selected_houses)]

    # ================= TRUE COUNTS =================
    total_projects = temp3["Project"].nunique()
    total_units = temp3["Unit"].nunique()
    total_houses = temp3["House"].nunique()
    total_products = len(temp3)

    st.subheader("📈 Live Workflow Summary")

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Projects", total_projects)
    k2.metric("Units", total_units)
    k3.metric("Houses", total_houses)
    k4.metric("Total Products", total_products)

    # ================= HOUSE BOTTLENECK STAGE =================
    house_stage = temp3.groupby("House")["Current Stage"].apply(
        lambda x: sorted(list(x), key=lambda y: stage_rank.get(y, 99))[0]
    ).reset_index(name="Bottleneck Stage")

    pending_products = temp3.groupby("House")["Product"].count().reset_index(name="Pending Products")

    house_stage = house_stage.merge(pending_products, on="House")

    # ================= STAGE WISE HOUSE COUNT =================
    stage_house_count = house_stage.groupby("Bottleneck Stage")["House"].count().reset_index(name="Houses Pending")

    for stage in workflow_stages:
        if stage not in stage_house_count["Bottleneck Stage"].values:
            stage_house_count.loc[len(stage_house_count)] = [stage, 0]

    stage_house_count["sort"] = stage_house_count["Bottleneck Stage"].map(stage_rank)
    stage_house_count = stage_house_count.sort_values("sort").drop("sort", axis=1)

    st.subheader("🚦 Stage Wise House Pending Summary")
    st.dataframe(stage_house_count, use_container_width=True, height=320)

    # ================= HOUSE DETAIL =================
    st.subheader("🏠 House Bottleneck Detail")
    house_stage = house_stage.sort_values("Bottleneck Stage", key=lambda x: x.map(stage_rank))
    st.dataframe(house_stage, use_container_width=True, height=350)

    # ================= PRODUCT DETAIL ONLY WHEN UNIT SELECTED =================
    if selected_unit != "All":

        st.subheader("🧩 Product Pending Distribution")

        product_stage = pd.pivot_table(
            temp3,
            index="Product",
            columns="Current Stage",
            values="House",
            aggfunc="count",
            fill_value=0
        ).reset_index()

        for stage in workflow_stages:
            if stage not in product_stage.columns:
                product_stage[stage] = 0

        total_qty = temp3.groupby("Product")["Product"].count().reset_index(name="Total Qty")
        product_stage = product_stage.merge(total_qty, on="Product")

        product_stage = product_stage[["Product", "Total Qty"] + workflow_stages]
        product_stage = product_stage.sort_values("Total Qty", ascending=False)

        st.dataframe(product_stage, use_container_width=True, height=420)
