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

    # ========================= MASTER COUNTS =========================
    cur.execute("SELECT COUNT(*) FROM projects")
    master_projects = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM units")
    master_units = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM houses")
    master_houses = cur.fetchone()[0]

    cur.execute("SELECT COALESCE(SUM(quantity),0) FROM products")
    master_products = cur.fetchone()[0]

    # ========================= LIVE WORKFLOW BASE =========================
    query = """
    WITH latest_tracking AS (
        SELECT
            house_id,
            product_id,
            stage_id,
            ROW_NUMBER() OVER (
                PARTITION BY house_id, product_id
                ORDER BY timestamp DESC
            ) AS rn
        FROM tracking_log
    )

    SELECT
        p.project_name,
        u.unit_name,
        h.house_no,
        pm.product_code,
        pr.quantity,
        COALESCE(s.stage_name, 'Not Started') AS current_stage

    FROM products pr
    JOIN houses h ON pr.house_id = h.house_id
    JOIN units u ON h.unit_id = u.unit_id
    JOIN projects p ON u.project_id = p.project_id
    JOIN products_master pm ON pr.product_id = pm.product_id

    LEFT JOIN latest_tracking lt
        ON pr.house_id = lt.house_id
        AND pr.product_id = lt.product_id
        AND lt.rn = 1

    LEFT JOIN stages s
        ON lt.stage_id = s.stage_id
    """

    cur.execute(query)
    rows = cur.fetchall()

    if not rows:
        st.warning("No data found.")
        return

    df = pd.DataFrame(rows, columns=[
        "Project", "Unit", "House", "Product", "Qty", "Current Stage"
    ])

    # explode quantity to actual product count
    expanded = []
    for _, r in df.iterrows():
        for i in range(int(r["Qty"])):
            expanded.append([
                r["Project"], r["Unit"], r["House"], r["Product"], r["Current Stage"]
            ])

    live_df = pd.DataFrame(expanded, columns=[
        "Project", "Unit", "House", "Product", "Current Stage"
    ])

    # ========================= FILTERS =========================
    st.subheader("📌 Drilldown Filters")

    c1, c2, c3 = st.columns(3)

    with c1:
        selected_project = st.selectbox(
            "Select Project",
            ["All"] + sorted(live_df["Project"].unique().tolist())
        )

    temp1 = live_df.copy()
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

    # ========================= KPI =========================
    visible_projects = temp3["Project"].nunique()
    visible_units = temp3["Unit"].nunique()
    visible_houses = temp3["House"].nunique()
    visible_products = len(temp3)

    st.subheader("📈 Live Workflow Summary")
    k1, k2, k3, k4 = st.columns(4)

    k1.metric("Projects", visible_projects if selected_project != "All" else master_projects)
    k2.metric("Units", visible_units if selected_project != "All" else master_units)
    k3.metric("Houses", visible_houses if selected_project != "All" else master_houses)
    k4.metric("Total Products", visible_products)

    # ========================= HOUSE PENDING SUMMARY =========================
    house_stage = temp3.groupby("House")["Current Stage"].apply(
        lambda x: sorted(list(x), key=lambda y: stage_rank[y])[0]
    ).reset_index(name="Bottleneck Stage")

    pending_qty = temp3.groupby("House")["Product"].count().reset_index(name="Pending Products")
    house_stage = house_stage.merge(pending_qty, on="House")

    stage_summary = house_stage.groupby("Bottleneck Stage")["House"].count().reset_index(name="Houses Pending")

    for s in workflow_stages:
        if s not in stage_summary["Bottleneck Stage"].values:
            stage_summary.loc[len(stage_summary)] = [s, 0]

    stage_summary["rank"] = stage_summary["Bottleneck Stage"].map(stage_rank)
    stage_summary = stage_summary.sort_values("rank").drop("rank", axis=1)

    st.subheader("🚦 Stage Wise House Pending Summary")
    st.dataframe(stage_summary, use_container_width=True, height=320)

    # ========================= HOUSE DETAIL =========================
    st.subheader("🏠 Which Houses Are Pending In Which Stage")
    house_stage = house_stage.sort_values("Bottleneck Stage", key=lambda x: x.map(stage_rank))
    st.dataframe(house_stage, use_container_width=True, height=350)

    # ========================= PRODUCT SUMMARY ONLY WHEN UNIT SELECTED =========================
    if selected_unit != "All":
        st.subheader("🧩 Product Stage Distribution Inside Selected Unit")

        product_stage = pd.pivot_table(
            temp3,
            index="Product",
            columns="Current Stage",
            values="House",
            aggfunc="count",
            fill_value=0
        ).reset_index()

        for s in workflow_stages:
            if s not in product_stage.columns:
                product_stage[s] = 0

        total_qty = temp3.groupby("Product")["Product"].count().reset_index(name="Total Qty")
        product_stage = product_stage.merge(total_qty, on="Product")

        product_stage = product_stage[
            ["Product", "Total Qty"] + workflow_stages
        ].sort_values("Total Qty", ascending=False)

        st.dataframe(product_stage, use_container_width=True, height=420)
