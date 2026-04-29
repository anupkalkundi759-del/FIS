def show_dashboard(conn, cur):
    import streamlit as st
    import pandas as pd

    st.title("📊 Workflow Intelligence Monitor")

    workflow_stages = [
        "No Product Loaded",
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

    # ================= LIVE WORKFLOW QUERY =================
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
        COALESCE(pm.product_code, 'NO PRODUCT') AS product_code,

        COALESCE(
            lt.stage_name,
            CASE
                WHEN pr.product_instance_id IS NULL THEN 'No Product Loaded'
                ELSE 'Not Started'
            END
        ) AS current_stage,

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
        "Project", "Unit", "House", "Product", "Current Stage", "Current Status"
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

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Projects", temp3["Project"].nunique())
    k2.metric("Units", temp3["Unit"].nunique())
    k3.metric("Houses", temp3["House"].nunique())
    k4.metric("Total Products", len(temp3[temp3["Product"] != "NO PRODUCT"]))

    # ================= HOUSE BOTTLENECK =================
    house_bottleneck = temp3.groupby("House")["Current Stage"].apply(
        lambda x: sorted(list(set(x)), key=lambda y: stage_rank.get(y, 999))[0]
    ).reset_index(name="Bottleneck Stage")

    pending_products = temp3[temp3["Current Stage"] != "Dispatch"].groupby("House")["Product"].count().reset_index(name="Pending Products")
    house_bottleneck = house_bottleneck.merge(pending_products, on="House", how="left").fillna(0)

    # ================= STAGE SUMMARY =================
    stage_summary = house_bottleneck.groupby("Bottleneck Stage")["House"].count().reset_index(name="Houses Pending")

    for s in workflow_stages:
        if s not in stage_summary["Bottleneck Stage"].values:
            stage_summary.loc[len(stage_summary)] = [s, 0]

    stage_summary["rank"] = stage_summary["Bottleneck Stage"].map(stage_rank)
    stage_summary = stage_summary.sort_values("rank").drop("rank", axis=1)

    st.subheader("🚦 Stage Wise House Pending Summary")
    st.dataframe(stage_summary, use_container_width=True, height=340)

    # ================= HOUSE DETAIL =================
    st.subheader("🏠 Which Houses Are Pending In Which Stage")
    house_bottleneck = house_bottleneck.sort_values("Bottleneck Stage", key=lambda x: x.map(stage_rank))
    st.dataframe(house_bottleneck, use_container_width=True, height=350)

    # ================= PRODUCT DETAIL WHEN UNIT SELECTED =================
    if selected_unit != "All":
        st.subheader("🧩 Product Pending Distribution In Selected Unit")

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

        total_qty = temp3.groupby("Product")["House"].count().reset_index(name="Total Qty")
        product_stage = product_stage.merge(total_qty, on="Product")

        ordered_cols = ["Product", "Total Qty"] + workflow_stages
        product_stage = product_stage[ordered_cols].sort_values("Total Qty", ascending=False)

        st.dataframe(product_stage, use_container_width=True, height=420)
