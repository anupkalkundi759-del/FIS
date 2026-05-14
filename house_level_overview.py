def show_dashboard(conn, cur):
    import streamlit as st
    import pandas as pd

    st.title("📊 Workflow Intelligence Monitor")

    workflow_stages = [
        "Yet To Start",
        "Measurement",
        "Cutting List",
        "Production",
        "Pre Assembly",
        "Polishing",
        "Final Assembly",
        "Dispatch"
    ]

    latest_query = """
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
        p.project_name,
        u.unit_name,
        h.house_no,
        h.house_id,
        pr.product_instance_id,
        COALESCE(pm.product_code,'NO PRODUCT') AS product_code,

        CASE
            WHEN lt.stage_name IS NULL THEN 'Yet To Start'
            WHEN lt.stage_name = 'Dispatch' AND lt.status = 'Completed' THEN 'Completed'
            ELSE lt.stage_name
        END AS current_stage

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
        "Project", "Unit", "House", "HouseID", "ProductInstance", "Product", "Current Stage"
    ])

    house_query = """
    SELECT
        p.project_name,
        u.unit_name,
        h.house_no,
        h.house_id
    FROM houses h
    JOIN units u ON h.unit_id = u.unit_id
    JOIN projects p ON u.project_id = p.project_id
    """
    cur.execute(house_query)
    house_rows = cur.fetchall()

    master_house_df = pd.DataFrame(house_rows, columns=["Project", "Unit", "House", "HouseID"])

    st.subheader("📌 Drilldown Filters")

    c1, c2, c3 = st.columns(3)

    with c1:
        selected_project = st.selectbox("Select Project", ["All"] + sorted(master_house_df["Project"].dropna().unique().tolist()))
    if selected_project != "All":
        latest_df = latest_df[latest_df["Project"] == selected_project]
        master_house_df = master_house_df[master_house_df["Project"] == selected_project]

    with c2:
        selected_unit = st.selectbox("Select Unit Type", ["All"] + sorted(master_house_df["Unit"].dropna().unique().tolist()))
    if selected_unit != "All":
        latest_df = latest_df[latest_df["Unit"] == selected_unit]
        master_house_df = master_house_df[master_house_df["Unit"] == selected_unit]

    with c3:
        house_options = sorted(master_house_df["House"].astype(str).dropna().unique().tolist())
        selected_houses = st.multiselect("Select House Number", house_options)
    if selected_houses:
        latest_df = latest_df[latest_df["House"].astype(str).isin(selected_houses)]
        master_house_df = master_house_df[master_house_df["House"].astype(str).isin(selected_houses)]

    total_houses = master_house_df["HouseID"].nunique()
    product_df = latest_df[latest_df["Product"] != "NO PRODUCT"].copy()
    total_products_scope = len(product_df)

    st.subheader("📈 Live Workflow Summary")
    k1, k2, k3 = st.columns(3)
    k1.metric("Projects", master_house_df["Project"].nunique())
    k2.metric("Total Houses", total_houses)
    k3.metric("Total Products", total_products_scope)

    st.subheader("🚦 Stage Completion Performance Matrix")

    stage_rank = {
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

    product_df["StageRank"] = product_df["Current Stage"].map(stage_rank).fillna(0)

    kpi_rows = []

    for stage in workflow_stages:
        current_rank = stage_rank[stage]

        pending_df = product_df[product_df["Current Stage"] == stage]

        if stage == "Yet To Start":
            completed_df = product_df[product_df["StageRank"] > 0]
        elif stage == "Dispatch":
            completed_df = product_df[product_df["StageRank"] == 8]
        else:
            completed_df = product_df[product_df["StageRank"] > current_rank]

        pending_products = len(pending_df)
        houses_impacted = pending_df["HouseID"].nunique()

        completed_products = len(completed_df)
        completion_pct = round((completed_products / total_products_scope) * 100, 2) if total_products_scope > 0 else 0

        kpi_rows.append([stage, total_products_scope, pending_products, houses_impacted, f"{completion_pct}%"])

    house_group = product_df.groupby("HouseID")["Current Stage"].apply(list)
    fully_dispatch_houses = 0
    for house, stages in house_group.items():
        if all(str(x) == "Completed" for x in stages):
            fully_dispatch_houses += 1

    overall_houses_impacted = total_houses - fully_dispatch_houses
    total_possible_progress = total_products_scope * 8
    achieved_progress = product_df["StageRank"].sum()
    overall_pending = len(product_df[product_df["StageRank"] < 8])

    overall_completion = round((achieved_progress / total_possible_progress) * 100, 2) if total_possible_progress > 0 else 0
    kpi_rows.append(["OVERALL COMPLETION", total_products_scope, overall_pending, overall_houses_impacted, f"{overall_completion}%"])

    kpi_df = pd.DataFrame(kpi_rows, columns=["Stage", "Total Products", "Pending Products", "Houses Impacted", "Completion %"])
    kpi_df.index = kpi_df.index + 1
    st.dataframe(kpi_df, use_container_width=True, height=400)

    st.subheader("🔍 House Wise Audit Analyzer")

    audit_stage_options = workflow_stages

    if "selected_audit_stage" not in st.session_state:
        st.session_state.selected_audit_stage = "Yet To Start"

    audit_preview_counts = {}
    for stg in audit_stage_options:
        audit_preview_counts[stg] = len(product_df[product_df["Current Stage"] == stg])

    stage_cols = st.columns(len(audit_stage_options))

    for i, stg in enumerate(audit_stage_options):
        with stage_cols[i]:
            if st.button(f"{stg} ({audit_preview_counts[stg]})", use_container_width=True):
                st.session_state.selected_audit_stage = stg

    audit_stage = st.session_state.selected_audit_stage

    audit_rows = []
    pending_exception_rows = []

    for house_id in sorted(master_house_df["HouseID"].unique()):
        house_products = product_df[product_df["HouseID"] == house_id].copy()
        house_no = master_house_df[master_house_df["HouseID"] == house_id]["House"].iloc[0]

        if house_products.empty:
            audit_rows.append([house_no, 0, 0, 0, "🔴 Not Started"])
            continue

        total_house_products = len(house_products)
        audit_rank = stage_rank[audit_stage]

        pending_df = house_products[house_products["Current Stage"] == audit_stage]

        if audit_stage == "Yet To Start":
            completed_df = house_products[house_products["StageRank"] > 0]
        elif audit_stage == "Dispatch":
            completed_df = house_products[house_products["StageRank"] == 8]
        else:
            completed_df = house_products[house_products["StageRank"] > audit_rank]

        completed_count = len(completed_df)
        pending_count = len(pending_df)

        if completed_count == total_house_products:
            house_status = "✅ Fully Completed"
        elif completed_count > 0:
            house_status = "🟡 Partial"
        elif pending_count > 0:
            house_status = "🔴 Not Started"
        else:
            house_status = "🔴 Not Started"

        audit_rows.append([house_no, total_house_products, completed_count, pending_count, house_status])

        for _, prow in pending_df.iterrows():
            pending_exception_rows.append([house_no, prow["Product"], f"{audit_stage} Pending"])

    audit_df = pd.DataFrame(audit_rows, columns=[
        "Unit",
        "Total Products",
        f"Completed at {audit_stage}",
        f"Pending at {audit_stage}",
        "House Status"
    ])

    st.subheader(f"🏠 {audit_stage} - House Audit Summary")
    st.dataframe(audit_df, use_container_width=True, height=420)

    fully_completed_houses = len(audit_df[audit_df["House Status"] == "✅ Fully Completed"])
    partial_houses = len(audit_df[audit_df["House Status"] == "🟡 Partial"])
    not_started_houses = len(audit_df[audit_df["House Status"] == "🔴 Not Started"])

    a1, a2, a3 = st.columns(3)
    a1.metric("✅ Fully Completed Houses", fully_completed_houses)
    a2.metric("🟡 Partial Houses", partial_houses)
    a3.metric("🔴 Not Started Houses", not_started_houses)

    st.subheader(f"📌 Pending Product Exception List - {audit_stage}")

    if pending_exception_rows:
        pending_df2 = pd.DataFrame(pending_exception_rows, columns=["Unit", "Pending Product", "Why Pending"])
        st.dataframe(pending_df2, use_container_width=True, height=420)
    else:
        st.success(f"No products are currently pending inside {audit_stage}.")
