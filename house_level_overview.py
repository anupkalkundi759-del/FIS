def show_dashboard(conn, cur):
    import streamlit as st
    import pandas as pd

    st.title("📊 Stage Breakdown Monitor")

    stage_order = {
        "Design & Engineering": 1,
        "Production": 2,
        "Pre Assembly": 3,
        "Polishing": 4,
        "Final Assembly": 5,
        "Dispatch": 6
    }

    selected_stage = st.selectbox("🔍 Select Workflow Stage", list(stage_order.keys()))
    selected_stage_rank = stage_order[selected_stage]

    # ================= LIVE DATA QUERY =================
    query = """
    WITH latest_log AS (
        SELECT
            product_instance_id,
            stage_id,
            status,
            timestamp,
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
        pr.quantity,
        s.stage_name,
        ll.status,
        ll.timestamp

    FROM products pr
    JOIN products_master pm ON pr.product_id = pm.product_id
    JOIN houses h ON pr.house_id = h.house_id
    JOIN units u ON h.unit_id = u.unit_id
    JOIN projects p ON u.project_id = p.project_id

    LEFT JOIN latest_log ll
        ON pr.id = ll.product_instance_id
        AND ll.rn = 1

    LEFT JOIN stages s
        ON ll.stage_id = s.stage_id

    ORDER BY p.project_name, u.unit_name, h.house_no
    """

    cur.execute(query)
    rows = cur.fetchall()

    if not rows:
        st.warning("No data available")
        return

    live_df = pd.DataFrame(rows, columns=[
        "Project", "Unit", "House",
        "Product", "Type", "Orientation", "Qty",
        "Current Stage", "Current Status", "Timestamp"
    ])

    live_df["Current Rank"] = live_df["Current Stage"].map(stage_order).fillna(0)

    live_df["Stage Result"] = live_df["Current Rank"].apply(
        lambda x: "Completed" if x >= selected_stage_rank else "Pending"
    )

    # ================= PROJECT LEVEL =================
    st.subheader(f"🏗 Project Level Status - {selected_stage}")

    project_df = live_df.groupby("Project").agg(
        Total_Products=("Qty", "sum"),
        Completed=("Qty", lambda x: x[live_df.loc[x.index, "Stage Result"] == "Completed"].sum()),
        Pending=("Qty", lambda x: x[live_df.loc[x.index, "Stage Result"] == "Pending"].sum())
    ).reset_index()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Projects", project_df["Project"].nunique())
    k2.metric("Total Products", int(project_df["Total_Products"].sum()))
    k3.metric(f"{selected_stage} Completed", int(project_df["Completed"].sum()))
    k4.metric(f"{selected_stage} Pending", int(project_df["Pending"].sum()))

    st.dataframe(project_df, use_container_width=True, height=250)

    # ================= DRILLDOWN FILTERS =================
    st.divider()
    st.subheader("📌 Drilldown Filters")

    c1, c2, c3 = st.columns(3)

    with c1:
        selected_project = st.selectbox("Select Project", ["All"] + sorted(live_df["Project"].unique().tolist()))

    unit_filtered = live_df.copy()
    if selected_project != "All":
        unit_filtered = unit_filtered[unit_filtered["Project"] == selected_project]

    with c2:
        selected_unit = st.selectbox("Select Unit", ["All"] + sorted(unit_filtered["Unit"].unique().tolist()))

    house_filtered = unit_filtered.copy()
    if selected_unit != "All":
        house_filtered = house_filtered[house_filtered["Unit"] == selected_unit]

    with c3:
        selected_house = st.selectbox("Select House", ["All"] + sorted(house_filtered["House"].astype(str).unique().tolist()))

    # ================= UNIT LEVEL =================
    st.subheader(f"🏢 Unit Level Status - {selected_stage}")

    unit_df = unit_filtered.groupby("Unit").agg(
        Total_Products=("Qty", "sum"),
        Completed=("Qty", lambda x: x[unit_filtered.loc[x.index, "Stage Result"] == "Completed"].sum()),
        Pending=("Qty", lambda x: x[unit_filtered.loc[x.index, "Stage Result"] == "Pending"].sum())
    ).reset_index()

    st.dataframe(unit_df, use_container_width=True, height=220)

    # ================= HOUSE LEVEL =================
    st.subheader(f"🏠 House Level Status - {selected_stage}")

    house_df = house_filtered.groupby("House").agg(
        Total_Products=("Qty", "sum"),
        Completed=("Qty", lambda x: x[house_filtered.loc[x.index, "Stage Result"] == "Completed"].sum()),
        Pending=("Qty", lambda x: x[house_filtered.loc[x.index, "Stage Result"] == "Pending"].sum())
    ).reset_index()

    st.dataframe(house_df, use_container_width=True, height=220)

    # ================= PRODUCT LEVEL =================
    st.subheader(f"🧩 Product Level Detailed Status - {selected_stage}")

    final_df = house_filtered.copy()
    if selected_house != "All":
        final_df = final_df[final_df["House"].astype(str) == selected_house]

    product_df = final_df[[
        "Project", "Unit", "House", "Product", "Type",
        "Orientation", "Qty", "Current Stage", "Stage Result", "Timestamp"
    ]].copy()

    product_df["Timestamp"] = pd.to_datetime(product_df["Timestamp"], errors="coerce").dt.strftime("%d-%m-%Y %I:%M %p")

    st.dataframe(product_df, use_container_width=True, height=420)
