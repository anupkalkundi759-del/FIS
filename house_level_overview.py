def show_dashboard(conn, cur):
    import streamlit as st
    import pandas as pd

    st.title("📊 Stage Breakdown Monitor")

    # ================= STAGE ORDER =================
    stage_order = {
        "Design & Engineering": 1,
        "Production": 2,
        "Pre Assembly": 3,
        "Polishing": 4,
        "Final Assembly": 5,
        "Dispatch": 6
    }

    selected_stage = st.selectbox("🔍 Select Workflow Stage", list(stage_order.keys()))
    selected_rank = stage_order[selected_stage]

    # ================= MASTER LIVE QUERY =================
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
        s.stage_name,
        ll.status,
        ll.timestamp

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
        "Product", "Type", "Orientation",
        "Current Stage", "Current Status", "Timestamp"
    ])

    df["Current Rank"] = df["Current Stage"].map(stage_order).fillna(0)

    # ================= 3 STATUS ENGINE =================
    def get_stage_result(row):
        rank = row["Current Rank"]
        status = row["Current Status"]

        if rank > selected_rank:
            return "Completed"
        elif rank == selected_rank:
            if status == "Completed":
                return "Completed"
            elif status == "In Progress":
                return "In Progress"
            else:
                return "Pending"
        else:
            return "Pending"

    df["Stage Result"] = df.apply(get_stage_result, axis=1)

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
        selected_house = st.selectbox(
            "Select House",
            ["All"] + sorted(temp2["House"].astype(str).dropna().unique().tolist())
        )

    temp3 = temp2.copy()
    if selected_house != "All":
        temp3 = temp3[temp3["House"].astype(str) == selected_house]

    # ================= TOP KPI =================
    st.subheader(f"📈 {selected_stage} Summary")

    total_projects = temp3["Project"].nunique()
    total_units = temp3["Unit"].nunique()
    total_houses = temp3["House"].nunique()
    total_products = len(temp3)

    completed = (temp3["Stage Result"] == "Completed").sum()
    in_progress = (temp3["Stage Result"] == "In Progress").sum()
    pending = (temp3["Stage Result"] == "Pending").sum()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Projects", total_projects)
    k2.metric("Units", total_units)
    k3.metric("Houses", total_houses)
    k4.metric("Total Products", total_products)

    k5, k6, k7 = st.columns(3)
    k5.metric("Completed", completed)
    k6.metric("In Progress", in_progress)
    k7.metric("Pending", pending)

    # ================= CONDITIONAL SUMMARY =================
    if selected_project == "All":
        st.subheader("🏗 Project Wise Summary")

        summary = temp3.groupby("Project").agg(
            Total_Products=("Product", "count"),
            Completed=("Stage Result", lambda x: (x == "Completed").sum()),
            In_Progress=("Stage Result", lambda x: (x == "In Progress").sum()),
            Pending=("Stage Result", lambda x: (x == "Pending").sum())
        ).reset_index()

        st.dataframe(summary, use_container_width=True, height=260)

    elif selected_unit == "All":
        st.subheader("🏢 Unit Wise Summary")

        summary = temp3.groupby("Unit").agg(
            Houses=("House", "nunique"),
            Total_Products=("Product", "count"),
            Completed=("Stage Result", lambda x: (x == "Completed").sum()),
            In_Progress=("Stage Result", lambda x: (x == "In Progress").sum()),
            Pending=("Stage Result", lambda x: (x == "Pending").sum())
        ).reset_index()

        st.dataframe(summary, use_container_width=True, height=260)

    elif selected_house == "All":
        st.subheader("🏠 House Wise Summary")

        summary = temp3.groupby("House").agg(
            Total_Products=("Product", "count"),
            Completed=("Stage Result", lambda x: (x == "Completed").sum()),
            In_Progress=("Stage Result", lambda x: (x == "In Progress").sum()),
            Pending=("Stage Result", lambda x: (x == "Pending").sum())
        ).reset_index()

        st.dataframe(summary, use_container_width=True, height=260)

    else:
        st.subheader("🧩 Product Quantity Status Summary")

        summary = temp3.groupby(["Product", "Type", "Orientation"]).agg(
            Total_Qty=("Product", "count"),
            Completed=("Stage Result", lambda x: (x == "Completed").sum()),
            In_Progress=("Stage Result", lambda x: (x == "In Progress").sum()),
            Pending=("Stage Result", lambda x: (x == "Pending").sum())
        ).reset_index()

        st.dataframe(summary, use_container_width=True, height=420)
