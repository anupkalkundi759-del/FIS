def show_dashboard(conn, cur):
    import streamlit as st
    import pandas as pd

    st.title("📊 Stage Breakdown Monitor")

    # ================= WORKFLOW =================
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

    # ================= MASTER STRUCTURE COUNTS =================
    cur.execute("SELECT COUNT(*) FROM projects")
    total_projects_master = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM units")
    total_units_master = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM houses")
    total_houses_master = cur.fetchone()[0]

    # ================= LIVE PRODUCT QUERY =================
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

    # ================= TRUE 3 STATUS ENGINE =================
    def classify(row):
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

    df["Stage Result"] = df.apply(classify, axis=1)

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
        selected_houses = st.multiselect("Select House (Multiple Allowed)", house_options)

    temp3 = temp2.copy()
    if selected_houses:
        temp3 = temp3[temp3["House"].astype(str).isin(selected_houses)]

    # ================= KPI =================
    st.subheader(f"📈 {selected_stage} Summary")

    total_products = len(temp3)
    completed = (temp3["Stage Result"] == "Completed").sum()
    in_progress = (temp3["Stage Result"] == "In Progress").sum()
    pending = (temp3["Stage Result"] == "Pending").sum()

    visible_projects = temp3["Project"].nunique() if selected_project != "All" else total_projects_master
    visible_units = temp3["Unit"].nunique() if selected_unit != "All" else (temp1["Unit"].nunique() if selected_project != "All" else total_units_master)
    visible_houses = temp3["House"].nunique() if selected_houses else (temp2["House"].nunique() if selected_unit != "All" else (temp1["House"].nunique() if selected_project != "All" else total_houses_master))

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Projects", visible_projects)
    k2.metric("Units", visible_units)
    k3.metric("Houses", visible_houses)
    k4.metric("Total Products", total_products)

    k5, k6, k7 = st.columns(3)
    k5.metric("Completed", completed)
    k6.metric("In Progress", in_progress)
    k7.metric("Pending", pending)

    # ================= SMART SUMMARY =================
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

        st.subheader("🏢 Unit Performance Summary")

        unit_summary = temp1.groupby("Unit").agg(
            Houses=("House", "nunique"),
            Total_Products=("Product", "count"),
            Completed=("Stage Result", lambda x: (x == "Completed").sum()),
            In_Progress=("Stage Result", lambda x: (x == "In Progress").sum()),
            Pending=("Stage Result", lambda x: (x == "Pending").sum())
        ).reset_index()

        unit_summary["Completion_%"] = round(
            (unit_summary["Completed"] / unit_summary["Total_Products"]) * 100, 1
        )

        st.dataframe(unit_summary, use_container_width=True, height=220)

        st.subheader("🚨 Top Pending Products in Selected Project")

        product_summary = temp1.groupby("Product").agg(
            Total_Qty=("Product", "count"),
            Completed=("Stage Result", lambda x: (x == "Completed").sum()),
            In_Progress=("Stage Result", lambda x: (x == "In Progress").sum()),
            Pending=("Stage Result", lambda x: (x == "Pending").sum())
        ).reset_index().sort_values("Pending", ascending=False)

        st.dataframe(product_summary, use_container_width=True, height=320)

    else:
        st.subheader("🧩 Selected Unit / House Product Summary")

        summary = temp3.groupby(["Product", "Type", "Orientation"]).agg(
            Total_Qty=("Product", "count"),
            Completed=("Stage Result", lambda x: (x == "Completed").sum()),
            In_Progress=("Stage Result", lambda x: (x == "In Progress").sum()),
            Pending=("Stage Result", lambda x: (x == "Pending").sum())
        ).reset_index().sort_values("Pending", ascending=False)

        st.dataframe(summary, use_container_width=True, height=420)
