def show_product_tracking(conn, cur):
    import streamlit as st
    import pandas as pd

    st.title("🔎 Product Tracking")

    # ================= FILTER DROPDOWNS =================
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    # Project
    cur.execute("SELECT DISTINCT project_name FROM projects ORDER BY project_name")
    projects = ["All"] + [p[0] for p in cur.fetchall()]
    selected_project = col1.selectbox("Project", projects)

    # Unit
    if selected_project == "All":
        cur.execute("SELECT DISTINCT unit_name FROM units ORDER BY unit_name")
    else:
        cur.execute("""
            SELECT DISTINCT u.unit_name
            FROM units u
            JOIN projects p ON u.project_id = p.project_id
            WHERE p.project_name = %s
        """, (selected_project,))
    units = ["All"] + [u[0] for u in cur.fetchall()]
    selected_unit = col2.selectbox("Unit", units)

    # House
    if selected_unit == "All":
        cur.execute("SELECT DISTINCT house_no FROM houses ORDER BY house_no")
    else:
        cur.execute("""
            SELECT DISTINCT h.house_no
            FROM houses h
            JOIN units u ON h.unit_id = u.unit_id
            WHERE u.unit_name = %s
        """, (selected_unit,))
    houses = ["All"] + [h[0] for h in cur.fetchall()]
    selected_house = col3.selectbox("House", houses)

    # Stage
    cur.execute("SELECT DISTINCT stage_name FROM stages ORDER BY stage_name")
    stages = ["All"] + [s[0] for s in cur.fetchall()]
    selected_stage = col4.selectbox("Stage", stages)

    # Status
    selected_status = col5.selectbox("Status", ["All", "Not Started", "In Progress", "Completed"])

    # Search
    search = col6.text_input("Search")

    # ================= MAIN QUERY (FAST) =================
    with st.spinner("Loading data..."):

        query = """
        SELECT 
            pm.product_code,
            pm.product_category,
            p.orientation,
            pr.project_name,
            u.unit_name,
            h.house_no,
            COALESCE(s.stage_name, 'Not Started') AS stage,
            COALESCE(t.status, 'Not Started') AS status,
            t.timestamp
        FROM products p
        JOIN products_master pm ON p.product_id = pm.product_id
        JOIN houses h ON p.house_id = h.house_id
        JOIN units u ON h.unit_id = u.unit_id
        JOIN projects pr ON u.project_id = pr.project_id

        LEFT JOIN LATERAL (
            SELECT t1.stage_id, t1.status, t1.timestamp
            FROM tracking_log t1
            WHERE t1.product_instance_id = p.product_instance_id
            ORDER BY t1.timestamp DESC
            LIMIT 1
        ) t ON TRUE

        LEFT JOIN stages s ON t.stage_id = s.stage_id

        WHERE 1=1
        """

        params = []

        if selected_project != "All":
            query += " AND pr.project_name = %s"
            params.append(selected_project)

        if selected_unit != "All":
            query += " AND u.unit_name = %s"
            params.append(selected_unit)

        if selected_house != "All":
            query += " AND h.house_no = %s"
            params.append(selected_house)

        if selected_stage != "All":
            query += " AND s.stage_name = %s"
            params.append(selected_stage)

        if selected_status != "All":
            query += " AND t.status = %s"
            params.append(selected_status)

        if search:
            query += " AND pm.product_code ILIKE %s"
            params.append(f"%{search}%")

        cur.execute(query, tuple(params))
        data = cur.fetchall()

    # ================= DATAFRAME =================
    if not data:
        st.warning("No data found")
        return

    df = pd.DataFrame(data, columns=[
        "Product", "Type", "Orientation",
        "Project", "Unit", "House",
        "Stage", "Status", "Timestamp"
    ])

    df["Date & Time"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    df["Date & Time"] = df["Date & Time"].dt.tz_localize("UTC").dt.tz_convert("Asia/Kolkata")
    df["Date & Time"] = df["Date & Time"].dt.strftime("%d-%m-%Y %H:%M")
    df = df.drop(columns=["Timestamp"])

    st.dataframe(df, use_container_width=True)

    # ================= BREAKDOWN (UNCHANGED) =================
    st.divider()
    st.subheader("🎯 Breakdown Filters")

    b1, b2 = st.columns(2)

    cur.execute("SELECT DISTINCT project_name FROM projects ORDER BY project_name")
    projects = ["All"] + [p[0] for p in cur.fetchall()]
    breakdown_project = b1.selectbox("Project (Breakdown)", projects)

    if breakdown_project == "All":
        cur.execute("SELECT DISTINCT unit_name FROM units ORDER BY unit_name")
    else:
        cur.execute("""
            SELECT DISTINCT u.unit_name
            FROM units u
            JOIN projects p ON u.project_id = p.project_id
            WHERE p.project_name = %s
        """, (breakdown_project,))
    units = ["All"] + [u[0] for u in cur.fetchall()]
    breakdown_unit = b2.selectbox("Unit (Breakdown)", units)

    if st.button("Load Breakdown"):

        with st.spinner("Calculating breakdown..."):

            query2 = """
            SELECT 
                pr.project_name,
                u.unit_name,
                pm.product_code,
                COUNT(*) AS total
            FROM products p
            JOIN products_master pm ON p.product_id = pm.product_id
            JOIN houses h ON p.house_id = h.house_id
            JOIN units u ON h.unit_id = u.unit_id
            JOIN projects pr ON u.project_id = pr.project_id
            WHERE 1=1
            """

            params2 = []

            if breakdown_project != "All":
                query2 += " AND pr.project_name = %s"
                params2.append(breakdown_project)

            if breakdown_unit != "All":
                query2 += " AND u.unit_name = %s"
                params2.append(breakdown_unit)

            query2 += """
            GROUP BY pr.project_name, u.unit_name, pm.product_code
            ORDER BY pr.project_name, u.unit_name
            """

            cur.execute(query2, tuple(params2))
            status_data = cur.fetchall()

        if status_data:
            status_df = pd.DataFrame(status_data, columns=[
                "Project", "Unit", "Product", "Total"
            ])
            st.dataframe(status_df, use_container_width=True)
        else:
            st.warning("No product status data available")
