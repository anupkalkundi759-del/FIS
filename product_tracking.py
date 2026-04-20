def show_product_tracking(conn, cur):
    import streamlit as st
    import pandas as pd

    st.title("🔎 Product Tracking")

    # ================= CACHE =================
    @st.cache_data
    def get_projects():
        cur.execute("SELECT DISTINCT project_name FROM projects ORDER BY project_name")
        return ["All"] + [p[0] for p in cur.fetchall()]

    @st.cache_data
    def get_units(project):
        if project == "All":
            cur.execute("SELECT DISTINCT unit_name FROM units ORDER BY unit_name")
        else:
            cur.execute("""
                SELECT DISTINCT u.unit_name
                FROM units u
                JOIN projects p ON u.project_id = p.project_id
                WHERE p.project_name = %s
            """, (project,))
        return ["All"] + [u[0] for u in cur.fetchall()]

    @st.cache_data
    def get_houses(unit):
        if unit == "All":
            cur.execute("SELECT DISTINCT house_no FROM houses ORDER BY house_no")
        else:
            cur.execute("""
                SELECT DISTINCT h.house_no
                FROM houses h
                JOIN units u ON h.unit_id = u.unit_id
                WHERE u.unit_name = %s
            """, (unit,))
        return ["All"] + [h[0] for h in cur.fetchall()]

    @st.cache_data
    def get_stages():
        cur.execute("SELECT DISTINCT stage_name FROM stages ORDER BY stage_name")
        return ["All"] + [s[0] for s in cur.fetchall()]

    # ================= FILTERS =================
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    selected_project = col1.selectbox("Project", get_projects())
    selected_unit = col2.selectbox("Unit", get_units(selected_project))
    selected_house = col3.selectbox("House", get_houses(selected_unit))
    selected_stage = col4.selectbox("Stage", get_stages())
    selected_status = col5.selectbox("Status", ["All", "Not Started", "In Progress", "Completed"])
    search = col6.text_input("Search")

    # ================= MAIN QUERY =================
    with st.spinner("Loading data..."):

        query = """
        WITH latest_stage AS (
            SELECT DISTINCT ON (product_instance_id)
                product_instance_id,
                stage_id,
                status,
                timestamp
            FROM tracking_log
            ORDER BY product_instance_id, timestamp DESC
        )
        SELECT 
            pm.product_code,
            pm.product_category,
            p.orientation,
            pr.project_name,
            u.unit_name,
            h.house_no,
            COALESCE(s.stage_name, 'Not Started') AS stage,
            COALESCE(ls.status, 'Not Started') AS status,
            ls.timestamp
        FROM products p
        JOIN products_master pm ON p.product_id = pm.product_id
        JOIN houses h ON p.house_id = h.house_id
        JOIN units u ON h.unit_id = u.unit_id
        JOIN projects pr ON u.project_id = pr.project_id
        LEFT JOIN latest_stage ls ON ls.product_instance_id = p.product_instance_id
        LEFT JOIN stages s ON ls.stage_id = s.stage_id
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
            query += " AND ls.status = %s"
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

    # ================= BREAKDOWN FILTER =================
    st.divider()
    st.subheader("🎯 Breakdown Filters")

    b1, b2 = st.columns(2)

    breakdown_project = b1.selectbox("Project (Breakdown)", get_projects(), key="b_proj")
    breakdown_unit = b2.selectbox("Unit (Breakdown)", get_units(breakdown_project), key="b_unit")

    # ================= STATUS BREAKDOWN =================
    st.subheader("📊 Product Status Breakdown")

    with st.spinner("Calculating breakdown..."):

        query2 = """
        WITH latest_stage AS (
            SELECT DISTINCT ON (product_instance_id)
                product_instance_id,
                stage_id,
                status
            FROM tracking_log
            ORDER BY product_instance_id, timestamp DESC
        )
        SELECT 
            pr.project_name,
            u.unit_name,
            pm.product_code,
            COUNT(*) AS total,
            COUNT(CASE WHEN s.stage_name = 'Dispatch' AND ls.status = 'Completed' THEN 1 END) AS completed,
            COUNT(*) - COUNT(CASE WHEN s.stage_name = 'Dispatch' AND ls.status = 'Completed' THEN 1 END) AS remaining,
            COUNT(CASE WHEN s.stage_name = 'Cutting' THEN 1 END) AS cutting,
            COUNT(CASE WHEN s.stage_name = 'Production' THEN 1 END) AS production,
            COUNT(CASE WHEN s.stage_name = 'Pre Assembly' THEN 1 END) AS pre_assembly,
            COUNT(CASE WHEN s.stage_name = 'Polishing' THEN 1 END) AS polishing,
            COUNT(CASE WHEN s.stage_name = 'Final Assembly' THEN 1 END) AS final_assembly
        FROM products p
        JOIN products_master pm ON p.product_id = pm.product_id
        JOIN houses h ON p.house_id = h.house_id
        JOIN units u ON h.unit_id = u.unit_id
        JOIN projects pr ON u.project_id = pr.project_id
        LEFT JOIN latest_stage ls ON ls.product_instance_id = p.product_instance_id
        LEFT JOIN stages s ON ls.stage_id = s.stage_id
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
        ORDER BY pr.project_name, u.unit_name, pm.product_code
        """

        cur.execute(query2, tuple(params2))
        status_data = cur.fetchall()

    if status_data:
        status_df = pd.DataFrame(status_data, columns=[
            "Project", "Unit", "Product",
            "Total", "Completed", "Remaining",
            "Cutting", "Production", "Pre Assembly", "Polishing", "Final Assembly"
        ])
        st.dataframe(status_df, use_container_width=True)
    else:
        st.warning("No product status data available")
