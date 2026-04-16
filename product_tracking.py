def show_product_tracking(conn, cur):
    import streamlit as st
    import pandas as pd

    st.title("🔎 Product Tracking")

    # ================= MAIN FILTERS =================
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    cur.execute("SELECT DISTINCT project_name FROM projects ORDER BY project_name")
    projects = ["All"] + [p[0] for p in cur.fetchall()]
    selected_project = col1.selectbox("Project", projects)

    if selected_project == "All":
        cur.execute("SELECT DISTINCT unit_name FROM units ORDER BY unit_name")
        units = ["All"] + [u[0] for u in cur.fetchall()]
    else:
        cur.execute("""
            SELECT DISTINCT u.unit_name
            FROM units u
            JOIN projects p ON u.project_id = p.project_id
            WHERE p.project_name = %s
        """, (selected_project,))
        units = ["All"] + [u[0] for u in cur.fetchall()]

    selected_unit = col2.selectbox("Unit", units)

    if selected_unit == "All":
        cur.execute("SELECT DISTINCT house_no FROM houses ORDER BY house_no")
        houses = ["All"] + [h[0] for h in cur.fetchall()]
    else:
        cur.execute("""
            SELECT DISTINCT h.house_no
            FROM houses h
            JOIN units u ON h.unit_id = u.unit_id
            WHERE u.unit_name = %s
        """, (selected_unit,))
        houses = ["All"] + [h[0] for h in cur.fetchall()]

    selected_house = col3.selectbox("House", houses)

    cur.execute("SELECT DISTINCT stage_name FROM stages ORDER BY stage_name")
    stages = ["All"] + [s[0] for s in cur.fetchall()]
    selected_stage = col4.selectbox("Stage", stages)

    statuses = ["All", "Not Started", "In Progress", "Completed"]
    selected_status = col5.selectbox("Status", statuses)

    search = col6.text_input("Search")

    # ================= MAIN TABLE =================
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
            SELECT stage_id, status, timestamp
            FROM tracking_log
            WHERE product_instance_id = p.id
            ORDER BY timestamp DESC
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

    df = pd.DataFrame(data, columns=[
        "Product", "Type", "Orientation",
        "Project", "Unit", "House",
        "Stage", "Status", "Timestamp"
    ])

    if df.empty:
        st.warning("No data found")
        return

    df["Date & Time"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    df["Date & Time"] = df["Date & Time"].dt.tz_localize("UTC").dt.tz_convert("Asia/Kolkata")
    df["Date & Time"] = df["Date & Time"].dt.strftime("%d-%m-%Y %H:%M")
    df = df.drop(columns=["Timestamp"])

    st.dataframe(df, use_container_width=True)

    # ================= NEW BREAKDOWN FILTER =================
    st.divider()
    st.subheader("🎯 Breakdown Filters")

    b1, b2 = st.columns(2)

    with b1:
        breakdown_project = st.selectbox("Project (Breakdown)", projects, key="b_proj")

    with b2:
        if breakdown_project == "All":
            breakdown_units = ["All"]
        else:
            cur.execute("""
                SELECT DISTINCT u.unit_name
                FROM units u
                JOIN projects p ON u.project_id = p.project_id
                WHERE p.project_name = %s
            """, (breakdown_project,))
            breakdown_units = ["All"] + [u[0] for u in cur.fetchall()]

        breakdown_unit = st.selectbox("Unit (Breakdown)", breakdown_units, key="b_unit")

    # ================= PRODUCT STATUS BREAKDOWN =================
    st.subheader("📊 Product Status Breakdown")

    query2 = """
        WITH latest_stage AS (
            SELECT 
                p.id,
                pr.project_name,
                u.unit_name,
                pm.product_code,
                s.stage_name,
                t.status,
                ROW_NUMBER() OVER (
                    PARTITION BY p.id 
                    ORDER BY s.sequence DESC, t.timestamp DESC
                ) as rn
            FROM products p
            JOIN products_master pm ON p.product_id = pm.product_id
            JOIN houses h ON p.house_id = h.house_id
            JOIN units u ON h.unit_id = u.unit_id
            JOIN projects pr ON u.project_id = pr.project_id
            LEFT JOIN tracking_log t ON t.product_instance_id = p.id
            LEFT JOIN stages s ON t.stage_id = s.stage_id
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
        )
        SELECT 
            project_name,
            unit_name,
            product_code,
            COUNT(*) AS total,
            COUNT(CASE WHEN stage_name = 'Dispatch' AND status = 'Completed' THEN 1 END) AS completed,
            COUNT(*) - COUNT(CASE WHEN stage_name = 'Dispatch' AND status = 'Completed' THEN 1 END) AS remaining,
            COUNT(CASE WHEN stage_name = 'Cutting' THEN 1 END) AS cutting,
            COUNT(CASE WHEN stage_name = 'Production' THEN 1 END) AS production,
            COUNT(CASE WHEN stage_name = 'Pre Assembly' THEN 1 END) AS pre_assembly,
            COUNT(CASE WHEN stage_name = 'Polishing' THEN 1 END) AS polishing,
            COUNT(CASE WHEN stage_name = 'Final Assembly' THEN 1 END) AS final_assembly
        FROM latest_stage
        WHERE rn = 1
        GROUP BY project_name, unit_name, product_code
        ORDER BY project_name, unit_name, product_code
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
