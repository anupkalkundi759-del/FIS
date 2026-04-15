def show_product_tracking(conn, cur):
    import streamlit as st
    import pandas as pd

    st.title("🔎 Product Tracking")

    # ================= INLINE FILTER + SEARCH =================
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    # PROJECT
    cur.execute("SELECT DISTINCT project_name FROM projects ORDER BY project_name")
    projects = ["All"] + [p[0] for p in cur.fetchall()]
    selected_project = col1.selectbox("Project", projects)

    # UNIT
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

    # HOUSE
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

    # STAGE
    cur.execute("SELECT DISTINCT stage_name FROM stages ORDER BY stage_name")
    stages = ["All"] + [s[0] for s in cur.fetchall()]
    selected_stage = col4.selectbox("Stage", stages)

    # STATUS
    statuses = ["All", "Not Started", "In Progress", "Completed"]
    selected_status = col5.selectbox("Status", statuses)

    # SEARCH
    search = col6.text_input("Search")

    # ================= QUERY =================
    query = """
        SELECT 
            pm.product_code,
            pm.product_type,   -- ✅ TYPE BACK
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
            WHERE product_instance_id = p.product_instance_id
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

    query += " ORDER BY pr.project_name, u.unit_name, h.house_no"

    cur.execute(query, tuple(params))
    data = cur.fetchall()

    df = pd.DataFrame(data, columns=[
        "Product",
        "Type",   # ✅ BACK
        "Project",
        "Unit",
        "House",
        "Stage",
        "Status",
        "Timestamp"
    ])

    if df.empty:
        st.warning("No data found")
        return

    # ================= DATE =================
    df["Date"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    df["Date"] = df["Date"].dt.tz_localize("UTC").dt.tz_convert("Asia/Kolkata")
    df["Date"] = df["Date"].dt.strftime("%d-%m-%Y")

    df = df.drop(columns=["Timestamp"])

    # ================= PROGRESS =================
    stage_order = {
        "Measurement": 1,
        "Cutting": 2,
        "Production": 3,
        "Pre Assembly": 4,
        "Polishing": 5,
        "Final Assembly": 6,
        "Dispatch": 7
    }

    TOTAL = len(stage_order)

    def progress(row):
        stage = row["Stage"]
        status = row["Status"]

        if stage not in stage_order:
            return 0

        base = (stage_order[stage] - 1) / TOTAL * 100

        if status == "Completed":
            return round((stage_order[stage] / TOTAL) * 100, 1)
        elif status == "In Progress":
            return round(base + (100 / TOTAL) * 0.5, 1)
        else:
            return round(base, 1)

    df["Progress %"] = df.apply(progress, axis=1)

    # ================= DISPLAY =================
    st.dataframe(df, use_container_width=True)
