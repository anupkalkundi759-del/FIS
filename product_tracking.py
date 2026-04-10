def show_product_tracking(conn, cur):
    import streamlit as st
    import pandas as pd

    st.title("🔎 Product Tracking")

    # ================= FILTERS =================
    col1, col2, col3, col4, col5 = st.columns(5)

    cur.execute("SELECT DISTINCT project_name FROM projects")
    projects = ["All"] + [p[0] for p in cur.fetchall()]

    cur.execute("SELECT DISTINCT unit_name FROM units")
    units = ["All"] + [u[0] for u in cur.fetchall()]

    cur.execute("SELECT DISTINCT house_no FROM houses")
    houses = ["All"] + [h[0] for h in cur.fetchall()]

    selected_project = col1.selectbox("Project", projects)
    selected_unit = col2.selectbox("Unit", units)
    selected_house = col3.selectbox("House", houses)
    search = col4.text_input("Search Product")
    limit = col5.selectbox("Rows", [50, 100, 200])

    # ================= QUERY =================
    query = """
        SELECT 
            pm.product_code,
            pm.product_code AS type,
            p.orientation,
            pr.project_name,
            u.unit_name,
            h.house_no,

            COALESCE(s.stage_name, 'Not Started') AS stage,
            COALESCE(t.status, 'Not Started') AS status,

            (t.timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata') AS ist_time

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

    if search:
        query += " AND pm.product_code ILIKE %s"
        params.append(f"%{search}%")

    query += " ORDER BY pr.project_name, u.unit_name, h.house_no LIMIT %s"
    params.append(limit)

    cur.execute(query, tuple(params))
    data = cur.fetchall()

    # ================= DATAFRAME =================
    df = pd.DataFrame(data, columns=[
        "Product", "Type", "Orientation",
        "Project", "Unit", "House",
        "Stage", "Status", "Date & Time"
    ])

    # ================= PROGRESS =================
    df["Progress %"] = df["Status"].map({
        "Not Started": 0,
        "In Progress": 50,
        "Completed": 100
    }).fillna(0)

    # ================= CLEAN =================
    df["Date & Time"] = df["Date & Time"].astype(str).replace("None", "-")

    st.dataframe(df, use_container_width=True)
