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
            p.product_instance_id,
            pm.product_code,
            p.orientation,
            pr.project_name,
            u.unit_name,
            h.house_no,

            COALESCE(MAX(t.stage), 'Not Started') as stage,
            COALESCE(MAX(t.status), 'Not Started') as status,
            MAX(t.updated_at) as last_update

        FROM products p
        JOIN products_master pm ON p.product_id = pm.product_id
        JOIN houses h ON p.house_id = h.house_id
        JOIN units u ON h.unit_id = u.unit_id
        JOIN projects pr ON u.project_id = pr.project_id

        LEFT JOIN tracking_log t 
            ON p.product_instance_id = t.product_instance_id

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

    query += """
        GROUP BY 
            p.product_instance_id,
            pm.product_code,
            p.orientation,
            pr.project_name,
            u.unit_name,
            h.house_no
        ORDER BY pr.project_name, u.unit_name, h.house_no
        LIMIT %s
    """

    params.append(limit)

    cur.execute(query, tuple(params))
    data = cur.fetchall()

    # ================= DISPLAY =================
    df = pd.DataFrame(data, columns=[
        "ID", "Product", "Orientation",
        "Project", "Unit", "House",
        "Stage", "Status", "Last Update"
    ])

    # 🔥 CLEAN DISPLAY
    df["Progress"] = df["Status"].map({
        "Not Started": "0%",
        "In Progress": "50%",
        "Completed": "100%"
    })

    df = df.drop(columns=["ID"])

    st.dataframe(df, use_container_width=True)

    # ================= QUICK STATS =================
    st.subheader("📊 Quick Stats")

    total = len(df)
    completed = len(df[df["Status"] == "Completed"])
    pending = len(df[df["Status"] == "Not Started"])

    col1, col2, col3 = st.columns(3)

    col1.metric("Total Items", total)
    col2.metric("Completed", completed)
    col3.metric("Pending", pending)
