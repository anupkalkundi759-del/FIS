def show_product_tracking(conn, cur):
    import streamlit as st
    import pandas as pd

    st.title("📦 Product Tracking")

    # ================= FILTERS =================
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        cur.execute("SELECT project_id, project_name FROM projects ORDER BY project_name")
        projects = cur.fetchall()
        project_dict = {p[1]: p[0] for p in projects}
        selected_project = st.selectbox("Project", ["All"] + list(project_dict.keys()))
        project_id = None if selected_project == "All" else project_dict[selected_project]

    with col2:
        if project_id:
            cur.execute("SELECT unit_id, unit_name FROM units WHERE project_id=%s ORDER BY unit_name", (project_id,))
        else:
            cur.execute("SELECT unit_id, unit_name FROM units ORDER BY unit_name")
        units = cur.fetchall()
        unit_dict = {u[1]: u[0] for u in units}
        selected_unit = st.selectbox("Unit", ["All"] + list(unit_dict.keys()))
        unit_id = None if selected_unit == "All" else unit_dict[selected_unit]

    with col3:
        if unit_id:
            cur.execute("SELECT house_id, house_no FROM houses WHERE unit_id=%s ORDER BY house_no", (unit_id,))
        elif project_id:
            cur.execute("""
                SELECT h.house_id, h.house_no
                FROM houses h
                JOIN units u ON h.unit_id = u.unit_id
                WHERE u.project_id=%s
                ORDER BY h.house_no
            """, (project_id,))
        else:
            cur.execute("SELECT house_id, house_no FROM houses ORDER BY house_no")

        houses = cur.fetchall()
        house_dict = {h[1]: h[0] for h in houses}
        selected_house = st.selectbox("House", ["All"] + list(house_dict.keys()))
        house_id = None if selected_house == "All" else house_dict[selected_house]

    with col4:
        selected_status = st.selectbox("Status", ["All", "In Progress", "Completed", "Dispatched"])

    # ================= LATEST LIVE PRODUCT STATUS =================
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
        h.house_no,
        pm.product_code,
        COALESCE(s.stage_name, 'Not Started') AS current_stage,
        COALESCE(ll.status, 'Not Started') AS current_status,
        ll.timestamp

    FROM products p
    JOIN houses h ON p.house_id = h.house_id
    JOIN units u ON h.unit_id = u.unit_id
    JOIN projects prj ON u.project_id = prj.project_id
    JOIN products_master pm ON p.product_id = pm.product_id

    LEFT JOIN latest_log ll
        ON p.product_instance_id = ll.product_instance_id
        AND ll.rn = 1

    LEFT JOIN stages s
        ON ll.stage_id = s.stage_id

    WHERE 1=1
    """

    params = []

    if project_id:
        query += " AND prj.project_id = %s"
        params.append(project_id)

    if unit_id:
        query += " AND u.unit_id = %s"
        params.append(unit_id)

    if house_id:
        query += " AND h.house_id = %s"
        params.append(house_id)

    if selected_status != "All":
        query += " AND COALESCE(ll.status, 'Not Started') = %s"
        params.append(selected_status)

    query += " ORDER BY h.house_no, pm.product_code"

    cur.execute(query, tuple(params))
    rows = cur.fetchall()

    if not rows:
        st.warning("No data found")
        return

    df = pd.DataFrame(rows, columns=[
        "House",
        "Product",
        "Current Stage",
        "Current Status",
        "Last Updated"
    ])

    df["Last Updated"] = pd.to_datetime(df["Last Updated"], errors="coerce").dt.strftime("%d-%m-%Y %I:%M %p")

    # ================= KPI =================
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Visible Products", len(df))
    k2.metric("In Progress", len(df[df["Current Status"] == "In Progress"]))
    k3.metric("Completed", len(df[df["Current Status"] == "Completed"]))
    k4.metric("Dispatched", len(df[df["Current Status"] == "Dispatched"]))

    # ================= TABLE =================
    st.dataframe(df, use_container_width=True, height=500)
