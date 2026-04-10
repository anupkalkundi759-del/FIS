def show_tracking(conn, cur):
    import streamlit as st

    st.title("🏭 Production Tracker")

    # PROJECT
    cur.execute("SELECT project_id, project_name FROM projects ORDER BY project_name")
    projects = cur.fetchall()
    project_dict = {p[1]: p[0] for p in projects}
    project = st.selectbox("Project", list(project_dict.keys()))
    project_id = project_dict[project]

    # UNIT
    cur.execute("SELECT unit_id, unit_name FROM units WHERE project_id=%s", (project_id,))
    units = cur.fetchall()
    unit_dict = {u[1]: u[0] for u in units}
    unit = st.selectbox("Unit", list(unit_dict.keys()))
    unit_id = unit_dict[unit]

    # HOUSE
    cur.execute("SELECT house_id, house_no FROM houses WHERE unit_id=%s", (unit_id,))
    houses = cur.fetchall()
    house_dict = {h[1]: h[0] for h in houses}
    house = st.selectbox("House", list(house_dict.keys()))
    house_id = house_dict[house]

    # PRODUCTS WITH ORIENTATION + COUNT
    cur.execute("""
        SELECT 
            pm.product_id,
            pm.product_code,
            p.orientation,
            COUNT(p.id) AS total,
            COUNT(t.id) FILTER (WHERE t.status='Completed') AS completed
        FROM products p
        JOIN products_master pm ON p.product_id = pm.product_id
        LEFT JOIN tracking_log t ON p.id = t.product_instance_id
        WHERE p.house_id = %s
        GROUP BY pm.product_id, pm.product_code, p.orientation
        ORDER BY pm.product_code, p.orientation
    """, (house_id,))

    data = cur.fetchall()

    product_map = {
        f"{d[1]} ({d[2]}) ({d[4]}/{d[3]})": (d[0], d[2])
        for d in data
    }

    product_label = st.selectbox("Select Product", list(product_map.keys()))
    product_id, orientation = product_map[product_label]

    # NEXT ITEM
    cur.execute("""
        SELECT p.id
        FROM products p
        LEFT JOIN tracking_log t 
            ON p.id = t.product_instance_id 
            AND t.status='Completed'
        WHERE p.house_id=%s
        AND p.product_id=%s
        AND p.orientation=%s
        AND t.id IS NULL
        LIMIT 1
    """, (house_id, product_id, orientation))

    row = cur.fetchone()

    if not row:
        st.success("✅ All items completed")
        return

    pid = row[0]

    # CURRENT STAGE
    cur.execute("""
        SELECT COALESCE(MAX(s.sequence),0)
        FROM tracking_log t
        JOIN stages s ON t.stage_id = s.stage_id
        WHERE t.product_instance_id=%s
    """, (pid,))
    stage = cur.fetchone()[0]

    st.info(f"Current Stage: {stage}")

    # STAGES
    cur.execute("SELECT stage_id, stage_name, sequence FROM stages ORDER BY sequence")
    stages = cur.fetchall()
    stage_map = {s[1]: (s[0], s[2]) for s in stages}

    s_name = st.selectbox("Stage", list(stage_map.keys()))
    stage_id, seq = stage_map[s_name]

    if seq > stage + 1:
        st.error("❌ Complete previous stage first")
        return

    status = st.selectbox("Status", ["Started", "In Progress", "Completed"])

    if st.button("Update"):
        cur.execute("""
            INSERT INTO tracking_log (product_instance_id, stage_id, status)
            VALUES (%s, %s, %s)
        """, (pid, stage_id, status))

        conn.commit()
        st.success("✅ Updated successfully")
