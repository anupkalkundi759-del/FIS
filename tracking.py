def show_tracking(conn, cur):
    import streamlit as st

    st.title("🏭 Production Tracker")

    # ================= PROJECT =================
    cur.execute("SELECT project_id, project_name FROM projects ORDER BY project_name")
    projects = cur.fetchall()

    if not projects:
        st.warning("No projects found")
        return

    project_dict = {p[1]: p[0] for p in projects}
    selected_project = st.selectbox("Select Project", list(project_dict.keys()))
    project_id = project_dict[selected_project]

    # ================= UNIT =================
    cur.execute("""
        SELECT unit_id, unit_name
        FROM units
        WHERE project_id=%s
        ORDER BY unit_name
    """, (project_id,))
    units = cur.fetchall()

    if not units:
        st.warning("No units found")
        return

    unit_dict = {u[1]: u[0] for u in units}
    selected_unit = st.selectbox("Select Unit", list(unit_dict.keys()))
    unit_id = unit_dict[selected_unit]

    # ================= HOUSE =================
    cur.execute("""
        SELECT house_id, house_no
        FROM houses
        WHERE unit_id=%s
        ORDER BY house_no
    """, (unit_id,))
    houses = cur.fetchall()

    if not houses:
        st.warning("No houses found")
        return

    house_dict = {h[1]: h[0] for h in houses}
    selected_house_no = st.selectbox("Select House", list(house_dict.keys()))
    house_id = house_dict[selected_house_no]

    # ================= PRODUCT LIST WITH PROGRESS =================
    cur.execute("""
        SELECT 
            pm.product_id,
            pm.product_code,
            COUNT(p.id) AS total,
            COUNT(t.id) FILTER (WHERE t.status='Completed') AS completed
        FROM products p
        JOIN products_master pm ON p.product_id = pm.product_id
        LEFT JOIN tracking_log t ON p.id = t.product_instance_id
        WHERE p.house_id = %s
        GROUP BY pm.product_id, pm.product_code
        ORDER BY pm.product_code
    """, (house_id,))

    products = cur.fetchall()

    if not products:
        st.warning("No products found")
        return

    # UI: show progress
    product_dict = {
        f"{p[1]}  ({p[3]}/{p[2]})": p[0]
        for p in products
    }

    selected_product_label = st.selectbox("Select Product", list(product_dict.keys()))
    product_id = product_dict[selected_product_label]

    # ================= GET NEXT PENDING ITEM =================
    cur.execute("""
        SELECT p.id
        FROM products p
        LEFT JOIN tracking_log t 
            ON p.id = t.product_instance_id 
            AND t.status = 'Completed'
        WHERE p.house_id = %s
        AND p.product_id = %s
        AND t.id IS NULL
        LIMIT 1
    """, (house_id, product_id))

    row = cur.fetchone()

    if not row:
        st.success("✅ All items completed for this product")
        return

    product_instance_id = row[0]

    # ================= CURRENT STAGE =================
    cur.execute("""
        SELECT COALESCE(MAX(s.sequence), 0)
        FROM tracking_log t
        JOIN stages s ON t.stage_id = s.stage_id
        WHERE t.product_instance_id = %s
    """, (product_instance_id,))

    current_stage = cur.fetchone()[0]
    st.info(f"Current Item Stage: {current_stage}")

    # ================= STAGES =================
    cur.execute("SELECT stage_id, stage_name, sequence FROM stages ORDER BY sequence")
    stages = cur.fetchall()

    stage_map = {s[1]: (s[0], s[2]) for s in stages}
    selected_stage_name = st.selectbox("Select Stage", list(stage_map.keys()))
    stage_id, selected_sequence = stage_map[selected_stage_name]

    # ================= VALIDATION =================
    if selected_sequence > current_stage + 1:
        st.error("❌ Complete previous stage first")
        return

    status = st.selectbox("Status", ["Started", "In Progress", "Completed"])

    # ================= SUBMIT =================
    if st.button("Update Item"):
        cur.execute("""
            INSERT INTO tracking_log (product_instance_id, stage_id, status)
            VALUES (%s, %s, %s)
        """, (product_instance_id, stage_id, status))

        conn.commit()
        st.success("✅ Item Updated")
