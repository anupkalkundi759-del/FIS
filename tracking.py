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

    # ================= PRODUCT =================
    cur.execute("""
        SELECT pm.product_id, pm.product_code
        FROM products p
        JOIN products_master pm ON p.product_id = pm.product_id
        WHERE p.house_id = %s
    """, (house_id,))
    products = cur.fetchall()

    if not products:
        st.warning("No products found")
        return

    product_dict = {p[1]: p[0] for p in products}
    selected_product = st.selectbox("Select Product", list(product_dict.keys()))
    product_id = product_dict[selected_product]

    # ================= COMPLETED STAGE =================
    cur.execute("""
        SELECT COALESCE(MAX(s.sequence), 0)
        FROM tracking_log t
        JOIN stages s ON t.stage_id = s.stage_id
        WHERE t.house_id=%s AND t.product_id=%s
    """, (house_id, product_id))

    completed_stage = cur.fetchone()[0]
    st.info(f"Completed Stage: {completed_stage}")

    # ================= STAGE =================
    cur.execute("SELECT stage_id, stage_name, sequence FROM stages ORDER BY sequence")
    stages = cur.fetchall()

    stage_map = {s[1]: (s[0], s[2]) for s in stages}
    selected_stage_name = st.selectbox("Select Stage", list(stage_map.keys()))
    stage_id, selected_sequence = stage_map[selected_stage_name]

    # ================= VALIDATION =================
    allowed_sequence = completed_stage + 1

    if selected_sequence > allowed_sequence:
        st.error("❌ Complete previous stage first")
        return

    status = st.selectbox("Status", ["Started", "In Progress", "Completed", "Pending"])

    # ================= SUBMIT =================
    if st.button("Submit"):
        cur.execute("""
            INSERT INTO tracking_log (house_id, product_id, stage_id, status)
            VALUES (%s, %s, %s, %s)
        """, (house_id, product_id, stage_id, status))

        conn.commit()
        st.success("✅ Stage Updated")
