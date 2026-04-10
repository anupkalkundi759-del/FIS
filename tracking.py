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

    # ================= UNIT =================
    cur.execute("""
        SELECT unit_id, unit_name
        FROM units
        WHERE project_id=%s
        ORDER BY unit_name
    """, (project_dict[selected_project],))

    units = cur.fetchall()

    if not units:
        st.warning("No units found")
        return

    unit_dict = {u[1]: u[0] for u in units}
    selected_unit = st.selectbox("Select Unit", list(unit_dict.keys()))

    # ================= HOUSE =================
    cur.execute("""
        SELECT house_id
        FROM houses
        WHERE unit_id=%s
        ORDER BY house_id
    """, (unit_dict[selected_unit],))

    houses = [h[0] for h in cur.fetchall()]

    if not houses:
        st.warning("No houses found")
        return

    selected_house = st.selectbox("Select House", houses)

    # ================= PRODUCT =================
    cur.execute("""
        SELECT product_code
        FROM house_products
        WHERE house_id=%s
    """, (selected_house,))

    products = [p[0] for p in cur.fetchall()]

    if not products:
        st.warning("No products found")
        return

    selected_product = st.selectbox("Select Product", products)

    # ================= COMPLETED STAGE =================
    cur.execute("""
        SELECT COALESCE(MAX(s.sequence), 0)
        FROM tracking_log t
        JOIN stages s ON t.stage_id = s.stage_id
        WHERE t.house_id=%s AND t.product_code=%s
    """, (selected_house, selected_product))

    completed_stage = cur.fetchone()[0]

    st.info(f"Completed Stage: {completed_stage}")

    # ================= STAGE SELECT =================
    cur.execute("SELECT stage_id, stage_name, sequence FROM stages ORDER BY sequence")
    stages = cur.fetchall()

    stage_names = [s[1] for s in stages]
    stage_map = {s[1]: (s[0], s[2]) for s in stages}

    selected_stage_name = st.selectbox("Select Stage", stage_names)
    stage_id, selected_sequence = stage_map[selected_stage_name]

    # ================= VALIDATION =================
    allowed_sequence = completed_stage + 1

    if selected_sequence > allowed_sequence:
        st.error("❌ Complete previous stage first")
        return

    # ================= STATUS =================
    status = st.selectbox("Status", ["Started", "In Progress", "Completed"])

    # ================= SUBMIT =================
    if st.button("Submit"):

        cur.execute("""
            INSERT INTO tracking_log (house_id, product_code, stage_id, status)
            VALUES (%s, %s, %s, %s)
        """, (selected_house, selected_product, stage_id, status))

        conn.commit()
        st.success("✅ Stage Updated Successfully")
