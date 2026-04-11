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
        SELECT 
            p.id,
            pm.product_code
        FROM products p
        JOIN products_master pm ON p.product_id = pm.product_id
        WHERE p.house_id = %s
        ORDER BY pm.product_code, p.id
    """, (house_id,))
    products = cur.fetchall()

    if not products:
        st.warning("No products found")
        return

    labels = [p[1] for p in products]
    ids = [p[0] for p in products]

    selected_index = st.selectbox(
        "Select Product",
        range(len(labels)),
        format_func=lambda x: labels[x]
    )

    product_instance_id = ids[selected_index]

    # ================= STAGES =================
    cur.execute("""
        SELECT stage_id, stage_name, sequence
        FROM stages
        ORDER BY sequence
    """)
    stages = cur.fetchall()

    stage_map = {s[1]: (s[0], s[2]) for s in stages}
    sequence_map = {s[2]: s[1] for s in stages}

    # ================= CURRENT STAGE =================
    cur.execute("""
        SELECT MAX(s.sequence)
        FROM tracking_log t
        JOIN stages s ON t.stage_id = s.stage_id
        WHERE t.product_instance_id = %s
        AND t.status = 'Completed'
    """, (product_instance_id,))

    current_stage = cur.fetchone()[0]

    if current_stage is None:
        st.info("Current Stage: Not Started")
        expected_stage = min(sequence_map.keys())
    else:
        st.info(f"Current Stage: {sequence_map[current_stage]}")
        expected_stage = current_stage + 1

    # ================= NEXT STAGE INFO =================
    if expected_stage in sequence_map:
        st.success(f"Next Allowed Stage: {sequence_map[expected_stage]}")
    else:
        st.success("🎉 All stages completed")
        return

    # ================= SELECT STAGE =================
    selected_stage_name = st.selectbox("Select Stage", list(stage_map.keys()))
    stage_id, selected_sequence = stage_map[selected_stage_name]

    # ================= VALIDATION =================
    if selected_sequence != expected_stage:
        st.error(f"❌ You must complete '{sequence_map[expected_stage]}' first")
        st.stop()

    # ================= DUPLICATE CHECK =================
    cur.execute("""
        SELECT 1 FROM tracking_log
        WHERE product_instance_id=%s 
        AND stage_id=%s 
        AND status='Completed'
    """, (product_instance_id, stage_id))

    if cur.fetchone():
        st.warning("⚠️ Stage already completed")
        st.stop()

    # ================= STATUS =================
    status = st.selectbox("Status", ["Completed"])

    # ================= UPDATE =================
    if st.button("Update Item"):

        cur.execute("""
            INSERT INTO tracking_log (product_instance_id, stage_id, status)
            VALUES (%s, %s, %s)
        """, (product_instance_id, stage_id, status))

        conn.commit()

        st.success(f"✅ {selected_stage_name} Completed")
        st.rerun()
