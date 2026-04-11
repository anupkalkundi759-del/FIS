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
        SELECT p.id, pm.product_code
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

    # ================= ACTIVITIES =================
    cur.execute("""
        SELECT activity_id, activity_name, sequence_order
        FROM activity_master
        WHERE sequence_order IS NOT NULL
        ORDER BY sequence_order
    """)
    activities = cur.fetchall()

    if not activities:
        st.error("❌ No valid activities found")
        st.stop()

    # Clean duplicates
    seq_map = {}
    activity_map = {}

    for act_id, name, seq in activities:
        if seq not in seq_map:
            seq_map[seq] = name
            activity_map[name] = (act_id, seq)

    sequences = sorted(seq_map.keys())

    # ================= LAST COMPLETED ACTIVITY =================
    cur.execute("""
        SELECT MAX(a.sequence_order)
        FROM tracking_log t
        JOIN activity_master a ON t.activity_id = a.activity_id
        WHERE t.product_instance_id = %s
        AND t.status = 'Completed'
    """, (product_instance_id,))

    last_completed = cur.fetchone()[0]

    if last_completed is None:
        st.info("Last Completed Activity: Not Started")
        expected_seq = sequences[0]
    else:
        st.info(f"Last Completed Activity: {seq_map.get(last_completed, 'Unknown')}")

        try:
            idx = sequences.index(last_completed)
            expected_seq = sequences[idx + 1]
        except (ValueError, IndexError):
            st.success("🎉 All activities completed")
            return

    # ================= NEXT ACTIVITY =================
    st.success(f"Next Allowed Activity: {seq_map[expected_seq]}")

    # ================= SELECT ACTIVITY =================
    selected_activity_name = st.selectbox("Select Activity", list(activity_map.keys()))
    activity_id, selected_seq = activity_map[selected_activity_name]

    # ================= VALIDATION =================
    if selected_seq != expected_seq:
        st.error(f"❌ You must complete '{seq_map[expected_seq]}' first")
        st.stop()

    # ================= DUPLICATE CHECK =================
    cur.execute("""
        SELECT 1 FROM tracking_log
        WHERE product_instance_id=%s 
        AND activity_id=%s 
        AND status='Completed'
    """, (product_instance_id, activity_id))

    if cur.fetchone():
        st.warning("⚠️ Activity already completed")
        st.stop()

    # ================= STATUS =================
    status = st.selectbox("Status", ["Completed"])

    # ================= UPDATE =================
    if st.button("Update Item"):

        cur.execute("""
            INSERT INTO tracking_log (product_instance_id, activity_id, status)
            VALUES (%s, %s, %s)
        """, (product_instance_id, activity_id, status))

        conn.commit()

        st.success(f"✅ {selected_activity_name} Completed")
        st.rerun()
