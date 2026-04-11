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

    # 🔥 Hidden uniqueness (clean UI)
    product_options = []
    for pid, code in products:
        product_options.append({
            "id": pid,
            "label": code
        })

    selected_product = st.selectbox(
        "Select Product",
        product_options,
        format_func=lambda x: x["label"]
    )

    product_instance_id = selected_product["id"]

    # ================= GET STAGES =================
    cur.execute("""
        SELECT stage_id, stage_name, sequence
        FROM stages
        WHERE sequence IS NOT NULL
        ORDER BY sequence
    """)
    stages = cur.fetchall()

    if not stages:
        st.error("No stages configured")
        return

    sequence_map = {s[2]: s[1] for s in stages}
    stage_map = {s[1]: (s[0], s[2]) for s in stages}
    sequences = sorted(sequence_map.keys())

    # ================= SAFE LAST STAGE =================
    cur.execute("""
        SELECT s.sequence, s.stage_name
        FROM tracking_log t
        JOIN stages s ON t.stage_id = s.stage_id
        WHERE t.product_instance_id = %s
        AND t.status = 'Completed'
        ORDER BY s.sequence DESC
        LIMIT 1
    """, (product_instance_id,))

    row = cur.fetchone()

    if row is None:
        st.info("Last Completed Stage: Not Started")
        expected_stage = sequences[0]
    else:
        last_seq = row[0]
        st.info(f"Last Completed Stage: {row[1]}")

        if last_seq not in sequences:
            st.warning("⚠️ Invalid stage history detected → resetting flow")
            expected_stage = sequences[0]
        else:
            idx = sequences.index(last_seq)
            if idx + 1 >= len(sequences):
                st.success("🎉 All stages completed")
                return
            expected_stage = sequences[idx + 1]

    # ================= NEXT STAGE =================
    st.success(f"Next Allowed Stage: {sequence_map[expected_stage]}")

    # ================= SELECT STAGE =================
    selected_stage = st.selectbox("Select Stage", list(stage_map.keys()))
    stage_id, selected_seq = stage_map[selected_stage]

    # ================= STRICT VALIDATION =================
    if selected_seq != expected_stage:
        st.error(f"❌ You must complete '{sequence_map[expected_stage]}' first")
        return

    # ================= DUPLICATE SAFE CHECK =================
    cur.execute("""
        SELECT COUNT(*)
        FROM tracking_log
        WHERE product_instance_id=%s
        AND stage_id=%s
        AND status='Completed'
    """, (product_instance_id, stage_id))

    if cur.fetchone()[0] > 0:
        st.warning("⚠️ Stage already completed for this item")
        return

    # ================= UPDATE =================
    if st.button("Update Item"):

        cur.execute("""
            INSERT INTO tracking_log (product_instance_id, stage_id, status)
            VALUES (%s, %s, 'Completed')
        """, (product_instance_id, stage_id))

        conn.commit()

        st.success(f"✅ {selected_stage} completed")
        st.rerun()
