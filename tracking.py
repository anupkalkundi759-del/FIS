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

    # ================= PRODUCT LIST =================
    cur.execute("""
        SELECT 
            pm.product_id,
            pm.product_code,
            COUNT(p.id) AS total,
            COUNT(DISTINCT t.product_instance_id) FILTER (WHERE t.status='Completed') AS completed
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

    product_dict = {
        f"{p[1]} ({p[3]}/{p[2]})": p[0]
        for p in products
    }

    selected_product_label = st.selectbox("Select Product", list(product_dict.keys()))
    product_id = product_dict[selected_product_label]

    # ================= GET ACTIVE ITEM =================
    cur.execute("""
        SELECT p.id
        FROM products p
        WHERE p.house_id = %s
        AND p.product_id = %s
        ORDER BY p.id
    """, (house_id, product_id))

    all_items = cur.fetchall()

    product_instance_id = None
    current_stage = None

    for item in all_items:
        pid = item[0]

        cur.execute("""
            SELECT MAX(s.sequence)
            FROM tracking_log t
            JOIN stages s ON t.stage_id = s.stage_id
            WHERE t.product_instance_id = %s
        """, (pid,))
        result = cur.fetchone()[0]

        if result is None:
            current_stage = None
            product_instance_id = pid
            break
        else:
            # Check if fully completed
            cur.execute("SELECT MAX(sequence) FROM stages")
            max_stage = cur.fetchone()[0]

            if result < max_stage:
                current_stage = result
                product_instance_id = pid
                break

    if not product_instance_id:
        st.success("✅ All items completed")
        return

    st.info(f"Current Item Stage: {current_stage if current_stage is not None else 'Not Started'}")

    # ================= STAGES =================
    cur.execute("SELECT stage_id, stage_name, sequence FROM stages ORDER BY sequence")
    stages = cur.fetchall()

    stage_map = {s[1]: (s[0], s[2]) for s in stages}
    selected_stage_name = st.selectbox("Select Stage", list(stage_map.keys()))
    stage_id, selected_sequence = stage_map[selected_stage_name]

    # ================= DETERMINE NEXT VALID STAGE =================
    if current_stage is None:
        # first stage = minimum sequence in DB
        cur.execute("SELECT MIN(sequence) FROM stages")
        expected_stage = cur.fetchone()[0]
    else:
        expected_stage = current_stage + 1

    # ================= VALIDATION =================
    if selected_sequence != expected_stage:
        st.error(f"❌ You must complete Stage {expected_stage} first")
        return

    # ================= DUPLICATE CHECK =================
    cur.execute("""
        SELECT 1 FROM tracking_log
        WHERE product_instance_id=%s AND stage_id=%s AND status='Completed'
    """, (product_instance_id, stage_id))

    if cur.fetchone():
        st.warning("⚠️ Stage already completed")
        return

    status = st.selectbox("Status", ["Started", "In Progress", "Completed"])

    # ================= SUBMIT =================
    if st.button("Update Item"):

        if status != "Completed":
            st.error("❌ Only 'Completed' allowed to move forward")
            return

        cur.execute("""
            INSERT INTO tracking_log (product_instance_id, stage_id, status)
            VALUES (%s, %s, %s)
        """, (product_instance_id, stage_id, status))

        conn.commit()

        st.success("✅ Stage Completed")
        st.rerun()
