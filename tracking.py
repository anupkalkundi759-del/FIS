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
    """, (project_dict[selected_project],))
    units = cur.fetchall()

    if not units:
        st.warning("No units found")
        return

    unit_dict = {u[1]: u[0] for u in units}
    selected_unit = st.selectbox("Select Unit", list(unit_dict.keys()))

    # ================= HOUSE =================
    cur.execute("""
        SELECT house_id, house_no 
        FROM houses 
        WHERE unit_id=%s
    """, (unit_dict[selected_unit],))
    houses = cur.fetchall()

    if not houses:
        st.warning("No houses found")
        return

    house_dict = {h[1]: h[0] for h in houses}
    selected_house = st.selectbox("Select House", list(house_dict.keys()))
    house_id = house_dict[selected_house]

    # ================= PRODUCTS (CLEAN) =================
    cur.execute("""
        SELECT DISTINCT pm.product_code
        FROM products p
        JOIN products_master pm ON p.product_id = pm.product_id
        WHERE p.house_id = %s
        ORDER BY pm.product_code
    """, (house_id,))

    products = cur.fetchall()

    if not products:
        st.warning("No products found")
        return

    product_list = [p[0] for p in products]
    selected_product = st.selectbox("Select Product", product_list)

    # ================= PICK NEXT AVAILABLE INSTANCE =================
    cur.execute("""
        SELECT p.product_instance_id
        FROM products p
        JOIN products_master pm ON p.product_id = pm.product_id
        WHERE p.house_id = %s
        AND pm.product_code = %s
        ORDER BY p.product_instance_id
    """, (house_id, selected_product))

    all_instances = cur.fetchall()

    selected_product_instance_id = None

    for inst in all_instances:
        pid = inst[0]

        # check last stage
        cur.execute("""
            SELECT s.sequence
            FROM tracking_log t
            JOIN stages s ON t.stage_id = s.stage_id
            WHERE t.product_instance_id = %s
            ORDER BY t.timestamp DESC
            LIMIT 1
        """, (pid,))
        res = cur.fetchone()

        if not res:
            selected_product_instance_id = pid
            break

        last_seq = res[0]

        # check if fully completed (last stage)
        cur.execute("SELECT MAX(sequence) FROM stages")
        max_seq = cur.fetchone()[0]

        if last_seq < max_seq:
            selected_product_instance_id = pid
            break

    if not selected_product_instance_id:
        st.success("✅ All items of this product are completed")
        return

    # ================= CURRENT STAGE =================
    cur.execute("""
        SELECT s.stage_name, s.sequence
        FROM tracking_log t
        JOIN stages s ON t.stage_id = s.stage_id
        WHERE t.product_instance_id = %s
        ORDER BY t.timestamp DESC
        LIMIT 1
    """, (selected_product_instance_id,))

    result = cur.fetchone()

    if result:
        current_stage, current_seq = result
    else:
        current_stage, current_seq = "Not Started", 0

    st.info(f"Last Completed Stage: {current_stage}")

    # ================= NEXT STAGE =================
    cur.execute("""
        SELECT stage_name, sequence
        FROM stages
        ORDER BY sequence
    """)
    stages = cur.fetchall()

    stage_map = {s[1]: s[0] for s in stages}
    max_seq = max(stage_map.keys())

    if current_seq == 0:
        next_seq = 1
    elif current_seq < max_seq:
        next_seq = current_seq + 1
    else:
        st.success("✅ Product fully completed")
        return

    next_stage = stage_map[next_seq]

    st.success(f"Next Allowed Stage: {next_stage}")

    # ================= INPUT =================
    selected_stage = st.selectbox("Select Stage", [s[0] for s in stages])
    status = st.selectbox("Status", ["In Progress", "Completed"])

    # ================= VALIDATION =================
    if selected_stage != next_stage:
        st.error(f"You must complete '{next_stage}' first")
        return

    # ================= UPDATE =================
    if st.button("Update"):

        cur.execute("""
            SELECT stage_id 
            FROM stages 
            WHERE stage_name = %s
        """, (selected_stage,))
        stage_id = cur.fetchone()[0]

        cur.execute("""
            INSERT INTO tracking_log (product_instance_id, stage_id, status, timestamp)
            VALUES (%s, %s, %s, NOW())
        """, (selected_product_instance_id, stage_id, status))

        conn.commit()

        st.success("✅ Updated successfully")
