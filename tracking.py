def show_tracking(conn, cur):
    import streamlit as st

    st.title("🏭 Production Tracker")

    # ================= PROJECT / UNIT / HOUSE =================
    col1, col2, col3 = st.columns(3)

    # ---------- PROJECT ----------
    with col1:
        cur.execute("SELECT project_id, project_name FROM projects ORDER BY project_name")
        projects = cur.fetchall()
        project_dict = {p[1]: p[0] for p in projects}

        project_options = ["All"] + list(project_dict.keys())
        selected_project = st.selectbox("Select Project", project_options)

    # ---------- UNIT ----------
    with col2:
        if selected_project == "All":
            cur.execute("SELECT unit_id, unit_name FROM units")
        else:
            cur.execute("""
                SELECT unit_id, unit_name 
                FROM units 
                WHERE project_id=%s
            """, (project_dict[selected_project],))

        units = cur.fetchall()
        unit_dict = {u[1]: u[0] for u in units}

        unit_options = ["All"] + list(unit_dict.keys())
        selected_unit = st.selectbox("Select Unit", unit_options)

    # ---------- HOUSE ----------
    with col3:
        if selected_unit == "All":
            cur.execute("SELECT house_id, house_no FROM houses")
        else:
            cur.execute("""
                SELECT house_id, house_no 
                FROM houses 
                WHERE unit_id=%s
            """, (unit_dict[selected_unit],))

        houses = cur.fetchall()
        house_dict = {h[1]: h[0] for h in houses}

        house_options = ["All"] + list(house_dict.keys())
        selected_house = st.selectbox("Select House", house_options)

        house_id = None if selected_house == "All" else house_dict[selected_house]

    # ================= PRODUCTS =================
    if house_id:
        cur.execute("""
            SELECT 
                p.product_instance_id,
                pm.product_code
            FROM products p
            JOIN products_master pm ON p.product_id = pm.product_id
            WHERE p.house_id = %s
            ORDER BY pm.product_code
        """, (house_id,))
    else:
        cur.execute("""
            SELECT 
                p.product_instance_id,
                pm.product_code
            FROM products p
            JOIN products_master pm ON p.product_id = pm.product_id
            ORDER BY pm.product_code
        """)

    products = cur.fetchall()

    if not products:
        st.warning("No products found")
        return

    product_map = {f"{p[1]}_{i}": p[0] for i, p in enumerate(products)}

    product_options = ["All"] + list(product_map.keys())
    selected_display = st.selectbox("Select Product", product_options)

    selected_product_instance_id = None if selected_display == "All" else product_map[selected_display]

    # ================= CURRENT STAGE =================
    if selected_product_instance_id:
        cur.execute("""
            SELECT s.stage_name
            FROM tracking_log t
            JOIN stages s ON t.stage_id = s.stage_id
            WHERE t.product_instance_id = %s
            ORDER BY t.timestamp DESC
            LIMIT 1
        """, (selected_product_instance_id,))

        result = cur.fetchone()
        current_stage = result[0] if result else "Not Started"
    else:
        current_stage = "Multiple"

    # ================= NEXT STAGE =================
    cur.execute("""
        SELECT stage_name, sequence
        FROM stages
        ORDER BY sequence
    """)
    stages = cur.fetchall()

    stage_sequence = [s[0] for s in stages]

    if selected_product_instance_id:
        if current_stage == "Not Started":
            next_stage = stage_sequence[0]
        else:
            try:
                idx = stage_sequence.index(current_stage)
                next_stage = stage_sequence[idx + 1]
            except:
                next_stage = "Completed"
    else:
        next_stage = "Bulk Mode"

    # ================= STATUS DISPLAY =================
    col4, col5 = st.columns(2)

    with col4:
        st.info(f"Last Completed Stage: {current_stage}")

    with col5:
        st.success(f"Next Allowed Stage: {next_stage}")

    # ================= INPUT =================
    selected_stage = st.selectbox("Select Stage", stage_sequence)
    status = st.selectbox("Status", ["In Progress", "Completed"])

    # ================= VALIDATION =================
    if selected_product_instance_id:
        if selected_stage != next_stage:
            st.error(f"You must complete '{next_stage}' first")
            return

    # ================= UPDATE =================
    LIMIT = 50

    if st.button("Update"):

        cur.execute("""
            SELECT stage_id 
            FROM stages 
            WHERE stage_name = %s
        """, (selected_stage,))
        stage_id = cur.fetchone()[0]

        updated = 0
        skipped = 0

        # ---------- SINGLE ----------
        if selected_product_instance_id:
            cur.execute("""
                INSERT INTO tracking_log (product_instance_id, stage_id, status, timestamp)
                VALUES (%s, %s, %s, NOW())
            """, (selected_product_instance_id, stage_id, status))
            updated = 1

        # ---------- BULK ----------
        else:
            if house_id:
                cur.execute("""
                    SELECT product_instance_id 
                    FROM products
                    WHERE house_id = %s
                    LIMIT %s
                """, (house_id, LIMIT))
            else:
                cur.execute("""
                    SELECT product_instance_id 
                    FROM products
                    LIMIT %s
                """, (LIMIT,))

            all_products = cur.fetchall()

            for p in all_products:
                pid = p[0]

                cur.execute("""
                    SELECT s.stage_name
                    FROM tracking_log t
                    JOIN stages s ON t.stage_id = s.stage_id
                    WHERE t.product_instance_id = %s
                    ORDER BY t.timestamp DESC
                    LIMIT 1
                """, (pid,))
                res = cur.fetchone()
                curr = res[0] if res else "Not Started"

                if curr == "Not Started":
                    expected = stage_sequence[0]
                else:
                    try:
                        idx = stage_sequence.index(curr)
                        expected = stage_sequence[idx + 1]
                    except:
                        expected = None

                if expected == selected_stage:
                    cur.execute("""
                        INSERT INTO tracking_log (product_instance_id, stage_id, status, timestamp)
                        VALUES (%s, %s, %s, NOW())
                    """, (pid, stage_id, status))
                    updated += 1
                else:
                    skipped += 1

        conn.commit()

        st.success(f"✅ Updated: {updated} products")
        if skipped > 0:
            st.warning(f"⚠ Skipped: {skipped} (not eligible)")
