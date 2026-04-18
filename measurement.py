def update_measurement(conn, cur):
    import streamlit as st

    st.title("📅 Measurement Update")

    # ================= CACHE =================
    @st.cache_data
    def get_projects():
        cur.execute("SELECT project_id, project_name FROM projects ORDER BY project_name")
        return cur.fetchall()

    @st.cache_data
    def get_units(project_id):
        cur.execute("""
            SELECT unit_id, unit_name 
            FROM units 
            WHERE project_id=%s
        """, (project_id,))
        return cur.fetchall()

    @st.cache_data
    def get_houses(unit_id):
        cur.execute("""
            SELECT house_id, house_no 
            FROM houses 
            WHERE unit_id=%s
        """, (unit_id,))
        return cur.fetchall()

    # ================= PROJECT =================
    projects = get_projects()

    if not projects:
        st.warning("No projects found")
        return

    project_dict = {p[1]: p[0] for p in projects}
    selected_project = st.selectbox("Project", list(project_dict.keys()))
    project_id = project_dict[selected_project]

    # ================= UNIT =================
    units = get_units(project_id)

    if not units:
        st.warning("No units found")
        return

    unit_dict = {u[1]: u[0] for u in units}
    selected_unit = st.selectbox("Unit", list(unit_dict.keys()))
    unit_id = unit_dict[selected_unit]

    # ================= HOUSE =================
    houses = get_houses(unit_id)

    if not houses:
        st.warning("No houses found")
        return

    house_dict = {h[1]: h[0] for h in houses}
    selected_house_no = st.selectbox("House", list(house_dict.keys()))
    selected_house_id = house_dict[selected_house_no]

    # ================= DATE =================
    measurement_date = st.date_input("Measurement Date")

    # ================= UPDATE =================
    if st.button("Update Measurement"):

        with st.spinner("Updating measurement..."):

            try:
                # ---------- UPDATE HOUSE ----------
                cur.execute("""
                    UPDATE houses
                    SET measurement_date=%s,
                        status='In Progress'
                    WHERE house_id=%s
                """, (measurement_date, selected_house_id))

                # ---------- GET STAGE ----------
                cur.execute("""
                    SELECT stage_id 
                    FROM stages 
                    WHERE stage_name = 'Measurement'
                """)
                stage = cur.fetchone()

                if not stage:
                    st.error("Measurement stage not found in stages table")
                    conn.rollback()
                    return

                stage_id = stage[0]

                # ---------- GET PRODUCTS COUNT (for display only) ----------
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM products 
                    WHERE house_id=%s
                """, (selected_house_id,))
                product_count = cur.fetchone()[0]

                if product_count == 0:
                    st.warning("No products found for this house")
                    conn.commit()
                    return

                # ---------- BULK INSERT (FAST REPLACEMENT OF LOOP) ----------
                cur.execute("""
                    INSERT INTO tracking_log (product_instance_id, stage_id, status, timestamp)
                    SELECT product_instance_id, %s, 'Completed', NOW()
                    FROM products
                    WHERE house_id = %s
                """, (stage_id, selected_house_id))

                conn.commit()

                st.success(f"✅ Measurement completed for House {selected_house_no}")
                st.info(f"📦 {product_count} products moved to 'Measurement Completed'")

                # ---------- REFRESH ----------
                st.rerun()

            except Exception as e:
                conn.rollback()
                st.error(f"Error: {str(e)}")
