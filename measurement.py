def update_measurement(conn, cur):
    import streamlit as st
    from datetime import datetime

    st.title("📅 Measurement Update")

    # ================= PROJECT =================
    cur.execute("SELECT project_id, project_name FROM projects ORDER BY project_name")
    projects = cur.fetchall()

    if not projects:
        st.warning("No projects found")
        return

    project_dict = {p[1]: p[0] for p in projects}
    selected_project = st.selectbox("Project", list(project_dict.keys()))

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
    selected_unit = st.selectbox("Unit", list(unit_dict.keys()))

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
    selected_house_no = st.selectbox("House", list(house_dict.keys()))
    selected_house_id = house_dict[selected_house_no]

    # ================= DATE =================
    measurement_date = st.date_input("Measurement Date")

    if st.button("Update Measurement"):

        try:
            # ================= UPDATE HOUSE =================
            cur.execute("""
                UPDATE houses
                SET measurement_date=%s,
                    status='In Progress'
                WHERE house_id=%s
            """, (measurement_date, selected_house_id))

            # ================= GET MEASUREMENT STAGE =================
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

            # ================= GET PRODUCTS =================
            cur.execute("""
                SELECT product_instance_id 
                FROM products 
                WHERE house_id=%s
            """, (selected_house_id,))
            products = cur.fetchall()

            if not products:
                st.warning("No products found for this house")
                conn.commit()
                return

            # ================= INSERT TRACKING =================
            for p in products:
                cur.execute("""
                    INSERT INTO tracking_log (product_instance_id, stage_id, status, timestamp)
                    VALUES (%s, %s, 'Completed', NOW())
                """, (p[0], stage_id))

            conn.commit()

            st.success(f"✅ Measurement completed for House {selected_house_no}")
            st.info(f"📦 {len(products)} products moved to 'Measurement Completed'")

        except Exception as e:
            conn.rollback()
            st.error(f"Error: {str(e)}")
