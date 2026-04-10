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
        SELECT house_no 
        FROM houses 
        WHERE unit_id=%s
        ORDER BY house_no
    """, (unit_dict[selected_unit],))
    houses = [h[0] for h in cur.fetchall()]

    if not houses:
        st.warning("No houses found")
        return

    selected_house = st.selectbox("House", houses)

    # 🔥 GET house_id
    cur.execute("""
        SELECT house_id 
        FROM houses 
        WHERE house_no=%s AND unit_id=%s
    """, (selected_house, unit_dict[selected_unit]))
    house_id = cur.fetchone()[0]

    # ================= DATE =================
    measurement_date = st.date_input("Measurement Date")

    if st.button("Update Measurement"):

        # ================= UPDATE HOUSE =================
        cur.execute("""
            UPDATE houses
            SET measurement_date=%s,
                status='In Progress'
            WHERE house_id=%s
        """, (measurement_date, house_id))

        # ================= GET STAGE ID =================
        cur.execute("""
            SELECT stage_id FROM stages
            WHERE LOWER(stage_name) = 'measurement'
        """)
        stage = cur.fetchone()

        if not stage:
            st.error("❌ 'Measurement' stage not found in stages table")
            st.stop()

        stage_id = stage[0]

        # ================= GET ALL PRODUCTS IN HOUSE =================
        cur.execute("""
            SELECT product_instance_id 
            FROM products
            WHERE house_id=%s
        """, (house_id,))
        product_instances = cur.fetchall()

        if not product_instances:
            st.warning("No products found for this house")
        else:
            # ================= INSERT TRACKING LOG =================
            for (pid,) in product_instances:
                cur.execute("""
                    INSERT INTO tracking_log (product_instance_id, stage_id, status, timestamp)
                    VALUES (%s, %s, %s, %s)
                """, (pid, stage_id, "Completed", datetime.now()))

        conn.commit()

        st.success("✅ Measurement Updated + Tracking Logged")
