def update_measurement(conn, cur):
    import streamlit as st

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
        SELECT house_id 
        FROM houses 
        WHERE unit_id=%s
    """, (unit_dict[selected_unit],))
    houses = [h[0] for h in cur.fetchall()]

    if not houses:
        st.warning("No houses found")
        return

    selected_house = st.selectbox("House", houses)

    # ================= DATE =================
    measurement_date = st.date_input("Measurement Date")

    if st.button("Update Measurement"):
        cur.execute("""
            UPDATE houses
            SET measurement_date=%s,
                status='In Progress'
            WHERE house_id=%s
        """, (measurement_date, selected_house))

        conn.commit()
        st.success("Measurement Updated")