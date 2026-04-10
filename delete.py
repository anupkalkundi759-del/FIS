def show_delete(conn, cur):
    import streamlit as st

    st.title("🗑 Delete Data")

    delete_type = st.radio(
        "Delete Level",
        ["Project", "Unit", "House"]
    )

    # PROJECT
    cur.execute("SELECT project_id, project_name FROM projects")
    projects = cur.fetchall()

    if not projects:
        st.warning("No data available")
        return

    project_dict = {p[1]: p[0] for p in projects}
    selected_project = st.selectbox("Project", list(project_dict.keys()))
    project_id = project_dict[selected_project]

    if delete_type == "Project":
        if st.button("Delete Project"):
            cur.execute("DELETE FROM projects WHERE project_id=%s", (project_id,))
            conn.commit()
            st.success("Project Deleted")

    elif delete_type == "Unit":
        cur.execute("SELECT unit_id, unit_name FROM units WHERE project_id=%s", (project_id,))
        units = cur.fetchall()

        if units:
            unit_dict = {u[1]: u[0] for u in units}
            selected_unit = st.selectbox("Unit", list(unit_dict.keys()))

            if st.button("Delete Unit"):
                cur.execute("DELETE FROM units WHERE unit_id=%s", (unit_dict[selected_unit],))
                conn.commit()
                st.success("Unit Deleted")

    elif delete_type == "House":
        cur.execute("""
            SELECT h.house_id
            FROM houses h
            JOIN units u ON h.unit_id = u.unit_id
            WHERE u.project_id=%s
        """, (project_id,))
        houses = [h[0] for h in cur.fetchall()]

        if houses:
            selected_house = st.selectbox("House", houses)

            if st.button("Delete House"):
                cur.execute("DELETE FROM houses WHERE house_id=%s", (selected_house,))
                conn.commit()
                st.success("House Deleted")