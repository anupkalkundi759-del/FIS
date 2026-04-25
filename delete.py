def show_delete(conn, cur):
    import streamlit as st

    st.title("🗑 Delete Data")

    delete_type = st.radio(
        "Delete Level",
        ["Project", "Unit", "House", "Product"]
    )

    # ================= PROJECT =================
    cur.execute("SELECT project_id, project_name FROM projects ORDER BY project_name")
    projects = cur.fetchall()

    if not projects:
        st.warning("No data available")
        return

    project_dict = {p[1]: p[0] for p in projects}
    selected_project = st.selectbox("Project", list(project_dict.keys()))
    project_id = project_dict[selected_project]

    confirm = st.checkbox("I confirm this delete action")

    # ================= PROJECT DELETE =================
    if delete_type == "Project":
        if st.button("Delete Project"):
            if not confirm:
                st.warning("Please confirm delete action")
                return

            cur.execute("""
                DELETE FROM tracking_log
                WHERE product_instance_id IN (
                    SELECT p.product_instance_id
                    FROM products p
                    JOIN houses h ON p.house_id = h.house_id
                    JOIN units u ON h.unit_id = u.unit_id
                    WHERE u.project_id = %s
                )
            """, (project_id,))

            cur.execute("""
                DELETE FROM house_config
                WHERE house_no IN (
                    SELECT h.house_no
                    FROM houses h
                    JOIN units u ON h.unit_id = u.unit_id
                    WHERE u.project_id = %s
                )
            """, (project_id,))

            cur.execute("DELETE FROM projects WHERE project_id=%s", (project_id,))
            conn.commit()
            st.success("Project Deleted Successfully")

    # ================= UNIT DELETE =================
    elif delete_type == "Unit":
        cur.execute("SELECT unit_id, unit_name FROM units WHERE project_id=%s ORDER BY unit_name", (project_id,))
        units = cur.fetchall()

        if units:
            unit_dict = {u[1]: u[0] for u in units}
            selected_unit = st.selectbox("Unit", list(unit_dict.keys()))
            unit_id = unit_dict[selected_unit]

            if st.button("Delete Unit"):
                if not confirm:
                    st.warning("Please confirm delete action")
                    return

                cur.execute("""
                    DELETE FROM tracking_log
                    WHERE product_instance_id IN (
                        SELECT p.product_instance_id
                        FROM products p
                        JOIN houses h ON p.house_id = h.house_id
                        WHERE h.unit_id = %s
                    )
                """, (unit_id,))

                cur.execute("""
                    DELETE FROM house_config
                    WHERE house_no IN (
                        SELECT house_no FROM houses WHERE unit_id = %s
                    )
                """, (unit_id,))

                cur.execute("DELETE FROM units WHERE unit_id=%s", (unit_id,))
                conn.commit()
                st.success("Unit Deleted Successfully")

    # ================= HOUSE DELETE =================
    elif delete_type == "House":
        cur.execute("""
            SELECT h.house_id, h.house_no
            FROM houses h
            JOIN units u ON h.unit_id = u.unit_id
            WHERE u.project_id=%s
            ORDER BY h.house_no
        """, (project_id,))
        houses = cur.fetchall()

        if houses:
            house_dict = {h[1]: h[0] for h in houses}
            selected_house = st.selectbox("House", list(house_dict.keys()))
            house_id = house_dict[selected_house]

            if st.button("Delete House"):
                if not confirm:
                    st.warning("Please confirm delete action")
                    return

                cur.execute("""
                    DELETE FROM tracking_log
                    WHERE product_instance_id IN (
                        SELECT product_instance_id FROM products WHERE house_id = %s
                    )
                """, (house_id,))

                cur.execute("DELETE FROM house_config WHERE house_no = %s", (selected_house,))
                cur.execute("DELETE FROM houses WHERE house_id=%s", (house_id,))
                conn.commit()
                st.success("House Deleted Successfully")

    # ================= PRODUCT DELETE =================
    elif delete_type == "Product":

        cur.execute("SELECT unit_id, unit_name FROM units WHERE project_id=%s ORDER BY unit_name", (project_id,))
        units = cur.fetchall()

        if not units:
            st.warning("No units found")
            return

        unit_dict = {u[1]: u[0] for u in units}
        selected_unit = st.selectbox("Unit", list(unit_dict.keys()))
        unit_id = unit_dict[selected_unit]

        cur.execute("SELECT house_id, house_no FROM houses WHERE unit_id=%s ORDER BY house_no", (unit_id,))
        houses = cur.fetchall()

        if not houses:
            st.warning("No houses found")
            return

        house_dict = {h[1]: h[0] for h in houses}
        selected_house = st.selectbox("House", list(house_dict.keys()))
        house_id = house_dict[selected_house]

        cur.execute("""
            SELECT p.product_instance_id, pm.product_code
            FROM products p
            JOIN products_master pm ON p.product_id = pm.product_id
            WHERE p.house_id=%s
            ORDER BY pm.product_code
        """, (house_id,))

        products = cur.fetchall()

        if not products:
            st.warning("No products found")
            return

        product_dict = {f"{selected_house} • {p[1]} • {i+1}": p[0] for i, p in enumerate(products)}
        selected_product = st.selectbox("Product", list(product_dict.keys()))
        product_id = product_dict[selected_product]

        if st.button("Delete Product"):
            if not confirm:
                st.warning("Please confirm delete action")
                return

            cur.execute("DELETE FROM tracking_log WHERE product_instance_id=%s", (product_id,))
            cur.execute("DELETE FROM products WHERE product_instance_id=%s", (product_id,))
            conn.commit()
            st.success("Product Deleted Successfully")
