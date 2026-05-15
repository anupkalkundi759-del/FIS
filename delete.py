def show_delete(conn, cur):
    import streamlit as st

    st.title("🗑 Delete Data")

    delete_type = st.radio(
        "Delete Level",
        ["Project", "Unit", "House", "Product", "BAC", "Actual Cost", "SLA"]
    )

    def get_quarters():
        cur.execute("""
            SELECT DISTINCT quarter
            FROM products
            WHERE quarter IS NOT NULL
            ORDER BY quarter DESC
        """)
        quarters = [q[0] for q in cur.fetchall()]

        if "2026-Q2" not in quarters:
            quarters.insert(0, "2026-Q2")

        return ["ALL"] + quarters

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

        quarter_options = get_quarters()
        selected_quarter = st.selectbox(
            "Quarter",
            quarter_options,
            index=quarter_options.index("2026-Q2") if "2026-Q2" in quarter_options else 0
        )

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
            AND (%s = 'ALL' OR p.quarter=%s)
            ORDER BY pm.product_code
        """, (house_id, selected_quarter, selected_quarter))

        products = cur.fetchall()

        if not products:
            st.warning("No products found")
            return

        product_dict = {
            f"{selected_house} • {p[1]} • ID {p[0]}": p[0]
            for p in products
        }

        selected_products = st.multiselect("Products", list(product_dict.keys()))
        product_ids = [product_dict[p] for p in selected_products]

        if st.button("Delete Selected Products"):
            if not confirm:
                st.warning("Please confirm delete action")
                return

            if not product_ids:
                st.warning("Please select products")
                return

            cur.execute("DELETE FROM tracking_log WHERE product_instance_id = ANY(%s)", (product_ids,))
            cur.execute("DELETE FROM products WHERE product_instance_id = ANY(%s)", (product_ids,))
            conn.commit()
            st.success(f"{len(product_ids)} Product(s) Deleted Successfully")

    # ================= BAC DELETE =================
    elif delete_type == "BAC":

        cur.execute("SELECT unit_id, unit_name FROM units WHERE project_id=%s ORDER BY unit_name", (project_id,))
        units = cur.fetchall()

        if not units:
            st.warning("No units found")
            return

        unit_dict = {u[1]: u[0] for u in units}
        selected_unit = st.selectbox("Unit", list(unit_dict.keys()))
        unit_id = unit_dict[selected_unit]

        cur.execute("""
            SELECT bac_amount
            FROM project_evm_baseline
            WHERE project_id=%s AND unit_id=%s
        """, (project_id, unit_id))

        bac_row = cur.fetchone()

        if bac_row:
            st.info(f"Current BAC Amount: {float(bac_row[0]):,.2f}")
        else:
            st.warning("No BAC found for selected Project + Unit")

        if st.button("Delete BAC"):
            if not confirm:
                st.warning("Please confirm delete action")
                return

            cur.execute("""
                DELETE FROM project_evm_baseline
                WHERE project_id=%s AND unit_id=%s
            """, (project_id, unit_id))

            conn.commit()
            st.success("BAC Deleted Successfully")

    # ================= ACTUAL COST DELETE =================
    elif delete_type == "Actual Cost":

        cur.execute("SELECT unit_id, unit_name FROM units WHERE project_id=%s ORDER BY unit_name", (project_id,))
        units = cur.fetchall()

        if not units:
            st.warning("No units found")
            return

        unit_dict = {u[1]: u[0] for u in units}
        selected_unit = st.selectbox("Unit", list(unit_dict.keys()))
        unit_id = unit_dict[selected_unit]

        cur.execute("""
            SELECT id, period_date, actual_cost, COALESCE(remarks, '')
            FROM evm_cost_log
            WHERE project_id=%s AND unit_id=%s
            ORDER BY period_date DESC, id DESC
        """, (project_id, unit_id))

        cost_rows = cur.fetchall()

        if not cost_rows:
            st.warning("No actual cost records found")
            return

        cost_dict = {
            f"{r[1]} • ₹{float(r[2]):,.2f} • {r[3]}": r[0]
            for r in cost_rows
        }

        selected_costs = st.multiselect("Actual Cost Records", list(cost_dict.keys()))
        cost_ids = [cost_dict[c] for c in selected_costs]

        if st.button("Delete Selected Actual Cost"):
            if not confirm:
                st.warning("Please confirm delete action")
                return

            if not cost_ids:
                st.warning("Please select actual cost records")
                return

            cur.execute("DELETE FROM evm_cost_log WHERE id = ANY(%s)", (cost_ids,))
            conn.commit()
            st.success(f"{len(cost_ids)} Actual Cost Record(s) Deleted Successfully")

    # ================= SLA DELETE =================
    elif delete_type == "SLA":

        cur.execute("""
            SELECT h.house_id, h.house_no
            FROM houses h
            JOIN units u ON h.unit_id = u.unit_id
            WHERE u.project_id=%s
            ORDER BY h.house_no
        """, (project_id,))

        houses = cur.fetchall()

        if not houses:
            st.warning("No houses found")
            return

        house_dict = {h[1]: h[0] for h in houses}
        selected_house = st.selectbox("House", list(house_dict.keys()))
        house_id = house_dict[selected_house]

        cur.execute("""
            SELECT sla_date, priority_level
            FROM sla_monitor
            WHERE house_id=%s
        """, (house_id,))

        sla_row = cur.fetchone()

        if sla_row:
            st.info(f"SLA Date: {sla_row[0]} | Priority: {sla_row[1]}")
        else:
            st.warning("No SLA found for selected house")

        if st.button("Delete SLA"):
            if not confirm:
                st.warning("Please confirm delete action")
                return

            cur.execute("DELETE FROM sla_monitor WHERE house_id=%s", (house_id,))
            conn.commit()
            st.success("SLA Deleted Successfully")
