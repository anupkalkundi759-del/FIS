def show_tracking(conn, cur):
    import streamlit as st
    import pandas as pd

    st.title("🏭 Production Tracker")

    # ================= CACHE FUNCTIONS =================
    @st.cache_data
    def get_projects():
        cur.execute("SELECT project_id, project_name FROM projects ORDER BY project_name")
        return cur.fetchall()

    @st.cache_data
    def get_units(project_id):
        if project_id:
            cur.execute("SELECT unit_id, unit_name FROM units WHERE project_id=%s", (project_id,))
        else:
            cur.execute("SELECT unit_id, unit_name FROM units")
        return cur.fetchall()

    @st.cache_data
    def get_houses(unit_id):
        if unit_id:
            cur.execute("SELECT house_id, house_no FROM houses WHERE unit_id=%s", (unit_id,))
        else:
            cur.execute("SELECT house_id, house_no FROM houses")
        return cur.fetchall()

    # 🔥 FIXED PRODUCT FILTER
    @st.cache_data
    def get_products(house_id, unit_id):
        if house_id:
            cur.execute("""
                SELECT p.product_instance_id, pm.product_code
                FROM products p
                JOIN products_master pm ON p.product_id = pm.product_id
                WHERE p.house_id = %s
                ORDER BY pm.product_code
            """, (house_id,))
        elif unit_id:
            cur.execute("""
                SELECT p.product_instance_id, pm.product_code
                FROM products p
                JOIN products_master pm ON p.product_id = pm.product_id
                JOIN houses h ON p.house_id = h.house_id
                WHERE h.unit_id = %s
                ORDER BY pm.product_code
            """, (unit_id,))
        else:
            cur.execute("""
                SELECT p.product_instance_id, pm.product_code
                FROM products p
                JOIN products_master pm ON p.product_id = pm.product_id
                ORDER BY pm.product_code
            """)
        return cur.fetchall()

    @st.cache_data
    def get_stages():
        cur.execute("SELECT stage_name FROM stages ORDER BY sequence")
        return [s[0] for s in cur.fetchall()]

    # ================= PROJECT / UNIT / HOUSE =================
    col1, col2, col3 = st.columns(3)

    with col1:
        projects = get_projects()
        project_dict = {p[1]: p[0] for p in projects}
        selected_project = st.selectbox("Select Project", ["All"] + list(project_dict.keys()))
        project_id = None if selected_project == "All" else project_dict[selected_project]

    with col2:
        units = get_units(project_id)
        unit_dict = {u[1]: u[0] for u in units}
        selected_unit = st.selectbox("Select Unit", ["All"] + list(unit_dict.keys()))
        unit_id = None if selected_unit == "All" else unit_dict[selected_unit]

    with col3:
        houses = get_houses(unit_id)
        house_dict = {h[1]: h[0] for h in houses}
        selected_house = st.selectbox("Select House", ["All"] + list(house_dict.keys()))
        house_id = None if selected_house == "All" else house_dict[selected_house]

    # ================= PRODUCTS =================
    products = get_products(house_id, unit_id)

    if not products:
        st.warning("No products found")
        return

    df = pd.DataFrame(products, columns=["product_instance_id", "product_code"])
    df["display"] = df["product_code"] + "_" + df.index.astype(str)
    df["Select"] = False

    st.subheader("Select Products")

    edited_df = st.data_editor(
        df[["Select", "display"]],
        use_container_width=True,
        hide_index=True
    )

    selected_rows = edited_df[edited_df["Select"] == True]

    if selected_rows.empty:
        st.warning("Select at least one product")
        return

    selected_ids = df.loc[selected_rows.index, "product_instance_id"].tolist()

    # ================= STAGES =================
    stage_sequence = get_stages()

    # ================= CURRENT STAGE =================
    cur.execute("""
        SELECT s.stage_name, t.status
        FROM tracking_log t
        JOIN stages s ON t.stage_id = s.stage_id
        WHERE t.product_instance_id = %s
        ORDER BY t.timestamp DESC
        LIMIT 1
    """, (selected_ids[0],))

    result = cur.fetchone()

    if result:
        current_stage = result[0]
        current_status = result[1]
    else:
        current_stage = "Not Started"
        current_status = None

    # ================= NEXT STAGE =================
    if current_stage == "Not Started":
        next_stage = stage_sequence[0]
    else:
        try:
            idx = stage_sequence.index(current_stage)
            next_stage = stage_sequence[idx + 1]
        except:
            next_stage = "Completed"

    # ================= DISPLAY =================
    col4, col5 = st.columns(2)

    if current_status == "In Progress":
        col4.warning(f"Current Stage: {current_stage} (In Progress)")
    elif current_status == "Completed":
        col4.info(f"Last Completed Stage: {current_stage}")
    else:
        col4.info("Last Completed Stage: Not Started")

    col5.success(f"Next Allowed Stage: {next_stage}")

    # ================= INPUT =================
    selected_stage = st.selectbox("Select Stage", stage_sequence)
    status = st.selectbox("Status", ["In Progress", "Completed"])

    # ================= 🔥 STRICT VALIDATION =================
    allowed_stages = []

    if current_status == "In Progress":
        allowed_stages = [current_stage]
    else:
        if next_stage != "Completed":
            allowed_stages = [next_stage]

    if selected_stage not in allowed_stages:
        if current_status == "In Progress":
            st.error(f"Complete current stage '{current_stage}' before moving forward")
        else:
            st.error(f"You must follow stage order. Next allowed: {next_stage}")
        return

    if selected_stage == current_stage and current_status == "Completed":
        st.warning("Stage already completed")
        return

    # ================= BULK UPDATE =================
    if st.button("Update Selected"):
        with st.spinner("Updating..."):

            cur.execute("SELECT stage_id FROM stages WHERE stage_name = %s", (selected_stage,))
            stage_id = cur.fetchone()[0]

            for pid in selected_ids:
                cur.execute("""
                    INSERT INTO tracking_log (product_instance_id, stage_id, status, timestamp)
                    VALUES (%s, %s, %s, NOW())
                """, (pid, stage_id, status))

            conn.commit()

            st.success(f"{len(selected_ids)} products updated successfully")

            st.rerun()
