def show_tracking(conn, cur):
    import streamlit as st
    import pandas as pd
    from psycopg2.extras import execute_values

    st.title("🏭 Production Tracker")

    # ================= DATA FUNCTIONS =================
    def get_projects():
        cur.execute("SELECT project_id, project_name FROM projects ORDER BY project_name")
        return cur.fetchall()

    def get_units(project_id):
        if project_id:
            cur.execute("SELECT unit_id, unit_name FROM units WHERE project_id=%s ORDER BY unit_name", (project_id,))
        else:
            cur.execute("SELECT unit_id, unit_name FROM units ORDER BY unit_name")
        return cur.fetchall()

    def get_products(house_ids, unit_id):
        if house_ids:
            cur.execute("""
                SELECT p.product_instance_id, pm.product_code, h.house_no
                FROM products p
                JOIN products_master pm ON p.product_id = pm.product_id
                JOIN houses h ON p.house_id = h.house_id
                WHERE p.house_id = ANY(%s)
                ORDER BY h.house_no, pm.product_code, p.product_instance_id
            """, (house_ids,))
        elif unit_id:
            cur.execute("""
                SELECT p.product_instance_id, pm.product_code, h.house_no
                FROM products p
                JOIN products_master pm ON p.product_id = pm.product_id
                JOIN houses h ON p.house_id = h.house_id
                WHERE h.unit_id = %s
                ORDER BY h.house_no, pm.product_code, p.product_instance_id
            """, (unit_id,))
        else:
            cur.execute("""
                SELECT p.product_instance_id, pm.product_code, h.house_no
                FROM products p
                JOIN products_master pm ON p.product_id = pm.product_id
                JOIN houses h ON p.house_id = h.house_id
                ORDER BY h.house_no, pm.product_code, p.product_instance_id
            """)
        return cur.fetchall()

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
        if unit_id:
            cur.execute("""
                SELECT house_id, house_no
                FROM houses
                WHERE unit_id = %s
                ORDER BY house_no
            """, (unit_id,))
        elif project_id:
            cur.execute("""
                SELECT h.house_id, h.house_no
                FROM houses h
                JOIN units u ON h.unit_id = u.unit_id
                WHERE u.project_id = %s
                ORDER BY h.house_no
            """, (project_id,))
        else:
            cur.execute("""
                SELECT house_id, house_no
                FROM houses
                ORDER BY house_no
            """)

        house_data = cur.fetchall()
        house_dict = {h[1]: h[0] for h in house_data}

        selected_houses = st.multiselect("Select House", options=list(house_dict.keys()))
        house_ids = [house_dict[h] for h in selected_houses] if selected_houses else None

    # ================= PRODUCTS =================
    products = get_products(house_ids, unit_id)

    if not products:
        st.warning("No products found")
        return

    df = pd.DataFrame(products, columns=["product_instance_id", "product_code", "house_no"])
    df["display"] = df["house_no"].astype(str) + " • " + df["product_code"]

    # ================= SEARCH =================
    search_text = st.text_input("🔍 Filter Products")

    if search_text:
        df = df[df["display"].str.contains(search_text, case=False, na=False)]

    # ================= SELECT ALL =================
    select_all = st.checkbox("Select All Visible Products", value=False)
    df["Select"] = select_all

    edited_df = st.data_editor(
        df[["Select", "display"]],
        use_container_width=True,
        hide_index=True,
        height=320
    )

    selected_rows = edited_df[edited_df["Select"] == True]

    if selected_rows.empty:
        st.info("Select products to continue")
        return

    selected_ids = df.loc[selected_rows.index, "product_instance_id"].tolist()
    st.success(f"{len(selected_ids)} products selected")

    # ================= STAGES =================
    stage_sequence = get_stages()

    # ================= GET CURRENT LIVE STAGES =================
    cur.execute("""
        WITH latest_stage AS (
            SELECT
                t.product_instance_id,
                s.stage_name,
                t.status,
                ROW_NUMBER() OVER (
                    PARTITION BY t.product_instance_id
                    ORDER BY t.timestamp DESC
                ) AS rn
            FROM tracking_log t
            JOIN stages s ON t.stage_id = s.stage_id
            WHERE t.product_instance_id = ANY(%s)
        )
        SELECT product_instance_id, stage_name, status
        FROM latest_stage
        WHERE rn = 1
    """, (selected_ids,))

    latest_data = cur.fetchall()

    if latest_data:
        latest_df = pd.DataFrame(latest_data, columns=["pid", "stage", "status"])
    else:
        latest_df = pd.DataFrame(columns=["pid", "stage", "status"])

    missing_ids = set(selected_ids) - set(latest_df["pid"].tolist())
    if missing_ids:
        extra = pd.DataFrame({
            "pid": list(missing_ids),
            "stage": ["Not Started"] * len(missing_ids),
            "status": [None] * len(missing_ids)
        })
        latest_df = pd.concat([latest_df, extra], ignore_index=True)

    unique_stages = latest_df["stage"].unique().tolist()

    if len(unique_stages) > 1:
        st.warning("⚠️ Mixed live stages detected. Please filter by house/search and select similar stage products only.")
        return

    current_stage = unique_stages[0]
    current_status = latest_df.iloc[0]["status"]

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

    # ================= VALIDATION =================
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
    if st.button("Update Selected", use_container_width=True):
        with st.spinner("Updating selected products..."):

            cur.execute("SELECT stage_id FROM stages WHERE stage_name = %s", (selected_stage,))
            stage_id = cur.fetchone()[0]

            data = [(pid, stage_id, status) for pid in selected_ids]

            execute_values(
                cur,
                """
                INSERT INTO tracking_log (product_instance_id, stage_id, status, timestamp)
                VALUES %s
                """,
                data,
                template="(%s, %s, %s, NOW())"
            )

            conn.commit()
            st.success(f"{len(selected_ids)} products updated successfully")
            st.rerun()
