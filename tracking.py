def show_tracking(conn, cur):
    import streamlit as st
    import pandas as pd
    from psycopg2.extras import execute_values

    st.title("🏭 Production Tracker")

    # ================= MASTER LOADERS =================
    @st.cache_data(ttl=300)
    def get_projects():
        cur.execute("SELECT project_id, project_name FROM projects ORDER BY project_name")
        return cur.fetchall()

    @st.cache_data(ttl=300)
    def get_units(project_id):
        if project_id:
            cur.execute("SELECT unit_id, unit_name FROM units WHERE project_id=%s ORDER BY unit_name", (project_id,))
        else:
            cur.execute("SELECT unit_id, unit_name FROM units ORDER BY unit_name")
        return cur.fetchall()

    @st.cache_data(ttl=300)
    def get_houses(project_id, unit_id):
        if unit_id:
            cur.execute("SELECT house_id, house_no FROM houses WHERE unit_id=%s ORDER BY house_no", (unit_id,))
        elif project_id:
            cur.execute("""
                SELECT h.house_id, h.house_no
                FROM houses h
                JOIN units u ON h.unit_id = u.unit_id
                WHERE u.project_id=%s
                ORDER BY h.house_no
            """, (project_id,))
        else:
            cur.execute("SELECT house_id, house_no FROM houses ORDER BY house_no")
        return cur.fetchall()

    @st.cache_data(ttl=600)
    def get_stages():
        cur.execute("SELECT stage_name FROM stages ORDER BY sequence")
        return [x[0] for x in cur.fetchall()]

    def get_products(house_ids, unit_id):
        if house_ids:
            cur.execute("""
                SELECT p.product_instance_id, pm.product_code, h.house_no
                FROM products p
                JOIN products_master pm ON p.product_id = pm.product_id
                JOIN houses h ON p.house_id = h.house_id
                WHERE p.house_id = ANY(%s)
                ORDER BY h.house_no, pm.product_code
            """, (house_ids,))
        elif unit_id:
            cur.execute("""
                SELECT p.product_instance_id, pm.product_code, h.house_no
                FROM products p
                JOIN products_master pm ON p.product_id = pm.product_id
                JOIN houses h ON p.house_id = h.house_id
                WHERE h.unit_id = %s
                ORDER BY h.house_no, pm.product_code
            """, (unit_id,))
        else:
            cur.execute("""
                SELECT p.product_instance_id, pm.product_code, h.house_no
                FROM products p
                JOIN products_master pm ON p.product_id = pm.product_id
                JOIN houses h ON p.house_id = h.house_id
                ORDER BY h.house_no, pm.product_code
            """)
        return cur.fetchall()

    # ================= FILTER BAR =================
    c1, c2, c3 = st.columns(3)

    with c1:
        projects = get_projects()
        project_dict = {x[1]: x[0] for x in projects}
        selected_project = st.selectbox("Select Project", ["All"] + list(project_dict.keys()))
        project_id = None if selected_project == "All" else project_dict[selected_project]

    with c2:
        units = get_units(project_id)
        unit_dict = {x[1]: x[0] for x in units}
        selected_unit = st.selectbox("Select Unit", ["All"] + list(unit_dict.keys()))
        unit_id = None if selected_unit == "All" else unit_dict[selected_unit]

    with c3:
        houses = get_houses(project_id, unit_id)
        house_dict = {x[1]: x[0] for x in houses}
        selected_houses = st.multiselect("Select House", list(house_dict.keys()))
        house_ids = [house_dict[h] for h in selected_houses] if selected_houses else None

    # ================= PRODUCT LOAD =================
    filter_signature = (project_id, unit_id, tuple(house_ids) if house_ids else None)

    if "fast_prod_sig" not in st.session_state or st.session_state.fast_prod_sig != filter_signature:
        products = get_products(house_ids, unit_id)
        prod_df = pd.DataFrame(products, columns=["product_instance_id", "product_code", "house_no"])
        if prod_df.empty:
            st.warning("No products found")
            return
        prod_df["display"] = prod_df["house_no"].astype(str) + " • " + prod_df["product_code"]
        st.session_state.fast_prod_df = prod_df
        st.session_state.fast_prod_sig = filter_signature

    df = st.session_state.fast_prod_df.copy()

    search_text = st.text_input("🔍 Filter Products")
    if search_text:
        df = df[df["display"].str.contains(search_text, case=False, na=False)]

    product_pick = st.multiselect(
        "Select Products",
        options=df["display"].tolist()
    )

    if not product_pick:
        st.info("Select products to continue")
        return

    selected_df = df[df["display"].isin(product_pick)].copy()
    selected_ids = selected_df["product_instance_id"].tolist()
    st.success(f"{len(selected_ids)} products selected")

    # ================= ULTRA FAST LIVE STAGE LOAD =================
    cur.execute("""
        SELECT product_instance_id, stage_name, status
        FROM product_current_stage
        WHERE product_instance_id = ANY(%s)
    """, (selected_ids,))
    live_data = cur.fetchall()

    live_df = pd.DataFrame(live_data, columns=["pid", "stage", "status"]) if live_data else pd.DataFrame(columns=["pid","stage","status"])

    missing_ids = set(selected_ids) - set(live_df["pid"].tolist())
    if missing_ids:
        extra = pd.DataFrame({
            "pid": list(missing_ids),
            "stage": ["Not Started"] * len(missing_ids),
            "status": [None] * len(missing_ids)
        })
        live_df = pd.concat([live_df, extra], ignore_index=True)

    matrix_df = selected_df.merge(live_df, left_on="product_instance_id", right_on="pid", how="left")
    matrix_df["stage"] = matrix_df["stage"].fillna("Not Started")

    stage_sequence = get_stages()

    st.markdown("### 📍 Current Live Stages Found")

    available_stages = []
    stage_counts = {}

    for stg in ["Not Started"] + stage_sequence:
        cnt = len(matrix_df[matrix_df["stage"] == stg])
        if cnt > 0:
            available_stages.append(stg)
            stage_counts[stg] = cnt

    cols = st.columns(len(available_stages))
    for i, stg in enumerate(available_stages):
        if cols[i].button(f"{stg} ({stage_counts[stg]})", use_container_width=True):
            st.session_state.inspect_stage = stg

    inspect_stage = st.session_state.get("inspect_stage", available_stages[0])

    stage_group = matrix_df[matrix_df["stage"] == inspect_stage].copy()

    st.info(f"Inspecting: {inspect_stage}")

    stage_search = st.text_input(f"🔎 Search inside {inspect_stage}")
    if stage_search:
        stage_group = stage_group[stage_group["display"].str.contains(stage_search, case=False, na=False)]

    move_pick = st.multiselect(
        "Choose Products To Move",
        options=stage_group["display"].tolist()
    )

    if not move_pick:
        return

    move_df = stage_group[stage_group["display"].isin(move_pick)].copy()
    move_ids = move_df["product_instance_id"].tolist()

    current_stage = inspect_stage
    current_status = move_df.iloc[0]["status"]

    if current_stage == "Not Started":
        next_stage = stage_sequence[0]
    else:
        try:
            idx = stage_sequence.index(current_stage)
            next_stage = stage_sequence[idx + 1]
        except:
            next_stage = "Completed"

    a, b = st.columns(2)
    a.info(f"Current Stage: {current_stage}")
    b.success(f"Next Allowed Stage: {next_stage}")

    movement_type = st.radio("Movement Type", ["Normal Forward Move", "Rework / Send Back"], horizontal=True)

    if movement_type == "Normal Forward Move":
        allowed_stage_options = stage_sequence
    else:
        if current_stage == "Not Started":
            allowed_stage_options = ["Not Started"]
        else:
            try:
                idx = stage_sequence.index(current_stage)
                allowed_stage_options = stage_sequence[:idx]
            except:
                allowed_stage_options = stage_sequence

    selected_stage = st.selectbox("Move Selected Products To Stage", allowed_stage_options)
    status = st.selectbox("Update Status", ["In Progress", "Completed"])

    if movement_type == "Rework / Send Back":
        rework_reason = st.selectbox("Rework Reason", [
            "Dimension Issue", "Weld Defect", "Hole Misalignment",
            "Surface Damage", "Assembly Mismatch", "Polish Rejection",
            "QC Failed", "Other"
        ])

    if st.button("Update Selected", use_container_width=True):

        if movement_type == "Normal Forward Move":
            if current_status == "In Progress":
                allowed_stages = [current_stage]
            else:
                allowed_stages = [next_stage] if next_stage != "Completed" else []

            if selected_stage not in allowed_stages:
                st.error("Invalid stage movement")
                return
        else:
            if selected_stage == current_stage:
                st.error("Rework stage cannot be same as current")
                return

        with st.spinner("Updating selected products..."):
            try:
                cur.execute("SELECT stage_id FROM stages WHERE stage_name=%s", (selected_stage,))
                stage_id = cur.fetchone()[0]

                data = [(pid, stage_id, status) for pid in move_ids]

                execute_values(
                    cur,
                    """
                    INSERT INTO tracking_log (product_instance_id, stage_id, status, timestamp)
                    VALUES %s
                    """,
                    data,
                    template="(%s, %s, %s, NOW())"
                )

                for pid in move_ids:
                    cur.execute("""
                        DELETE FROM product_current_stage WHERE product_instance_id=%s
                    """, (pid,))
                    cur.execute("""
                        INSERT INTO product_current_stage (product_instance_id, stage_id, stage_name, status, updated_at)
                        VALUES (%s,%s,%s,%s,NOW())
                    """, (pid, stage_id, selected_stage, status))

                conn.commit()

                st.success(f"{len(move_ids)} products updated successfully")
                st.rerun()

            except Exception as e:
                conn.rollback()
                st.error(f"Update failed: {e}")
