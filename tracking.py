def show_tracking(conn, cur):
    import streamlit as st
    import pandas as pd
    from psycopg2.extras import execute_values

    st.title("🏭 Production Tracker")

    try:
        if conn.closed != 0:
            st.error("Database connection lost. Please refresh once.")
            return
    except:
        st.error("Database connection issue. Please refresh.")
        return

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
        return [s[0] for s in cur.fetchall()]

    @st.cache_data(ttl=300)
    def get_products_cached(house_ids_tuple, unit_id):
        house_ids = list(house_ids_tuple) if house_ids_tuple else None

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
        house_data = get_houses(project_id, unit_id)
        house_dict = {h[1]: h[0] for h in house_data}
        selected_houses = st.multiselect("Select House", options=list(house_dict.keys()))
        house_ids = [house_dict[h] for h in selected_houses] if selected_houses else None

    products = get_products_cached(tuple(house_ids) if house_ids else tuple(), unit_id)
    df = pd.DataFrame(products, columns=["product_instance_id", "product_code", "house_no"]) if products else pd.DataFrame()

    if df.empty:
        st.warning("No products found")
        return

    df["display"] = df["house_no"].astype(str) + " • " + df["product_code"]

    search_text = st.text_input("🔍 Filter Products")
    if search_text:
        df = df[df["display"].str.contains(search_text, case=False, na=False)]

    select_all = st.checkbox("Select All Visible Products")
    df["Select"] = select_all

    with st.expander("📦 Product Selection Table", expanded=False):
        edited_df = st.data_editor(
            df[["Select", "display"]],
            use_container_width=True,
            hide_index=True,
            height=300,
            key="main_product_editor"
        )

    selected_rows = edited_df[edited_df["Select"] == True]

    if selected_rows.empty:
        st.info("Select products to continue")
        return

    selected_ids = df.loc[selected_rows.index, "product_instance_id"].tolist()
    st.success(f"{len(selected_ids)} products selected")

    stage_sequence = get_stages()

    cur.execute("""
        WITH latest_stage AS (
            SELECT
                t.product_instance_id,
                s.stage_name,
                t.status,
                ROW_NUMBER() OVER (
                    PARTITION BY t.product_instance_id
                    ORDER BY t.timestamp DESC, t.ctid DESC
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

    matrix_df = df[df["product_instance_id"].isin(selected_ids)][["product_instance_id", "display"]].copy()
    matrix_df = matrix_df.merge(latest_df, left_on="product_instance_id", right_on="pid", how="left")
    matrix_df["stage"] = matrix_df["stage"].fillna("Not Started")
    matrix_df["status"] = matrix_df["status"].fillna("Not Started")

    if len(stage_sequence) > 0:
        last_stage_name = stage_sequence[-1]
        matrix_df.loc[
            (matrix_df["stage"] == last_stage_name) &
            (matrix_df["status"] == "Completed"),
            "stage"
        ] = "Completed"

    st.markdown("### 📍 Current Live Stages Found")

    available_stages = []
    stage_counts = {}

    for stg in ["Not Started"] + stage_sequence + ["Completed"]:
        cnt = len(matrix_df[matrix_df["stage"] == stg])
        if cnt > 0:
            available_stages.append(stg)
            stage_counts[stg] = cnt

    stage_cols = st.columns(len(available_stages))
    for i, stg in enumerate(available_stages):
        if stage_cols[i].button(f"{stg} ({stage_counts[stg]})", use_container_width=True):
            st.session_state["inspect_stage"] = stg

    inspect_stage = st.session_state.get("inspect_stage", available_stages[0])
    stage_group = matrix_df[matrix_df["stage"] == inspect_stage].copy()

    st.info(f"Inspecting: {inspect_stage}")

    stage_search = st.text_input(f"🔎 Search inside {inspect_stage}", key="stage_search_box")
    if stage_search:
        stage_group = stage_group[stage_group["display"].str.contains(stage_search, case=False, na=False)]

    select_stage_all = st.checkbox(f"Select All Visible in {inspect_stage}", key="stage_select_all")

    shown_rows = stage_group[["product_instance_id", "display"]].copy()
    shown_rows["Move"] = select_stage_all

    edited_stage = st.data_editor(
        shown_rows[["Move", "display"]],
        use_container_width=True,
        hide_index=True,
        key="stage_move_editor"
    )

    chosen = edited_stage[edited_stage["Move"] == True]

    if chosen.empty:
        return

    move_ids = shown_rows.loc[chosen.index, "product_instance_id"].tolist()
    current_status = stage_group.iloc[0]["status"]
    current_stage = inspect_stage

    if current_stage == "Not Started":
        next_stage = stage_sequence[0]
    else:
        try:
            idx = stage_sequence.index(current_stage)
            next_stage = stage_sequence[idx + 1]
        except:
            next_stage = "Completed"

    col4, col5 = st.columns(2)
    col4.info(f"Current Stage: {current_stage} ({current_status})")
    col5.success(f"Next Allowed Stage: {next_stage}")

    movement_type = st.radio("Movement Type", ["Normal Forward Move", "Rework / Send Back"], horizontal=True, key="movement_selector")

    with st.form(f"tracking_update_form_{movement_type}"):

        if movement_type == "Normal Forward Move":
            allowed_stage_options = stage_sequence
            selected_stage = st.selectbox("Move Selected Products To Stage", allowed_stage_options)
            status = st.selectbox("Update Status", ["In Progress", "Completed"])
        else:
            if current_stage == "Not Started":
                allowed_stage_options = ["Not Started"]
            else:
                try:
                    idx = stage_sequence.index(current_stage)
                    allowed_stage_options = ["Not Started"] + stage_sequence[:idx]
                except:
                    allowed_stage_options = ["Not Started"]

            selected_stage = st.selectbox("Move Selected Products To Stage", allowed_stage_options)
            status = st.selectbox("Update Status", ["In Progress"])
            rework_reason = st.selectbox("Rework Reason", [
                "Dimension Issue", "Weld Defect", "Hole Misalignment",
                "Surface Damage", "Assembly Mismatch", "Polish Rejection",
                "QC Failed", "Other"
            ])
            rework_note = st.text_input("Type Reason (Optional)")

        submitted = st.form_submit_button("Update Selected")

    if submitted:

        if movement_type == "Normal Forward Move":

            if current_status == "In Progress":
                if not (selected_stage == current_stage and status == "Completed"):
                    st.error("Complete current stage first before moving ahead")
                    return

            elif current_status == "Completed":
                if selected_stage != next_stage and not (current_stage == stage_sequence[-1] and selected_stage == current_stage):
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

                data = []

                for pid in move_ids:
                    data.append((pid, stage_id, status))

                    if movement_type == "Normal Forward Move" and selected_stage == current_stage and status == "Completed":
                        if current_stage in stage_sequence:
                            idx = stage_sequence.index(current_stage)
                            if idx + 1 < len(stage_sequence):
                                next_stage_name = stage_sequence[idx + 1]
                                cur.execute("SELECT stage_id FROM stages WHERE stage_name=%s", (next_stage_name,))
                                auto_next_stage_id = cur.fetchone()[0]
                                data.append((pid, auto_next_stage_id, "In Progress"))

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

                if "inspect_stage" in st.session_state:
                    del st.session_state["inspect_stage"]

                st.success(f"{len(move_ids)} products updated successfully")
                st.rerun()

            except Exception as e:
                try:
                    conn.rollback()
                except:
                    pass
                st.error(f"Update failed: {e}")
