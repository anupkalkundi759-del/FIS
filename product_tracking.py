def show_product_tracking(conn, cur):
    import streamlit as st
    import pandas as pd

    st.title("🔎 Product Tracking")

    # ================= FILTERS =================
    col1, col2, col3, col4 = st.columns(4)

    cur.execute("SELECT DISTINCT project_name FROM projects")
    projects = ["All"] + [p[0] for p in cur.fetchall()]

    cur.execute("SELECT DISTINCT unit_name FROM units")
    units = ["All"] + [u[0] for u in cur.fetchall()]

    cur.execute("SELECT DISTINCT house_no FROM houses")
    houses = ["All"] + [h[0] for h in cur.fetchall()]

    selected_project = col1.selectbox("Project", projects)
    selected_unit = col2.selectbox("Unit", units)
    selected_house = col3.selectbox("House", houses)
    search = col4.text_input("Search Product")

    # ================= QUERY =================
    query = """
        SELECT 
            pm.product_code,
            pm.product_category,
            p.orientation,
            pr.project_name,
            u.unit_name,
            h.house_no,

            COALESCE(s.stage_name, 'Not Started') AS stage,
            COALESCE(t.status, 'Not Started') AS status,

            t.timestamp

        FROM products p
        JOIN products_master pm ON p.product_id = pm.product_id
        JOIN houses h ON p.house_id = h.house_id
        JOIN units u ON h.unit_id = u.unit_id
        JOIN projects pr ON u.project_id = pr.project_id

        LEFT JOIN LATERAL (
            SELECT stage_id, status, timestamp
            FROM tracking_log
            WHERE product_instance_id = p.product_instance_id
            ORDER BY timestamp DESC
            LIMIT 1
        ) t ON TRUE

        LEFT JOIN stages s ON t.stage_id = s.stage_id

        WHERE 1=1
    """

    params = []

    if selected_project != "All":
        query += " AND pr.project_name = %s"
        params.append(selected_project)

    if selected_unit != "All":
        query += " AND u.unit_name = %s"
        params.append(selected_unit)

    if selected_house != "All":
        query += " AND h.house_no = %s"
        params.append(selected_house)

    if search:
        query += " AND pm.product_code ILIKE %s"
        params.append(f"%{search}%")

    query += " ORDER BY pr.project_name, u.unit_name, h.house_no"

    cur.execute(query, tuple(params))
    data = cur.fetchall()

    # ================= DATAFRAME =================
    df = pd.DataFrame(data, columns=[
        "Product", "Type", "Orientation",
        "Project", "Unit", "House",
        "Stage", "Status", "Timestamp"
    ])

    if df.empty:
        st.warning("No data found")
        return

    # ================= TIME =================
    df["Date & Time"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    df["Date & Time"] = df["Date & Time"].dt.tz_localize("UTC", errors='coerce').dt.tz_convert("Asia/Kolkata")
    df["Date & Time"] = df["Date & Time"].astype(str).replace("NaT", "-")

    df = df.drop(columns=["Timestamp"])

    # ================= PROGRESS =================
    df["Progress %"] = df["Status"].map({
        "Not Started": 0,
        "In Progress": 50,
        "Completed": 100
    }).fillna(0)

    # ================= DISPLAY =================
    st.dataframe(df, use_container_width=True)

    # ================= STAGE CONTROL =================
    st.divider()
    st.subheader("⚙️ Stage Control")

    # Unique product selection
    df["Label"] = df["Product"] + " | House " + df["House"].astype(str)
    selected_label = st.selectbox("Select Product", df["Label"].unique())

    selected_row = df[df["Label"] == selected_label].iloc[0]

    product_code = selected_row["Product"]
    house_no = selected_row["House"]

    # ================= GET product_instance_id =================
    cur.execute("""
        SELECT p.product_instance_id
        FROM products p
        JOIN houses h ON p.house_id = h.house_id
        JOIN products_master pm ON p.product_id = pm.product_id
        WHERE pm.product_code = %s AND h.house_no = %s
        LIMIT 1
    """, (product_code, house_no))

    result = cur.fetchone()

    if not result:
        st.error("Product instance not found")
        return

    product_instance_id = result[0]

    # ================= CURRENT STAGE =================
    cur.execute("""
        SELECT s.stage_name, a.sequence_order
        FROM tracking_log t
        JOIN stages s ON t.stage_id = s.stage_id
        JOIN activity_master a ON s.stage_name = a.activity_name
        WHERE t.product_instance_id = %s
        ORDER BY t.timestamp DESC
        LIMIT 1
    """, (product_instance_id,))

    current = cur.fetchone()

    if current:
        current_stage, current_seq = current
    else:
        current_stage = "Not Started"
        current_seq = 0

    st.info(f"📍 Current Stage: {current_stage}")

    # ================= NEXT STAGE =================
    cur.execute("""
        SELECT activity_name, sequence_order
        FROM activity_master
        WHERE sequence_order = %s
    """, (current_seq + 1,))

    next_stage = cur.fetchone()

    # ================= MOVE BUTTON =================
    if next_stage:
        next_stage_name, _ = next_stage

        if st.button(f"➡ Move to {next_stage_name}"):

            cur.execute("""
                SELECT stage_id FROM stages WHERE stage_name=%s
            """, (next_stage_name,))
            stage_row = cur.fetchone()

            if not stage_row:
                st.error("Stage not found")
                return

            stage_id = stage_row[0]

            cur.execute("""
                INSERT INTO tracking_log (product_instance_id, stage_id, status, timestamp)
                VALUES (%s, %s, 'Completed', NOW())
            """, (product_instance_id, stage_id))

            conn.commit()

            st.success(f"✅ Moved to {next_stage_name}")
            st.rerun()

    else:
        st.success("✅ All stages completed")
