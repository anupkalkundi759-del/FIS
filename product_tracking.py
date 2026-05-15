def show_product_tracking(conn, cur):
    import streamlit as st
    import pandas as pd

    st.title("🔎 Product Tracking")

    stage_rank = {
        "Measurement": 0,
        "Cutting List": 1,
        "Production": 2,
        "Pre Assembly": 3,
        "Polishing": 4,
        "Final Assembly": 5,
        "Dispatch": 6,
        "Completed": 7,
        "Not Started": 0
    }

    def get_projects():
        cur.execute("SELECT DISTINCT project_name FROM projects ORDER BY project_name")
        return ["All"] + [p[0] for p in cur.fetchall()]

    def get_units(project):
        if project == "All":
            cur.execute("SELECT DISTINCT unit_name FROM units ORDER BY unit_name")
        else:
            cur.execute("""
                SELECT DISTINCT u.unit_name
                FROM units u
                JOIN projects p ON u.project_id = p.project_id
                WHERE p.project_name = %s
                ORDER BY u.unit_name
            """, (project,))
        return ["All"] + [u[0] for u in cur.fetchall()]

    def get_houses(unit):
        if unit == "All":
            cur.execute("SELECT DISTINCT house_no FROM houses ORDER BY house_no")
        else:
            cur.execute("""
                SELECT DISTINCT h.house_no
                FROM houses h
                JOIN units u ON h.unit_id = u.unit_id
                WHERE u.unit_name = %s
                ORDER BY h.house_no
            """, (unit,))
        return ["All"] + [h[0] for h in cur.fetchall()]

    def get_stages():
        return ["All", "Measurement", "Cutting List", "Production", "Pre Assembly", "Polishing", "Final Assembly", "Dispatch"]

    col1, col2, col3, col4, col5, col6 = st.columns(6)

    selected_project = col1.selectbox("Select Project", get_projects())
    selected_unit = col2.selectbox("Select Unit Type", get_units(selected_project))
    selected_house = col3.selectbox("Select House Number", get_houses(selected_unit))
    selected_stage = col4.selectbox("Select Stage", get_stages())
    selected_status = col5.selectbox("Select Status", ["All", "Not Started", "In Progress", "Completed"])
    search = col6.text_input("Search")

    with st.spinner("Loading data..."):

        query = """
        WITH latest_tracking AS (
            SELECT
                t.product_instance_id,
                s.stage_name,
                t.status,
                t.timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY t.product_instance_id
                    ORDER BY t.timestamp DESC, t.ctid DESC
                ) AS rn
            FROM tracking_log t
            JOIN stages s ON t.stage_id = s.stage_id
        )

        SELECT 
            pm.product_code,
            pm.product_category,
            p.orientation,
            pr.project_name,
            u.unit_name,
            h.house_no,

            CASE
                WHEN lt.stage_name = 'Dispatch' AND lt.status = 'Completed' THEN 'Completed'
                WHEN lt.stage_name IS NULL THEN 'Not Started'
                ELSE lt.stage_name
            END AS stage,

            CASE
                WHEN lt.stage_name IS NULL THEN 'Not Started'
                WHEN lt.stage_name = 'Dispatch' AND lt.status = 'Completed' THEN 'Completed'
                ELSE lt.status
            END AS status,

            lt.timestamp

        FROM products p
        JOIN products_master pm ON p.product_id = pm.product_id
        JOIN houses h ON p.house_id = h.house_id
        JOIN units u ON h.unit_id = u.unit_id
        JOIN projects pr ON u.project_id = pr.project_id
        LEFT JOIN latest_tracking lt
            ON lt.product_instance_id = p.product_instance_id
            AND lt.rn = 1
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

        query += " ORDER BY h.house_no, pm.product_code"

        cur.execute(query, tuple(params))
        data = cur.fetchall()

    if not data:
        st.warning("No data found")
        return

    df = pd.DataFrame(data, columns=[
        "Product", "Type", "Orientation",
        "Project", "Unit Type", "House Number",
        "Stage", "Status", "Timestamp"
    ])

    df["LiveRank"] = df["Stage"].map(stage_rank).fillna(0)

    if selected_stage != "All":
        target_rank = stage_rank[selected_stage]

        if selected_status == "Not Started":
            if selected_stage == "Measurement":
                df = df[df["LiveRank"] == 0]
            else:
                df = df[df["LiveRank"] < target_rank]

            df["Stage"] = selected_stage
            df["Status"] = "Not Started"

        elif selected_status == "In Progress":
            df = df[
                (df["Stage"] == selected_stage) &
                (~df["Stage"].isin(["Not Started", "Completed"]))
            ]
            df["Stage"] = selected_stage
            df["Status"] = "In Progress"

        elif selected_status == "Completed":
            df = df[df["LiveRank"] > target_rank]

            df["Stage"] = selected_stage
            df["Status"] = "Completed"

        else:
            pass

    elif selected_status != "All":
        df = df[df["Status"] == selected_status]

    if df.empty:
        st.warning("No data found")
        return

    # FIXED ONLY THIS
    running_count = len(df[df["Status"] == "In Progress"])

    df["Date & Time"] = pd.to_datetime(df["Timestamp"], errors="coerce", utc=True)
    df["Date & Time"] = df["Date & Time"].dt.tz_convert("Asia/Kolkata")
    df["Date & Time"] = df["Date & Time"].dt.strftime("%d-%m-%Y %H:%M")
    df = df.drop(columns=["Timestamp", "LiveRank"])

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total Products", len(df))
    k2.metric("In Progress", running_count)

    # FIXED COMPLETED KPI
    if selected_stage == "Dispatch" and selected_status == "Completed":
        completed_count = len(df)
    else:
        completed_count = len(df[df["Status"] == "Completed"])

    k3.metric("Completed", completed_count)

    k4.metric("Not Started", len(df[df["Status"] == "Not Started"]))

    st.dataframe(df, use_container_width=True, height=420)

    st.divider()
    st.subheader("🎯 Breakdown Filters")

    b1, b2 = st.columns(2)

    breakdown_project = b1.selectbox("Select Project", get_projects(), key="b_proj")
    breakdown_unit = b2.selectbox("Select Unit Type", get_units(breakdown_project), key="b_unit")

    st.subheader("📊 Product Status Breakdown")

    with st.spinner("Calculating breakdown..."):

        query2 = """
        WITH latest_tracking AS (
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
        )

        SELECT 
            pr.project_name,
            u.unit_name,
            pm.product_code,
            COUNT(*) AS total,

            COUNT(CASE WHEN (lt.stage_name = 'Dispatch' AND lt.status = 'Completed') THEN 1 END) AS completed,

            COUNT(*) - COUNT(CASE WHEN (lt.stage_name = 'Dispatch' AND lt.status = 'Completed') THEN 1 END) AS remaining,

            COUNT(CASE WHEN lt.stage_name IS NULL THEN 1 END) AS "Yet To Start",
            COUNT(CASE WHEN lt.stage_name = 'Measurement' THEN 1 END) AS "Measurement",
            COUNT(CASE WHEN lt.stage_name = 'Cutting List' THEN 1 END) AS "Cutting List",
            COUNT(CASE WHEN lt.stage_name = 'Production' THEN 1 END) AS "Production",
            COUNT(CASE WHEN lt.stage_name = 'Pre Assembly' THEN 1 END) AS "Pre Assembly",
            COUNT(CASE WHEN lt.stage_name = 'Polishing' THEN 1 END) AS "Polishing",
            COUNT(CASE WHEN lt.stage_name = 'Final Assembly' THEN 1 END) AS "Final Assembly",
            COUNT(CASE WHEN lt.stage_name = 'Dispatch' AND COALESCE(lt.status,'') != 'Completed' THEN 1 END) AS "Dispatch"

        FROM products p
        JOIN products_master pm ON p.product_id = pm.product_id
        JOIN houses h ON p.house_id = h.house_id
        JOIN units u ON h.unit_id = u.unit_id
        JOIN projects pr ON u.project_id = pr.project_id
        LEFT JOIN latest_tracking lt
            ON lt.product_instance_id = p.product_instance_id
            AND lt.rn = 1
        WHERE 1=1
        """

        params2 = []

        if breakdown_project != "All":
            query2 += " AND pr.project_name = %s"
            params2.append(breakdown_project)

        if breakdown_unit != "All":
            query2 += " AND u.unit_name = %s"
            params2.append(breakdown_unit)

        query2 += """
        GROUP BY pr.project_name, u.unit_name, pm.product_code
        ORDER BY pr.project_name, u.unit_name, pm.product_code
        """

        cur.execute(query2, tuple(params2))
        status_data = cur.fetchall()

    if status_data:
        status_df = pd.DataFrame(status_data, columns=[
            "Project", "Unit", "Product",
            "Total", "Completed", "Remaining",
            "Yet To Start", "Measurement", "Cutting List", "Production",
            "Pre Assembly", "Polishing", "Final Assembly", "Dispatch"
        ])
        st.dataframe(status_df, use_container_width=True, height=420)
    else:
        st.warning("No product status data available")
