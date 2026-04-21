def show_dashboard(conn, cur):
    import streamlit as st
    import pandas as pd

    st.title("📊 Dashboard")

    # ================= PROJECT OVERVIEW =================
    st.subheader("🏗 Project Overview")

    cur.execute("""
        SELECT 
            p.project_name,
            COUNT(DISTINCT h.house_id) AS total_houses,
            COUNT(DISTINCT pr.product_instance_id) AS total_products,

            COUNT(DISTINCT CASE 
                WHEN s.stage_name = 'Final Assembly' AND t.status = 'Completed'
                THEN pr.product_instance_id
            END) AS completed,

            COUNT(DISTINCT CASE 
                WHEN s.stage_name = 'Dispatch' AND t.status = 'Completed' 
                THEN pr.product_instance_id 
            END) AS dispatched

        FROM projects p
        LEFT JOIN units u ON p.project_id = u.project_id
        LEFT JOIN houses h ON u.unit_id = h.unit_id
        LEFT JOIN products pr ON h.house_id = pr.house_id
        LEFT JOIN tracking_log t ON pr.product_instance_id = t.product_instance_id
        LEFT JOIN stages s ON t.stage_id = s.stage_id

        GROUP BY p.project_name
        ORDER BY p.project_name
    """)

    data = cur.fetchall()

    if not data:
        st.warning("No data available")
        return

    df = pd.DataFrame(data, columns=[
        "Project",
        "Total Houses",
        "Total Products",
        "Completed",
        "Dispatched"
    ])

    df["Pending"] = df["Total Products"] - df["Dispatched"]

    st.dataframe(df, use_container_width=True)

    # ================= FILTER ROW =================
    st.divider()
    st.subheader("📌 Project Detailed View")

    col1, col2, col3 = st.columns(3)

    # PROJECT
    with col1:
        selected_project = st.selectbox("Project", df["Project"].tolist())

    # UNIT
    with col2:
        cur.execute("""
            SELECT DISTINCT u.unit_name
            FROM units u
            JOIN projects p ON u.project_id = p.project_id
            WHERE p.project_name = %s
            ORDER BY u.unit_name
        """, (selected_project,))
        
        units = [row[0] for row in cur.fetchall()]
        selected_unit = st.selectbox("Unit", units)

    # HOUSE
    with col3:
        cur.execute("""
            SELECT h.house_no
            FROM houses h
            JOIN units u ON h.unit_id = u.unit_id
            JOIN projects p ON u.project_id = p.project_id
            WHERE p.project_name = %s AND u.unit_name = %s
            ORDER BY h.house_no
        """, (selected_project, selected_unit))

        houses = [row[0] for row in cur.fetchall()]
        houses.insert(0, "All")
        selected_house = st.selectbox("House", houses)

    # ================= HOUSE LEVEL =================
    st.subheader("🏠 House-Level Status")

    query = """
        SELECT 
            h.house_no,

            COUNT(DISTINCT pr.product_instance_id) AS total_products,

            COUNT(DISTINCT CASE 
                WHEN s.stage_name = 'Dispatch' AND t.status = 'Completed' 
                THEN pr.product_instance_id 
            END) AS dispatched,

            COUNT(DISTINCT pr.product_instance_id) -
            COUNT(DISTINCT CASE 
                WHEN s.stage_name = 'Dispatch' AND t.status = 'Completed' 
                THEN pr.product_instance_id 
            END) AS pending,

            MAX(t.timestamp) AS last_update

        FROM projects p
        JOIN units u ON p.project_id = u.project_id
        JOIN houses h ON u.unit_id = h.unit_id
        JOIN products pr ON h.house_id = pr.house_id

        LEFT JOIN tracking_log t ON pr.product_instance_id = t.product_instance_id
        LEFT JOIN stages s ON t.stage_id = s.stage_id

        WHERE p.project_name = %s
        AND u.unit_name = %s
    """

    params = [selected_project, selected_unit]

    if selected_house != "All":
        query += " AND h.house_no = %s"
        params.append(selected_house)

    query += " GROUP BY h.house_no ORDER BY h.house_no"

    cur.execute(query, tuple(params))
    house_data = cur.fetchall()

    house_df = pd.DataFrame(house_data, columns=[
        "House",
        "Total Products",
        "Dispatched",
        "Pending",
        "Last Update"
    ])

    # FORMAT TIME CLEANLY
    house_df["Last Update"] = pd.to_datetime(
        house_df["Last Update"], errors='coerce'
    ).dt.strftime("%Y-%m-%d %H:%M")

    st.dataframe(house_df, use_container_width=True)
