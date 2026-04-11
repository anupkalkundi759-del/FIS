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
                WHEN s.stage_name = 'Dispatch' AND t.status = 'Completed' 
                THEN pr.product_instance_id 
            END) AS dispatched,

            COUNT(DISTINCT pr.product_instance_id) -
            COUNT(DISTINCT CASE 
                WHEN s.stage_name = 'Dispatch' AND t.status = 'Completed' 
                THEN pr.product_instance_id 
            END) AS pending,

            MAX(CASE 
                WHEN s.stage_name = 'Dispatch' AND t.status = 'Completed' 
                THEN t.timestamp 
            END) AS last_dispatch_time

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
        "Dispatched",
        "Pending",
        "Last Dispatch Time"
    ])

    st.dataframe(df, use_container_width=True)

    # ================= PROJECT SELECTION =================
    st.divider()
    st.subheader("📌 Project Detailed View")

    project_list = df["Project"].tolist()
    selected_project = st.selectbox("Select Project", project_list)

    # ================= HOUSE LEVEL DATA =================
    cur.execute("""
        SELECT 
            h.house_no,

            COUNT(pr.product_instance_id) AS total_products,

            COUNT(DISTINCT CASE 
                WHEN s.stage_name = 'Dispatch' AND t.status = 'Completed' 
                THEN pr.product_instance_id 
            END) AS dispatched,

            COUNT(pr.product_instance_id) -
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

        GROUP BY h.house_no
        ORDER BY h.house_no
    """, (selected_project,))

    house_data = cur.fetchall()

    if not house_data:
        st.warning("No house-level data")
        return

    house_df = pd.DataFrame(house_data, columns=[
        "House",
        "Total Products",
        "Dispatched",
        "Pending",
        "Last Update"
    ])

    st.dataframe(house_df, use_container_width=True)

    # ================= KPI =================
    st.divider()
    st.subheader("📊 Key Metrics")

    col1, col2, col3 = st.columns(3)

    col1.metric("Total Houses", house_df.shape[0])
    col2.metric("Total Products", house_df["Total Products"].sum())
    col3.metric("Pending Products", house_df["Pending"].sum())
