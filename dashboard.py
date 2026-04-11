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

    # ================= PROJECT SELECT =================
    st.divider()
    st.subheader("📌 Project Detailed View")

    selected_project = st.selectbox("Select Project", df["Project"].tolist())

    # ================= HOUSE LEVEL =================
    st.subheader("🏠 House-Level Status")

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

    house_df = pd.DataFrame(house_data, columns=[
        "House",
        "Total Products",
        "Dispatched",
        "Pending",
        "Last Update"
    ])

    st.dataframe(house_df, use_container_width=True)

    # ================= STAGE-WISE BREAKDOWN =================
    st.divider()
    st.subheader("⚙️ Stage-wise Product Status")

    cur.execute("""
        SELECT 
            s.stage_name,
            COUNT(*) AS total_products

        FROM products pr

        LEFT JOIN LATERAL (
            SELECT stage_id
            FROM tracking_log
            WHERE product_instance_id = pr.product_instance_id
            ORDER BY timestamp DESC
            LIMIT 1
        ) t ON TRUE

        LEFT JOIN stages s ON t.stage_id = s.stage_id
        JOIN houses h ON pr.house_id = h.house_id
        JOIN units u ON h.unit_id = u.unit_id
        JOIN projects p ON u.project_id = p.project_id

        WHERE p.project_name = %s

        GROUP BY s.stage_name
        ORDER BY s.stage_name
    """, (selected_project,))

    stage_data = cur.fetchall()

    stage_df = pd.DataFrame(stage_data, columns=[
        "Stage",
        "No. of Products"
    ])

    st.dataframe(stage_df, use_container_width=True)
