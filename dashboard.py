def show_dashboard(conn, cur):
    import streamlit as st
    import pandas as pd

    st.title("📊 Dashboard")
    st.subheader("🏗 Project Overview")

    # ================= MAIN QUERY =================
    cur.execute("""
        SELECT 
            p.project_name,

            COUNT(DISTINCT h.house_id) AS total_houses,

            COUNT(pr.product_instance_id) AS total_products,

            COUNT(CASE 
                WHEN t.status = 'Completed' THEN 1 
            END) AS completed,

            COUNT(CASE 
                WHEN s.stage_name = 'Dispatch' AND t.status = 'Completed' THEN 1 
            END) AS dispatched,

            COUNT(pr.product_instance_id) - 
            COUNT(CASE WHEN t.status = 'Completed' THEN 1 END) AS pending,

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
        "Completed",
        "Dispatched",
        "Pending",
        "Last Dispatch Time"
    ])

    st.dataframe(df, use_container_width=True)
