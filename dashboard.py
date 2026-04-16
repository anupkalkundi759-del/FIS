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
        "Completed",
        "Dispatched",
        "Pending",
        "Last Dispatch Time"
    ])

    st.dataframe(df, use_container_width=True)

    # ================= PRODUCT DISTRIBUTION =================
    st.divider()
    st.subheader("📦 Product Distribution (Project → Unit → Product)")

    cur.execute("""
        SELECT 
            p.project_name,
            u.unit_name,
            pm.product_code,
            COUNT(pr.product_instance_id) AS product_count,
            COUNT(DISTINCT h.house_id) AS total_houses

        FROM projects p
        JOIN units u ON p.project_id = u.project_id
        JOIN houses h ON u.unit_id = h.unit_id
        JOIN products pr ON h.house_id = pr.house_id
        JOIN products_master pm ON pr.product_id = pm.product_id

        GROUP BY 
            p.project_name, 
            u.unit_name, 
            pm.product_code

        ORDER BY 
            p.project_name, 
            u.unit_name, 
            pm.product_code
    """)

    product_data = cur.fetchall()

    if product_data:
        product_df = pd.DataFrame(product_data, columns=[
            "Project",
            "Unit",
            "Product",
            "Product Count",
            "Total Houses"
        ])

        st.dataframe(product_df, use_container_width=True)
    else:
        st.warning("No product distribution data available")

    # ================= PRODUCT STATUS BREAKDOWN =================
    st.divider()
    st.subheader("📊 Product Status Breakdown (Live Stage Tracking)")

    cur.execute("""
        WITH latest_stage AS (
            SELECT 
                pr.product_instance_id,
                p.project_name,
                u.unit_name,
                pm.product_code,
                s.stage_name,
                s.sequence,
                t.status,
                ROW_NUMBER() OVER (
                    PARTITION BY pr.product_instance_id 
                    ORDER BY s.sequence DESC, t.timestamp DESC
                ) as rn

            FROM products pr
            JOIN houses h ON pr.house_id = h.house_id
            JOIN units u ON h.unit_id = u.unit_id
            JOIN projects p ON u.project_id = p.project_id
            JOIN products_master pm ON pr.product_id = pm.product_id
            LEFT JOIN tracking_log t ON pr.product_instance_id = t.product_instance_id
            LEFT JOIN stages s ON t.stage_id = s.stage_id
        )

        SELECT 
            project_name,
            unit_name,
            product_code,

            COUNT(*) AS total,

            COUNT(CASE 
                WHEN stage_name = 'Dispatch' AND status = 'Completed' 
                THEN 1 END) AS completed,

            COUNT(*) - COUNT(CASE 
                WHEN stage_name = 'Dispatch' AND status = 'Completed' 
                THEN 1 END) AS remaining,

            COUNT(CASE WHEN stage_name = 'Cutting' THEN 1 END) AS cutting,
            COUNT(CASE WHEN stage_name = 'Production' THEN 1 END) AS production,
            COUNT(CASE WHEN stage_name = 'Pre Assembly' THEN 1 END) AS pre_assembly,
            COUNT(CASE WHEN stage_name = 'Polishing' THEN 1 END) AS polishing,
            COUNT(CASE WHEN stage_name = 'Final Assembly' THEN 1 END) AS final_assembly

        FROM latest_stage
        WHERE rn = 1

        GROUP BY project_name, unit_name, product_code
        ORDER BY project_name, unit_name, product_code
    """)

    status_data = cur.fetchall()

    if status_data:
        status_df = pd.DataFrame(status_data, columns=[
            "Project",
            "Unit",
            "Product",
            "Total",
            "Completed",
            "Remaining",
            "Cutting",
            "Production",
            "Pre Assembly",
            "Polishing",
            "Final Assembly"
        ])

        st.dataframe(status_df, use_container_width=True)
    else:
        st.warning("No product status data available")

    # ================= PROJECT + UNIT INLINE =================
    st.divider()
    st.subheader("📌 Project Detailed View")

    col1, col2 = st.columns(2)

    with col1:
        selected_project = st.selectbox("Project", df["Project"].tolist())

    with col2:
        cur.execute("""
            SELECT DISTINCT u.unit_name
            FROM units u
            JOIN projects p ON u.project_id = p.project_id
            WHERE p.project_name = %s
            ORDER BY u.unit_name
        """, (selected_project,))

        units = [row[0] for row in cur.fetchall()]

        if not units:
            st.warning("No units found for this project")
            return

        selected_unit = st.selectbox("Unit", units)

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
        AND u.unit_name = %s

        GROUP BY h.house_no
        ORDER BY h.house_no
    """, (selected_project, selected_unit))

    house_data = cur.fetchall()

    house_df = pd.DataFrame(house_data, columns=[
        "House",
        "Total Products",
        "Dispatched",
        "Pending",
        "Last Update"
    ])

    st.dataframe(house_df, use_container_width=True)
