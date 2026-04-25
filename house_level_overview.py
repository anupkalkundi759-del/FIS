def show_dashboard(conn, cur):
    import streamlit as st
    import pandas as pd

    st.title("📊 House Level Overview")

    # ================= LATEST LIVE STATUS CTE =================
    latest_cte = """
    WITH latest_log AS (
        SELECT
            product_instance_id,
            stage_id,
            status,
            timestamp,
            ROW_NUMBER() OVER (
                PARTITION BY product_instance_id
                ORDER BY timestamp DESC
            ) rn
        FROM tracking_log
    )
    """

    # ================= PROJECT OVERVIEW =================
    st.subheader("🏗 Project Overview")

    query1 = latest_cte + """
    SELECT
        p.project_name,
        COUNT(DISTINCT h.house_id) AS total_houses,
        COUNT(DISTINCT pr.product_instance_id) AS total_products,

        COUNT(DISTINCT CASE
            WHEN s.stage_name = 'Final Assembly' AND ll.status = 'Completed'
            THEN pr.product_instance_id
        END) AS completed,

        COUNT(DISTINCT CASE
            WHEN s.stage_name = 'Dispatch' AND ll.status = 'Completed'
            THEN pr.product_instance_id
        END) AS dispatched

    FROM projects p
    LEFT JOIN units u ON p.project_id = u.project_id
    LEFT JOIN houses h ON u.unit_id = h.unit_id
    LEFT JOIN products pr ON h.house_id = pr.house_id
    LEFT JOIN latest_log ll
        ON pr.product_instance_id = ll.product_instance_id
        AND ll.rn = 1
    LEFT JOIN stages s
        ON ll.stage_id = s.stage_id

    GROUP BY p.project_name
    ORDER BY p.project_name
    """

    cur.execute(query1)
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

    # ================= KPI ROW =================
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Projects", len(df))
    k2.metric("Total Houses", int(df["Total Houses"].sum()))
    k3.metric("Completed Products", int(df["Completed"].sum()))
    k4.metric("Dispatched Products", int(df["Dispatched"].sum()))

    st.dataframe(df, use_container_width=True, height=220)

    # ================= FILTER =================
    st.divider()
    st.subheader("📌 Project Detailed View")

    col1, col2, col3 = st.columns(3)

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
        selected_unit = st.selectbox("Unit", units)

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

    query2 = latest_cte + """
    SELECT
        h.house_no,

        COUNT(DISTINCT pr.product_instance_id) AS total_products,

        COUNT(DISTINCT CASE
            WHEN s.stage_name = 'Dispatch' AND ll.status = 'Completed'
            THEN pr.product_instance_id
        END) AS dispatched,

        COUNT(DISTINCT pr.product_instance_id) -
        COUNT(DISTINCT CASE
            WHEN s.stage_name = 'Dispatch' AND ll.status = 'Completed'
            THEN pr.product_instance_id
        END) AS pending,

        MAX(ll.timestamp) AS last_update

    FROM projects p
    JOIN units u ON p.project_id = u.project_id
    JOIN houses h ON u.unit_id = h.unit_id
    JOIN products pr ON h.house_id = pr.house_id

    LEFT JOIN latest_log ll
        ON pr.product_instance_id = ll.product_instance_id
        AND ll.rn = 1

    LEFT JOIN stages s
        ON ll.stage_id = s.stage_id

    WHERE p.project_name = %s
    AND u.unit_name = %s
    """

    params = [selected_project, selected_unit]

    if selected_house != "All":
        query2 += " AND h.house_no = %s"
        params.append(selected_house)

    query2 += " GROUP BY h.house_no ORDER BY h.house_no"

    cur.execute(query2, tuple(params))
    house_data = cur.fetchall()

    house_df = pd.DataFrame(house_data, columns=[
        "House",
        "Total Products",
        "Dispatched",
        "Pending",
        "Last Update"
    ])

    if not house_df.empty:
        house_df["Last Update"] = pd.to_datetime(
            house_df["Last Update"], errors="coerce"
        ).dt.strftime("%d-%m-%Y %I:%M %p")

    # ================= HOUSE KPI =================
    hk1, hk2, hk3 = st.columns(3)
    hk1.metric("Visible Houses", len(house_df))
    hk2.metric("Visible Dispatched", int(house_df["Dispatched"].sum()) if not house_df.empty else 0)
    hk3.metric("Visible Pending", int(house_df["Pending"].sum()) if not house_df.empty else 0)

    st.dataframe(house_df, use_container_width=True, height=350)
