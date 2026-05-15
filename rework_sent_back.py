def show_rework_history(conn, cur):

    import streamlit as st
    import pandas as pd

    st.title("🔁 Rework / Sent Back History")

    cur.execute("""
        SELECT
            r.product_code,
            r.house_no,
            r.from_stage,
            r.to_stage,
            r.reason,
            r.note,
            r.timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata' AS india_timestamp,
            pr.project_name,
            u.unit_name
        FROM rework_sentback_log r
        LEFT JOIN houses h ON r.house_no = h.house_no
        LEFT JOIN units u ON h.unit_id = u.unit_id
        LEFT JOIN projects pr ON u.project_id = pr.project_id
        ORDER BY r.timestamp DESC
    """)

    data = cur.fetchall()

    if not data:
        st.warning("No rework / sent back history found")
        return

    df = pd.DataFrame(data, columns=[
        "Product",
        "House",
        "From Stage",
        "To Stage",
        "Reason",
        "Note",
        "Timestamp",
        "Project",
        "Unit"
    ])

    df["Timestamp"] = pd.to_datetime(df["Timestamp"]).dt.strftime("%Y-%m-%d %I:%M:%S %p")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        selected_project = st.selectbox(
            "Select Project",
            ["All"] + sorted(df["Project"].dropna().astype(str).unique().tolist())
        )

    filtered_for_units = df.copy()
    if selected_project != "All":
        filtered_for_units = filtered_for_units[filtered_for_units["Project"].astype(str) == selected_project]

    with col2:
        selected_unit = st.selectbox(
            "Select Unit Type",
            ["All"] + sorted(filtered_for_units["Unit"].dropna().astype(str).unique().tolist())
        )

    filtered_for_houses = filtered_for_units.copy()
    if selected_unit != "All":
        filtered_for_houses = filtered_for_houses[filtered_for_houses["Unit"].astype(str) == selected_unit]

    with col3:
        selected_house = st.selectbox(
            "Select House Number",
            ["All"] + sorted(filtered_for_houses["House"].dropna().astype(str).unique().tolist())
        )

    with col4:
        selected_reason = st.selectbox(
            "Rework / Sent Back Reason",
            ["All"] + sorted(df["Reason"].dropna().astype(str).unique().tolist())
        )

    search_text = st.text_input("🔍 Search Product / House / Note")

    filtered_df = df.copy()

    if selected_project != "All":
        filtered_df = filtered_df[filtered_df["Project"].astype(str) == selected_project]

    if selected_unit != "All":
        filtered_df = filtered_df[filtered_df["Unit"].astype(str) == selected_unit]

    if selected_house != "All":
        filtered_df = filtered_df[filtered_df["House"].astype(str) == selected_house]

    if selected_reason != "All":
        filtered_df = filtered_df[filtered_df["Reason"].astype(str) == selected_reason]

    if search_text:
        filtered_df = filtered_df[
            filtered_df["Product"].astype(str).str.contains(search_text, case=False, na=False) |
            filtered_df["House"].astype(str).str.contains(search_text, case=False, na=False) |
            filtered_df["Note"].astype(str).str.contains(search_text, case=False, na=False)
        ]

    display_df = filtered_df[[
        "Project",
        "Unit",
        "Product",
        "House",
        "From Stage",
        "To Stage",
        "Reason",
        "Note",
        "Timestamp"
    ]]

    st.markdown(f"### Records Found: {len(display_df)}")

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True
    )

    csv = display_df.to_csv(index=False).encode("utf-8")

    st.download_button(
        "⬇ Download Rework / Sent Back Report",
        csv,
        file_name="rework_sentback_history.csv",
        mime="text/csv"
    )
