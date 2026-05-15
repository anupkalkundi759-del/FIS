def show_rework_history(conn, cur):

    import streamlit as st
    import pandas as pd

    st.title("🔁 Rework / Send Back History")

    cur.execute("""
        SELECT
            product_code,
            house_no,
            from_stage,
            to_stage,
            reason,
            note,
            timestamp
        FROM rework_sentback_log
        ORDER BY timestamp DESC
    """)

    data = cur.fetchall()

    if not data:
        st.warning("No rework / send back history found")
        return

    df = pd.DataFrame(data, columns=[
        "Product",
        "House",
        "From Stage",
        "To Stage",
        "Reason",
        "Note",
        "Timestamp"
    ])

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        selected_house = st.selectbox(
            "Filter House",
            ["All"] + sorted(df["House"].dropna().astype(str).unique().tolist())
        )

    with col2:
        selected_from_stage = st.selectbox(
            "Filter From Stage",
            ["All"] + sorted(df["From Stage"].dropna().astype(str).unique().tolist())
        )

    with col3:
        selected_to_stage = st.selectbox(
            "Filter To Stage",
            ["All"] + sorted(df["To Stage"].dropna().astype(str).unique().tolist())
        )

    with col4:
        selected_reason = st.selectbox(
            "Filter Reason",
            ["All"] + sorted(df["Reason"].dropna().astype(str).unique().tolist())
        )

    search_text = st.text_input("🔍 Search Product / House / Note")

    filtered_df = df.copy()

    if selected_house != "All":
        filtered_df = filtered_df[filtered_df["House"].astype(str) == selected_house]

    if selected_from_stage != "All":
        filtered_df = filtered_df[filtered_df["From Stage"].astype(str) == selected_from_stage]

    if selected_to_stage != "All":
        filtered_df = filtered_df[filtered_df["To Stage"].astype(str) == selected_to_stage]

    if selected_reason != "All":
        filtered_df = filtered_df[filtered_df["Reason"].astype(str) == selected_reason]

    if search_text:
        filtered_df = filtered_df[
            filtered_df["Product"].astype(str).str.contains(search_text, case=False, na=False) |
            filtered_df["House"].astype(str).str.contains(search_text, case=False, na=False) |
            filtered_df["Note"].astype(str).str.contains(search_text, case=False, na=False)
        ]

    st.markdown(f"### Records Found: {len(filtered_df)}")

    st.dataframe(
        filtered_df,
        use_container_width=True,
        hide_index=True
    )

    csv = filtered_df.to_csv(index=False).encode("utf-8")

    st.download_button(
        "⬇ Download Rework / Send Back Report",
        csv,
        file_name="rework_sentback_history.csv",
        mime="text/csv"
    )
