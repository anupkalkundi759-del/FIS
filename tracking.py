def show_tracking(conn, cur):
    import streamlit as st

    st.title("📍 Simple Tracking")

    cur.execute("""
        SELECT house_id, measurement_date, predicted_finish, status
        FROM houses
        ORDER BY house_id
    """)

    data = cur.fetchall()

    for row in data:
        st.write({
            "House": row[0],
            "Measurement": row[1],
            "Predicted Finish": row[2],
            "Status": row[3]
        })