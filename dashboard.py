def show_dashboard(conn, cur):
    import streamlit as st

    st.title("📊 Dashboard")

    cur.execute("SELECT COUNT(*) FROM houses")
    total = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM houses WHERE predicted_finish IS NOT NULL")
    predicted = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM houses WHERE predicted_finish < CURRENT_DATE")
    delayed = cur.fetchone()[0]

    st.metric("Total Houses", total)
    st.metric("Predicted", predicted)
    st.metric("Delayed", delayed)