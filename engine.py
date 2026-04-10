def run_engine(conn, cur):
    import streamlit as st
    from datetime import timedelta
    import pandas as pd

    st.title("⚙️ Scheduling Engine")

    # ================= LOAD DATA =================
    cur.execute("""
        SELECT activity_name, sequence_order, duration_days
        FROM activity_master
        ORDER BY sequence_order
    """)
    activities = cur.fetchall()

    cur.execute("SELECT stage_name, capacity_per_day FROM stage_capacity")
    capacity_map = dict(cur.fetchall())

    cur.execute("""
        SELECT house_id, measurement_date
        FROM houses
        WHERE measurement_date IS NOT NULL
        ORDER BY measurement_date
    """)
    houses = cur.fetchall()

    if not houses:
        st.warning("No measurement data available")
        return

    results = []

    # ================= ENGINE =================
    for house_id, start_date in houses:

        current_date = start_date

        for act_name, seq, duration in activities:

            # skip non-capacity stages
            if act_name in ["Measurement", "Cutting List"]:
                current_date += timedelta(days=duration)
                continue

            # capacity logic (basic placeholder)
            capacity = capacity_map.get(act_name, 999)

            delay = 0
            if capacity < 3:
                delay = 2  # simple assumption

            current_date += timedelta(days=duration + delay)

        results.append((house_id, current_date))

        # update DB
        cur.execute("""
            UPDATE houses
            SET predicted_finish=%s
            WHERE house_id=%s
        """, (current_date, house_id))

    conn.commit()

    df = pd.DataFrame(results, columns=["House", "Predicted Finish"])
    st.dataframe(df)

    st.success("Engine Run Complete")