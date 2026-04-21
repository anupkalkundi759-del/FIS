def run_engine(conn, cur):
    import streamlit as st
    import pandas as pd
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    st.title("⚙️ Scheduling Intelligence Engine")

    today = datetime.now(ZoneInfo("Asia/Kolkata"))

    # ================= TABLES =================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS house_config (
            house_no TEXT PRIMARY KEY,
            sla_date DATE
        )
    """)
    conn.commit()

    # ================= ACTIVITIES =================
    cur.execute("""
        SELECT activity_name, sequence_order, duration_days
        FROM activity_master
        ORDER BY sequence_order
    """)
    act = cur.fetchall()

    activity_df = pd.DataFrame(act, columns=["stage", "seq", "days"])
    activity_df["days"] = activity_df["days"].astype(int)
    total_duration = int(activity_df["days"].sum())

    # ================= FILTERS =================
    col1, col2, col3 = st.columns(3)

    with col1:
        cur.execute("SELECT project_id, project_name FROM projects ORDER BY project_name")
        projects = cur.fetchall()
        project_dict = {p[1]: p[0] for p in projects}
        selected_project = st.selectbox("Project", list(project_dict.keys()))

    with col2:
        cur.execute("SELECT unit_id, unit_name FROM units WHERE project_id=%s",
                    (project_dict[selected_project],))
        units = cur.fetchall()
        unit_dict = {u[1]: u[0] for u in units}
        selected_unit = st.selectbox("Unit", list(unit_dict.keys()))

    with col3:
        cur.execute("SELECT house_no FROM houses WHERE unit_id=%s",
                    (unit_dict[selected_unit],))
        houses = [h[0] for h in cur.fetchall()]
        selected_house_filter = st.selectbox("House Filter", ["All"] + houses)

    # ================= TRACKING =================
    cur.execute("""
        SELECT h.house_no, s.stage_name,
               MIN(t.timestamp), MAX(t.timestamp)
        FROM products p
        JOIN houses h ON p.house_id = h.house_id
        JOIN tracking_log t ON t.product_instance_id = p.product_instance_id
        JOIN stages s ON t.stage_id = s.stage_id
        WHERE h.unit_id = %s
        AND t.status = 'Completed'
        GROUP BY h.house_no, s.stage_name
    """, (unit_dict[selected_unit],))

    data = cur.fetchall()

    if not data:
        st.warning("No tracking data available.")
        data = []

    df = pd.DataFrame(data, columns=["house","stage","start","end"])

    if not df.empty:
        df["start"] = pd.to_datetime(df["start"]).dt.tz_localize("Asia/Kolkata")
        df["end"] = pd.to_datetime(df["end"]).dt.tz_localize("Asia/Kolkata")
        house_group = df.groupby("house")
    else:
        house_group = {}

    # ================= PREFETCH =================
    cur.execute("""
        SELECT h.house_no,
               COUNT(p.product_instance_id),
               s.stage_name,
               COUNT(DISTINCT CASE WHEN t.status='Completed' THEN t.product_instance_id END)
        FROM houses h
        LEFT JOIN products p ON p.house_id = h.house_id
        LEFT JOIN tracking_log t ON t.product_instance_id = p.product_instance_id
        LEFT JOIN stages s ON t.stage_id = s.stage_id
        WHERE h.unit_id = %s
        GROUP BY h.house_no, s.stage_name
    """, (unit_dict[selected_unit],))

    progress_df = pd.DataFrame(cur.fetchall(),
        columns=["house","total","stage","completed"])

    total_map = progress_df.groupby("house")["total"].max().to_dict()
    stage_map = {(r["house"], r["stage"]): r["completed"] for _, r in progress_df.iterrows()}

    results = []

    for house in total_map.keys():

        if selected_house_filter != "All" and house != selected_house_filter:
            continue

        house_data = house_group.get_group(house) if house in house_group else pd.DataFrame()

        start_date = house_data["start"].min() if not house_data.empty else today
        current_pointer = start_date

        total_products = total_map.get(house, 0)
        earned_duration = 0
        stage_delays = []

        for _, row in activity_df.iterrows():
            stage = row["stage"]
            duration = row["days"]

            stage_data = house_data[house_data["stage"] == stage] if not house_data.empty else pd.DataFrame()

            planned_finish = current_pointer + timedelta(days=duration)

            if not stage_data.empty:
                actual_start = stage_data["start"].min()
                actual_finish = stage_data["end"].max()

                actual_duration = max(1, (actual_finish - actual_start).days)
                delta = actual_duration - duration

                if delta > 0:
                    stage_delays.append(delta)
                    current_pointer += timedelta(days=delta)

                elif delta < 0:
                    gain = min(abs(delta), 2)
                    current_pointer -= timedelta(days=gain)

                current_pointer = actual_finish

            else:
                current_pointer = planned_finish

            completed = stage_map.get((house, stage), 0)
            if total_products:
                completion_ratio = completed / total_products
                earned_duration += completion_ratio * duration

        # 🔥 HYBRID FIX
        timeline_finish = current_pointer
        remaining_work = max(0, total_duration - earned_duration)

        predicted_finish = timeline_finish + timedelta(days=remaining_work * 0.5)

        remaining_total_days = max(0, (predicted_finish.date() - today.date()).days)

        # ✅ ACTUAL FINISH FIX
        final_stage = activity_df.iloc[-1]["stage"]
        completed_final = stage_map.get((house, final_stage), 0)

        finished = (total_products > 0) and (completed_final == total_products)

        last = house_data["end"].max() if not house_data.empty else None

        actual_display = last.date() if finished and last is not None else "Not Finished"

        results.append({
            "House": house,
            "Progress %": round((earned_duration / total_duration) * 100, 1),
            "Predicted Finish": predicted_finish.date(),
            "Actual Finish": actual_display,
            "Remaining (Total)": f"{remaining_total_days} days"
        })

    st.subheader("🏠 House Intelligence")
    st.dataframe(pd.DataFrame(results))
