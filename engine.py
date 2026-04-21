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
    cur.execute("""
        CREATE TABLE IF NOT EXISTS delay_trend (
            date DATE,
            total_delay INT
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

    # ================= SLA =================
    st.subheader("⚙️ SLA Assignment")

    c1, c2, c3, c4, c5 = st.columns([2,2,2,2,1])

    with c1:
        cur.execute("SELECT project_id, project_name FROM projects ORDER BY project_name")
        projects = cur.fetchall()
        project_dict = {p[1]: p[0] for p in projects}
        selected_project = st.selectbox("Project", list(project_dict.keys()))

    with c2:
        cur.execute("SELECT unit_id, unit_name FROM units WHERE project_id=%s",
                    (project_dict[selected_project],))
        units = cur.fetchall()
        unit_dict = {u[1]: u[0] for u in units}
        selected_unit = st.selectbox("Unit", list(unit_dict.keys()))

    with c3:
        cur.execute("SELECT house_no FROM houses WHERE unit_id=%s",
                    (unit_dict[selected_unit],))
        houses = [h[0] for h in cur.fetchall()]
        selected_house = st.selectbox("House", houses)

    with c4:
        sla_date = st.date_input("SLA Date")

    with c5:
        st.write("")
        if st.button("Save SLA"):
            if sla_date < today.date():
                st.error("SLA cannot be in the past")
            else:
                cur.execute("""
                    INSERT INTO house_config (house_no, sla_date)
                    VALUES (%s, %s)
                    ON CONFLICT (house_no)
                    DO UPDATE SET sla_date = EXCLUDED.sla_date
                """, (selected_house, sla_date))
                conn.commit()
                st.success("SLA Saved")

    cur.execute("SELECT house_no, sla_date FROM house_config")
    config_map = {r[0]: r[1] for r in cur.fetchall()}

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

    # ================= LATEST =================
    cur.execute("""
        SELECT h.house_no, s.stage_name, t.status, t.timestamp
        FROM products p
        JOIN houses h ON p.house_id = h.house_id
        JOIN tracking_log t ON t.product_instance_id = p.product_instance_id
        JOIN stages s ON t.stage_id = s.stage_id
        WHERE h.unit_id = %s
    """, (unit_dict[selected_unit],))

    latest_df = pd.DataFrame(cur.fetchall(),
        columns=["house","stage","status","time"])

    if not latest_df.empty:
        latest_df["time"] = pd.to_datetime(latest_df["time"]).dt.tz_localize("Asia/Kolkata")

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

    # ================= ENGINE =================
    results, sla_results, stage_delay_summary = [], [], {}

    for house in total_map.keys():

        house_data = house_group.get_group(house) if house in house_group else pd.DataFrame()

        start_date = house_data["start"].min() if not house_data.empty else today
        current_pointer = start_date
        stage_delays = []

        total_products = total_map.get(house, 0)
        earned_duration = 0

        critical_path = []

        for _, row in activity_df.iterrows():
            stage = row["stage"]
            duration = row["days"]

            stage_data = house_data[house_data["stage"] == stage] if not house_data.empty and "stage" in house_data.columns else pd.DataFrame()

            planned_finish = current_pointer + timedelta(days=duration)

            if not stage_data.empty:
                actual_start = stage_data["start"].min()
                actual_finish = stage_data["end"].max()

                actual_duration = max(1, (actual_finish - actual_start).days)
                delta = actual_duration - duration

                if delta > 0:
                    stage_delays.append((stage, delta))
                    critical_path.append(stage)
                    current_pointer += timedelta(days=delta)

                elif delta < 0:
                    gain = min(abs(delta), 2)
                    current_pointer -= timedelta(days=gain)

                current_pointer = actual_finish

            else:
                current_pointer = planned_finish

            completed = stage_map.get((house, stage), 0)
            if total_products:
                completion_ratio = min(1, completed / total_products)
                earned_duration += completion_ratio * duration

        progress = (earned_duration / total_duration) * 100 if total_duration else 0

        predicted_finish = current_pointer
        remaining_total_days = max(0, (predicted_finish.date() - today.date()).days)

        h_latest = latest_df[latest_df["house"] == house] if not latest_df.empty else pd.DataFrame()

        if not h_latest.empty:
            row = h_latest.sort_values("time").iloc[-1]
            current_stage = f"{row['stage']} ({row['status']})"
        else:
            current_stage = "Not Started"

        sla = config_map.get(house)
        expected = pd.to_datetime(sla).tz_localize("Asia/Kolkata") if sla else None

        final_stage = activity_df.iloc[-1]["stage"]
        finished = final_stage in house_data["stage"].values if not house_data.empty and "stage" in house_data.columns else False

        last = house_data["end"].max() if not house_data.empty and "end" in house_data.columns else predicted_finish

        actual_display = last.date() if finished else "Not Finished"

        delay_days = max(0, (last - predicted_finish).days)

        if stage_delays:
            s, d = max(stage_delays, key=lambda x: x[1])
            reason = f"{s} delay"
        else:
            reason = "on track"

        result_row = {
            "House": house,
            "Stage": current_stage,
            "Progress %": round(progress,1),
            "Predicted Finish": predicted_finish.date(),
            "Actual Finish": actual_display,
            "Remaining (Total)": f"{remaining_total_days} days",
            "Delay (Days)": delay_days,
            "Delay Reason": reason,
            "Critical Path": " → ".join(critical_path) if critical_path else "None"
        }

        if expected is not None:
            d = (predicted_finish - expected).days
            status = "On Track" if d <= 0 else "Delay"

            sla_results.append({
                **result_row,
                "SLA": expected.date(),
                "Status": status
            })
        else:
            results.append(result_row)

    st.subheader("🚨 Priority Table (SLA Only)")
    st.dataframe(pd.DataFrame(sla_results))

    st.subheader("🏠 House Intelligence")
    st.dataframe(pd.DataFrame(results))

    st.subheader("🚨 Early Warning")
    st.dataframe(pd.DataFrame([r for r in sla_results if r["Status"]=="Delay"]))
