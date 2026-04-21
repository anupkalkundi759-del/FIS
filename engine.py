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
    total_duration = activity_df["days"].sum()

    # ================= PROJECT SELECTION =================
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

    # ================= HOUSE FILTER (🔥 NEW FIX) =================
    st.subheader("🔍 Filter Intelligence")

    house_filter = st.selectbox(
        "Select House (or All)",
        ["All"] + houses
    )

    # ================= CONFIG =================
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

    df = pd.DataFrame(cur.fetchall(), columns=["house","stage","start","end"])

    if not df.empty:
        df["start"] = pd.to_datetime(df["start"])
        df["end"] = pd.to_datetime(df["end"])
        house_group = df.groupby("house")
    else:
        house_group = {}

    # ================= PROGRESS =================
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

        if house_filter != "All" and house != house_filter:
            continue

        house_data = house_group.get_group(house) if house in house_group else pd.DataFrame()

        # ✅ START DATE FIX (no data case handled)
        start_date = house_data["start"].min() if not house_data.empty else today

        current_pointer = start_date
        earned_duration = 0
        stage_delays = []

        total_products = total_map.get(house, 0)

        for _, row in activity_df.iterrows():
            stage = row["stage"]
            duration = row["days"]

            stage_data = house_data[house_data["stage"] == stage] if not house_data.empty else pd.DataFrame()

            if not stage_data.empty:
                actual_start = stage_data["start"].min()
                actual_finish = stage_data["end"].max()

                actual_duration = max(1, (actual_finish - actual_start).days)
                delay = actual_duration - duration

                if delay > 0:
                    stage_delays.append((stage, delay))

                current_pointer = actual_finish
            else:
                current_pointer += timedelta(days=duration)

            completed = stage_map.get((house, stage), 0)
            if total_products:
                earned_duration += (completed / total_products) * duration

        # ================= FINAL METRICS =================
        progress = (earned_duration / total_duration) * 100 if total_duration else 0

        predicted_finish = current_pointer
        remaining_total_days = max(0, (predicted_finish.date() - today.date()).days)

        # ✅ ACTUAL FINISH FIX (strict)
        if not house_data.empty:
            last_activity = house_data["end"].max()
            actual_finish = last_activity.date()
        else:
            actual_finish = "Not Finished"

        delay_days = max(0, (pd.to_datetime(actual_finish) - predicted_finish).days) \
            if actual_finish != "Not Finished" else 0

        # ================= RESULT =================
        results.append({
            "House": house,
            "Progress %": round(progress, 1),
            "Predicted Finish": predicted_finish.date(),
            "Actual Finish": actual_finish,
            "Remaining (Total)": f"{remaining_total_days} days",
            "Delay (Days)": delay_days,
            "Delay Reason": max(stage_delays, key=lambda x: x[1])[0] if stage_delays else "On Track"
        })

        for s, d in stage_delays:
            stage_delay_summary.setdefault(s, {"delay":0,"count":0})
            stage_delay_summary[s]["delay"] += d
            stage_delay_summary[s]["count"] += 1

    # ================= OUTPUT =================
    st.subheader("🏠 House Intelligence")
    st.dataframe(pd.DataFrame(results))

    # ================= BOTTLENECK =================
    insight = [{"Stage":k,"Total Delay":v["delay"],"Affected Houses":v["count"]}
               for k,v in stage_delay_summary.items() if v["delay"]>0]

    st.subheader("🧠 Stage Delay Insight")
    st.dataframe(pd.DataFrame(insight)) if insight else st.info("No delays")

    # ================= TREND =================
    total_delay_today = sum(v["delay"] for v in stage_delay_summary.values())

    cur.execute("DELETE FROM delay_trend WHERE date = CURRENT_DATE")
    cur.execute("INSERT INTO delay_trend VALUES (CURRENT_DATE, %s)", (int(total_delay_today),))
    conn.commit()

    trend_df = pd.read_sql("SELECT * FROM delay_trend ORDER BY date", conn)

    st.subheader("📈 Delay Trend")
    st.line_chart(trend_df.set_index("date")) if not trend_df.empty else st.info("No data yet")
