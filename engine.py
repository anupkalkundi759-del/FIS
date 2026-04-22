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
            date DATE PRIMARY KEY,
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

    # ================= COMPLETED TRACKING =================
    cur.execute("""
        SELECT h.house_no, s.stage_name,
               MIN(t.timestamp), MAX(t.timestamp)
        FROM products p
        JOIN houses h ON p.house_id = h.house_id
        JOIN tracking_log t ON t.product_id = p.product_id
        JOIN stages s ON t.stage_id = s.stage_id
        WHERE h.unit_id = %s AND t.status = 'Completed'
        GROUP BY h.house_no, s.stage_name
    """, (unit_dict[selected_unit],))

    df = pd.DataFrame(cur.fetchall(), columns=["house","stage","start","end"])

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
        JOIN tracking_log t ON t.product_id = p.product_id
        JOIN stages s ON t.stage_id = s.stage_id
        WHERE h.unit_id = %s
    """, (unit_dict[selected_unit],))

    latest_df = pd.DataFrame(cur.fetchall(),
        columns=["house","stage","status","time"])

    if not latest_df.empty:
        latest_df["time"] = pd.to_datetime(latest_df["time"]).dt.tz_localize("Asia/Kolkata")

    # ================= PROGRESS =================
    cur.execute("""
        SELECT h.house_no,
               COUNT(p.product_id),
               s.stage_name,
               COUNT(DISTINCT CASE WHEN t.status='Completed' THEN t.product_id END)
        FROM houses h
        LEFT JOIN products p ON p.house_id = h.house_id
        LEFT JOIN tracking_log t ON t.product_id = p.product_id
        LEFT JOIN stages s ON t.stage_id = s.stage_id
        WHERE h.unit_id = %s
        GROUP BY h.house_no, s.stage_name
    """, (unit_dict[selected_unit],))

    progress_df = pd.DataFrame(cur.fetchall(),
        columns=["house","total","stage","completed"])

    total_map = progress_df.groupby("house")["total"].max().to_dict()
    stage_map = {(r["house"], r["stage"]): r["completed"] for _, r in progress_df.iterrows()}

    # ================= ENGINE =================
    results, sla_results = [], []
    stage_delay_summary = {}

    for house in total_map.keys():

        house_data = house_group.get_group(house) if house in house_group else pd.DataFrame()

        start_date = house_data["start"].min() if not house_data.empty else today
        total_products = total_map.get(house, 0)

        # CURRENT STAGE
        h_latest = latest_df[latest_df["house"] == house]
        base_stage = h_latest["stage"].value_counts().idxmax() if not h_latest.empty else "Measurement"

        # STAGE START
        if not house_data.empty:
            s_data = house_data[house_data["stage"] == base_stage]
            stage_start = s_data["start"].iloc[0] if not s_data.empty else start_date
        else:
            stage_start = start_date

        # STAGE DETAILS
        stage_row = activity_df[activity_df["stage"] == base_stage]
        stage_duration = int(stage_row["days"].values[0]) if not stage_row.empty else 1
        stage_elapsed = max(0, (today - stage_start).days)

        current_seq = int(stage_row["seq"].values[0]) if not stage_row.empty else 1
        remaining_future = activity_df[activity_df["seq"] > current_seq]["days"].sum()

        predicted_finish = today + timedelta(days=(stage_duration - stage_elapsed) + remaining_future)

        remaining_total_days = max(0, (predicted_finish - today).days)

        # PROGRESS
        earned_duration = 0
        for _, row in activity_df.iterrows():
            stage = row["stage"]
            duration = row["days"]
            completed = stage_map.get((house, stage), 0)

            ratio = (completed / total_products) if total_products else 0
            ratio = min(1, ratio)

            earned_duration += ratio * duration

        total_duration = activity_df["days"].sum()
        progress = (earned_duration / total_duration) * 100 if total_duration else 0

        # STAGE REMAINING
        stage_remaining = max(0, stage_duration - stage_elapsed)

        # ACTUAL FINISH
        completed_products = stage_map.get((house, "Dispatch"), 0)

        if total_products and completed_products == total_products:
            actual_finish = house_data["end"].max().date() if not house_data.empty else "Completed"
        else:
            actual_finish = "Not Finished"

        # DELAY
        delay_days = max(0, (today - predicted_finish).days)

        # BOTTLENECK TRACK
        stage_delay_summary.setdefault(base_stage, {"delay": 0, "count": 0})
        stage_delay_summary[base_stage]["delay"] += delay_days
        stage_delay_summary[base_stage]["count"] += 1

        # SLA
        sla = config_map.get(house)
        if sla:
            expected = pd.to_datetime(sla).tz_localize("Asia/Kolkata")
            d = (predicted_finish - expected).days

            status = "🟢 On Track" if d < 0 else "🟢 On Time" if d == 0 else "🔴 Delay"
            impact = "On Time" if d == 0 else f"{'Ahead by' if d<0 else 'Miss by'} {abs(d)} days"

            sla_results.append({
                "House": house,
                "Stage": base_stage,
                "SLA": expected.date(),
                "Predicted": predicted_finish.date(),
                "Status": status,
                "Impact": impact
            })

        else:
            results.append({
                "House": house,
                "Stage": base_stage,
                "Progress %": round(progress, 1),
                "Predicted Finish": predicted_finish.date(),
                "Actual Finish": actual_finish,
                "Remaining (Stage)": f"{stage_remaining} days",
                "Remaining (Total)": f"{remaining_total_days} days",
                "Delay (Days)": delay_days,
                "Delay Reason": "on track" if delay_days == 0 else "delayed"
            })

    # ================= OUTPUT =================
    st.subheader("🚨 Priority Table (SLA Only)")
    st.dataframe(pd.DataFrame(sla_results))

    st.subheader("🏠 House Intelligence")
    st.dataframe(pd.DataFrame(results))

    # ================= EARLY WARNING =================
    early = [r for r in sla_results if "Miss" in r["Impact"]]
    st.subheader("🚨 Early Warning")
    st.dataframe(pd.DataFrame(early)) if early else st.success("No early risks")

    # ================= BOTTLENECK =================
    bottleneck = [{"Stage": k, "Total Delay": v["delay"], "Houses": v["count"]}
                  for k, v in stage_delay_summary.items()]

    st.subheader("🧠 Bottleneck")
    st.dataframe(pd.DataFrame(bottleneck))

    # ================= TREND =================
    total_delay_today = sum(r["Delay (Days)"] for r in results)

    cur.execute("DELETE FROM delay_trend WHERE date = CURRENT_DATE")
    cur.execute("INSERT INTO delay_trend VALUES (CURRENT_DATE, %s)", (total_delay_today,))
    conn.commit()

    trend_df = pd.read_sql("SELECT * FROM delay_trend ORDER BY date", conn)

    st.subheader("📈 Delay Trend")
    st.line_chart(trend_df.set_index("date"))
