def run_engine(conn, cur):
    import streamlit as st
    import pandas as pd
    from datetime import datetime, timedelta

    st.title("⚙️ Scheduling Intelligence Engine")
    today = datetime.now()

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

    # ================= SLA ASSIGN =================
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
        GROUP BY h.house_no, s.stage_name
    """, (unit_dict[selected_unit],))

    data = cur.fetchall()

    if not data:
        st.warning("No tracking data available.")
        return

    df = pd.DataFrame(data, columns=["house","stage","start","end"])
    df["start"] = pd.to_datetime(df["start"])
    df["end"] = pd.to_datetime(df["end"])
    house_group = df.groupby("house")

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
    latest_df["time"] = pd.to_datetime(latest_df["time"])

    # ================= ENGINE =================
    results, sla_results, stage_delay_summary = [], [], {}

    for house in df["house"].unique():

        house_data = house_group.get_group(house)
        start_date = house_data["start"].min()

        current_pointer = start_date
        stage_delays = []

        critical_stage = None
        max_delay = 0

        for _, row in activity_df.iterrows():
            stage = row["stage"]
            duration = row["days"]

            stage_data = house_data[house_data["stage"] == stage]

            if not stage_data.empty:
                actual_start = stage_data["start"].iloc[0]
                actual_finish = stage_data["end"].iloc[0]

                actual_duration = (actual_finish - actual_start).days
                if actual_duration == 0:
                    actual_duration = duration

                delay = actual_duration - duration

                if delay > 0:
                    stage_delays.append((stage, delay))

                    if delay > max_delay:
                        max_delay = delay
                        critical_stage = stage

                current_pointer = actual_start + timedelta(days=actual_duration)
            else:
                current_pointer += timedelta(days=duration)

        # ================= RESCHEDULING =================
        total_delay_impact = sum([d for _, d in stage_delays])
        current_pointer += timedelta(days=total_delay_impact)

        predicted_finish = current_pointer

        # ================= CURRENT STAGE =================
        h_latest = latest_df[latest_df["house"] == house]

        if not h_latest.empty:
            row = h_latest.sort_values("time").iloc[-1]
            current_stage = f"{row['stage']} ({row['status']})"
        else:
            current_stage = "Not Started"

        # ================= REMAINING =================
        remaining_total_days = (predicted_finish - today).days
        remaining_total_days = max(0, remaining_total_days)

        # ================= RISK =================
        if remaining_total_days <= 3:
            risk = "🔴 Critical"
        elif remaining_total_days <= 5:
            risk = "🟠 High Risk"
        elif remaining_total_days <= 7:
            risk = "🟡 Watch"
        else:
            risk = "🟢 Normal"

        # ================= SLA SPLIT =================
        sla = config_map.get(house)
        expected_finish = pd.to_datetime(sla) if sla else None

        if expected_finish is not None:

            delay_days = (predicted_finish - expected_finish).days

            if delay_days < 0:
                status = "🟢 On Track"
                impact = f"Ahead by {abs(delay_days)} days"
            elif delay_days == 0:
                status = "🟢 On Time"
                impact = "On Time"
            else:
                status = "🔴 Delay"
                impact = f"Miss by {delay_days} days"

            sla_results.append({
                "House": house,
                "Stage": current_stage,
                "SLA": expected_finish.date(),
                "Predicted": predicted_finish.date(),
                "Status": status,
                "Impact": impact,
                "Critical Stage": f"🔴 {critical_stage}" if critical_stage else "None",
                "Delay Impact": total_delay_impact
            })

        else:

            results.append({
                "House": house,
                "Stage": current_stage,
                "Predicted Finish": predicted_finish.date(),
                "Remaining (Total)": f"{remaining_total_days} days",
                "Critical Stage": f"🔴 {critical_stage}" if critical_stage else "None",
                "Delay Impact": total_delay_impact,
                "Risk Level": risk
            })

        for s, d in stage_delays:
            stage_delay_summary.setdefault(s, {"delay":0,"count":0})
            stage_delay_summary[s]["delay"] += d
            stage_delay_summary[s]["count"] += 1

    # ================= OUTPUT =================
    st.subheader("🚨 Priority Table (SLA Only)")
    st.dataframe(pd.DataFrame(sla_results)) if sla_results else st.info("No SLA houses")

    st.subheader("🏠 House Intelligence (Non-SLA Only)")
    st.dataframe(pd.DataFrame(results)) if results else st.info("All houses under SLA")

    # ================= EARLY WARNING =================
    early = []
    for r in sla_results:
        if "Miss by" in r["Impact"]:
            early.append({
                "House": r["House"],
                "Issue": "Will miss SLA",
                "Critical Stage": r["Critical Stage"],
                "Delay Impact": r["Delay Impact"]
            })

    st.subheader("🚨 Early Warning")
    st.dataframe(pd.DataFrame(early)) if early else st.success("No early risks")

    # ================= BOTTLENECK =================
    insight = [{"Stage":k,"Total Delay":v["delay"],"Affected Houses":v["count"]}
               for k,v in stage_delay_summary.items() if v["delay"]>0]

    st.subheader("🧠 Stage Delay Insight")
    st.dataframe(pd.DataFrame(insight)) if insight else st.info("No stage delays detected yet")

    # ================= TREND =================
    total_delay_today = sum(v["delay"] for v in stage_delay_summary.values())

    cur.execute("DELETE FROM delay_trend WHERE date = CURRENT_DATE")
    cur.execute("INSERT INTO delay_trend VALUES (CURRENT_DATE, %s)", (int(total_delay_today),))
    conn.commit()

    trend_df = pd.read_sql("SELECT * FROM delay_trend ORDER BY date", conn)

    st.subheader("📈 Delay Trend")
    st.line_chart(trend_df.set_index("date")) if not trend_df.empty else st.info("No data yet")
