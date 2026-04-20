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
        GROUP BY h.house_no, s.stage_name
    """, (unit_dict[selected_unit],))

    data = cur.fetchall()

    if data:
        df = pd.DataFrame(data, columns=["house","stage","start","end"])
        df["start"] = pd.to_datetime(df["start"])
        df["end"] = pd.to_datetime(df["end"])
    else:
        df = pd.DataFrame(columns=["house","stage","start","end"])

    if df.empty:
        st.warning("No tracking data available. SLA evaluation not started.")
        return

    # ================= ENGINE =================
    results = []
    stage_delay_summary = {}

    all_houses = set(df["house"].unique())

    for house in all_houses:

        house_data = df[df["house"] == house]
        start_date = house_data["start"].min()

        current_pointer = start_date
        earned_duration = 0
        stage_delays = []

        for _, row in activity_df.iterrows():
            stage = row["stage"]
            duration = row["days"]

            stage_data = house_data[house_data["stage"] == stage]
            planned_finish = current_pointer + timedelta(days=duration)

            if not stage_data.empty:
                actual_start = stage_data["start"].iloc[0]
                actual_finish = stage_data["end"].iloc[0]

                actual_duration = (actual_finish - actual_start).days
                delay = actual_duration - duration

                if delay > 0:
                    stage_delays.append((stage, delay))

                current_pointer = actual_finish
                earned_duration += duration
            else:
                current_pointer = planned_finish

        predicted_finish = current_pointer
        progress = (earned_duration / total_duration) * 100 if total_duration else 0
        current_stage = house_data.iloc[-1]["stage"]

        # ================= PRODUCT LOGIC =================
        cur.execute("""
        SELECT COUNT(*) FROM products p
        JOIN houses h ON p.house_id = h.house_id
        WHERE h.house_no = %s
        """, (house,))
        total_products = cur.fetchone()[0] or 1

        cur.execute("""
        SELECT COUNT(DISTINCT t.product_instance_id)
        FROM tracking_log t
        JOIN products p ON t.product_instance_id = p.product_instance_id
        JOIN houses h ON p.house_id = h.house_id
        JOIN stages s ON t.stage_id = s.stage_id
        WHERE h.house_no = %s AND s.stage_name = %s
        """, (house, current_stage))
        completed_products = cur.fetchone()[0] or 0

        pending_ratio = 1 - (completed_products / total_products)

        remaining_total = (predicted_finish - today).days
        remaining_stage = (current_pointer - today).days
        adjusted_stage = remaining_stage * pending_ratio

        if adjusted_stage < 1:
            stage_display = "Less than 1 day"
        else:
            stage_display = f"{int(adjusted_stage)} days"

        if remaining_total == 0:
            total_display = 0
        else:
            total_display = f"{max(0, remaining_total)} days"

        delay_days = max(0, remaining_total)

        # ================= DELAY REASON =================
        if stage_delays:
            stage_name, d = max(stage_delays, key=lambda x: x[1])
            if d <= 1:
                reason = f"{stage_name} Slight Delay"
            elif d <= 3:
                reason = f"{stage_name} Delay"
            else:
                reason = f"{stage_name} Backlog"
        else:
            reason = "On Track"

        actual_finish = house_data["end"].max()
        actual_display = actual_finish.date() if pd.notnull(actual_finish) else "Not Finished"

        results.append({
            "House": house,
            "Stage": current_stage,
            "Progress %": round(progress,1),
            "Predicted Finish": predicted_finish.date(),
            "Actual Finish": actual_display,
            "Final Finish": predicted_finish.date(),
            "Remaining (Stage)": stage_display,
            "Remaining (Total)": total_display,
            "Delay (Days)": delay_days,
            "Delay Reason": reason
        })

        for stage, delay in stage_delays:
            if stage not in stage_delay_summary:
                stage_delay_summary[stage] = {"delay":0,"count":0}
            stage_delay_summary[stage]["delay"] += delay
            stage_delay_summary[stage]["count"] += 1

    # ================= OUTPUT =================
    st.subheader("🏠 House Intelligence")

    result_df = pd.DataFrame(results)

    # 🔴 COLOR CODING
    def highlight_delay(row):
        if row["Delay (Days)"] > 0:
            return ["background-color: #ffcccc"] * len(row)
        return [""] * len(row)

    st.dataframe(result_df.style.apply(highlight_delay, axis=1))

    # ================= BOTTLENECK =================
    st.subheader("📊 Stage Bottleneck Ranking")

    bottleneck_data = [
        {"Stage": k, "Total Delay": v["delay"], "Affected Houses": v["count"]}
        for k, v in stage_delay_summary.items()
    ]

    if bottleneck_data:
        bottleneck_df = pd.DataFrame(bottleneck_data).sort_values(by="Total Delay", ascending=False)
        st.dataframe(bottleneck_df)

        top = bottleneck_df.iloc[0]
        st.error(f"🚧 Bottleneck Stage: {top['Stage']} ({top['Total Delay']} days delay)")
    else:
        st.info("No bottlenecks detected")
