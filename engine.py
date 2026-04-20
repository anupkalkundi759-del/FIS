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

    if not data:
        st.warning("No tracking data available.")
        return

    df = pd.DataFrame(data, columns=["house","stage","start","end"])
    df["start"] = pd.to_datetime(df["start"])
    df["end"] = pd.to_datetime(df["end"])

    # ================= ENGINE =================
    results = []
    sla_results = []
    stage_delay_summary = {}

    for house in df["house"].unique():

        house_data = df[df["house"] == house]
        start_date = house_data["start"].min()

        current_pointer = start_date
        earned_duration = 0
        stage_delays = []

        for _, row in activity_df.iterrows():
            stage = row["stage"]
            planned_days = row["days"]

            stage_data = house_data[house_data["stage"] == stage]

            if not stage_data.empty:
                actual_start = stage_data["start"].iloc[0]
                actual_finish = stage_data["end"].iloc[0]

                actual_duration = max(0, (actual_finish - actual_start).days)

                # 🔥 FIX 1: FAST + DELAY LOGIC
                if actual_duration <= planned_days:
                    effective_duration = actual_duration
                else:
                    effective_duration = actual_duration
                    delay = actual_duration - planned_days
                    stage_delays.append((stage, delay))

                current_pointer = actual_start + timedelta(days=effective_duration)
                earned_duration += effective_duration
            else:
                current_pointer += timedelta(days=planned_days)

        predicted_finish = current_pointer
        progress = (earned_duration / total_duration) * 100 if total_duration else 0
        current_stage = house_data.iloc[-1]["stage"]

        sla = config_map.get(house)
        expected_finish = pd.to_datetime(sla) if sla else None

        # ================= SLA LOGIC (UNCHANGED) =================
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
                "Impact": impact
            })

        # ================= 🔥 HOUSE INTELLIGENCE FIX =================
        else:
            pred_display = predicted_finish.date() if progress >= 15 else "Not Reliable Yet"

            # PRODUCT COUNT
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

            # 🔥 FIX 2: CORRECT STAGE REMAINING
            stage_duration = activity_df[activity_df["stage"] == current_stage]["days"].values
            stage_duration = int(stage_duration[0]) if len(stage_duration) > 0 else 1

            remaining_stage = stage_duration * pending_ratio
            stage_display = "Less than 1 day" if remaining_stage < 1 else f"{int(remaining_stage)} days"

            # 🔥 FIX 3: CORRECT TOTAL REMAINING
            remaining_total = max(0, (predicted_finish - today).days)
            total_display = f"{remaining_total} days" if remaining_total > 0 else 0

            if completed_products == total_products:
                actual_finish = house_data["end"].max()
                actual_display = actual_finish.date()
            else:
                actual_display = "Not Finished"

            planned_finish = start_date + timedelta(days=total_duration)

            if completed_products == total_products:
                delay_days = max(0, (actual_finish - planned_finish).days)
            else:
                delay_days = max(0, (predicted_finish - planned_finish).days)

            if stage_delays:
                stage_name, d = max(stage_delays, key=lambda x: x[1])
                if d <= 1:
                    reason = f"{stage_name} slight delay"
                elif d <= 3:
                    reason = f"{stage_name} delay"
                else:
                    reason = f"{stage_name} backlog"
            else:
                reason = "on track"

            results.append({
                "House": house,
                "Stage": current_stage,
                "Progress %": round(progress,1),
                "Predicted Finish": pred_display,
                "Actual Finish": actual_display,
                "Remaining (Stage)": stage_display,
                "Remaining (Total)": total_display,
                "Delay (Days)": delay_days,
                "Delay Reason": reason
            })

        # ================= STAGE SUMMARY =================
        for stage, delay in stage_delays:
            if stage not in stage_delay_summary:
                stage_delay_summary[stage] = {"delay":0,"count":0}
            stage_delay_summary[stage]["delay"] += delay
            stage_delay_summary[stage]["count"] += 1

    # ================= OUTPUT =================
    st.subheader("🚨 Priority Table (SLA Only)")
    st.dataframe(pd.DataFrame(sla_results))

    st.subheader("🏠 House Intelligence (Non-SLA Only)")

    if results:
        def highlight_delay(row):
            return ["background-color: #ffcccc"] * len(row) if row["Delay (Days)"] > 0 else [""] * len(row)

        st.dataframe(pd.DataFrame(results).style.apply(highlight_delay, axis=1))
    else:
        st.info("All houses are under SLA monitoring")

    # ================= EARLY WARNING =================
    early_data = []
    for row in sla_results:
        if "Miss by" in row["Impact"]:
            delay_days = int(row["Impact"].split(" ")[-2])
            early_data.append({
                "House": row["House"],
                "Issue": "Will miss SLA",
                "Delay (days)": delay_days
            })

    st.subheader("🚨 Early Warning")
    st.dataframe(pd.DataFrame(early_data)) if early_data else st.success("No early risks")

    # ================= STAGE INSIGHT =================
    insight_data = [
        {"Stage": k, "Total Delay": v["delay"], "Affected Houses": v["count"]}
        for k, v in stage_delay_summary.items() if v["delay"] > 0
    ]

    st.subheader("🧠 Stage Delay Insight")
    st.dataframe(pd.DataFrame(insight_data)) if insight_data else st.info("No stage delays detected yet")

    # ================= TREND =================
    total_delay_today = sum([v["delay"] for v in stage_delay_summary.values()])

    cur.execute("DELETE FROM delay_trend WHERE date = CURRENT_DATE")
    cur.execute("INSERT INTO delay_trend VALUES (CURRENT_DATE, %s)", (int(total_delay_today),))
    conn.commit()

    trend_df = pd.read_sql("SELECT * FROM delay_trend ORDER BY date", conn)

    st.subheader("📈 Delay Trend")
    st.line_chart(trend_df.set_index("date")) if not trend_df.empty else st.info("No data yet")
