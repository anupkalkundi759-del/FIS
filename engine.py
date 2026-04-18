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

    # ================= PROJECT / UNIT =================
    col1, col2 = st.columns(2)

    with col1:
        cur.execute("SELECT project_id, project_name FROM projects ORDER BY project_name")
        projects = cur.fetchall()
        project_dict = {p[1]: p[0] for p in projects}
        selected_project = st.selectbox("Project", list(project_dict.keys()))

    with col2:
        cur.execute("""
            SELECT unit_id, unit_name 
            FROM units WHERE project_id=%s
        """, (project_dict[selected_project],))
        units = cur.fetchall()
        unit_dict = {u[1]: u[0] for u in units}
        selected_unit = st.selectbox("Unit", list(unit_dict.keys()))

    # ================= SLA INPUT =================
    st.subheader("⚙️ SLA Assignment")

    cur.execute("SELECT house_no FROM houses WHERE unit_id=%s", (unit_dict[selected_unit],))
    houses = [h[0] for h in cur.fetchall()]

    c1, c2, c3 = st.columns([2,2,1])

    with c1:
        selected_house = st.selectbox("House", houses)

    with c2:
        sla_date = st.date_input("SLA Date")

    with c3:
        if st.button("Save SLA"):
            cur.execute("""
                INSERT INTO house_config (house_no, sla_date)
                VALUES (%s, %s)
                ON CONFLICT (house_no)
                DO UPDATE SET sla_date = EXCLUDED.sla_date
            """, (selected_house, sla_date))
            conn.commit()
            st.success("Saved")

    # ================= LOAD SLA =================
    cur.execute("SELECT house_no, sla_date FROM house_config")
    config_map = {r[0]: r[1] for r in cur.fetchall()}

    # ================= TRACKING =================
    cur.execute("""
        SELECT h.house_no, p.product_instance_id, s.stage_name,
               t.timestamp, s.sequence
        FROM products p
        JOIN houses h ON p.house_id = h.house_id
        JOIN tracking_log t ON t.product_instance_id = p.product_instance_id
        JOIN stages s ON t.stage_id = s.stage_id
        WHERE h.unit_id = %s
    """, (unit_dict[selected_unit],))

    df = pd.DataFrame(cur.fetchall(), columns=["house","product","stage","time","seq"])
    df["time"] = pd.to_datetime(df["time"])

    results = []
    sla_results = []
    early_warnings = []
    stage_delay_summary = {}

    # ================= ENGINE =================
    for house in df["house"].unique():

        house_data = df[df["house"] == house]
        start_date = house_data[house_data["stage"]=="Measurement"]["time"].min()

        current_pointer = start_date
        earned_duration = 0
        stage_delays = []

        for _, row in activity_df.iterrows():
            stage = row["stage"]
            duration = row["days"]

            stage_data = house_data[house_data["stage"] == stage]

            planned_finish = current_pointer + timedelta(days=duration)

            if not stage_data.empty:
                actual_start = stage_data["time"].min()
                actual_finish = actual_start + timedelta(days=duration)

                delay = (actual_finish - planned_finish).days

                if delay > 0:
                    stage_delays.append((stage, delay))

                current_pointer = actual_finish
                earned_duration += duration
            else:
                current_pointer = planned_finish

        predicted_finish = current_pointer
        planned_finish_total = start_date + timedelta(days=total_duration)

        progress = (earned_duration / total_duration) * 100

        sla = config_map.get(house)
        expected_finish = pd.to_datetime(sla) if sla else None

        latest = house_data.sort_values("seq").iloc[-1]
        current_stage = latest["stage"]

        # -------- SLA --------
        if expected_finish is not None:

            delay_days = (predicted_finish - expected_finish).days

            if delay_days <= 0:
                status = "🟢 On Track"
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

            if delay_days > 0:
                early_warnings.append({
                    "House": house,
                    "Issue": "Will miss SLA",
                    "Delay": delay_days
                })

        else:
            results.append({
                "House": house,
                "Stage": current_stage,
                "Progress %": round(progress,1),
                "Predicted Finish": predicted_finish.date()
            })

        # -------- STAGE SUMMARY --------
        for stage, delay in stage_delays:
            if stage not in stage_delay_summary:
                stage_delay_summary[stage] = {"delay":0,"count":0}
            stage_delay_summary[stage]["delay"] += delay
            stage_delay_summary[stage]["count"] += 1

    # ================= OUTPUT =================

    st.subheader("🚨 Priority Table (SLA Only)")
    st.dataframe(pd.DataFrame(sla_results))

    st.subheader("🏠 House Intelligence")
    st.dataframe(pd.DataFrame(results))

    st.subheader("🚨 Early Warning")
    st.dataframe(pd.DataFrame(early_warnings))

    # -------- STAGE INSIGHT --------
    insight_data = []
    for stage, val in stage_delay_summary.items():
        insight_data.append({
            "Stage": stage,
            "Total Delay": val["delay"],
            "Affected Houses": val["count"]
        })

    insight_df = pd.DataFrame(insight_data)

    st.subheader("🧠 Stage Delay Insight")
    st.dataframe(insight_df)

    # -------- TOP STAGE --------
    if not insight_df.empty:
        top = insight_df.sort_values(by="Total Delay", ascending=False).iloc[0]
        st.subheader("🚀 Top Delayed Stage")
        st.error(f"{top['Stage']} → {top['Total Delay']} days ({top['Affected Houses']} houses)")

    # -------- TREND --------
    total_delay_today = sum([v["delay"] for v in stage_delay_summary.values()])

    cur.execute("DELETE FROM delay_trend WHERE date = CURRENT_DATE")
    cur.execute("INSERT INTO delay_trend VALUES (CURRENT_DATE, %s)", (total_delay_today,))
    conn.commit()

    trend_df = pd.read_sql("SELECT * FROM delay_trend ORDER BY date", conn)

    st.subheader("📈 Delay Trend")
    st.line_chart(trend_df.set_index("date"))
