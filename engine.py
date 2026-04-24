def run_engine(conn, cur):
    import streamlit as st
    import pandas as pd
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    st.title("⚙️ Scheduling Intelligence Engine")

    # ================= TIME =================
    IST = ZoneInfo("Asia/Kolkata")
    today = datetime.now(IST)

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
        WHERE h.unit_id = %s AND t.status = 'Completed'
        GROUP BY h.house_no, s.stage_name
    """, (unit_dict[selected_unit],))

    df = pd.DataFrame(cur.fetchall(), columns=["house","stage","start","end"])

    # 🔴 FIX 1: remove bad stage rows
    df = df.dropna(subset=["stage"])

    if not df.empty:
        df["start"] = pd.to_datetime(df["start"], errors="coerce").dt.tz_localize("Asia/Kolkata")
        df["end"] = pd.to_datetime(df["end"], errors="coerce").dt.tz_localize("Asia/Kolkata")
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

    # 🔴 FIX 2: ensure safe dataframe
    if latest_df.empty:
        latest_df = pd.DataFrame(columns=["house","stage","status","time"])

    latest_df = latest_df.dropna(subset=["stage"])

    if not latest_df.empty:
        latest_df["time"] = pd.to_datetime(latest_df["time"], errors="coerce").dt.tz_localize("Asia/Kolkata")

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

    # 🔴 FIX 3: remove null stage
    progress_df = progress_df.dropna(subset=["stage"])

    total_map = progress_df.groupby("house")["total"].max().to_dict()
    stage_map = {(r["house"], r["stage"]): r["completed"] for _, r in progress_df.iterrows()}

    # ================= ENGINE =================
    results, sla_results, stage_delay_summary = [], [], {}

    for house in total_map.keys():

        house_data = house_group.get_group(house) if house in house_group else pd.DataFrame()

        # 🔴 FIX 4: safe filtering
        h_latest = latest_df[latest_df["house"] == house] if "stage" in latest_df.columns else pd.DataFrame()

        total_products = total_map.get(house, 0)
        earned_duration = 0
        stage_delays = []

        # ===== CURRENT STAGE =====
        if not h_latest.empty and "stage" in h_latest.columns:

            in_progress = h_latest[h_latest["status"] == "In Progress"]["stage"].dropna().unique()

            if len(in_progress) > 0:
                current_stage = ", ".join([f"{s} (In Progress)" for s in in_progress])
                base_stage = in_progress[0]
            else:
                row = h_latest.sort_values("time").iloc[-1]
                current_stage = f"{row['stage']} ({row['status']})"
                base_stage = row["stage"]
        else:
            current_stage = "Not Started"
            base_stage = None

        # ===== PROGRESS + DELAY =====
        for _, row in activity_df.iterrows():
            stage = row["stage"]
            duration = row["days"]

            completed = stage_map.get((house, stage), 0)
            ratio = completed / total_products if total_products else 0
            earned_duration += ratio * duration

            if not house_data.empty:
                s_data = house_data[house_data["stage"] == stage]
                if not s_data.empty:
                    actual = (s_data["end"].iloc[0] - s_data["start"].iloc[0]).days
                    if actual > duration:
                        delay = actual - duration
                        stage_delays.append((stage, delay))
                        stage_delay_summary.setdefault(stage, {"delay":0,"count":0})
                        stage_delay_summary[stage]["delay"] += delay
                        stage_delay_summary[stage]["count"] += 1

        progress = (earned_duration / total_duration) * 100 if total_duration else 0

        # ===== TOTAL REMAINING =====
        if not house_data.empty:
            project_start = house_data["start"].min()
        else:
            project_start = h_latest["time"].min() if not h_latest.empty else today

        elapsed_total = (today - project_start).days

        remaining_total_days = total_duration - elapsed_total
        remaining_total_days = max(remaining_total_days, total_duration - earned_duration)

        predicted_finish = today + timedelta(days=int(remaining_total_days))

        # ===== STAGE REMAINING =====
        if base_stage:
            stage_duration = activity_df[activity_df["stage"] == base_stage]["days"].values
            stage_duration = int(stage_duration[0]) if len(stage_duration)>0 else 1

            completed = stage_map.get((house, base_stage), 0)
            ratio = completed / total_products if total_products else 0

            s_data = house_data[house_data["stage"] == base_stage]

            if not s_data.empty:
                stage_start = s_data["start"].min()
            else:
                s_live = h_latest[
                    (h_latest["stage"] == base_stage) &
                    (h_latest["status"] == "In Progress")
                ]
                stage_start = s_live["time"].min() if not s_live.empty else None

            elapsed_stage = (today - stage_start).days if stage_start is not None else 0

            rem_stage = max(stage_duration - elapsed_stage,
                            stage_duration * (1 - ratio))

            stage_display = "Completed" if rem_stage <= 0 else f"{int(rem_stage)} days"
        else:
            stage_display = "-"

        last = house_data["end"].max() if not house_data.empty else predicted_finish
        actual_finish = last.date() if progress >= 99 else "Not Finished"

        delay_days = max(0, (today - predicted_finish).days)

        reason = "on track"
        if stage_delays:
            s, d = max(stage_delays, key=lambda x: x[1])
            reason = f"{s} delay ({d}d)"

        # ===== SLA =====
        sla = config_map.get(house)
        if sla:
            expected = pd.to_datetime(sla).tz_localize("Asia/Kolkata")
            d = (predicted_finish - expected).days

            status = "🟢 On Track" if d < 0 else "🟢 On Time" if d == 0 else "🔴 Delay"
            impact = "On Time" if d == 0 else f"{'Ahead by' if d<0 else 'Miss by'} {abs(d)} days"

            sla_results.append({
                "House": house,
                "Stage": current_stage,
                "SLA": expected.date(),
                "Predicted": predicted_finish.date(),
                "Status": status,
                "Impact": impact
            })
        else:
            results.append({
                "House": house,
                "Stage": current_stage,
                "Progress %": round(progress,1),
                "Predicted Finish": predicted_finish.date(),
                "Actual Finish": actual_finish,
                "Remaining (Stage)": stage_display,
                "Remaining (Total)": f"{int(remaining_total_days)} days",
                "Delay (Days)": delay_days,
                "Delay Reason": reason
            })

    # ================= OUTPUT =================
    st.subheader("🚨 Priority Table (SLA Only)")
    st.dataframe(pd.DataFrame(sla_results))

    st.subheader("🏠 House Intelligence (Non-SLA Only)")
    st.dataframe(pd.DataFrame(results))

    # ================= EARLY WARNING =================
    early = []
    for r in sla_results:
        if "Miss by" in r["Impact"]:
            early.append({
                "House": r["House"],
                "Issue": "Will miss SLA",
                "Delay (days)": int(r["Impact"].split(" ")[-2])
            })

    st.subheader("🚨 Early Warning")
    st.dataframe(pd.DataFrame(early)) if early else st.success("No early risks")

    # ================= BOTTLENECK =================
    insight = [{"Stage":k,"Total Delay":v["delay"],"Affected Houses":v["count"]}
               for k,v in stage_delay_summary.items() if v["delay"]>0]

    st.subheader("🧠 Stage Delay Insight")
    st.dataframe(pd.DataFrame(insight)) if insight else st.info("No stage delays detected yet")
