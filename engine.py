def run_engine(conn, cur):
    import streamlit as st
    import pandas as pd
    from datetime import datetime, timedelta

    st.title("⚙️ Scheduling Intelligence Engine")
    today = datetime.now()

    # ================= CONFIG =================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS house_config (
            house_no TEXT PRIMARY KEY,
            urgency INT DEFAULT 0,
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

    if not act:
        st.error("No activity master found")
        return

    activity_df = pd.DataFrame(act, columns=["stage", "seq", "days"])
    activity_df["days"] = activity_df["days"].astype(int)
    total_duration = int(activity_df["days"].sum())

    # ================= UI =================
    st.subheader("⚙️ House Configuration")

    col1, col2, col3, col4, col5 = st.columns(5)

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
        selected_house = st.selectbox("House", houses)

    urgency_map_ui = {"Low": 0, "Medium": 1, "High": 2, "Critical": 3}

    with col4:
        urgency_label = st.selectbox("Urgency", list(urgency_map_ui.keys()))
        urgency = urgency_map_ui[urgency_label]

    with col5:
        sla_date = st.date_input("SLA Deadline")

    if st.button("Save Configuration"):
        cur.execute("""
            INSERT INTO house_config (house_no, urgency, sla_date)
            VALUES (%s, %s, %s)
            ON CONFLICT (house_no)
            DO UPDATE SET urgency = EXCLUDED.urgency,
                          sla_date = EXCLUDED.sla_date
        """, (selected_house, urgency, sla_date))
        conn.commit()
        st.success("Saved")

    # ================= LOAD CONFIG =================
    cur.execute("SELECT house_no, urgency, sla_date FROM house_config")
    config_map = {row[0]: {"urgency": row[1], "sla": row[2]} for row in cur.fetchall()}

    # ================= DATA =================
    cur.execute("""
        SELECT 
            h.house_no,
            s.stage_name,
            t.timestamp,
            s.sequence
        FROM products p
        JOIN houses h ON p.house_id = h.house_id
        JOIN tracking_log t ON t.product_instance_id = p.id
        JOIN stages s ON t.stage_id = s.stage_id
        WHERE h.unit_id = %s
    """, (unit_dict[selected_unit],))

    data = cur.fetchall()

    if not data:
        st.warning("No tracking data")
        return

    df = pd.DataFrame(data, columns=["house", "stage", "time", "seq"])
    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    results = []
    early_warnings = []
    stage_analysis = []

    def priority_color(score):
        if score >= 80:
            return "🔴 Critical"
        elif score >= 50:
            return "🟠 High"
        elif score >= 20:
            return "🟡 Medium"
        else:
            return "🟢 Low"

    # ================= CORE LOOP =================
    for house in df["house"].unique():

        house_df = df[df["house"] == house].sort_values("seq")

        # ===== CURRENT STAGE (MAX SEQ) =====
        max_row = house_df.loc[house_df["seq"].idxmax()]
        current_stage = max_row["stage"]
        current_time = max_row["time"]
        max_seq = max_row["seq"]

        # ===== PROGRESS =====
        completed_days = activity_df[activity_df["seq"] < max_seq]["days"].sum()

        current_stage_days = activity_df[activity_df["seq"] == max_seq]["days"]
        current_stage_days = int(current_stage_days.values[0]) if not current_stage_days.empty else 1

        days_in_stage = max(0, (today - current_time).days)
        partial = min(days_in_stage / current_stage_days, 1)

        completed_days += int(current_stage_days * partial)
        house_progress = (completed_days / total_duration) * 100 if total_duration else 0

        # ===== START DATE =====
        measurement_rows = house_df[house_df["stage"].str.contains("Measurement", case=False)]
        start_date = measurement_rows["time"].min() if not measurement_rows.empty else house_df["time"].min()

        # ===== PRODUCTIVITY =====
        recent_rates = []
        for i in range(len(house_df) - 1):
            t1 = house_df.iloc[i]["time"]
            t2 = house_df.iloc[i + 1]["time"]
            if pd.notna(t1) and pd.notna(t2):
                actual = max(1, (t2 - t1).days)
                planned = activity_df[activity_df["stage"] == house_df.iloc[i]["stage"]]["days"]
                planned = int(planned.values[0]) if not planned.empty else 1
                recent_rates.append(actual / planned)

                stage_analysis.append({
                    "Stage": house_df.iloc[i]["stage"],
                    "Delay": actual - planned
                })

        productivity_rate = sum(recent_rates[-2:]) / 2 if len(recent_rates) >= 2 else 1

        # ===== CONFIG =====
        config = config_map.get(house, {})
        urgency_val = config.get("urgency", 0)
        sla_date = config.get("sla")

        expected_finish = pd.to_datetime(sla_date) if sla_date else None

        # ===== PREDICTION =====
        remaining_days = total_duration * (1 - house_progress / 100)
        predicted_finish = start_date + timedelta(days=int(remaining_days * productivity_rate))

        delay = (predicted_finish - expected_finish).days if expected_finish else None

        # ===== DISPLAY =====
        if house_progress < 5:
            delay_display = "Just started"
        elif delay is None:
            delay_display = "No SLA"
        elif delay < 0:
            delay_display = f"Ahead by {abs(delay)} days"
        elif delay == 0:
            delay_display = "On time"
        else:
            delay_display = f"Delayed by {delay} days"

        # ===== PRIORITY =====
        delay_factor = max(0, delay) if delay else 0
        priority_score = (delay_factor * 3) + (100 - house_progress) + (urgency_val * 20)

        # ===== REASONS =====
        reason_priority = "Will miss SLA" if delay and delay > 0 else "On track"

        reason_intel = (
            "Not started" if house_progress < 5 else
            "Slow progress" if house_progress < 40 else
            "Behind schedule" if delay and delay > 0 else
            "On track"
        )

        # ===== EARLY WARNING =====
        if expected_finish and predicted_finish > expected_finish:
            early_warnings.append({
                "House": house,
                "Issue": "Will miss SLA",
                "Reason": f"{delay} days delay"
            })

        results.append({
            "House": house,
            "Stage": current_stage,
            "Progress %": round(house_progress, 1),
            "Delay": delay_display,
            "SLA": expected_finish.date() if expected_finish else None,
            "Predicted Finish": predicted_finish.date(),
            "Priority": priority_color(priority_score),
            "Reason_Priority": reason_priority,
            "Reason_Intel": reason_intel
        })

    result_df = pd.DataFrame(results)

    # ================= OUTPUT =================
    st.subheader("🚨 Priority Houses")
    st.dataframe(result_df[result_df["SLA"].notna()][
        ["House","Stage","Delay","SLA","Priority","Reason_Priority"]
    ])

    st.subheader("🚨 Early Warnings")
    st.dataframe(pd.DataFrame(early_warnings) if early_warnings else [])

    st.subheader("🏠 House Intelligence")
    st.dataframe(result_df[
        ["House","Stage","Progress %","Delay","Predicted Finish","Reason_Intel"]
    ])

    # ===== BOTTLENECK =====
    stage_df = pd.DataFrame(stage_analysis)
    if not stage_df.empty:
        bottleneck = stage_df.groupby("Stage")["Delay"].mean()
        if not bottleneck.empty:
            st.error(f"🚨 Bottleneck: {bottleneck.idxmax()}")
