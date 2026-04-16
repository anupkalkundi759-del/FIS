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
    total_duration = activity_df["days"].sum()
    total_seq = activity_df["seq"].max()
    first_stage = activity_df.iloc[0]["stage"]

    # ================= PROJECT =================
    col1, col2 = st.columns(2)

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

    # ================= SLA =================
    st.subheader("⚙️ SLA Assignment")

    cur.execute("SELECT house_no FROM houses WHERE unit_id=%s",
                (unit_dict[selected_unit],))
    houses = [h[0] for h in cur.fetchall()]

    col1, col2 = st.columns(2)

    with col1:
        selected_house = st.selectbox("House", houses)

    with col2:
        sla_date = st.date_input("SLA (Optional)")

    if st.button("Save SLA"):
        cur.execute("""
            INSERT INTO house_config (house_no, sla_date)
            VALUES (%s, %s)
            ON CONFLICT (house_no)
            DO UPDATE SET sla_date = EXCLUDED.sla_date
        """, (selected_house, sla_date))
        conn.commit()
        st.success("Saved")

    # FIXED: Only fetch SLA for current houses
    cur.execute("""
        SELECT house_no, sla_date 
        FROM house_config
        WHERE house_no = ANY(%s)
    """, (houses,))
    config_map = {r[0]: r[1] for r in cur.fetchall()}

    # ================= TRACKING (FIXED - LEFT JOIN) =================
    cur.execute("""
        SELECT 
            h.house_no,
            s.stage_name,
            t.timestamp,
            s.sequence
        FROM houses h
        LEFT JOIN products p ON p.house_id = h.house_id
        LEFT JOIN tracking_log t ON t.product_instance_id = p.product_instance_id
        LEFT JOIN stages s ON t.stage_id = s.stage_id
        WHERE h.unit_id = %s
    """, (unit_dict[selected_unit],))

    df = pd.DataFrame(cur.fetchall(), columns=["house","stage","time","seq"])

    if df.empty:
        st.warning("No data available")
        return

    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    results = []
    early_warnings = []

    # ================= MAIN LOOP =================
    for house in df["house"].unique():

        house_df = df[df["house"] == house].dropna(subset=["time"])

        # -------- NO TRACKING --------
        if house_df.empty:
            results.append({
                "House": house,
                "Stage": "Not Started",
                "Progress %": 0,
                "Delay": "Not started",
                "Predicted Finish": None,
                "SLA": None,
                "Priority": None,
                "Reason": "No activity"
            })
            continue

        house_df = house_df.sort_values("time")

        meas = house_df[house_df["stage"] == first_stage]
        if meas.empty:
            start_date = house_df["time"].min()
        else:
            start_date = meas["time"].min()

        # CURRENT STAGE
        latest_row = house_df.loc[house_df["time"].idxmax()]
        current_stage = latest_row["stage"]

        # PROGRESS
        max_seq_reached = house_df["seq"].max()
        progress = (max_seq_reached / total_seq) * 100

        # PLANNED
        planned_finish = start_date + timedelta(days=int(total_duration))

        # PREDICTION
        if progress < 20 or current_stage == first_stage:
            predicted = planned_finish
        else:
            remaining_days = activity_df[activity_df["seq"] > max_seq_reached]["days"].sum()
            predicted = today + timedelta(days=int(remaining_days))

        # DELAY
        delay_days = (predicted - planned_finish).days

        if delay_days < 0:
            delay_display = f"Ahead {abs(delay_days)}d"
        elif delay_days == 0:
            delay_display = "On time"
        else:
            delay_display = f"Delay {delay_days}d"

        # SLA + PRIORITY
        sla = config_map.get(house)
        expected_finish = pd.to_datetime(sla) if sla else None

        def get_priority(score):
            if score >= 80: return "🔴 Critical"
            elif score >= 50: return "🟠 High"
            elif score >= 20: return "🟡 Medium"
            else: return "🟢 Low"

        if expected_finish:
            sla_delay = (predicted - expected_finish).days
            priority = get_priority(max(0, sla_delay) * 10)
        else:
            priority = None

        # EARLY WARNING
        if expected_finish and predicted > expected_finish:
            early_warnings.append({
                "House": house,
                "Stage": current_stage,
                "Predicted Finish": predicted.date(),
                "SLA": expected_finish.date(),
                "Delay (days)": (predicted - expected_finish).days
            })

        # REASON
        if progress < 5:
            reason = "Just started"
        elif progress < 40:
            reason = "In progress"
        elif progress < 90:
            reason = "Advanced stage"
        else:
            reason = "Near completion"

        results.append({
            "House": house,
            "Stage": current_stage,
            "Progress %": round(progress,1),
            "Delay": delay_display,
            "Predicted Finish": predicted.date(),
            "SLA": expected_finish,
            "Priority": priority,
            "Reason": reason
        })

    result_df = pd.DataFrame(results)

    # ================= BOTTLENECK =================
    unique_stages = df["seq"].nunique()

    if unique_stages <= 1:
        bottleneck_msg = "⚠️ Project still in initial stage (Measurement)"
    else:
        latest_house_stage = df.dropna(subset=["time"]).loc[df.groupby("house")["time"].idxmax()]
        stage_counts = latest_house_stage.groupby("stage").size()
        bottleneck_stage = stage_counts.idxmax()
        bottleneck_msg = f"Most Congested Stage: {bottleneck_stage}"

    # ================= OUTPUT =================

    st.subheader("🚨 Priority Table (SLA Only)")
    priority_df = result_df[result_df["SLA"].notna()]
    st.dataframe(priority_df[["House","Stage","Delay","SLA","Priority","Reason"]])

    st.subheader("🏠 House Intelligence")
    st.dataframe(result_df[["House","Stage","Progress %","Delay","Predicted Finish","Reason"]])

    st.subheader("🚨 Early Warning")
    if early_warnings:
        st.dataframe(pd.DataFrame(early_warnings))
    else:
        st.success("No early risks")

    st.subheader("🚧 Bottleneck")
    if unique_stages <= 1:
        st.warning(bottleneck_msg)
    else:
        st.error(bottleneck_msg)
