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

    cur.execute("SELECT house_no, sla_date FROM house_config")
    config_map = {r[0]: r[1] for r in cur.fetchall()}

    # ================= TRACKING =================
    cur.execute("""
        SELECT h.house_no, s.stage_name, t.timestamp, s.sequence
        FROM products p
        JOIN houses h ON p.house_id = h.house_id
        JOIN tracking_log t ON t.product_instance_id = p.product_instance_id
        JOIN stages s ON t.stage_id = s.stage_id
        WHERE h.unit_id = %s
    """, (unit_dict[selected_unit],))

    df = pd.DataFrame(cur.fetchall(), columns=["house","stage","time","seq"])
    if df.empty:
        st.warning("No tracking data")
        return

    df["time"] = pd.to_datetime(df["time"])

    results = []
    early_warnings = []

    # ================= MAIN LOOP =================
    for house in df["house"].unique():

        house_df = df[df["house"] == house].sort_values("time")

        # -------- START DATE --------
        meas = house_df[house_df["stage"] == "Measurement"]
        if meas.empty:
            continue

        start_date = meas["time"].min()

        # -------- PROGRESS --------
        max_seq_reached = house_df["seq"].max()
        progress = (max_seq_reached / total_seq) * 100

        # -------- CURRENT STAGE --------
        current = house_df.loc[house_df["seq"].idxmax()]
        current_stage = current["stage"]

        # -------- PLANNED --------
        planned_finish = start_date + timedelta(days=int(total_duration))

        # -------- STAGE PERFORMANCE --------
        stage_perf = []

        house_df_sorted = house_df.sort_values("seq")

        for i in range(len(house_df_sorted)-1):
            s1 = house_df_sorted.iloc[i]
            s2 = house_df_sorted.iloc[i+1]

            actual_days = (s2["time"] - s1["time"]).days

            planned_days = activity_df[
                activity_df["stage"] == s1["stage"]
            ]["days"].values[0]

            if planned_days > 0:
                stage_perf.append(actual_days / planned_days)

        # -------- PRODUCTIVITY FACTOR --------
        if len(stage_perf) >= 2:
            productivity = sum(stage_perf[-2:]) / 2
        elif stage_perf:
            productivity = stage_perf[-1]
        else:
            productivity = 1

        productivity = max(0.7, min(productivity, 1.5))

        # -------- REMAINING WORK --------
        remaining_stages = activity_df[activity_df["seq"] > max_seq_reached]
        remaining_days = remaining_stages["days"].sum()

        # -------- SMART PREDICTION --------
        if progress < 20 or current_stage == "Measurement":
            predicted = planned_finish
        else:
            adjusted_remaining = remaining_days * productivity
            predicted = today + timedelta(days=int(adjusted_remaining))

        # -------- DELAY --------
        delay_days = (predicted - planned_finish).days

        if delay_days < 0:
            delay_display = f"Ahead {abs(delay_days)}d"
        elif delay_days == 0:
            delay_display = "On time"
        else:
            delay_display = f"Delay {delay_days}d"

        # -------- SLA --------
        sla = config_map.get(house)
        expected_finish = pd.to_datetime(sla) if sla else None

        # -------- PRIORITY --------
        def get_priority(score):
            if score >= 80: return "🔴 Critical"
            elif score >= 50: return "🟠 High"
            elif score >= 20: return "🟡 Medium"
            else: return "🟢 Low"

        if expected_finish:
            sla_delay = (predicted - expected_finish).days
            priority = get_priority(max(0, sla_delay)*10)
        else:
            priority = None

        # -------- REASON --------
        if progress < 5:
            reason = "Just started"
        elif delay_days > 0:
            reason = "Delayed execution"
        elif progress < 40:
            reason = "In progress"
        else:
            reason = "On track"

        # -------- EARLY WARNING --------
        if expected_finish and predicted > expected_finish:
            early_warnings.append({
                "House": house,
                "Issue": "Will miss SLA",
                "Delay": (predicted - expected_finish).days
            })

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
    latest_stage_df = df.sort_values("time").groupby("house").tail(1)
    stage_counts = latest_stage_df.groupby("stage").size()
    bottleneck_stage = stage_counts.idxmax()

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
    st.error(f"Most Congested Stage: {bottleneck_stage}")
