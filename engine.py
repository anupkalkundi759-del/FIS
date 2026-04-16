def run_engine(conn, cur):
    import streamlit as st
    import pandas as pd
    from datetime import datetime, timedelta

    st.title("⚙️ Scheduling Intelligence Engine")

    today = datetime.now()

    # ================= CONFIG TABLE =================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS house_config (
            house_no TEXT PRIMARY KEY,
            sla_date DATE
        )
    """)
    conn.commit()

    # ================= LOAD ACTIVITIES =================
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
            FROM units 
            WHERE project_id=%s
        """, (project_dict[selected_project],))
        units = cur.fetchall()
        unit_dict = {u[1]: u[0] for u in units}
        selected_unit = st.selectbox("Unit", list(unit_dict.keys()))

    # ================= SLA ASSIGNMENT =================
    st.subheader("⚙️ SLA Assignment")

    cur.execute("""
        SELECT house_no FROM houses WHERE unit_id=%s
    """, (unit_dict[selected_unit],))
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

    # ================= LOAD CONFIG =================
    cur.execute("SELECT house_no, sla_date FROM house_config")
    config_map = {
        r[0]: r[1]
        for r in cur.fetchall()
    }

    # ================= LOAD TRACKING =================
    cur.execute("""
        SELECT 
            h.house_no,
            s.stage_name,
            t.timestamp,
            s.sequence
        FROM products p
        JOIN houses h ON p.house_id = h.house_id
        JOIN tracking_log t ON t.product_instance_id = p.product_instance_id
        JOIN stages s ON t.stage_id = s.stage_id
        WHERE h.unit_id = %s
    """, (unit_dict[selected_unit],))

    df = pd.DataFrame(cur.fetchall(), columns=["house", "stage", "time", "seq"])

    if df.empty:
        st.warning("No tracking data")
        return

    df["time"] = pd.to_datetime(df["time"])

    # ================= PRODUCT PROGRESS =================
    prod_list = []

    for _, row in df.iterrows():
        seq = int(row["seq"])
        time = row["time"]

        completed = activity_df[activity_df["seq"] < seq]["days"].sum()

        stage_days = activity_df[activity_df["seq"] == seq]["days"]
        stage_days = int(stage_days.values[0]) if not stage_days.empty else 1

        days_spent = max(0, (today - time).days)

        # ✅ MINIMUM BASELINE FIX
        if days_spent == 0:
            progress_stage = 0.05
        else:
            progress_stage = min(days_spent / stage_days, 1)

        completed += stage_days * progress_stage
        progress = (completed / total_duration) * 100

        prod_list.append({
            "house": row["house"],
            "stage": row["stage"],
            "seq": seq,
            "time": time,
            "progress": progress
        })

    prod_df = pd.DataFrame(prod_list)

    # ================= HOUSE LEVEL =================
    results = []
    early_warnings = []
    stuck_stages = []

    for house in prod_df["house"].unique():

        house_df = prod_df[prod_df["house"] == house]

        # -------- STRICT START --------
        meas = df[(df["house"] == house) & (df["stage"] == "Measurement")]
        if meas.empty:
            continue

        start_date = meas["time"].min()

        progress = house_df["progress"].mean()

        current = house_df.loc[house_df["seq"].idxmin()]
        current_stage = current["stage"]
        current_time = current["time"]

        # -------- PRODUCTIVITY --------
        recent = house_df.sort_values("seq")
        rates = []

        for i in range(len(recent)-1):
            t1 = recent.iloc[i]["time"]
            t2 = recent.iloc[i+1]["time"]

            actual = max(1, (t2 - t1).days)
            planned = activity_df[
                activity_df["stage"] == recent.iloc[i]["stage"]
            ]["days"].values[0]

            rates.append(actual / planned)

        productivity = sum(rates[-2:])/2 if len(rates)>=2 else 1
        productivity = max(0.7, min(productivity, 1.5))

        # -------- SLA --------
        sla = config_map.get(house)
        expected_finish = pd.to_datetime(sla) if sla else None

        # -------- PREDICTION --------
        remaining = total_duration * (1 - progress/100)
        predicted = today + timedelta(days=int(remaining * productivity))

        delay = None
        if expected_finish:
            delay = (predicted - expected_finish).days

        # -------- DELAY DISPLAY --------
        if expected_finish is None:
            delay_display = "No SLA"
        elif delay < 0:
            delay_display = f"Ahead {abs(delay)}d"
        elif delay == 0:
            delay_display = "On time"
        else:
            delay_display = f"Delay {delay}d"

        # -------- PRIORITY --------
        def get_priority(score):
            if score >= 80: return "🔴 Critical"
            elif score >= 50: return "🟠 High"
            elif score >= 20: return "🟡 Medium"
            else: return "🟢 Low"

        if expected_finish is None:
            priority = "No SLA"
        else:
            priority_score = max(0, delay) * 10
            priority = get_priority(priority_score)

        # -------- REASON --------
        if progress < 5:
            reason = "Not started"
        elif expected_finish and delay > 0:
            reason = "Will miss SLA"
        else:
            reason = "On track"

        # -------- EARLY WARNING --------
        if expected_finish and predicted > expected_finish:
            early_warnings.append({
                "House": house,
                "Issue": "Will be delayed",
                "Delay (days)": delay
            })

        # -------- BOTTLENECK --------
        stage_days = activity_df[activity_df["stage"] == current_stage]["days"].values[0]
        if (today - current_time).days > stage_days:
            stuck_stages.append(current_stage)

        results.append({
            "House": house,
            "Stage": current_stage,
            "Progress %": round(progress,1),
            "Delay": delay_display,
            "SLA": expected_finish,
            "Predicted Finish": predicted.date(),
            "Priority": priority,
            "Reason": reason
        })

    result_df = pd.DataFrame(results)

    # ================= OUTPUT =================

    st.subheader("🚨 Priority Table")
    priority_df = result_df[["House","Stage","Delay","SLA","Priority","Reason"]]
    st.dataframe(priority_df)

    st.subheader("🏠 House Intelligence")
    house_df = result_df[["House","Stage","Progress %","Predicted Finish","Reason"]]
    st.dataframe(house_df)

    st.subheader("🚨 Early Warning")
    if early_warnings:
        st.dataframe(pd.DataFrame(early_warnings))
    else:
        st.success("No early risks")

    st.subheader("🚧 Bottleneck")
    if stuck_stages:
        bottleneck = pd.Series(stuck_stages).value_counts().idxmax()
        st.error(f"Most Stuck Stage: {bottleneck}")
    else:
        st.success("No bottleneck detected")
