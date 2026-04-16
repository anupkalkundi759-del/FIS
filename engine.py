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

    cur.execute("SELECT house_no FROM houses WHERE unit_id=%s", (unit_dict[selected_unit],))
    houses = [h[0] for h in cur.fetchall()]

    col1, col2 = st.columns(2)

    with col1:
        selected_house = st.selectbox("House", houses)

    with col2:
        use_sla = st.checkbox("Set SLA")
        sla_date = None
        if use_sla:
            sla_date = st.date_input("SLA Date")

    if st.button("Save SLA"):
        if sla_date:
            cur.execute("""
                INSERT INTO house_config (house_no, sla_date)
                VALUES (%s, %s)
                ON CONFLICT (house_no)
                DO UPDATE SET sla_date = EXCLUDED.sla_date
            """, (selected_house, sla_date))
        else:
            cur.execute("DELETE FROM house_config WHERE house_no = %s", (selected_house,))
        
        conn.commit()
        st.success("Saved")

    # ================= LOAD CONFIG =================
    cur.execute("SELECT house_no, sla_date FROM house_config")
    config_map = {r[0]: r[1] for r in cur.fetchall()}

    # ================= LOAD TRACKING =================
    cur.execute("""
        SELECT 
            h.house_no,
            p.product_instance_id,
            s.stage_name,
            t.timestamp,
            s.sequence
        FROM products p
        JOIN houses h ON p.house_id = h.house_id
        JOIN tracking_log t ON t.product_instance_id = p.product_instance_id
        JOIN stages s ON t.stage_id = s.stage_id
        WHERE h.unit_id = %s
    """, (unit_dict[selected_unit],))

    df = pd.DataFrame(cur.fetchall(), columns=["house", "product", "stage", "time", "seq"])

    if df.empty:
        st.warning("No tracking data")
        return

    df["time"] = pd.to_datetime(df["time"])

    # ================= HOUSE LEVEL =================
    results = []
    early_warnings = []
    stuck_stages = []

    for house in df["house"].unique():

        house_data = df[df["house"] == house]

        # -------- START DATE --------
        meas = house_data[house_data["stage"] == "Measurement"]
        if meas.empty:
            continue

        start_date = meas["time"].min()

        # -------- TOTAL PRODUCTS --------
        total_products = house_data["product"].nunique()

        # -------- PROGRESS (STRICT STAGE COMPLETION) --------
        total_stages = len(activity_df)
        completed_stages = 0

        for stage in activity_df["stage"]:
            stage_products = house_data[house_data["stage"] == stage]["product"].nunique()

            if stage_products == total_products:
                completed_stages += 1

        progress = (completed_stages / total_stages) * 100 if total_stages else 0

        # -------- CURRENT STAGE --------
        latest = house_data.sort_values("seq").iloc[-1]
        current_stage = latest["stage"]
        current_time = latest["time"]

        # -------- PLANNED FINISH --------
        planned_finish = start_date + timedelta(days=total_duration)

        predicted = planned_finish

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

        if expected_finish is None:
            priority = None
        else:
            sla_delay = (planned_finish - expected_finish).days
            priority_score = max(0, sla_delay) * 10
            priority = get_priority(priority_score)

        # -------- REASON --------
        if progress == 0:
            reason = "Not started"
        elif progress < 100:
            reason = "In progress"
        else:
            reason = "Completed"

        # -------- EARLY WARNING --------
        if expected_finish and planned_finish > expected_finish:
            early_warnings.append({
                "House": house,
                "Issue": "Will miss SLA",
                "Delay (days)": (planned_finish - expected_finish).days
            })

        # -------- BOTTLENECK --------
        stage_days = activity_df[activity_df["stage"] == current_stage]["days"].values[0]
        if (today - current_time).days > stage_days:
            stuck_stages.append(current_stage)

        results.append({
            "House": house,
            "Stage": current_stage,
            "Progress %": round(progress, 1),
            "Delay": delay_display,
            "SLA": expected_finish,
            "Predicted Finish": planned_finish.date(),
            "Priority": priority,
            "Reason": reason
        })

    result_df = pd.DataFrame(results)

    # ================= OUTPUT =================

    st.subheader("🚨 Priority Table (SLA Only)")
    priority_df = result_df[result_df["SLA"].notna()]
    priority_df = priority_df[["House","Stage","Delay","SLA","Priority","Reason"]]
    st.dataframe(priority_df)

    st.subheader("🏠 House Intelligence")
    house_df = result_df[["House","Stage","Progress %","Delay","Predicted Finish","Reason"]]
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
