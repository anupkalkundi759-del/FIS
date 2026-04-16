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

    total_duration = activity_df["days"].sum()

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

    # ================= SLA =================
    st.subheader("⚙️ SLA Assignment")

    cur.execute("SELECT house_no FROM houses WHERE unit_id=%s", (unit_dict[selected_unit],))
    houses = [h[0] for h in cur.fetchall()]

    col1, col2 = st.columns(2)

    with col1:
        selected_house = st.selectbox("House", houses)

    with col2:
        sla_date = st.date_input("SLA (Optional)", value=None)

    if st.button("Save SLA"):
        if sla_date:
            cur.execute("""
                INSERT INTO house_config (house_no, sla_date)
                VALUES (%s, %s)
                ON CONFLICT (house_no)
                DO UPDATE SET sla_date = EXCLUDED.sla_date
            """, (selected_house, sla_date))
        else:
            cur.execute("DELETE FROM house_config WHERE house_no=%s", (selected_house,))
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
    alerts = []

    for house in df["house"].unique():

        house_data = df[df["house"] == house]

        meas = house_data[house_data["stage"] == "Measurement"]
        if meas.empty:
            continue

        start_date = meas["time"].min()

        # -------- PROGRESS --------
        total_stages = len(activity_df)
        total_products = house_data["product"].nunique()

        progress_sum = 0
        for stage in activity_df["stage"]:
            stage_products = house_data[house_data["stage"] == stage]["product"].nunique()
            stage_completion = stage_products / total_products if total_products else 0
            progress_sum += stage_completion

        progress = (progress_sum / total_stages) * 100 if total_stages else 0

        latest = house_data.sort_values("seq").iloc[-1]
        current_stage = latest["stage"]
        current_time = latest["time"]

        # -------- TIMELINE --------
        planned_finish = start_date + timedelta(days=int(total_duration))
        predicted_finish = planned_finish

        remaining_days = (predicted_finish - today).days

        # -------- ALERT LOGIC --------
        if remaining_days < 0:
            alert = "🔴 Overdue"
        elif remaining_days <= 3:
            alert = "🟠 At Risk"
        elif remaining_days <= 7:
            alert = "🟡 Approaching"
        else:
            alert = "🟢 Safe"

        # -------- DELAY --------
        delay_days = (predicted_finish - planned_finish).days
        delay_display = "On time" if delay_days == 0 else f"Delay {delay_days}d"

        sla = config_map.get(house)
        expected_finish = pd.to_datetime(sla) if sla else None

        if expected_finish and predicted_finish > expected_finish:
            alerts.append({
                "House": house,
                "Issue": "Will miss SLA",
                "Delay (days)": (predicted_finish - expected_finish).days
            })

        # -------- BOTTLENECK --------
        stage_days = activity_df[activity_df["stage"] == current_stage]["days"].values[0]
        if (today - current_time).days > stage_days:
            alerts.append({
                "House": house,
                "Issue": f"Stuck in {current_stage}",
                "Delay (days)": (today - current_time).days
            })

        results.append({
            "House": house,
            "Stage": current_stage,
            "Progress %": round(progress, 1),
            "Remaining Days": remaining_days,
            "Status Alert": alert,
            "Delay": delay_display,
            "Predicted Finish": predicted_finish.date()
        })

    result_df = pd.DataFrame(results)

    # ================= OUTPUT =================

    st.subheader("🚨 Alerts Summary")
    if alerts:
        st.dataframe(pd.DataFrame(alerts))
    else:
        st.success("No major alerts")

    st.subheader("🏠 House Intelligence")
    st.dataframe(result_df)
