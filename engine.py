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

    # ================= SLA FETCH (FIXED 🔥) =================
    cur.execute("""
        SELECT hc.house_no, hc.sla_date
        FROM house_config hc
        JOIN houses h ON hc.house_no = h.house_no
        WHERE h.unit_id = %s
    """, (unit_dict[selected_unit],))
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

        house_df = df[df["house"] == house]

        meas = house_df[house_df["stage"] == "Measurement"]
        if meas.empty:
            continue

        start_date = meas["time"].min()

        house_df_sorted = house_df.sort_values("seq")
        current = house_df_sorted.iloc[-1]
        current_stage = current["stage"]

        max_seq_reached = house_df["seq"].max()
        progress = (max_seq_reached / total_seq) * 100

        elapsed_days = max(1, (today - start_date).days)
        speed = progress / elapsed_days

        if speed == 0:
            predicted = start_date + timedelta(days=int(total_duration))
        else:
            remaining = 100 - progress
            days_needed = remaining / speed
            predicted = today + timedelta(days=int(days_needed))

        planned_finish = start_date + timedelta(days=int(total_duration))

        delay_days = (predicted - planned_finish).days

        if delay_days < 0:
            delay_display = f"Ahead {abs(delay_days)}d"
        elif delay_days == 0:
            delay_display = "On time"
        else:
            delay_display = f"Delay {delay_days}d"

        sla = config_map.get(house)
        expected_finish = pd.to_datetime(sla) if sla else None

        # ================= PRIORITY =================
        if expected_finish:
            sla_delay = (predicted - expected_finish).days

            if sla_delay > 5:
                priority = "🔴 Critical"
            elif sla_delay > 2:
                priority = "🟠 High"
            elif sla_delay > 0:
                priority = "🟡 Medium"
            else:
                priority = "🟢 On Track"
        else:
            priority = None

        if progress < 5:
            reason = "Just started"
        elif delay_days > 0:
            reason = "Delayed execution"
        elif progress < 40:
            reason = "In progress"
        else:
            reason = "On track"

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
    stage_counts = df.groupby("stage").size()
    bottleneck_stage = stage_counts.idxmax()
    st.error(f"Most Congested Stage: {bottleneck_stage}")
