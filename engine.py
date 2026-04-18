def run_engine(conn, cur):
    import streamlit as st
    import pandas as pd
    from datetime import datetime, timedelta

    st.title("⚙️ Scheduling Intelligence Engine")
    today = datetime.now()

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

    # ================= SLA =================
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

    results = []
    sla_results = []
    early_warnings = []
    stuck_stages = []

    # ================= MAIN LOOP =================
    for house in df["house"].unique():

        house_data = df[df["house"] == house]

        meas = house_data[house_data["stage"] == "Measurement"]
        if meas.empty:
            continue

        start_date = meas["time"].min()

        current_pointer = start_date
        total_delay = 0
        earned_duration = 0

        # ================= STAGE-WISE LOGIC =================
        for _, row in activity_df.iterrows():

            stage = row["stage"]
            duration = row["days"]

            stage_data = house_data[house_data["stage"] == stage]

            planned_start = current_pointer
            planned_finish = planned_start + timedelta(days=duration)

            if not stage_data.empty:

                actual_start = stage_data["time"].min()
                actual_finish = stage_data["time"].max()  # ✅ REAL FINISH

                delay = (actual_finish - planned_finish).days
                delay = max(delay, 0)

                total_delay += delay

                # FS dependency: next starts after actual finish
                current_pointer = actual_finish

                earned_duration += duration

            else:
                # future stages shift with accumulated delay
                current_pointer = planned_finish + timedelta(days=total_delay)

        predicted_finish = current_pointer
        planned_finish_total = start_date + timedelta(days=total_duration)

        # ================= PROGRESS =================
        progress = (earned_duration / total_duration) * 100 if total_duration else 0

        # ================= SLA =================
        sla = config_map.get(house)
        expected_finish = pd.to_datetime(sla) if sla else None

        # ================= REMAINING =================
        rem_days = (expected_finish - today).days if expected_finish else (predicted_finish - today).days

        # ================= ALERT =================
        if rem_days < 0:
            alert = "🔴 Overdue"
        elif rem_days <= 3:
            alert = "🟠 At Risk"
        elif rem_days <= 7:
            alert = "🟡 Approaching"
        else:
            alert = "🟢 Safe"

        # ================= DELAY =================
        delay_days = (predicted_finish - planned_finish_total).days

        if delay_days < 0:
            delay_display = f"Ahead vs Plan {abs(delay_days)}d"
        elif delay_days == 0:
            delay_display = "On Plan"
        else:
            delay_display = f"Delay vs Plan {delay_days}d"

        # ================= CURRENT STAGE =================
        latest = house_data.sort_values("seq").iloc[-1]
        current_stage = latest["stage"]
        current_time = latest["time"]

        # ================= BOTTLENECK =================
        stage_days = activity_df[activity_df["stage"] == current_stage]["days"].values[0]
        if (today - current_time).days > stage_days:
            stuck_stages.append(current_stage)

        # ================= SLA TABLE =================
        if expected_finish:

            sla_status = "On Track" if predicted_finish <= expected_finish else "Delay"

            sla_results.append({
                "House": house,
                "Stage": current_stage,
                "Remaining Days": int(rem_days),
                "Predicted Finish": predicted_finish.date(),
                "SLA Date": expected_finish.date(),
                "SLA Status": sla_status
            })

            if predicted_finish > expected_finish:
                early_warnings.append({
                    "House": house,
                    "Issue": "Will miss SLA",
                    "Delay (days)": int((predicted_finish - expected_finish).days)
                })

        else:
            results.append({
                "House": house,
                "Stage": current_stage,
                "Progress %": round(progress, 1),
                "Remaining Days": int(rem_days),
                "Status Alert": alert,
                "Delay": delay_display,
                "Predicted Finish": predicted_finish.date()
            })

    # ================= OUTPUT =================
    st.subheader("🚨 Priority Table (SLA Only)")
    st.dataframe(pd.DataFrame(sla_results))

    st.subheader("🏠 House Intelligence (Non-SLA Only)")
    st.dataframe(pd.DataFrame(results))

    st.subheader("🚨 Early Warning")
    if early_warnings:
        st.dataframe(pd.DataFrame(early_warnings))
    else:
        st.success("No early risks")

    st.subheader("🚧 Bottleneck")
    if stuck_stages:
        st.error(f"Most Stuck Stage: {pd.Series(stuck_stages).value_counts().idxmax()}")
    else:
        st.success("No bottleneck detected")
