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
            urgency INT DEFAULT 0,
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

    # ================= CONFIG UI =================
    st.subheader("⚙️ House Configuration (SLA + Urgency)")

    # -------- PROJECT --------
    cur.execute("SELECT DISTINCT project_name FROM houses")
    projects = [p[0] for p in cur.fetchall()]

    col1, col2, col3 = st.columns(3)

    with col1:
        selected_project = st.selectbox("Project", projects)

    # -------- UNIT --------
    cur.execute("""
        SELECT DISTINCT unit_name 
        FROM houses 
        WHERE project_name = %s
    """, (selected_project,))
    units = [u[0] for u in cur.fetchall()]

    with col2:
        selected_unit = st.selectbox("Unit", units)

    # -------- HOUSE --------
    cur.execute("""
        SELECT house_no 
        FROM houses 
        WHERE project_name = %s AND unit_name = %s
    """, (selected_project, selected_unit))
    houses = [h[0] for h in cur.fetchall()]

    with col3:
        selected_house = st.selectbox("House", houses)

    # -------- URGENCY + SLA --------
    urgency_map_ui = {
        "Low": 0,
        "Medium": 1,
        "High": 2,
        "Critical": 3
    }

    col4, col5 = st.columns(2)

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
    config_map = {
        row[0]: {"urgency": row[1], "sla": row[2]}
        for row in cur.fetchall()
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
        JOIN tracking_log t ON t.product_instance_id = p.id
        JOIN stages s ON t.stage_id = s.stage_id
    """)

    data = cur.fetchall()

    if not data:
        st.warning("No tracking data")
        return

    df = pd.DataFrame(data, columns=["house", "stage", "time", "seq"])
    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    # ================= PRODUCT PROGRESS =================
    product_progress = []

    for _, row in df.iterrows():
        seq = int(row["seq"])
        time = row["time"]

        completed_days = activity_df[activity_df["seq"] < seq]["days"].sum()

        current_stage_days = activity_df[activity_df["seq"] == seq]["days"]
        current_stage_days = int(current_stage_days.values[0]) if not current_stage_days.empty else 0

        days_in_stage = max(0, (today - time).days)
        partial = min(days_in_stage / current_stage_days, 1) if current_stage_days > 0 else 0

        completed_days += int(current_stage_days * partial)

        progress = (completed_days / total_duration) * 100 if total_duration else 0

        product_progress.append({
            "house": row["house"],
            "progress": progress,
            "stage": row["stage"],
            "seq": seq,
            "time": time
        })

    prod_df = pd.DataFrame(product_progress)

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

    # ================= HOUSE LOOP =================
    for house in prod_df["house"].unique():

        house_products = prod_df[prod_df["house"] == house]

        house_progress = house_products["progress"].mean()

        min_row = house_products.loc[house_products["seq"].idxmin()]
        current_stage = min_row["stage"]
        current_time = min_row["time"]

        measurement_rows = df[(df["house"] == house) & (df["stage"] == "Measurement")]

        if not measurement_rows.empty:
            start_date = measurement_rows["time"].min()
        else:
            start_date = df[df["house"] == house]["time"].min()

        # ================= PRODUCTIVITY =================
        house_df = df[df["house"] == house].sort_values("seq").reset_index(drop=True)

        recent_rates = []

        for i in range(len(house_df) - 1):
            t1 = house_df.loc[i, "time"]
            t2 = house_df.loc[i+1, "time"]

            if pd.notna(t1) and pd.notna(t2):
                actual_days = max(1, (t2 - t1).days)

                planned_days = activity_df[
                    activity_df["stage"] == house_df.loc[i, "stage"]
                ]["days"]

                planned_days = int(planned_days.values[0]) if not planned_days.empty else 1

                delay_stage = actual_days - planned_days

                stage_analysis.append({
                    "Stage": house_df.loc[i, "stage"],
                    "Delay": delay_stage
                })

                recent_rates.append(actual_days / planned_days)

        if len(recent_rates) >= 2:
            productivity_rate = sum(recent_rates[-2:]) / 2
        elif recent_rates:
            productivity_rate = recent_rates[-1]
        else:
            productivity_rate = 1

        productivity_rate = max(0.7, min(productivity_rate, 1.5))

        # ================= CONFIG =================
        config = config_map.get(house, {})
        urgency_val = config.get("urgency", 0)
        sla_date = config.get("sla")

        urgency_label_display = [k for k, v in urgency_map_ui.items() if v == urgency_val][0]

        # ================= EXPECTED FINISH =================
        if sla_date:
            expected_finish = pd.to_datetime(sla_date)
        else:
            expected_finish = start_date + timedelta(days=int(total_duration))

        # ================= PREDICTION =================
        if house_progress < 5:
            predicted_finish = None
            delay = None
        else:
            remaining_days = total_duration * (1 - house_progress / 100)
            predicted_finish = today + timedelta(days=int(remaining_days * productivity_rate))
            delay = (predicted_finish - expected_finish).days

        # ================= DELAY DISPLAY =================
        if house_progress < 5:
            delay_display = "Not started"
        elif delay < 0:
            delay_display = f"Ahead by {abs(delay)} days"
        elif delay == 0:
            delay_display = "On time"
        else:
            delay_display = f"Delayed by {delay} days"

        # ================= PRIORITY =================
        days_to_sla = (expected_finish - today).days

        delay_factor = max(0, delay) if delay is not None else 0

        priority_score = (
            (delay_factor * 3) +
            (100 - house_progress) +
            (urgency_val * 15) -
            days_to_sla
        )

        # ================= REASON =================
        if house_progress < 5:
            reason = "Not started"
        elif delay is not None and delay > 0:
            reason = "Delayed project"
        elif days_to_sla < 7:
            reason = "Approaching SLA"
        elif house_progress < 40:
            reason = "Slow progress"
        elif urgency_val >= 2:
            reason = "High priority (manual)"
        else:
            reason = "On track"

        # ================= EARLY WARNING =================
        stage_days = activity_df[activity_df["stage"] == current_stage]["days"]
        stage_days = int(stage_days.values[0]) if not stage_days.empty else 1

        days_in_stage = (today - current_time).days

        if days_in_stage > stage_days:
            early_warnings.append({
                "House": house,
                "Issue": f"Delay in {current_stage}"
            })

        # ================= RESULTS =================
        results.append({
            "House": house,
            "Stage": current_stage,
            "Progress %": round(house_progress, 1),
            "Delay": delay_display,
            "SLA": expected_finish.date(),
            "Predicted Finish": predicted_finish.date() if predicted_finish else "N/A",
            "Urgency": urgency_label_display,
            "Priority Score": round(priority_score, 1),
            "Priority": priority_color(priority_score),
            "Reason": reason
        })

    result_df = pd.DataFrame(results)

    # ================= KPI =================
    st.subheader("📊 Overview")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Houses", len(result_df))
    col2.metric("Delayed Houses", len(result_df[result_df["Delay"].str.contains("Delayed", na=False)]))
    col3.metric("Avg Progress", round(result_df["Progress %"].mean(), 1))

    # ================= PRIORITY =================
    st.subheader("🚨 Priority Houses")
    st.dataframe(result_df.sort_values("Priority Score", ascending=False).head(5))

    # ================= EARLY WARNING =================
    st.subheader("🚨 Early Warnings")
    if early_warnings:
        st.dataframe(pd.DataFrame(early_warnings))
    else:
        st.success("No bottlenecks - all stages performing within plan")

    # ================= BOTTLENECK =================
    stage_df = pd.DataFrame(stage_analysis)

    if not stage_df.empty:
        bottleneck_df = stage_df.groupby("Stage")["Delay"].mean()
        bottleneck_df = bottleneck_df[bottleneck_df > 0]

        if not bottleneck_df.empty:
            st.error(f"🚨 Major Bottleneck: {bottleneck_df.idxmax()}")
        else:
            st.success("No bottlenecks - all stages performing within plan")

    # ================= FINAL =================
    st.subheader("🏠 House Intelligence")
    st.dataframe(result_df, use_container_width=True)
