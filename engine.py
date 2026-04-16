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

    # ================= PROJECT =================
    col1, col2 = st.columns(2)

    with col1:
        cur.execute("SELECT project_id, project_name FROM projects")
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

    rows = cur.fetchall()

    # 🔴 IMPORTANT FIX: don't kill execution
    if not rows:
        st.warning("No tracking data available")

    df = pd.DataFrame(rows, columns=["house","stage","time","seq"]) if rows else pd.DataFrame(
        columns=["house","stage","time","seq"]
    )

    if not df.empty:
        df["time"] = pd.to_datetime(df["time"])

    results = []
    early_warnings = []
    stage_delay_list = []

    # ================= MAIN LOOP =================
    if not df.empty:

        for house in df["house"].unique():

            house_df = df[df["house"] == house].sort_values("time")

            start_date = house_df["time"].min()

            latest = house_df.loc[house_df["time"].idxmax()]
            current_stage = latest["stage"]
            current_seq = latest["seq"]

            # -------- WEIGHTED PROGRESS --------
            completed_weight = activity_df[
                activity_df["seq"] <= current_seq
            ]["days"].sum()

            progress = (completed_weight / total_duration) * 100

            # -------- PLANNED --------
            planned_finish = start_date + timedelta(days=int(total_duration))

            # -------- ACTUAL --------
            elapsed_days = (today - start_date).days

            # -------- VARIANCE --------
            variance = elapsed_days - completed_weight

            # -------- STAGE DELAY --------
            for i in range(len(house_df)-1):
                s1 = house_df.iloc[i]
                s2 = house_df.iloc[i+1]

                if s2["seq"] > s1["seq"]:
                    actual = (s2["time"] - s1["time"]).days
                    planned = activity_df[
                        activity_df["stage"] == s1["stage"]
                    ]["days"].values[0]

                    stage_delay_list.append({
                        "Stage": s1["stage"],
                        "Delay": actual - planned
                    })

            # -------- CRITICAL STAGE --------
            remaining_path = activity_df[activity_df["seq"] >= current_seq]
            critical_stage = remaining_path.iloc[0]["stage"]

            # -------- PREDICTION --------
            if completed_weight < 5:
                predicted = planned_finish
            else:
                remaining = total_duration - completed_weight
                productivity = elapsed_days / max(1, completed_weight)
                predicted = today + timedelta(days=int(remaining * productivity))

            # -------- DELAY --------
            delay_days = (predicted - planned_finish).days

            if delay_days > 0:
                delay_display = f"Delay {delay_days}d"
            elif delay_days < 0:
                delay_display = f"Ahead {abs(delay_days)}d"
            else:
                delay_display = "On time"

            # -------- SLA --------
            sla = config_map.get(house)
            expected_finish = pd.to_datetime(sla) if sla else None

            if expected_finish and predicted > expected_finish:
                early_warnings.append({
                    "House": house,
                    "Stage": current_stage,
                    "Delay": (predicted - expected_finish).days
                })

            results.append({
                "House": house,
                "Stage": current_stage,
                "Progress %": round(progress,1),
                "Delay": delay_display,
                "Predicted Finish": predicted.date(),
                "Variance": variance,
                "Critical Stage": critical_stage
            })

    result_df = pd.DataFrame(results)

    # ================= BOTTLENECK =================
    if df.empty:
        bottleneck_msg = "No data"
    elif df["seq"].nunique() == 1:
        bottleneck_msg = "Initial stage"
    else:
        latest_stage = df.loc[df.groupby("house")["time"].idxmax()]
        bottleneck_stage = latest_stage.groupby("stage").size().idxmax()
        bottleneck_msg = f"Most Congested Stage: {bottleneck_stage}"

    # ================= OUTPUT =================

    st.subheader("🏠 House Intelligence")
    st.dataframe(result_df)

    st.subheader("🚨 Early Warning")
    if early_warnings:
        st.dataframe(pd.DataFrame(early_warnings))
    else:
        st.success("No early risks")

    st.subheader("📉 Stage Delay")
    if stage_delay_list:
        stage_df = pd.DataFrame(stage_delay_list)
        st.dataframe(stage_df.groupby("Stage")["Delay"].mean())

    st.subheader("🚧 Bottleneck")
    st.warning(bottleneck_msg)
