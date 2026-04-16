def run_engine(conn, cur):
    import streamlit as st
    import pandas as pd
    from datetime import datetime, timedelta

    st.title("⚙️ Scheduling Intelligence Engine")
    today = datetime.now()

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

    selected_house = st.selectbox("House", houses)
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

    df = pd.DataFrame(rows, columns=["house","stage","time","seq"])

    if df.empty:
        st.warning("No tracking data available")

    # 🔴 ALWAYS RUN STRUCTURE
    results = []
    early_warnings = []
    stage_delay_list = []

    if not df.empty:
        df["time"] = pd.to_datetime(df["time"])

        for house in df["house"].unique():

            house_df = df[df["house"] == house].sort_values("time")

            start_date = house_df["time"].min()

            latest = house_df.loc[house_df["time"].idxmax()]
            current_stage = latest["stage"]
            current_seq = latest["seq"]

            # -------- PROGRESS --------
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
                    "Delay": (predicted - expected_finish).days
                })

            results.append({
                "House": house,
                "Stage": current_stage,
                "Progress %": round(progress,1),
                "Delay": delay_display,
                "Predicted Finish": predicted.date(),
                "Variance": variance
            })

    result_df = pd.DataFrame(results)

    # ================= OUTPUT =================

    st.subheader("🏠 House Intelligence")
    st.dataframe(result_df)

    st.subheader("🚨 Early Warning")
    if early_warnings:
        st.dataframe(pd.DataFrame(early_warnings))
    else:
        st.success("No early risks")
