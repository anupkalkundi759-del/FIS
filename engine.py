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
    total_seq = activity_df["seq"].max()
    first_stage = activity_df.iloc[0]["stage"]

    # ================= SINGLE CONTROL ROW =================
    col1, col2, col3, col4, col5 = st.columns(5)

    # PROJECT
    with col1:
        cur.execute("SELECT project_id, project_name FROM projects ORDER BY project_name")
        projects = cur.fetchall()
        project_dict = {p[1]: p[0] for p in projects}
        selected_project = st.selectbox("Project", list(project_dict.keys()))

    # UNIT
    with col2:
        cur.execute("""
            SELECT unit_id, unit_name 
            FROM units 
            WHERE project_id=%s
        """, (project_dict[selected_project],))
        units = cur.fetchall()
        unit_dict = {u[1]: u[0] for u in units}
        selected_unit = st.selectbox("Unit", list(unit_dict.keys()))

    # HOUSES
    cur.execute("""
        SELECT house_no 
        FROM houses 
        WHERE unit_id=%s
    """, (unit_dict[selected_unit],))
    houses = [h[0] for h in cur.fetchall()]

    # HOUSE
    with col3:
        selected_house = st.selectbox("House", houses)

    # SLA
    with col4:
        sla_date = st.date_input("SLA")

    # SAVE BUTTON
    with col5:
        if st.button("💾 Save"):
            cur.execute("""
                INSERT INTO house_config (house_no, sla_date)
                VALUES (%s, %s)
                ON CONFLICT (house_no)
                DO UPDATE SET sla_date = EXCLUDED.sla_date
            """, (selected_house, sla_date))
            conn.commit()
            st.success("Saved")

    # ================= SLA FETCH =================
    cur.execute("""
        SELECT house_no, sla_date 
        FROM house_config
        WHERE house_no = ANY(%s)
    """, (houses,))
    config_map = {r[0]: r[1] for r in cur.fetchall()}

    # ================= TRACKING =================
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

    # ================= MAIN ENGINE =================
    for house in df["house"].unique():

        house_df = df[df["house"] == house].dropna(subset=["time"])

        # 👉 ONLY ACTIVE HOUSES
        if house_df.empty:
            continue

        house_df = house_df.sort_values("time")

        start_date = house_df["time"].min()

        latest_row = house_df.loc[house_df["time"].idxmax()]
        current_stage = latest_row["stage"]

        max_seq = house_df["seq"].max()
        progress = (max_seq / total_seq) * 100

        planned_finish = start_date + timedelta(days=int(total_duration))

        if progress < 20:
            predicted = planned_finish
        else:
            remaining_days = activity_df[activity_df["seq"] > max_seq]["days"].sum()
            predicted = today + timedelta(days=int(remaining_days))

        delay_days = (predicted - planned_finish).days

        delay_display = (
            f"Ahead {abs(delay_days)}d" if delay_days < 0
            else "On time" if delay_days == 0
            else f"Delay {delay_days}d"
        )

        sla = config_map.get(house)
        expected_finish = pd.to_datetime(sla) if sla else None

        priority = None
        if expected_finish:
            sla_delay = (predicted - expected_finish).days

            if sla_delay > 5:
                priority = "🔴 Critical"
            elif sla_delay > 2:
                priority = "🟠 High"
            elif sla_delay > 0:
                priority = "🟡 Medium"
            else:
                priority = "🟢 Low"

            if predicted > expected_finish:
                early_warnings.append({
                    "House": house,
                    "Stage": current_stage,
                    "Delay": sla_delay
                })

        results.append({
            "House": house,
            "Stage": current_stage,
            "Progress %": round(progress, 1),
            "Delay": delay_display,
            "Predicted Finish": predicted.date(),
            "Reason": "Active"
        })

    result_df = pd.DataFrame(results)

    # ================= BOTTLENECK =================
    valid_df = df.dropna(subset=["time"])

    if valid_df.empty:
        bottleneck_msg = "No active work"
    else:
        latest = valid_df.loc[valid_df.groupby("house")["time"].idxmax()]
        stage_counts = latest.groupby("stage").size()
        bottleneck_msg = f"Most Congested Stage: {stage_counts.idxmax()}"

    # ================= OUTPUT =================
    st.subheader("🏠 House Intelligence")
    st.dataframe(result_df)

    st.subheader("🚨 Early Warning")
    if early_warnings:
        st.dataframe(pd.DataFrame(early_warnings))
    else:
        st.success("No early risks")

    st.subheader("🚧 Bottleneck")
    st.info(bottleneck_msg)
