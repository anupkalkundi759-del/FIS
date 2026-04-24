def run_engine(conn, cur):
    import streamlit as st
    import pandas as pd
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    st.title("⚙️ Scheduling Intelligence Engine")

    # ================= TIME =================
    tz = ZoneInfo("Asia/Kolkata")
    today = datetime.now(tz)

    # ================= TABLE =================
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

    activity_df = pd.DataFrame(act, columns=["stage","seq","days"])

    if activity_df.empty:
        st.error("No activities found")
        return

    activity_df["days"] = activity_df["days"].astype(int)
    total_duration = int(activity_df["days"].sum())

    # ================= UI =================
    st.subheader("⚙️ SLA Assignment")

    c1, c2, c3, c4, c5 = st.columns([2,2,2,2,1])

    with c1:
        cur.execute("SELECT project_id, project_name FROM projects ORDER BY project_name")
        projects = cur.fetchall()
        project_dict = {p[1]: p[0] for p in projects}
        selected_project = st.selectbox("Project", list(project_dict.keys()))

    with c2:
        cur.execute("SELECT unit_id, unit_name FROM units WHERE project_id=%s",
                    (project_dict[selected_project],))
        units = cur.fetchall()
        unit_dict = {u[1]: u[0] for u in units}
        selected_unit = st.selectbox("Unit", list(unit_dict.keys()))

    with c3:
        cur.execute("SELECT house_no FROM houses WHERE unit_id=%s",
                    (unit_dict[selected_unit],))
        houses = [h[0] for h in cur.fetchall()]
        selected_house = st.selectbox("House", houses)

    with c4:
        sla_date = st.date_input("SLA Date")

    with c5:
        if st.button("Save SLA"):
            if sla_date < today.date():
                st.error("SLA cannot be in the past")
            else:
                cur.execute("""
                    INSERT INTO house_config (house_no, sla_date)
                    VALUES (%s, %s)
                    ON CONFLICT (house_no)
                    DO UPDATE SET sla_date = EXCLUDED.sla_date
                """, (selected_house, sla_date))
                conn.commit()
                st.success("SLA Saved")

    # ================= SLA =================
    cur.execute("SELECT house_no, sla_date FROM house_config")
    config_map = {r[0]: r[1] for r in cur.fetchall()}

    # ================= START DATE =================
    cur.execute("""
        SELECT h.house_no, MIN(t.timestamp)
        FROM houses h
        JOIN products p ON p.house_id = h.house_id
        JOIN tracking_log t ON t.product_instance_id = p.product_instance_id
        WHERE h.unit_id = %s
        GROUP BY h.house_no
    """, (unit_dict[selected_unit],))

    start_df = pd.DataFrame(cur.fetchall(), columns=["house","start"])

    if not start_df.empty:
        start_df["start"] = pd.to_datetime(start_df["start"])
        # 🔥 FIX TIMEZONE ISSUE
        start_df["start"] = start_df["start"].dt.tz_localize("Asia/Kolkata")

    start_map = dict(zip(start_df["house"], start_df["start"])) if not start_df.empty else {}

    # ================= PROGRESS QUERY =================
    cur.execute("""
        WITH latest_stage AS (
            SELECT 
                t.product_instance_id,
                t.stage_id,
                t.status,
                ROW_NUMBER() OVER (
                    PARTITION BY t.product_instance_id 
                    ORDER BY t.timestamp DESC
                ) AS rn
            FROM tracking_log t
        )

        SELECT 
            h.house_no,
            COALESCE(s.stage_name, 'Not Started') AS stage,
            COUNT(p.product_instance_id) AS total,
            COUNT(DISTINCT CASE 
                WHEN t.status = 'Completed' 
                THEN t.product_instance_id 
            END) AS completed

        FROM houses h
        LEFT JOIN products p ON p.house_id = h.house_id

        LEFT JOIN latest_stage ls 
            ON p.product_instance_id = ls.product_instance_id 
            AND ls.rn = 1

        LEFT JOIN stages s 
            ON ls.stage_id = s.stage_id

        LEFT JOIN tracking_log t 
            ON t.product_instance_id = p.product_instance_id

        WHERE h.unit_id = %s

        GROUP BY h.house_no, s.stage_name
        ORDER BY h.house_no
    """, (unit_dict[selected_unit],))

    progress_df = pd.DataFrame(cur.fetchall())

    if progress_df.empty:
        st.warning("No tracking data")
        return

    progress_df.columns = ["house","stage","total","completed"]

    # ================= DOMINANT STAGE =================
    dominant_stage_map = {}
    stage_map = {}

    grouped = progress_df.groupby("house")

    for house, g in grouped:
        g_sorted = g.sort_values("total", ascending=False)
        dominant_stage_map[house] = g_sorted.iloc[0]["stage"]

        for _, r in g.iterrows():
            stage_map[(house, r["stage"])] = r["completed"]

    # ================= FLOW INTELLIGENCE =================
    stage_wip = progress_df.groupby("stage")["total"].sum().to_dict()
    stage_completed = progress_df.groupby("stage")["completed"].sum().to_dict()

    stage_rate = {}
    bottleneck_stage = None
    min_rate = float("inf")

    for stage in stage_wip:
        total = stage_wip.get(stage, 0)
        comp = stage_completed.get(stage, 0)

        rate = comp / total if total else 0
        stage_rate[stage] = rate

        if rate < min_rate and total > 5:
            min_rate = rate
            bottleneck_stage = stage

    # ================= ENGINE =================
    results, sla_results = [], []

    for house, g in grouped:

        total_products = g["total"].sum()
        completed_products = g["completed"].sum()

        progress_physical = completed_products / total_products if total_products else 0

        # ===== TIME =====
        start_date = start_map.get(house)
        if pd.isna(start_date) or start_date is None:
            start_date = today

        days_elapsed = max(1, (today - start_date).days)

        # ===== HYBRID =====
        progress_time = days_elapsed / total_duration if total_duration else 0
        progress = max(progress_physical, progress_time)

        progress_percent = round(progress * 100, 1)

        # ===== PRODUCTION RATE =====
        rate = progress_physical / days_elapsed if days_elapsed > 0 else 0

        if rate > 0:
            remaining_total_days = int((1 - progress_physical) / rate)
        else:
            remaining_total_days = total_duration

        predicted_finish = today + timedelta(days=remaining_total_days)

        # ===== STAGE =====
        current_stage = dominant_stage_map.get(house, "Not Started")

        stage_row = activity_df[activity_df["stage"] == current_stage]
        stage_duration = int(stage_row["days"].values[0]) if not stage_row.empty else 1

        stage_completed = stage_map.get((house, current_stage), 0)
        stage_ratio = stage_completed / total_products if total_products else 0

        rem_stage = int(stage_duration * (1 - stage_ratio))
        stage_display = "Completed" if rem_stage <= 0 else f"{rem_stage} days"

        actual_finish = predicted_finish.date() if progress >= 0.99 else "Not Finished"

        # ===== SLA =====
        sla = config_map.get(house)

        if sla:
            expected = pd.to_datetime(sla).tz_localize("Asia/Kolkata")

            delay_days = (predicted_finish - expected).days

            status = "🟢 On Track" if delay_days < 0 else "🟢 On Time" if delay_days == 0 else "🔴 Delay"
            impact = "On Time" if delay_days == 0 else f"{'Ahead by' if delay_days<0 else 'Miss by'} {abs(delay_days)} days"

            sla_results.append({
                "House": house,
                "Stage": current_stage,
                "SLA": expected.date(),
                "Predicted": predicted_finish.date(),
                "Status": status,
                "Impact": impact
            })

        reason = "on track"
        if progress_physical < 0.3:
            reason = "Low production rate"
        elif bottleneck_stage == current_stage:
            reason = "Bottleneck stage"

        results.append({
            "House": house,
            "Stage": current_stage,
            "Progress %": progress_percent,
            "Predicted Finish": predicted_finish.date(),
            "Actual Finish": actual_finish,
            "Remaining (Stage)": stage_display,
            "Remaining (Total)": f"{remaining_total_days} days",
            "Delay (Days)": 0,
            "Delay Reason": reason
        })

    # ================= OUTPUT =================
    st.subheader("🚨 Priority Table (SLA Only)")
    st.dataframe(pd.DataFrame(sla_results))

    st.subheader("🏠 House Intelligence")
    st.dataframe(pd.DataFrame(results))

    # ================= FLOW =================
    st.subheader("🏭 Flow Intelligence")

    flow_data = []
    for stage in stage_wip:
        flow_data.append({
            "Stage": stage,
            "WIP": stage_wip.get(stage, 0),
            "Completed": stage_completed.get(stage, 0),
            "Efficiency %": round(stage_rate.get(stage, 0)*100,1)
        })

    st.dataframe(pd.DataFrame(flow_data))

    if bottleneck_stage:
        st.error(f"🚨 Bottleneck Stage: {bottleneck_stage}")
    else:
        st.success("No bottleneck detected")

    # ================= EARLY WARNING =================
    early = []
    for r in sla_results:
        if "Miss by" in r["Impact"]:
            early.append({
                "House": r["House"],
                "Issue": "Will miss SLA",
                "Delay (days)": int(r["Impact"].split(" ")[-2])
            })

    st.subheader("🚨 Early Warning")
    st.dataframe(pd.DataFrame(early)) if early else st.success("No early risks")
