def run_engine(conn, cur):
    import streamlit as st
    import pandas as pd
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    st.title("⚙️ Scheduling Intelligence Engine")

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
        start_df["start"] = pd.to_datetime(start_df["start"], utc=True).dt.tz_convert(tz)

    start_map = dict(zip(start_df["house"], start_df["start"])) if not start_df.empty else {}

    # ================= PROGRESS =================
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
        st.warning("No data")
        return

    progress_df.columns = ["house","stage","total","completed"]

    # ================= FLOW =================
    stage_wip = progress_df.groupby("stage")["total"].sum().to_dict()
    stage_completed_dict = progress_df.groupby("stage")["completed"].sum().to_dict()

    stage_rate = {}
    bottleneck_stage = None
    min_rate = float("inf")

    for stage in stage_wip:
        total = stage_wip.get(stage, 0)
        comp = stage_completed_dict.get(stage, 0)

        rate = comp / total if total else 0
        stage_rate[stage] = rate

        if rate < min_rate and total > 5:
            min_rate = rate
            bottleneck_stage = stage

    # ================= ENGINE =================
    results, sla_results = [], []

    grouped = progress_df.groupby("house")

    for house, g in grouped:

        total_products = g["total"].sum()
        completed_products = g["completed"].sum()

        progress = completed_products / total_products if total_products else 0
        progress_percent = round(progress * 100, 1)

        # ===== START DATE =====
        start_date = start_map.get(house, today)
        if pd.isna(start_date):
            start_date = today

        days_elapsed = max(1, (today - start_date).days)

        # ===== PRODUCTION RATE (REAL FIX) =====
        rate = completed_products / days_elapsed

        remaining_products = total_products - completed_products

        if rate > 0:
            remaining_total_days = int(remaining_products / rate)
        else:
            remaining_total_days = total_duration - days_elapsed  # 👈 DAILY DECAY

        remaining_total_days = max(0, remaining_total_days)

        predicted_finish = today + timedelta(days=remaining_total_days)

        # ===== CURRENT STAGE =====
        current_stage = g.sort_values("total", ascending=False).iloc[0]["stage"]

        stage_total = g[g["stage"] == current_stage]["total"].sum()
        stage_completed = g[g["stage"] == current_stage]["completed"].sum()

        stage_row = activity_df[activity_df["stage"] == current_stage]
        stage_duration = int(stage_row["days"].values[0]) if not stage_row.empty else 1

        if stage_total > 0:
            stage_ratio = stage_completed / stage_total
        else:
            stage_ratio = 0

        rem_stage = int(stage_duration * (1 - stage_ratio))
        stage_display = "Completed" if rem_stage <= 0 else f"{rem_stage} days"

        actual_finish = predicted_finish.date() if progress >= 0.99 else "Not Finished"

        # ===== SLA =====
        sla = config_map.get(house)

        if sla:
            expected = pd.to_datetime(sla).tz_localize(tz)
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
        if progress < 0.3:
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
    st.subheader("🏠 House Intelligence")
    st.dataframe(pd.DataFrame(results))

    st.subheader("🚨 Priority Table (SLA Only)")
    st.dataframe(pd.DataFrame(sla_results))

    # ================= FLOW =================
    st.subheader("🏭 Flow Intelligence")

    flow_data = []
    for stage in stage_wip:
        flow_data.append({
            "Stage": stage,
            "WIP": stage_wip.get(stage, 0),
            "Completed": stage_completed_dict.get(stage, 0),
            "Efficiency %": round(stage_rate.get(stage, 0)*100,1)
        })

    st.dataframe(pd.DataFrame(flow_data))

    if bottleneck_stage:
        st.error(f"🚨 Bottleneck Stage: {bottleneck_stage}")

    # ================= EARLY WARNING =================
    early = []
    for r in sla_results:
        if "Miss by" in r["Impact"]:
            early.append({
                "House": r["House"],
                "Delay": r["Impact"]
            })

    st.subheader("🚨 Early Warning")
    st.dataframe(pd.DataFrame(early)) if early else st.success("No risks")
