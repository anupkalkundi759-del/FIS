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

    # ================= HOUSE START DATE =================
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

    # ================= CURRENT LIVE STAGE DATA =================
    cur.execute("""
        WITH latest_stage AS (
            SELECT 
                t.product_instance_id,
                t.stage_id,
                t.status,
                t.timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY t.product_instance_id
                    ORDER BY t.timestamp DESC
                ) AS rn
            FROM tracking_log t
        ),

        current_products AS (
            SELECT
                h.house_no,
                p.product_instance_id,
                COALESCE(s.stage_name, 'Not Started') AS stage,
                ls.status,
                ls.timestamp
            FROM houses h
            LEFT JOIN products p ON p.house_id = h.house_id
            LEFT JOIN latest_stage ls
                ON p.product_instance_id = ls.product_instance_id
                AND ls.rn = 1
            LEFT JOIN stages s
                ON ls.stage_id = s.stage_id
            WHERE h.unit_id = %s
        )

        SELECT
            house_no,
            stage,
            COUNT(product_instance_id) AS total,
            COUNT(CASE WHEN status='Completed' THEN 1 END) AS completed,
            MIN(timestamp) AS first_seen_stage
        FROM current_products
        GROUP BY house_no, stage
        ORDER BY house_no
    """, (unit_dict[selected_unit],))

    progress_df = pd.DataFrame(cur.fetchall())
    if progress_df.empty:
        st.warning("No data")
        return

    progress_df.columns = ["house","stage","total","completed","first_seen_stage"]

    if not progress_df.empty:
        progress_df["first_seen_stage"] = pd.to_datetime(progress_df["first_seen_stage"], utc=True, errors="coerce")
        progress_df["first_seen_stage"] = progress_df["first_seen_stage"].dt.tz_convert(tz)

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

        if total > 5 and rate < min_rate:
            min_rate = rate
            bottleneck_stage = stage

    # ================= ENGINE =================
    results, sla_results, early = [], [], []

    grouped = progress_df.groupby("house")

    for house, g in grouped:

        total_products = g["total"].sum()
        completed_products = g["completed"].sum()

        # ===== BETTER CURRENT STAGE =====
        current_stage_row = g.sort_values(["first_seen_stage","total"], ascending=[False,False]).iloc[0]
        current_stage = current_stage_row["stage"]

        # ===== HOUSE START =====
        start_date = start_map.get(house, today)
        if pd.isna(start_date):
            start_date = today

        days_elapsed = max(1, (today - start_date).days)

        # ===== CURRENT STAGE START =====
        stage_start = current_stage_row["first_seen_stage"]
        if pd.isna(stage_start):
            stage_start = start_date

        stage_days_elapsed = max(1, (today - stage_start).days)

        # ===== GLOBAL PROGRESS % =====
        current_seq_row = activity_df[activity_df["stage"] == current_stage]
        current_seq = int(current_seq_row["seq"].values[0]) if not current_seq_row.empty else 1

        progress = ((current_seq - 1) / len(activity_df)) if len(activity_df) > 0 else 0

        stage_total = g[g["stage"] == current_stage]["total"].sum()
        stage_completed = g[g["stage"] == current_stage]["completed"].sum()

        stage_ratio = (stage_completed / stage_total) if stage_total > 0 else 0

        progress += (stage_ratio / len(activity_df)) if len(activity_df) > 0 else 0
        progress_percent = round(progress * 100, 1)

        # ===== CURRENT STAGE DURATION =====
        stage_row = activity_df[activity_df["stage"] == current_stage]
        stage_duration = int(stage_row["days"].values[0]) if not stage_row.empty else 1

        # dynamic daily remaining
        rem_stage = max(0, stage_duration - stage_days_elapsed)

        # if partial completed, soften remaining based on ratio
        if stage_ratio > 0:
            rem_stage = max(0, int(rem_stage * (1 - stage_ratio)))

        stage_display = "Completed" if rem_stage <= 0 else f"{rem_stage} days"

        # ===== REMAINING TOTAL =====
        remaining_stages = activity_df[activity_df["seq"] >= current_seq]
        downstream_duration = int(remaining_stages["days"].sum())

        # production velocity
        velocity = progress_percent / days_elapsed if days_elapsed > 0 else 0

        if velocity > 0:
            efficiency = min(1.8, max(0.4, velocity / 10))
            remaining_total_days = max(0, int((downstream_duration - stage_days_elapsed) / efficiency))
        else:
            remaining_total_days = max(0, downstream_duration - stage_days_elapsed)

        predicted_finish = today + timedelta(days=remaining_total_days)

        actual_finish = predicted_finish.date() if progress_percent >= 99 else "Not Finished"

        # ===== SLA TABLE =====
        sla = config_map.get(house)
        delay_days = 0
        status = "🟢 On Track"
        impact = "On Time"

        if sla:
            expected = pd.to_datetime(sla).tz_localize(tz)
            delay_days = (predicted_finish - expected).days

            status = "🟢 On Track" if delay_days < 0 else "🟢 On Time" if delay_days == 0 else "🔴 Delay"
            impact = "On Time" if delay_days == 0 else f"{'Ahead by' if delay_days < 0 else 'Miss by'} {abs(delay_days)} days"

            sla_results.append({
                "House": house,
                "Stage": current_stage,
                "SLA": expected.date(),
                "Predicted": predicted_finish.date(),
                "Status": status,
                "Impact": impact
            })

        # ===== SMART ALERT REASON =====
        reason = "On track"

        if stage_days_elapsed > stage_duration and stage_ratio < 0.5:
            reason = "Stage stagnation"
        elif bottleneck_stage == current_stage:
            reason = "Bottleneck stage"
        elif velocity < 1:
            reason = "Low production rate"
        elif delay_days > 0:
            reason = "SLA miss risk"

        # ===== RESULTS =====
        results.append({
            "House": house,
            "Stage": current_stage,
            "Progress %": progress_percent,
            "Predicted Finish": predicted_finish.date(),
            "Actual Finish": actual_finish,
            "Remaining (Stage)": stage_display,
            "Remaining (Total)": f"{remaining_total_days} days",
            "Delay (Days)": delay_days,
            "Delay Reason": reason
        })

        # ===== EARLY WARNING FOR ALL HOUSES =====
        if (
            delay_days > 0
            or stage_days_elapsed > stage_duration
            or velocity < 1
            or bottleneck_stage == current_stage
        ):
            early.append({
                "House": house,
                "Stage": current_stage,
                "Issue": reason,
                "Predicted Finish": predicted_finish.date()
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
            "Efficiency %": round(stage_rate.get(stage, 0) * 100, 1)
        })

    st.dataframe(pd.DataFrame(flow_data))

    if bottleneck_stage:
        st.error(f"🚨 Bottleneck Stage: {bottleneck_stage}")

    # ================= EARLY WARNING =================
    st.subheader("🚨 Early Warning")
    st.dataframe(pd.DataFrame(early)) if early else st.success("No risks")
