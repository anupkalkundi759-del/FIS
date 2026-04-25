def run_engine(conn, cur):
    import streamlit as st
    import pandas as pd
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    st.title("⚙️ Scheduling Intelligence Engine")

    tz = ZoneInfo("Asia/Kolkata")
    today = datetime.now(tz)

    # ================= HOUSE CONFIG TABLE =================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS house_config (
            house_no TEXT PRIMARY KEY,
            sla_date DATE
        )
    """)
    conn.commit()

    # ================= ACTIVITY MASTER =================
    cur.execute("""
        SELECT activity_name, sequence_order, duration_days
        FROM activity_master
        ORDER BY sequence_order
    """)
    act = cur.fetchall()

    activity_df = pd.DataFrame(act, columns=["stage", "seq", "days"])
    if activity_df.empty:
        st.error("No activities found")
        return

    activity_df["days"] = activity_df["days"].astype(int)
    total_duration = int(activity_df["days"].sum())
    seq_map = dict(zip(activity_df["stage"], activity_df["seq"]))
    day_map = dict(zip(activity_df["stage"], activity_df["days"]))

    production_stages = [s for s in activity_df["stage"].tolist() if s.lower() not in ["dispatch"]]

    # ================= SLA UI =================
    st.subheader("⚙️ SLA Assignment")
    c1, c2, c3, c4, c5 = st.columns([2,2,2,2,1])

    with c1:
        cur.execute("SELECT project_id, project_name FROM projects ORDER BY project_name")
        projects = cur.fetchall()
        project_dict = {p[1]: p[0] for p in projects}
        selected_project = st.selectbox("Project", list(project_dict.keys()))

    with c2:
        cur.execute("SELECT unit_id, unit_name FROM units WHERE project_id=%s ORDER BY unit_name",
                    (project_dict[selected_project],))
        units = cur.fetchall()
        unit_dict = {u[1]: u[0] for u in units}
        selected_unit = st.selectbox("Unit", list(unit_dict.keys()))

    with c3:
        cur.execute("SELECT house_no FROM houses WHERE unit_id=%s ORDER BY house_no",
                    (unit_dict[selected_unit],))
        houses = [h[0] for h in cur.fetchall()]
        selected_house = st.selectbox("House", houses)

    with c4:
        sla_date = st.date_input("SLA Date")

    with c5:
        if st.button("Save SLA"):
            if sla_date < today.date():
                st.error("SLA cannot be in past")
            else:
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

    # ================= HOUSE START DATE =================
    cur.execute("""
        SELECT h.house_no, MIN(t.timestamp)
        FROM houses h
        JOIN products p ON p.house_id = h.house_id
        JOIN tracking_log t ON t.product_instance_id = p.product_instance_id
        WHERE h.unit_id = %s
        GROUP BY h.house_no
    """, (unit_dict[selected_unit],))

    start_df = pd.DataFrame(cur.fetchall(), columns=["house", "start"])
    if not start_df.empty:
        start_df["start"] = pd.to_datetime(start_df["start"], utc=True).dt.tz_convert(tz)
    start_map = dict(zip(start_df["house"], start_df["start"])) if not start_df.empty else {}

    # ================= CURRENT LIVE PRODUCT STATUS =================
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
                ) rn
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

    progress_df = pd.DataFrame(cur.fetchall(), columns=["house","stage","total","completed","first_seen_stage"])

    if progress_df.empty:
        st.warning("No data")
        return

    progress_df["first_seen_stage"] = pd.to_datetime(progress_df["first_seen_stage"], utc=True, errors="coerce")
    progress_df["first_seen_stage"] = progress_df["first_seen_stage"].dt.tz_convert(tz)

    # ================= FLOW INTELLIGENCE =================
    flow_df = progress_df[progress_df["stage"].isin(production_stages)]

    stage_wip = flow_df.groupby("stage")["total"].sum().to_dict()
    stage_completed_dict = flow_df.groupby("stage")["completed"].sum().to_dict()

    stage_rate = {}
    bottleneck_stage = None
    min_rate = float("inf")

    for stage in stage_wip:
        total = stage_wip.get(stage, 0)
        comp = stage_completed_dict.get(stage, 0)
        rate = comp / total if total else 0
        stage_rate[stage] = rate

        if total >= 5 and rate < min_rate:
            min_rate = rate
            bottleneck_stage = stage

    # ================= ENGINE =================
    results = []
    sla_results = []
    early = []

    grouped = progress_df.groupby("house")

    for house, g in grouped:

        active = g.copy()
        active["seq"] = active["stage"].map(lambda x: seq_map.get(x, 0))
        current_stage_row = active.sort_values(["seq", "first_seen_stage"], ascending=[False, False]).iloc[0]
        current_stage = current_stage_row["stage"]

        start_date = start_map.get(house, today)
        if pd.isna(start_date):
            start_date = today

        days_elapsed = max(1, (today - start_date).days)

        # ===== NOT STARTED HANDLING =====
        if current_stage == "Not Started":
            predicted_finish = "Awaiting Start"
            actual_finish = "Not Finished"
            rem_stage_display = "Not Started"
            rem_total_days = total_duration
            delay_days = 0
            reason = "Awaiting Start"
            progress_percent = 0

        else:
            stage_start = current_stage_row["first_seen_stage"]
            if pd.isna(stage_start):
                stage_start = start_date

            stage_days_elapsed = max(1, (today - stage_start).days)

            current_seq = seq_map.get(current_stage, 1)
            stage_duration = day_map.get(current_stage, 1)

            stage_total = g[g["stage"] == current_stage]["total"].sum()
            stage_completed = g[g["stage"] == current_stage]["completed"].sum()
            stage_ratio = (stage_completed / stage_total) if stage_total > 0 else 0

            progress = ((current_seq - 1) / len(activity_df)) + (stage_ratio / len(activity_df))
            progress_percent = round(progress * 100, 1)

            rem_stage = max(0, stage_duration - stage_days_elapsed)
            if stage_ratio > 0:
                rem_stage = max(0, int(rem_stage * (1 - stage_ratio)))

            rem_stage_display = "Completed" if rem_stage <= 0 else f"{rem_stage} days"

            downstream_only = activity_df[activity_df["seq"] > current_seq]["days"].sum()
            rem_total_days = int(rem_stage + downstream_only)

            # smoothing based on actual elapsed factory time
            if days_elapsed > 0 and progress_percent > 0:
                expected_progress = min(100, (days_elapsed / total_duration) * 100)
                lag_factor = max(0, expected_progress - progress_percent)
                rem_total_days += int(lag_factor / 10)

            predicted_finish = today + timedelta(days=rem_total_days)
            actual_finish = predicted_finish.date() if progress_percent >= 99 else "Not Finished"

            delay_days = 0
            if house in config_map:
                expected = pd.to_datetime(config_map[house]).tz_localize(tz)
                delay_days = (predicted_finish - expected).days

            # ===== SMART REASON =====
            if delay_days > 3:
                reason = "Critical Delay"
            elif delay_days > 0:
                reason = "SLA Miss Risk"
            elif stage_days_elapsed > stage_duration + 1 and stage_ratio < 0.4:
                reason = "Stage Slowdown"
            elif bottleneck_stage == current_stage:
                reason = "Bottleneck Queue"
            elif progress_percent < ((days_elapsed / total_duration) * 100) * 0.7:
                reason = "Low Production Rate"
            else:
                reason = "On Track"

            if house in config_map:
                sla_results.append({
                    "House": house,
                    "Stage": current_stage,
                    "SLA": expected.date(),
                    "Predicted": predicted_finish.date(),
                    "Delay": delay_days,
                    "Issue": reason
                })

            # ===== EARLY WARNING ONLY REAL RISKS =====
            if reason in ["Critical Delay", "SLA Miss Risk", "Stage Slowdown", "Bottleneck Queue"]:
                early.append({
                    "House": house,
                    "Stage": current_stage,
                    "Issue": reason,
                    "Predicted Finish": predicted_finish.date()
                })

        results.append({
            "House": house,
            "Stage": current_stage,
            "Progress %": progress_percent,
            "Predicted Finish": predicted_finish if isinstance(predicted_finish, str) else predicted_finish.date(),
            "Actual Finish": actual_finish,
            "Remaining (Stage)": rem_stage_display,
            "Remaining (Total)": f"{rem_total_days} days",
            "Delay (Days)": delay_days,
            "Delay Reason": reason
        })

    # ================= OUTPUT =================
    st.subheader("🏠 House Intelligence")
    st.dataframe(pd.DataFrame(results), use_container_width=True, height=350)

    st.subheader("🚨 Priority Table (SLA Only)")
    if sla_results:
        sla_df = pd.DataFrame(sla_results).sort_values("Delay", ascending=False)
        st.dataframe(sla_df, use_container_width=True, height=220)
    else:
        st.info("No SLA monitored houses")

    st.subheader("🏭 Flow Intelligence")
    flow_data = []
    for stage in stage_wip:
        flow_data.append({
            "Stage": stage,
            "WIP": stage_wip.get(stage, 0),
            "Completed": stage_completed_dict.get(stage, 0),
            "Efficiency %": round(stage_rate.get(stage, 0) * 100, 1)
        })

    st.dataframe(pd.DataFrame(flow_data), use_container_width=True, height=250)

    if bottleneck_stage:
        st.error(f"🚨 Bottleneck Stage: {bottleneck_stage}")

    st.subheader("🚨 Early Warning")
    if early:
        st.dataframe(pd.DataFrame(early), use_container_width=True, height=250)
    else:
        st.success("No major risks")
