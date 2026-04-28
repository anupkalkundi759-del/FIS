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
    production_stages = [s for s in activity_df["stage"].tolist() if s.lower() != "dispatch"]

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

    # ================= REAL ACTUAL DISPATCH DATE =================
    cur.execute("""
        WITH latest_log AS (
            SELECT
                p.product_instance_id,
                h.house_no,
                s.stage_name,
                t.status,
                t.timestamp,
                ROW_NUMBER() OVER(
                    PARTITION BY p.product_instance_id
                    ORDER BY t.timestamp DESC
                ) rn
            FROM products p
            JOIN houses h ON p.house_id = h.house_id
            LEFT JOIN tracking_log t ON p.product_instance_id = t.product_instance_id
            LEFT JOIN stages s ON t.stage_id = s.stage_id
            WHERE h.unit_id = %s
        )
        SELECT
            house_no,
            COUNT(*) as total_products,
            COUNT(CASE WHEN stage_name='Dispatch' AND status='Completed' THEN 1 END) as dispatched_products,
            MAX(CASE WHEN stage_name='Dispatch' AND status='Completed' THEN timestamp END) as dispatch_finish
        FROM latest_log
        WHERE rn=1
        GROUP BY house_no
    """, (unit_dict[selected_unit],))

    dispatch_df = pd.DataFrame(cur.fetchall(), columns=["house","total_products","dispatched_products","dispatch_finish"])
    if not dispatch_df.empty:
        dispatch_df["dispatch_finish"] = pd.to_datetime(dispatch_df["dispatch_finish"], utc=True, errors="coerce")
        dispatch_df["dispatch_finish"] = dispatch_df["dispatch_finish"].dt.tz_convert(tz)
    dispatch_map = dispatch_df.set_index("house").to_dict("index") if not dispatch_df.empty else {}

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
                COALESCE(ls.status, 'Pending') as status,
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
            MIN(timestamp) AS first_seen_stage,
            MAX(timestamp) AS last_movement
        FROM current_products
        GROUP BY house_no, stage
        ORDER BY house_no
    """, (unit_dict[selected_unit],))

    progress_df = pd.DataFrame(cur.fetchall(), columns=["house","stage","total","completed","first_seen_stage","last_movement"])

    if progress_df.empty:
        st.warning("No data")
        return

    progress_df["first_seen_stage"] = pd.to_datetime(progress_df["first_seen_stage"], utc=True, errors="coerce").dt.tz_convert(tz)
    progress_df["last_movement"] = pd.to_datetime(progress_df["last_movement"], utc=True, errors="coerce").dt.tz_convert(tz)

    # ================= FLOW INTELLIGENCE REAL =================
    flow_df = progress_df[progress_df["stage"].isin(production_stages)]

    flow_data = []
    bottleneck_stage = None
    worst_congestion = -1

    for stage in production_stages:
        sdf = flow_df[flow_df["stage"] == stage]
        wip = int(sdf["total"].sum())
        comp = int(sdf["completed"].sum())

        avg_age = 0
        if not sdf.empty:
            ages = []
            for _, r in sdf.iterrows():
                if pd.notna(r["first_seen_stage"]):
                    ages.append((today - r["first_seen_stage"]).days)
            avg_age = round(sum(ages)/len(ages),1) if ages else 0

        eff = round((comp/wip)*100,1) if wip else 0

        congestion_score = avg_age + (wip - comp)

        if wip >= 5 and congestion_score > worst_congestion:
            worst_congestion = congestion_score
            bottleneck_stage = stage

        flow_data.append({
            "Stage": stage,
            "WIP": wip,
            "Completed": comp,
            "Efficiency %": eff
        })

    # ================= ENGINE =================
    results = []
    sla_results = []
    early = []

    grouped = progress_df.groupby("house")

    for house, g in grouped:

        dispatch_info = dispatch_map.get(house, {})
        total_products = dispatch_info.get("total_products", 0)
        dispatched_products = dispatch_info.get("dispatched_products", 0)
        dispatch_finish = dispatch_info.get("dispatch_finish", None)

        start_date = start_map.get(house, None)

        if pd.isna(start_date) or start_date is None:
            current_stage = "Not Started"
            progress_percent = 0
            predicted_finish = "Awaiting Start"
            actual_finish = "Not Finished"
            rem_stage_display = "Not Started"
            rem_total_days = total_duration
            delay_days = 0
            reason = "Awaiting Start"

        elif total_products > 0 and dispatched_products == total_products:
            current_stage = "Dispatch"
            progress_percent = 100
            actual_finish = dispatch_finish.date() if pd.notna(dispatch_finish) else today.date()
            predicted_finish = actual_finish
            rem_stage_display = "Completed"
            rem_total_days = 0

            planned_finish = start_date + timedelta(days=total_duration)
            delay_days = max(0, (dispatch_finish - planned_finish).days) if pd.notna(dispatch_finish) else 0

            if house in config_map:
                sla_gap = (dispatch_finish.date() - config_map[house]).days if pd.notna(dispatch_finish) else 0
                delay_days = max(delay_days, sla_gap)

            reason = "Completed"

        else:
            live = g[g["stage"].isin(production_stages)].copy()
            live["seq"] = live["stage"].map(lambda x: seq_map.get(x, 0))
            live["pending"] = live["total"] - live["completed"]

            incomplete = live[live["pending"] > 0]

            if incomplete.empty:
                current_stage_row = live.sort_values(["seq"], ascending=[False]).iloc[0]
            else:
                current_stage_row = incomplete.sort_values(["pending","seq"], ascending=[False, True]).iloc[0]

            current_stage = current_stage_row["stage"]
            current_seq = seq_map.get(current_stage, 1)
            stage_duration = day_map.get(current_stage, 1)

            stage_total = current_stage_row["total"]
            stage_completed = current_stage_row["completed"]
            stage_ratio = stage_completed / stage_total if stage_total else 0

            stage_start = current_stage_row["first_seen_stage"] if pd.notna(current_stage_row["first_seen_stage"]) else start_date
            stage_age = max(1, (today - stage_start).days)

            progress = ((current_seq - 1) / len(activity_df)) + (stage_ratio / len(activity_df))
            progress_percent = round(progress * 100, 1)

            rem_stage = max(0, int(stage_duration - stage_age))
            rem_stage = max(0, int(rem_stage * (1 - stage_ratio)))
            rem_stage_display = "Completed" if rem_stage <= 0 else f"{rem_stage} days"

            downstream_only = activity_df[activity_df["seq"] > current_seq]["days"].sum()
            rem_total_days = int(rem_stage + downstream_only)

            planned_finish = start_date + timedelta(days=total_duration)
            planned_progress_today = min(100, ((today - start_date).days / total_duration) * 100)

            historical_delay = max(0, int((planned_progress_today - progress_percent) / 4))
            predicted_finish_dt = planned_finish + timedelta(days=historical_delay + rem_stage)

            predicted_finish = predicted_finish_dt.date()
            actual_finish = "Not Finished"

            internal_delay = max(0, (today - planned_finish).days)
            forecast_delay = max(0, (predicted_finish_dt - planned_finish).days)
            delay_days = max(internal_delay, forecast_delay)

            if house in config_map:
                sla_delay = (predicted_finish_dt.date() - config_map[house]).days
                delay_days = max(delay_days, sla_delay)

            stagnant_days = 0
            if pd.notna(current_stage_row["last_movement"]):
                stagnant_days = (today - current_stage_row["last_movement"]).days

            if delay_days > 7:
                reason = "Critical Delay"
            elif house in config_map and predicted_finish_dt.date() > config_map[house]:
                reason = "SLA Miss Risk"
            elif stagnant_days >= stage_duration:
                reason = "No Daily Movement"
            elif bottleneck_stage == current_stage:
                reason = "Bottleneck Queue"
            elif stage_age > stage_duration:
                reason = "Stage Slowdown"
            else:
                reason = "On Track"

            if house in config_map:
                sla_results.append({
                    "House": house,
                    "Stage": current_stage,
                    "SLA": config_map[house],
                    "Predicted": predicted_finish,
                    "Delay": delay_days,
                    "Issue": reason
                })

            if reason in ["Critical Delay","SLA Miss Risk","No Daily Movement","Bottleneck Queue","Stage Slowdown"]:
                early.append({
                    "House": house,
                    "Stage": current_stage,
                    "Issue": reason,
                    "Predicted Finish": predicted_finish
                })

        results.append({
            "House": house,
            "Stage": current_stage,
            "Progress %": progress_percent,
            "Predicted Finish": predicted_finish,
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
    st.dataframe(pd.DataFrame(flow_data), use_container_width=True, height=250)

    if bottleneck_stage:
        st.error(f"🚨 Bottleneck Stage: {bottleneck_stage}")

    st.subheader("🚨 Early Warning")
    if early:
        st.dataframe(pd.DataFrame(early), use_container_width=True, height=250)
    else:
        st.success("No major risks")
