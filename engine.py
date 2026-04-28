def run_engine(conn, cur):
    import streamlit as st
    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    st.title("⚙️ Scheduling Intelligence Engine")

    tz = ZoneInfo("Asia/Kolkata")
    today = datetime.now(tz)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS house_config (
            house_no TEXT PRIMARY KEY,
            sla_date DATE
        )
    """)
    conn.commit()

    cur.execute("""
        SELECT activity_name, sequence_order, duration_days
        FROM activity_master
        ORDER BY sequence_order
    """)
    act = cur.fetchall()
    activity_df = pd.DataFrame(act, columns=["stage", "seq", "days"])

    if activity_df.empty:
        st.error("No activity master found")
        return

    total_duration = int(activity_df["days"].sum())
    seq_map = dict(zip(activity_df["stage"], activity_df["seq"]))
    dur_map = dict(zip(activity_df["stage"], activity_df["days"]))
    stage_list = activity_df["stage"].tolist()

    f1, f2, f3, f4 = st.columns(4)

    with f1:
        cur.execute("SELECT project_id, project_name FROM projects ORDER BY project_name")
        projects = cur.fetchall()
        project_dict = {"All": None, **{p[1]: p[0] for p in projects}}
        selected_project = st.selectbox("Project", list(project_dict.keys()))
        project_id = project_dict[selected_project]

    with f2:
        if project_id:
            cur.execute("SELECT unit_id, unit_name FROM units WHERE project_id=%s ORDER BY unit_name", (project_id,))
        else:
            cur.execute("SELECT unit_id, unit_name FROM units ORDER BY unit_name")
        units = cur.fetchall()
        unit_dict = {"All": None, **{u[1]: u[0] for u in units}}
        selected_unit = st.selectbox("Unit", list(unit_dict.keys()))
        unit_id = unit_dict[selected_unit]

    with f3:
        if unit_id:
            cur.execute("SELECT house_no FROM houses WHERE unit_id=%s ORDER BY house_no", (unit_id,))
        elif project_id:
            cur.execute("""
                SELECT h.house_no
                FROM houses h
                JOIN units u ON h.unit_id = u.unit_id
                WHERE u.project_id=%s
                ORDER BY h.house_no
            """, (project_id,))
        else:
            cur.execute("SELECT house_no FROM houses ORDER BY house_no")
        house_opts = [h[0] for h in cur.fetchall()]
        selected_house = st.selectbox("House Filter", ["All"] + house_opts)

    with f4:
        st.markdown("### SLA Assignment")
        sla_house = st.selectbox("SLA House", house_opts, key="sla_house")
        sla_date = st.date_input("SLA Date")

        if st.button("Save SLA"):
            cur.execute("""
                INSERT INTO house_config (house_no, sla_date)
                VALUES (%s, %s)
                ON CONFLICT (house_no)
                DO UPDATE SET sla_date = EXCLUDED.sla_date
            """, (sla_house, sla_date))
            conn.commit()
            st.success("SLA Saved")

    cur.execute("SELECT house_no, sla_date FROM house_config")
    sla_map = {r[0]: r[1] for r in cur.fetchall()}

    where_sql = ""
    params = []

    if selected_house != "All":
        where_sql += " AND h.house_no = %s"
        params.append(selected_house)
    elif unit_id:
        where_sql += " AND h.unit_id = %s"
        params.append(unit_id)
    elif project_id:
        where_sql += " AND u.project_id = %s"
        params.append(project_id)

    query = f"""
        SELECT
            p.product_instance_id,
            h.house_no,
            s.stage_name,
            s.sequence,
            t.status,
            t.timestamp
        FROM tracking_log t
        JOIN products p ON t.product_instance_id = p.product_instance_id
        JOIN houses h ON p.house_id = h.house_id
        JOIN units u ON h.unit_id = u.unit_id
        JOIN stages s ON t.stage_id = s.stage_id
        WHERE 1=1 {where_sql}
        ORDER BY t.timestamp
    """

    cur.execute(query, tuple(params))
    raw = cur.fetchall()

    if not raw:
        st.warning("No tracking history found")
        return

    hist = pd.DataFrame(raw, columns=["pid", "house", "stage", "seq", "status", "timestamp"])
    hist["timestamp"] = pd.to_datetime(hist["timestamp"], utc=True).dt.tz_convert(tz)

    latest = hist.sort_values("timestamp").groupby("pid").tail(1).copy()
    house_total_products = latest.groupby("house")["pid"].count().to_dict()
    house_last_move = hist.groupby("house")["timestamp"].max().to_dict()

    seven_days = today - timedelta(days=7)
    recent_completed = hist[(hist["status"] == "Completed") & (hist["timestamp"] >= seven_days)]
    throughput_map = recent_completed.groupby("house")["pid"].count().to_dict()

    flow_data = []
    bottleneck_stage = None
    bottleneck_score = -999

    for stg in stage_list:
        stage_hist = hist[hist["stage"] == stg]
        current_wip = len(latest[(latest["stage"] == stg) & (latest["status"] != "Completed")])
        entered_7d = len(stage_hist[stage_hist["timestamp"] >= seven_days])
        exited_7d = len(stage_hist[(stage_hist["status"] == "Completed") & (stage_hist["timestamp"] >= seven_days)])
        live_wait = latest[(latest["stage"] == stg) & (latest["status"] != "Completed")]

        if not live_wait.empty:
            avg_wait = round((today - live_wait["timestamp"]).dt.days.mean(), 1)
        else:
            avg_wait = 0

        efficiency = round((exited_7d / entered_7d) * 100, 1) if entered_7d > 0 else 100
        queue_pressure = current_wip + entered_7d - exited_7d
        score = queue_pressure + avg_wait

        if score > bottleneck_score:
            bottleneck_score = score
            bottleneck_stage = stg

        flow_data.append({
            "Stage": stg,
            "WIP": current_wip,
            "Entered 7d": entered_7d,
            "Exited 7d": exited_7d,
            "Avg Wait Days": avg_wait,
            "Efficiency %": efficiency,
            "Queue Pressure": queue_pressure
        })

    flow_df = pd.DataFrame(flow_data)

    results = []
    priority_rows = []
    warning_rows = []

    for house in sorted(latest["house"].unique()):
        house_latest = latest[latest["house"] == house].copy()
        total_products = house_total_products.get(house, 0)

        stage_pending = {}
        for stg in stage_list:
            pending = len(house_latest[(house_latest["stage"] == stg) & (house_latest["status"] != "Completed")])
            stage_pending[stg] = pending

        non_zero = {k: v for k, v in stage_pending.items() if v > 0}

        if non_zero:
            dominant_stage = max(non_zero.items(), key=lambda x: x[1])[0]
        else:
            dominant_stage = "Dispatch"

        earned = 0
        for _, row in house_latest.iterrows():
            stg = row["stage"]
            status = row["status"]

            if stg in seq_map:
                prev_stages = activity_df[activity_df["seq"] < seq_map[stg]]["days"].sum()
                earned += prev_stages
                if status == "Completed":
                    earned += dur_map[stg]
                else:
                    earned += dur_map[stg] * 0.5

        total_possible = total_products * total_duration if total_products > 0 else 1
        progress_percent = round((earned / total_possible) * 100, 1)

        idle_days = (today - house_last_move[house]).days

        throughput = throughput_map.get(house, 0) / 7
        throughput = round(max(throughput, 0.3), 2)

        remaining_work = total_possible - earned
        remaining_days = int(max(1, remaining_work / max(throughput, 0.3) / 3))

        predicted_finish = (today + timedelta(days=remaining_days)).date()

        actual_finish = "Not Finished"
        dispatch_done = len(house_latest[(house_latest["stage"] == "Dispatch") & (house_latest["status"] == "Completed")])

        if dispatch_done == total_products and total_products > 0:
            actual_finish = house_last_move[house].date()

        delay_days = 0
        expected = None
        if house in sla_map:
            expected = pd.to_datetime(sla_map[house]).date()
            delay_days = (predicted_finish - expected).days

        pending_stage_qty = stage_pending.get(dominant_stage, 0)
        stage_remaining_days = max(1, int((pending_stage_qty / max(total_products,1)) * dur_map.get(dominant_stage,1)))

        reasons = []

        if delay_days > 3:
            reasons.append("Critical SLA Risk")
        elif delay_days > 0:
            reasons.append("SLA Miss Risk")

        if idle_days >= 3:
            reasons.append("No Movement")

        if dominant_stage == bottleneck_stage:
            reasons.append("Bottleneck Hold")

        if throughput <= 0.5:
            reasons.append("Slow Throughput")

        if not reasons:
            reasons.append("On Track")

        delay_reason = ", ".join(reasons)

        risk_score = max(delay_days, 0) + idle_days + (5 if dominant_stage == bottleneck_stage else 0) + (3 if throughput <= 0.5 else 0)

        results.append({
            "House": house,
            "Stage": dominant_stage,
            "Progress %": progress_percent,
            "Products": f"{dispatch_done}/{total_products}",
            "Avg Daily Throughput": throughput,
            "Predicted Finish": predicted_finish,
            "Actual Finish": actual_finish,
            "Remaining (Stage)": f"{stage_remaining_days} days",
            "Remaining (Total)": f"{remaining_days} days",
            "Idle Days": idle_days,
            "Delay (Days)": delay_days,
            "Delay Reason": delay_reason,
            "Risk Score": risk_score
        })

        if house in sla_map:
            priority_rows.append({
                "House": house,
                "Stage": dominant_stage,
                "SLA": expected,
                "Predicted": predicted_finish,
                "Delay": delay_days,
                "Idle": idle_days,
                "Risk": risk_score,
                "Issue": delay_reason
            })

        if (delay_days > 0 or idle_days >= 3 or dominant_stage == bottleneck_stage or throughput <= 0.5):
            warning_rows.append({
                "House": house,
                "Stage": dominant_stage,
                "Idle Days": idle_days,
                "Predicted Finish": predicted_finish,
                "Issue": delay_reason
            })

    result_df = pd.DataFrame(results)

    total_houses = len(result_df)
    critical = len(result_df[result_df["Risk Score"] >= 8])
    ontrack = len(result_df[result_df["Delay Reason"] == "On Track"])
    avg_tp = round(result_df["Avg Daily Throughput"].mean(), 2) if not result_df.empty else 0

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Total Houses", total_houses)
    k2.metric("Critical Houses", critical)
    k3.metric("On Track", ontrack)
    k4.metric("Avg Throughput", avg_tp)
    k5.metric("Current Bottleneck", bottleneck_stage)

    st.subheader("🏠 House Intelligence")
    st.dataframe(result_df.sort_values("Risk Score", ascending=False), use_container_width=True, height=420)

    st.subheader("🚨 Priority Table (SLA Only)")
    if priority_rows:
        pri_df = pd.DataFrame(priority_rows).sort_values(["Risk", "Delay"], ascending=False)
        st.dataframe(pri_df, use_container_width=True, height=260)
    else:
        st.info("No SLA monitored houses")

    st.subheader("🏭 Flow Intelligence")
    st.dataframe(flow_df, use_container_width=True, height=300)

    if bottleneck_stage:
        st.error(f"🚨 Current Bottleneck Stage: {bottleneck_stage}")

    st.subheader("🚨 Early Warning")
    if warning_rows:
        warn_df = pd.DataFrame(warning_rows).sort_values("Idle Days", ascending=False)
        st.dataframe(warn_df, use_container_width=True, height=260)
    else:
        st.success("No major operational risks detected")
