def run_engine(conn, cur):
    import streamlit as st
    import pandas as pd
    from datetime import datetime, timedelta

    st.title("⚙️ Scheduling Intelligence Engine")
    today = datetime.now()

    # ================= TABLES =================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS house_config (
            house_no TEXT PRIMARY KEY,
            sla_date DATE
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS delay_trend (
            date DATE,
            total_delay INT
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

    activity_df = pd.DataFrame(act, columns=["stage", "seq", "days"])
    activity_df["days"] = activity_df["days"].astype(int)
    total_duration = int(activity_df["days"].sum())

    # ================= SLA =================
    cur.execute("SELECT house_no, sla_date FROM house_config")
    config_map = {r[0]: r[1] for r in cur.fetchall()}

    # ================= TRACKING =================
    cur.execute("""
        SELECT h.house_no, s.stage_name,
               MIN(t.timestamp), MAX(t.timestamp)
        FROM products p
        JOIN houses h ON p.house_id = h.house_id
        JOIN tracking_log t ON t.product_instance_id = p.product_instance_id
        JOIN stages s ON t.stage_id = s.stage_id
        WHERE t.status = 'Completed'
        GROUP BY h.house_no, s.stage_name
    """)

    data = cur.fetchall()
    if not data:
        st.warning("No tracking data available.")
        return

    df = pd.DataFrame(data, columns=["house","stage","start","end"])
    df["start"] = pd.to_datetime(df["start"])
    df["end"] = pd.to_datetime(df["end"])
    house_group = df.groupby("house")

    # ================= LATEST =================
    cur.execute("""
        SELECT h.house_no, s.stage_name, t.status, t.timestamp
        FROM products p
        JOIN houses h ON p.house_id = h.house_id
        JOIN tracking_log t ON t.product_instance_id = p.product_instance_id
        JOIN stages s ON t.stage_id = s.stage_id
    """)

    latest_df = pd.DataFrame(cur.fetchall(),
        columns=["house","stage","status","time"])
    latest_df["time"] = pd.to_datetime(latest_df["time"])

    # ================= PREFETCH =================
    cur.execute("""
        SELECT h.house_no,
               COUNT(p.product_instance_id),
               s.stage_name,
               COUNT(DISTINCT CASE WHEN t.status='Completed' THEN t.product_instance_id END)
        FROM houses h
        LEFT JOIN products p ON p.house_id = h.house_id
        LEFT JOIN tracking_log t ON t.product_instance_id = p.product_instance_id
        LEFT JOIN stages s ON t.stage_id = s.stage_id
        GROUP BY h.house_no, s.stage_name
    """)

    progress_df = pd.DataFrame(cur.fetchall(),
        columns=["house","total","stage","completed"])

    total_map = progress_df.groupby("house")["total"].max().to_dict()
    stage_map = {(r["house"], r["stage"]): r["completed"] for _, r in progress_df.iterrows()}

    # ================= ENGINE =================
    results, sla_results, stage_delay_summary = [], [], {}

    for house in df["house"].unique():

        house_data = house_group.get_group(house)
        start_date = house_data["start"].min()

        current_pointer = start_date
        stage_delays = []

        # 🔥 NEW
        critical_stage = None
        max_delay = 0

        total_products = total_map.get(house, 0)
        earned_duration = 0

        for _, row in activity_df.iterrows():
            stage = row["stage"]
            duration = row["days"]

            stage_data = house_data[house_data["stage"] == stage]
            planned_finish = current_pointer + timedelta(days=duration)

            if not stage_data.empty:
                actual_start = stage_data["start"].iloc[0]
                actual_finish = stage_data["end"].iloc[0]

                actual_duration = (actual_finish - actual_start).days

                # 🔥 FIX: bulk update
                if actual_duration == 0:
                    actual_duration = duration

                delay = actual_duration - duration

                if delay > 0:
                    stage_delays.append((stage, delay))

                    if delay > max_delay:
                        max_delay = delay
                        critical_stage = stage

                current_pointer = actual_start + timedelta(days=actual_duration)
            else:
                current_pointer = planned_finish

            completed = stage_map.get((house, stage), 0)
            earned_duration += (completed / total_products) * duration if total_products else 0

        # ================= PROGRESS =================
        progress = (earned_duration / total_duration) * 100 if total_duration else 0

        # 🔥 TIME VARIATION
        days_elapsed = (today - start_date).days
        time_progress = (days_elapsed / total_duration) * 100 if total_duration else 0
        progress = max(progress, time_progress)

        # 🔥 REMAINING
        if days_elapsed <= 1:
            remaining_total_days = total_duration
        else:
            remaining_total_days = int(total_duration * (1 - progress/100))

        remaining_total_days = max(0, remaining_total_days)

        # 🔥 DYNAMIC RESCHEDULING
        total_delay_impact = sum([d for _, d in stage_delays])
        current_pointer = current_pointer + timedelta(days=total_delay_impact)

        predicted_finish = current_pointer
        if predicted_finish < today:
            predicted_finish = today + timedelta(days=remaining_total_days)

        # ================= CURRENT =================
        h_latest = latest_df[latest_df["house"] == house]

        if not h_latest.empty:
            row = h_latest.sort_values("time").iloc[-1]
            current_stage = f"{row['stage']} ({row['status']})"
        else:
            current_stage = "Not Started"

        # 🔥 RISK
        if remaining_total_days <= 3 and progress < 90:
            risk = "🔴 Critical"
        elif remaining_total_days <= 5 and progress < 80:
            risk = "🟠 High Risk"
        elif remaining_total_days <= 7:
            risk = "🟡 Watch"
        else:
            risk = "🟢 Normal"

        # ================= OUTPUT =================
        results.append({
            "House": house,
            "Stage": current_stage,
            "Progress %": round(progress,1),
            "Predicted Finish": predicted_finish.date(),
            "Remaining (Total)": f"{remaining_total_days} days",
            "Critical Stage": f"🔴 {critical_stage}" if critical_stage else "None",
            "Delay Impact": total_delay_impact,
            "Risk Level": risk
        })

        # ================= DELAY SUMMARY =================
        for s, d in stage_delays:
            stage_delay_summary.setdefault(s, {"delay":0,"count":0})
            stage_delay_summary[s]["delay"] += d
            stage_delay_summary[s]["count"] += 1

    # ================= OUTPUT =================
    st.subheader("🏠 House Intelligence")
    st.dataframe(pd.DataFrame(results))

    # ================= EARLY WARNING =================
    early = []
    for r in results:
        if "Critical" in r["Risk Level"] or "High Risk" in r["Risk Level"]:
            early.append({
                "House": r["House"],
                "Issue": r["Risk Level"],
                "Critical Stage": r["Critical Stage"],
                "Delay Impact": r["Delay Impact"]
            })

    st.subheader("🚨 Early Warning")
    st.dataframe(pd.DataFrame(early)) if early else st.success("No early risks")

    # ================= BOTTLENECK =================
    insight = [{"Stage":k,"Total Delay":v["delay"],"Affected Houses":v["count"]}
               for k,v in stage_delay_summary.items() if v["delay"]>0]

    st.subheader("🧠 Stage Delay Insight")
    st.dataframe(pd.DataFrame(insight)) if insight else st.info("No delays detected")
