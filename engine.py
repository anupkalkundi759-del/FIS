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
    activity_df = pd.DataFrame(cur.fetchall(),
        columns=["stage", "seq", "days"])
    activity_df["days"] = activity_df["days"].astype(int)
    total_duration = activity_df["days"].sum()

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
        GROUP BY h.house_no, s.stage_name
    """)
    df = pd.DataFrame(cur.fetchall(),
        columns=["house","stage","start","end"])
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

    # ================= PRODUCT PROGRESS =================
    cur.execute("""
        SELECT h.house_no, s.stage_name,
               COUNT(*) FILTER (WHERE t.status='Completed'),
               COUNT(*)
        FROM products p
        JOIN houses h ON p.house_id = h.house_id
        JOIN tracking_log t ON t.product_instance_id = p.product_instance_id
        JOIN stages s ON t.stage_id = s.stage_id
        GROUP BY h.house_no, s.stage_name
    """)
    p_df = pd.DataFrame(cur.fetchall(),
        columns=["house","stage","completed","total"])

    total_map = p_df.groupby("house")["total"].max().to_dict()
    comp_map = {(r["house"], r["stage"]): r["completed"] for _, r in p_df.iterrows()}

    # ================= ENGINE =================
    results, sla_results, early, reschedule = [], [], [], []
    stage_delay_summary = {}

    for house in df["house"].unique():

        house_data = house_group.get_group(house)
        start_date = house_data["start"].min()

        current_pointer = start_date
        stage_delays = []
        critical_stage = None
        max_delay = 0

        current_stage_name = None
        remaining_stage_days = 0

        # ---------- LOOP ----------
        for _, row in activity_df.iterrows():
            stage = row["stage"]
            duration = row["days"]

            stage_data = house_data[house_data["stage"] == stage]

            if not stage_data.empty:
                actual_start = stage_data["start"].iloc[0]
                actual_finish = stage_data["end"].iloc[0]

                actual_duration = max(1, (actual_finish - actual_start).days)
                delay = actual_duration - duration

                if delay > 0:
                    stage_delays.append((stage, delay))
                    if delay > max_delay:
                        max_delay = delay
                        critical_stage = stage

                if actual_finish >= today:
                    current_stage_name = stage
                    remaining_stage_days = max(0, duration - actual_duration)

                current_pointer = actual_start + timedelta(days=actual_duration)

            else:
                if current_stage_name is None:
                    current_stage_name = stage
                    remaining_stage_days = duration

                current_pointer += timedelta(days=duration)

        # ---------- RESCHEDULE ----------
        total_delay = sum([d for _, d in stage_delays])
        current_pointer += timedelta(days=total_delay)
        predicted_finish = current_pointer

        total_days = (predicted_finish - start_date).days
        remaining_total = max(0, (predicted_finish - today).days)

        # ---------- CURRENT ----------
        h_latest = latest_df[latest_df["house"] == house]
        if not h_latest.empty:
            r = h_latest.sort_values("time").iloc[-1]
            current_stage = f"{r['stage']} ({r['status']})"
        else:
            current_stage = "Not Started"

        # ---------- PROGRESS ----------
        earned = 0
        total_products = total_map.get(house, 0)

        for _, row in activity_df.iterrows():
            stg = row["stage"]
            dur = row["days"]
            comp = comp_map.get((house, stg), 0)

            if total_products > 0:
                earned += (comp / total_products) * dur

        progress = (earned / total_duration) * 100 if total_duration else 0

        # ---------- DELAY REASON ----------
        delay_reason = ", ".join([f"{s}+{d}" for s,d in stage_delays]) if stage_delays else "On Track"

        # ---------- CRITICAL TYPE ----------
        if remaining_total <= 3:
            critical_type = "🔴 Critical"
        elif remaining_total <= 7:
            critical_type = "🟠 Risk"
        else:
            critical_type = "🟢 Normal"

        # ---------- MAIN ----------
        results.append({
            "House": house,
            "Progress %": round(progress,1),
            "Stage": current_stage,
            "Remaining (Stage)": f"{remaining_stage_days} days",
            "Remaining (Total)": f"{remaining_total} days",
            "Delay Reason": delay_reason,
            "Critical Type": critical_type
        })

        # ---------- SLA ----------
        sla = config_map.get(house)
        if sla:
            expected = pd.to_datetime(sla)
            delay_days = (predicted_finish - expected).days

            status = "On Track" if delay_days <= 0 else "Delay"
            impact = f"{abs(delay_days)} days"

            sla_results.append({
                "House": house,
                "SLA": expected.date(),
                "Predicted": predicted_finish.date(),
                "Status": status,
                "Impact": impact,
                "Critical Stage": critical_stage if critical_stage else "None"
            })

            if delay_days > 0:
                early.append({
                    "House": house,
                    "Issue": "Will miss SLA",
                    "Critical Stage": critical_stage,
                    "Delay": delay_days
                })

        # ---------- RESCHEDULE ----------
        if total_days > 22:
            reschedule.append({
                "House": house,
                "Predicted Finish": predicted_finish.date(),
                "Total Days": total_days,
                "Delay Reason": delay_reason,
                "Critical Stage": critical_stage if critical_stage else "None"
            })

        # ---------- BOTTLENECK ----------
        for s, d in stage_delays:
            stage_delay_summary.setdefault(s, {"delay":0,"count":0})
            stage_delay_summary[s]["delay"] += d
            stage_delay_summary[s]["count"] += 1

    # ================= OUTPUT =================
    st.subheader("🏠 House Intelligence")
    st.dataframe(pd.DataFrame(results))

    st.subheader("🚨 Priority Table (SLA Only)")
    st.dataframe(pd.DataFrame(sla_results)) if sla_results else st.info("No SLA")

    st.subheader("🚨 Early Warning")
    st.dataframe(pd.DataFrame(early)) if early else st.success("No risk")

    st.subheader("🧠 Bottleneck")
    insight = [{"Stage":k,"Delay":v["delay"],"Houses":v["count"]}
               for k,v in stage_delay_summary.items()]
    st.dataframe(pd.DataFrame(insight))

    st.subheader("🔥 Dynamic Rescheduling (>22 days)")
    st.dataframe(pd.DataFrame(reschedule)) if reschedule else st.success("No delays")
