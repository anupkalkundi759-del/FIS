def run_engine(conn, cur):
    import streamlit as st
    import pandas as pd
    from datetime import datetime, timedelta

    st.title("⚙️ Advanced Scheduling Intelligence Engine")

    today = datetime.now()

    # ================= CONFIG =================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS house_config (
            house_no TEXT PRIMARY KEY,
            urgency INT DEFAULT 0,
            sla_date DATE
        )
    """)
    conn.commit()

    # ================= LOAD MASTER =================
    cur.execute("""
        SELECT activity_name, sequence_order, duration_days
        FROM activity_master
        ORDER BY sequence_order
    """)
    act = cur.fetchall()

    activity_df = pd.DataFrame(act, columns=["stage", "seq", "days"])
    total_duration = activity_df["days"].sum()

    # ================= LOAD CONFIG =================
    cur.execute("SELECT house_no, urgency, sla_date FROM house_config")
    config_map = {
        r[0]: {"urgency": r[1], "sla": r[2]}
        for r in cur.fetchall()
    }

    # ================= LOAD TRACKING =================
    cur.execute("""
        SELECT h.house_no, s.stage_name, t.timestamp, s.sequence
        FROM products p
        JOIN houses h ON p.house_id = h.house_id
        JOIN tracking_log t ON t.product_instance_id = p.product_instance_id
        JOIN stages s ON t.stage_id = s.stage_id
    """)

    df = pd.DataFrame(cur.fetchall(), columns=["house", "stage", "time", "seq"])
    df["time"] = pd.to_datetime(df["time"])

    if df.empty:
        st.warning("No data")
        return

    results = []
    early_warnings = []
    stage_delay_map = {}

    # ================= CORE ENGINE =================
    for house in df["house"].unique():

        house_df = df[df["house"] == house].sort_values("seq")

        # -------- START DATE --------
        meas = house_df[house_df["stage"] == "Measurement"]
        start_date = meas["time"].min() if not meas.empty else house_df["time"].min()

        # -------- CRITICAL PATH --------
        # since sequential, CP = full chain
        cp_duration = total_duration

        # -------- PROGRESS --------
        progress = 0
        current_stage = house_df.iloc[-1]["stage"]
        current_time = house_df.iloc[-1]["time"]

        completed_days = 0
        delays = []

        for i in range(len(house_df) - 1):
            s1 = house_df.iloc[i]
            s2 = house_df.iloc[i + 1]

            actual = (s2["time"] - s1["time"]).days
            planned = int(activity_df[activity_df["stage"] == s1["stage"]]["days"].values[0])

            delay = actual - planned
            delays.append(delay)

            stage_delay_map[s1["stage"]] = stage_delay_map.get(s1["stage"], 0) + max(0, delay)

            completed_days += planned

        progress = (completed_days / total_duration) * 100 if total_duration else 0

        # -------- PRODUCTIVITY --------
        if delays:
            productivity = sum([(d+1)/1 for d in delays[-2:]]) / len(delays[-2:])
            productivity = max(0.7, min(productivity, 1.5))
        else:
            productivity = 1

        # -------- PREDICTION --------
        remaining = total_duration * (1 - progress / 100)
        predicted_finish = today + timedelta(days=int(remaining * productivity))

        # -------- BASELINE PLAN --------
        expected_finish = start_date + timedelta(days=int(total_duration))

        # -------- SLA --------
        config = config_map.get(house, {})
        sla = pd.to_datetime(config.get("sla")) if config.get("sla") else None
        urgency = config.get("urgency", 0)

        # -------- DELAY --------
        if sla is not None:
            delay_days = (predicted_finish - sla).days
        else:
            delay_days = (predicted_finish - expected_finish).days

        # -------- REAL-TIME DELAY PROPAGATION --------
        propagated_delay = sum([d for d in delays if d > 0])
        predicted_finish += timedelta(days=int(propagated_delay * 0.3))

        # -------- AI REASON DETECTION --------
        if delays:
            max_delay_stage = house_df.iloc[delays.index(max(delays))]["stage"]
        else:
            max_delay_stage = "None"

        if delay_days > 5:
            reason = f"Major delay in {max_delay_stage}"
        elif progress < 30:
            reason = "Slow execution"
        elif urgency >= 2:
            reason = "High priority (manual)"
        else:
            reason = "On track"

        # -------- DYNAMIC RESEQUENCING --------
        # simple logic: if stage delay too high → flag resequence
        resequence_flag = False
        if delays and max(delays) > 3:
            resequence_flag = True

        # -------- EARLY WARNING --------
        if sla is not None:
            days_left = (sla - today).days
            needed_rate = (100 - progress) / max(days_left, 1)

            if needed_rate > 3:
                early_warnings.append({
                    "House": house,
                    "Warning": "Will miss SLA",
                    "Reason": reason
                })

        # -------- PRIORITY SCORE --------
        priority_score = (
            max(0, delay_days) * 3 +
            (100 - progress) +
            urgency * 20
        )

        # -------- STORE --------
        results.append({
            "House": house,
            "Stage": current_stage,
            "Progress %": round(progress, 1),
            "Predicted Finish": predicted_finish.date(),
            "Delay (days)": delay_days,
            "SLA": sla.date() if sla is not None else None,
            "Urgency": urgency,
            "Critical Path (days)": cp_duration,
            "Resequence Needed": resequence_flag,
            "Reason": reason,
            "Priority Score": priority_score
        })

    result_df = pd.DataFrame(results)

    # ================= PRIORITY TABLE =================
    st.subheader("🚨 Priority Table (SLA Only)")
    priority_df = result_df[result_df["SLA"].notna()]
    st.dataframe(priority_df.sort_values("Priority Score", ascending=False))

    # ================= HOUSE INTELLIGENCE =================
    st.subheader("🏠 House Intelligence")
    house_df = result_df[[
        "House", "Stage", "Progress %",
        "Predicted Finish", "Delay (days)", "Reason"
    ]]
    st.dataframe(house_df)

    # ================= EARLY WARNING =================
    st.subheader("⚠️ Early Warning System")
    if early_warnings:
        st.dataframe(pd.DataFrame(early_warnings))
    else:
        st.success("No risks detected")

    # ================= BOTTLENECK =================
    st.subheader("⛔ Bottleneck Detection")

    if stage_delay_map:
        bottleneck = max(stage_delay_map, key=stage_delay_map.get)
        st.error(f"Major Bottleneck Stage: {bottleneck}")
    else:
        st.success("No bottlenecks")
