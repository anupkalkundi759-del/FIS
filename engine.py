def run_engine(conn, cur):
    import streamlit as st
    import pandas as pd
    from datetime import datetime, timedelta

    st.title("⚙️ Scheduling Intelligence Engine")

    today = datetime.now()

    # ================= CONFIG TABLE =================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS house_config (
            house_no TEXT PRIMARY KEY,
            sla_date DATE
        )
    """)
    conn.commit()

    # ================= LOAD ACTIVITY =================
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

    # ================= LOAD CONFIG =================
    cur.execute("SELECT house_no, sla_date FROM house_config")
    config_map = {
        r[0]: {"sla": r[1]}
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

    if df.empty:
        st.warning("No tracking data")
        return

    df["time"] = pd.to_datetime(df["time"])

    results = []
    early_warnings = []
    stage_delay = {}

    # ================= CORE LOGIC =================
    for house in df["house"].unique():

        hdf = df[df["house"] == house].sort_values("seq")

        # ---------- START DATE ----------
        meas = hdf[hdf["stage"] == "Measurement"]
        start_date = meas["time"].min() if not meas.empty else hdf["time"].min()

        # ---------- CURRENT STAGE ----------
        current_stage = hdf.iloc[-1]["stage"]

        # ---------- PROGRESS ----------
        completed_days = 0
        delays = []

        for i in range(len(hdf) - 1):
            s1 = hdf.iloc[i]
            s2 = hdf.iloc[i + 1]

            actual = (s2["time"] - s1["time"]).days

            planned_series = activity_df[activity_df["stage"] == s1["stage"]]["days"]
            planned = int(planned_series.values[0]) if not planned_series.empty else 1

            delay = actual - planned
            delays.append(delay)

            if delay > 0:
                stage_delay[s1["stage"]] = stage_delay.get(s1["stage"], 0) + delay

            completed_days += planned

        progress = (completed_days / total_duration) * 100 if total_duration else 0

        # ---------- PRODUCTIVITY ----------
        if delays:
            avg_delay = sum(delays[-2:]) / max(len(delays[-2:]), 1)
            productivity = max(0.8, min(1.5, 1 + avg_delay / 10))
        else:
            productivity = 1

        # ---------- PREDICTED FINISH ----------
        predicted_finish = start_date + timedelta(days=int(total_duration * productivity))

        # ---------- BASELINE ----------
        expected_finish = start_date + timedelta(days=int(total_duration))

        # ---------- SLA ----------
        cfg = config_map.get(house, {})
        sla = pd.to_datetime(cfg.get("sla")) if cfg.get("sla") else None

        # ---------- DELAY ----------
        if sla is not None:
            delay_days = (predicted_finish - sla).days
        else:
            delay_days = (predicted_finish - expected_finish).days

        if delay_days > 0:
            delay_text = f"Delayed {delay_days} days"
        elif delay_days < 0:
            delay_text = f"Ahead {abs(delay_days)} days"
        else:
            delay_text = "On time"

        # ---------- REASON ----------
        if delay_days > 5:
            reason = "Major delay"
        elif progress < 30:
            reason = "Slow progress"
        else:
            reason = "On track"

        # ---------- EARLY WARNING ----------
        if sla is not None:
            days_left = (sla - today).days
            if progress < 80 and days_left < 5:
                early_warnings.append({
                    "House": house,
                    "Warning": "Likely to miss SLA",
                    "Reason": reason
                })

        # ---------- PRIORITY SCORE (INTERNAL ONLY) ----------
        priority_score = (
            max(0, delay_days) * 3 +
            (100 - progress)
        )

        priority_label = (
            "🔴 Critical" if priority_score > 80 else
            "🟠 High" if priority_score > 50 else
            "🟡 Medium" if priority_score > 20 else
            "🟢 Low"
        )

        results.append({
            "House": house,
            "Stage": current_stage,
            "Progress %": round(progress, 1),
            "Delay": delay_text,
            "Predicted Finish": predicted_finish.date(),
            "SLA": sla.date() if sla is not None else None,
            "Priority": priority_label,
            "Reason": reason,
            "Priority Score": priority_score
        })

    result_df = pd.DataFrame(results)

    # ================= PRIORITY TABLE =================
    st.subheader("🚨 Priority Table (SLA Only)")

    priority_df = result_df[result_df["SLA"].notna()].copy()

    if not priority_df.empty:
        priority_df = priority_df.sort_values("Priority Score", ascending=False)
        priority_df = priority_df.drop(columns=["Priority Score"])

        priority_df = priority_df[[
            "House", "Stage", "Delay", "SLA",
            "Priority", "Reason"
        ]]

        st.dataframe(priority_df)
    else:
        st.info("No SLA houses configured")

    # ================= HOUSE INTELLIGENCE =================
    st.subheader("🏠 House Intelligence")

    house_df = result_df[[
        "House", "Stage", "Progress %",
        "Delay", "Predicted Finish", "Reason"
    ]]

    st.dataframe(house_df)

    # ================= EARLY WARNING =================
    st.subheader("⚠️ Early Warning")

    if early_warnings:
        st.dataframe(pd.DataFrame(early_warnings))
    else:
        st.success("No immediate risks")

    # ================= BOTTLENECK =================
    st.subheader("⛔ Bottleneck")

    if stage_delay:
        bottleneck_stage = max(stage_delay, key=stage_delay.get)
        st.error(f"Most delayed stage: {bottleneck_stage}")
    else:
        st.success("No bottlenecks")
