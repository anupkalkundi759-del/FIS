def run_engine(conn, cur):
    import streamlit as st
    import pandas as pd
    from datetime import datetime, timedelta

    st.title("⚙️ Scheduling Intelligence Engine")

    TARGET_DAYS = 45
    today = datetime.now()

    # ================= LOAD ACTIVITIES =================
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

    # ================= LOAD TRACKING =================
    cur.execute("""
        SELECT 
            h.house_no,
            s.stage_name,
            t.timestamp,
            s.sequence
        FROM products p
        JOIN houses h ON p.house_id = h.house_id
        JOIN tracking_log t ON t.product_instance_id = p.id
        JOIN stages s ON t.stage_id = s.stage_id
        ORDER BY h.house_no, s.sequence
    """)

    data = cur.fetchall()

    if not data:
        st.warning("No tracking data")
        return

    df = pd.DataFrame(data, columns=["house", "stage", "time", "seq"])
    df["time"] = pd.to_datetime(df["time"])

    results = []
    stage_analysis = []

    # ================= CORE ENGINE =================
    for house in df["house"].unique():

        house_df = df[df["house"] == house].sort_values("seq")

        if house_df.empty:
            continue

        start_date = house_df["time"].min()
        current_row = house_df.iloc[-1]

        current_stage = current_row["stage"]
        current_seq = current_row["seq"]

        # ===== PROGRESS =====
        completed_days = activity_df[activity_df["seq"] <= current_seq]["days"].sum()
        remaining_days = activity_df[activity_df["seq"] > current_seq]["days"].sum()

        progress = (completed_days / total_duration) * 100 if total_duration else 0

        # ===== PLAN VS ACTUAL =====
        actual_elapsed = max(1, (today - start_date).days)  # avoid zero
        planned_progress = (actual_elapsed / TARGET_DAYS) * 100

        # ===== FORECAST (FIXED) =====
        if completed_days > 0:
            performance = actual_elapsed / completed_days
            performance = max(1, performance)  # 🔥 prevent unrealistic fast prediction
        else:
            performance = 1

        predicted_finish = today + timedelta(days=int(remaining_days * performance))
        expected_finish = start_date + timedelta(days=TARGET_DAYS)

        delay = (predicted_finish - expected_finish).days

        # ===== ALERT =====
        if delay > 10:
            alert = "🔴 Critical"
        elif delay > 5:
            alert = "🟠 Warning"
        else:
            alert = "🟢 On Track"

        # ================= STAGE-WISE ANALYSIS (FIXED) =================
        house_df = house_df.reset_index(drop=True)

        for i in range(len(house_df) - 1):
            stage_name = house_df.loc[i, "stage"]
            start_time = house_df.loc[i, "time"]
            next_time = house_df.loc[i + 1, "time"]

            actual_duration = (next_time - start_time).days

            # 🔥 skip invalid / zero durations
            if actual_duration <= 0:
                continue

            planned_row = activity_df[activity_df["stage"] == stage_name]
            planned_duration = planned_row["days"].values[0] if not planned_row.empty else 0

            delay_stage = actual_duration - planned_duration

            stage_analysis.append({
                "House": house,
                "Stage": stage_name,
                "Planned Days": planned_duration,
                "Actual Days": actual_duration,
                "Delay": delay_stage
            })

        # ===== PRIORITY =====
        priority_score = delay + remaining_days

        results.append({
            "House": house,
            "Stage": current_stage,
            "Progress %": round(progress, 1),
            "Planned %": round(planned_progress, 1),
            "Delay (days)": delay,
            "Predicted Finish": predicted_finish.date(),
            "Alert": alert,
            "Priority Score": priority_score
        })

    result_df = pd.DataFrame(results)
    stage_df = pd.DataFrame(stage_analysis)

    # ================= WOOD (MANUAL INPUT SYSTEM) =================
    cur.execute("""
        SELECT total_stock FROM wood_inventory
        ORDER BY id DESC LIMIT 1
    """)
    stock = cur.fetchone()
    total_stock = stock[0] if stock else 0

    cur.execute("SELECT SUM(consumption) FROM wood_consumption")
    used = cur.fetchone()[0] or 0

    remaining = total_stock - used

    st.subheader("📦 Wood Status")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Stock", total_stock)
    col2.metric("Used", used)
    col3.metric("Remaining", remaining)

    # ================= OUTPUT =================
    st.subheader("📊 House Intelligence")
    st.dataframe(result_df, use_container_width=True)

    st.subheader("🚨 Priority Houses")
    st.dataframe(
        result_df.sort_values(by="Priority Score", ascending=False).head(5),
        use_container_width=True
    )

    # ================= STAGE ANALYSIS =================
    if not stage_df.empty:
        st.subheader("⏱ Stage-wise Delay Analysis")
        st.dataframe(stage_df, use_container_width=True)

        bottleneck = (
            stage_df.groupby("Stage")["Actual Days"]
            .mean()
            .reset_index()
            .sort_values(by="Actual Days", ascending=False)
        )

        st.subheader("🔥 Bottleneck Stages")
        st.dataframe(bottleneck, use_container_width=True)
