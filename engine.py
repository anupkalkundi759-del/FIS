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
    early_warnings = []

    # ================= CORE ENGINE =================
    for house in df["house"].unique():

        house_df = df[df["house"] == house].sort_values("seq")

        start_date = house_df["time"].min()
        current_row = house_df.iloc[-1]

        current_stage = current_row["stage"]
        current_seq = current_row["seq"]

        # ===== ES / EF =====
        es = start_date
        ef = start_date

        for _, row in activity_df.iterrows():
            duration = row["days"]

            if row["seq"] == 1:
                es = start_date
                ef = es + timedelta(days=duration)
            else:
                es = ef
                ef = es + timedelta(days=duration)

            if row["seq"] == current_seq:
                break

        # ===== PROGRESS =====
        completed_days = activity_df[activity_df["seq"] <= current_seq]["days"].sum()
        remaining_days = activity_df[activity_df["seq"] > current_seq]["days"].sum()

        progress = (completed_days / total_duration) * 100 if total_duration else 0

        actual_elapsed = max(1, (today - start_date).days)
        planned_progress = (actual_elapsed / TARGET_DAYS) * 100

        performance = actual_elapsed / completed_days if completed_days > 0 else 1

        predicted_finish = today + timedelta(days=int(remaining_days * performance))
        expected_finish = start_date + timedelta(days=TARGET_DAYS)

        delay = (predicted_finish - expected_finish).days

        # ===== EARLY WARNING =====
        if progress < planned_progress:
            early_warnings.append({
                "House": house,
                "Planned %": round(planned_progress,1),
                "Actual %": round(progress,1)
            })

        # ===== STAGE ANALYSIS =====
        house_df = house_df.reset_index(drop=True)

        for i in range(len(house_df) - 1):
            stage_name = house_df.loc[i, "stage"]
            start_time = house_df.loc[i, "time"]
            next_time = house_df.loc[i + 1, "time"]

            actual_duration = (next_time - start_time).days

            stage_analysis.append({
                "Stage": stage_name,
                "Actual Days": actual_duration
            })

        priority_score = delay + remaining_days

        results.append({
            "House": house,
            "Stage": current_stage,
            "Progress %": round(progress, 1),
            "Planned %": round(planned_progress, 1),
            "Delay": delay,
            "ES": es.date(),
            "EF": ef.date(),
            "Predicted Finish": predicted_finish.date(),
            "Priority": priority_score
        })

    result_df = pd.DataFrame(results)
    stage_df = pd.DataFrame(stage_analysis)
    early_df = pd.DataFrame(early_warnings)

    # ================= FLOW METRICS =================
    st.markdown("## 📊 Flow Metrics")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📦 Stage Load (WIP)")
        stage_load = df["stage"].value_counts()
        st.bar_chart(stage_load)

    with col2:
        st.subheader("⚡ Bottleneck")
        if not stage_df.empty:
            bottleneck = stage_df.groupby("Stage")["Actual Days"].mean().sort_values(ascending=False)
            st.bar_chart(bottleneck)

    # ================= THROUGHPUT =================
    st.subheader("🚀 Throughput (Last 7 Days)")

    last_week = df[df["time"] >= today - timedelta(days=7)]
    throughput = last_week.groupby(last_week["time"].dt.date)["house"].nunique()

    if not throughput.empty:
        st.line_chart(throughput)
    else:
        st.info("Not enough recent data")

    # ================= KPI =================
    st.markdown("## 📊 Overview")

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Houses", len(result_df))
    col2.metric("Delayed Houses", len(result_df[result_df["Delay"] > 0]))
    col3.metric("Avg Progress", round(result_df["Progress %"].mean(),1))

    # ================= EARLY WARNING =================
    st.subheader("🚨 Early Warnings")

    if early_df.empty:
        st.success("All houses on track")
    else:
        st.dataframe(early_df, use_container_width=True)

    # ================= PRIORITY =================
    st.subheader("🚨 Priority Houses")
    st.dataframe(result_df.sort_values("Priority", ascending=False).head(5))

    # ================= MAIN TABLE =================
    st.subheader("🏠 House Intelligence")
    st.dataframe(result_df, use_container_width=True)
