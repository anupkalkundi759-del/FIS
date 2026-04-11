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

    # ================= WOOD INPUT =================
    st.subheader("🪵 Wood Input")

    col1, col2 = st.columns(2)

    with col1:
        stock_input = st.number_input("Total Wood Stock", min_value=0, step=1)
        if st.button("Update Stock"):
            cur.execute("INSERT INTO wood_inventory (total_stock) VALUES (%s)", (stock_input,))
            conn.commit()
            st.success("Stock Updated")
            st.rerun()

    with col2:
        house_input = st.text_input("House No")
        consumption_input = st.number_input("Consumption", min_value=0, step=1)

        if st.button("Add Consumption"):
            if house_input:
                cur.execute("""
                    INSERT INTO wood_consumption (house_no, consumption)
                    VALUES (%s, %s)
                """, (house_input, consumption_input))
                conn.commit()
                st.success("Consumption Added")
                st.rerun()
            else:
                st.warning("Enter House No")

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

        # ================= PROGRESS =================
        completed_days = activity_df[activity_df["seq"] <= current_seq]["days"].sum()
        remaining_days = activity_df[activity_df["seq"] > current_seq]["days"].sum()

        progress = (completed_days / total_duration) * 100 if total_duration else 0

        actual_elapsed = max(1, (today - start_date).days)
        planned_progress = (actual_elapsed / TARGET_DAYS) * 100

        performance = actual_elapsed / completed_days if completed_days > 0 else 1

        # ✅ FIXED LOGIC
        predicted_finish = start_date + timedelta(days=int(total_duration * performance))
        expected_finish = start_date + timedelta(days=TARGET_DAYS)

        delay = (predicted_finish - expected_finish).days

        # ================= EARLY WARNING =================
        if progress < planned_progress:
            early_warnings.append({
                "House": house,
                "Planned %": round(planned_progress,1),
                "Actual %": round(progress,1)
            })

        # ================= STAGE ANALYSIS =================
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

        # ================= PRIORITY =================
        priority_score = delay + remaining_days

        results.append({
            "House": house,
            "Stage": current_stage,
            "Progress %": round(progress, 1),
            "Planned %": round(planned_progress, 1),
            "Delay": delay,
            "Expected Finish": expected_finish.date(),
            "Predicted Finish": predicted_finish.date(),
            "Priority": priority_score
        })

    result_df = pd.DataFrame(results)
    stage_df = pd.DataFrame(stage_analysis)
    early_df = pd.DataFrame(early_warnings)

    # ================= KPI =================
    st.subheader("📊 Overview")

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Houses", len(result_df))
    col2.metric("Delayed Houses", len(result_df[result_df["Delay"] > 0]))
    col3.metric("Avg Progress", round(result_df["Progress %"].mean(),1))

    # ================= WOOD =================
    cur.execute("SELECT total_stock FROM wood_inventory ORDER BY id DESC LIMIT 1")
    stock = cur.fetchone()
    total_stock = stock[0] if stock else 0

    cur.execute("SELECT SUM(consumption) FROM wood_consumption")
    used = cur.fetchone()[0] or 0

    remaining = total_stock - used

    st.subheader("📦 Wood Status")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total", total_stock)
    col2.metric("Used", used)
    col3.metric("Remaining", remaining)

    # ================= GRAPH =================
    st.subheader("📈 Progress vs Planned")
    st.line_chart(result_df[["Progress %", "Planned %"]])

    # ================= EARLY WARNING =================
    st.subheader("🚨 Early Warnings")
    if early_df.empty:
        st.success("All houses on track")
    else:
        st.dataframe(early_df)

    # ================= PRIORITY =================
    st.subheader("🚨 Priority Houses")
    st.dataframe(result_df.sort_values("Priority", ascending=False).head(5))

    # ================= BOTTLENECK =================
    st.subheader("🔥 Bottleneck Stages")
    if not stage_df.empty:
        bottleneck = stage_df.groupby("Stage")["Actual Days"].mean().sort_values(ascending=False)
        st.bar_chart(bottleneck)

    # ================= FINAL TABLE =================
    st.subheader("🏠 House Intelligence")
    st.dataframe(result_df, use_container_width=True)
