def run_engine(conn, cur):
    import streamlit as st
    import pandas as pd
    from datetime import datetime, timedelta

    st.title("⚙️ Scheduling Intelligence Engine")

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
    activity_df = activity_df.sort_values("seq").reset_index(drop=True)

    # 🔥 FORCE INT (CRITICAL FIX)
    activity_df["days"] = activity_df["days"].astype(int)

    # ================= FORWARD PASS =================
    activity_df["ES"] = 0
    activity_df["EF"] = 0

    for i in range(len(activity_df)):
        if i == 0:
            activity_df.loc[i, "ES"] = 0
        else:
            activity_df.loc[i, "ES"] = int(activity_df.loc[i-1, "EF"])

        activity_df.loc[i, "EF"] = int(activity_df.loc[i, "ES"] + activity_df.loc[i, "days"])

    project_duration = int(activity_df["EF"].max())
    total_duration = int(activity_df["days"].sum())

    # ================= BACKWARD PASS =================
    activity_df["LF"] = project_duration
    activity_df["LS"] = 0

    for i in reversed(range(len(activity_df))):
        if i == len(activity_df) - 1:
            activity_df.loc[i, "LF"] = project_duration
        else:
            activity_df.loc[i, "LF"] = int(activity_df.loc[i+1, "LS"])

        activity_df.loc[i, "LS"] = int(activity_df.loc[i, "LF"] - activity_df.loc[i, "days"])

    activity_df["Float"] = activity_df["LS"] - activity_df["ES"]

    # ================= WOOD =================
    cur.execute("SELECT total_stock FROM wood_inventory ORDER BY id DESC LIMIT 1")
    stock = cur.fetchone()
    total_stock = int(stock[0]) if stock else 0

    cur.execute("SELECT SUM(consumption) FROM wood_consumption")
    used = cur.fetchone()[0] or 0
    used = int(used)

    remaining_stock = total_stock - used

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
    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    results = []
    stage_analysis = []
    early_warnings = []

    # ================= CORE ENGINE =================
    for house in df["house"].unique():

        house_df = df[df["house"] == house].sort_values("seq").reset_index(drop=True)

        if house_df.empty:
            continue

        start_date = house_df["time"].min()
        current_row = house_df.iloc[-1]

        current_stage = current_row["stage"]
        current_seq = int(current_row["seq"])

        # ================= PROGRESS =================
        completed_days = activity_df[activity_df["seq"] < current_seq]["days"].sum()

        current_stage_days = activity_df[activity_df["seq"] == current_seq]["days"]
        current_stage_days = int(current_stage_days.values[0]) if not current_stage_days.empty else 0

        current_stage_start = current_row["time"]
        days_in_stage = max(0, (today - current_stage_start).days)

        partial_progress = min(days_in_stage / current_stage_days, 1) if current_stage_days > 0 else 0
        completed_days += int(current_stage_days * partial_progress)

        progress = (completed_days / total_duration) * 100 if total_duration else 0

        # ================= PLANNED =================
        actual_elapsed = max(1, (today - start_date).days)
        planned_days = min(actual_elapsed, total_duration)
        planned_progress = (planned_days / total_duration) * 100

        # ================= PRODUCTIVITY =================
        productivity_rate = actual_elapsed / completed_days if completed_days > 0 else 1

        remaining_days = total_duration - completed_days
        predicted_finish = today + timedelta(days=int(remaining_days * productivity_rate))

        expected_finish = start_date + timedelta(days=int(total_duration))

        delay = (predicted_finish - expected_finish).days

        # ================= CRITICAL DELAY =================
        critical_delay = 0

        for _, row in activity_df.iterrows():
            stage_name = row["stage"]
            planned_finish = start_date + timedelta(days=int(row["EF"]))

            actual_stage = house_df[house_df["stage"] == stage_name]

            if not actual_stage.empty:
                actual_time = actual_stage.iloc[-1]["time"]

                if pd.notna(actual_time) and actual_time > planned_finish:
                    critical_delay += (actual_time - planned_finish).days

        # ================= BLOCKED =================
        if pd.notna(current_row["time"]):
            days_since_update = (today - current_row["time"]).days
            if days_since_update > 3:
                early_warnings.append({
                    "House": house,
                    "Issue": f"Blocked at {current_stage}"
                })

        # ================= STAGE ANALYSIS =================
        for i in range(len(house_df) - 1):
            stage_name = house_df.loc[i, "stage"]
            start_time = house_df.loc[i, "time"]
            next_time = house_df.loc[i + 1, "time"]

            if pd.notna(start_time) and pd.notna(next_time):
                actual_duration = (next_time - start_time).days

                planned_days_stage = activity_df[activity_df["stage"] == stage_name]["days"]
                planned_days_stage = int(planned_days_stage.values[0]) if not planned_days_stage.empty else 1

                delay_stage = actual_duration - planned_days_stage

                stage_analysis.append({
                    "Stage": stage_name,
                    "Delay": delay_stage
                })

        # ================= RESOURCE =================
        resource_flag = "OK" if remaining_stock >= 0 else "Shortage"

        priority_score = delay + remaining_days

        results.append({
            "House": house,
            "Stage": current_stage,
            "Progress %": round(progress, 1),
            "Planned %": round(planned_progress, 1),
            "Delay": delay,
            "Critical Delay": critical_delay,
            "Expected Finish": expected_finish.date(),
            "Predicted Finish": predicted_finish.date(),
            "Resource": resource_flag,
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
    col3.metric("Avg Progress", round(result_df["Progress %"].mean(), 1))

    # ================= WOOD =================
    st.subheader("📦 Wood Status")

    col1, col2, col3 = st.columns(3)
    col1.metric("Total", total_stock)
    col2.metric("Used", used)
    col3.metric("Remaining", remaining_stock)

    # ================= GRAPH =================
    st.subheader("📈 Progress vs Planned")
    if not result_df.empty:
        st.line_chart(result_df[["Progress %", "Planned %"]])

    # ================= WARNINGS =================
    st.subheader("🚨 Early Warnings")
    if early_df.empty:
        st.success("All houses on track")
    else:
        st.dataframe(early_df)

    # ================= PRIORITY =================
    st.subheader("🚨 Priority Houses")
    if not result_df.empty:
        st.dataframe(result_df.sort_values("Priority", ascending=False).head(5))

    # ================= BOTTLENECK =================
    st.subheader("🔥 Bottleneck Stages")
    if not stage_df.empty:
        bottleneck = stage_df.groupby("Stage")["Delay"].mean().sort_values(ascending=False)
        st.bar_chart(bottleneck)

    # ================= FINAL =================
    st.subheader("🏠 House Intelligence")
    st.dataframe(result_df, use_container_width=True)
