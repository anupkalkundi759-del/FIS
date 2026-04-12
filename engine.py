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
    activity_df["days"] = activity_df["days"].astype(int)
    total_duration = int(activity_df["days"].sum())

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
        consumption_input = st.number_input("Consumption per House", min_value=0, step=1)

        if st.button("Set Consumption"):
            if house_input:
                cur.execute("""
                    INSERT INTO wood_consumption (house_no, consumption)
                    VALUES (%s, %s)
                """, (house_input, consumption_input))
                conn.commit()
                st.success("Consumption Updated")
                st.rerun()

    # ================= STOCK =================
    cur.execute("SELECT total_stock FROM wood_inventory ORDER BY id DESC LIMIT 1")
    stock = cur.fetchone()
    total_stock = int(stock[0]) if stock else 0

    cur.execute("SELECT house_no, consumption FROM wood_consumption")
    consumption_data = dict(cur.fetchall())

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

    # ================= ENGINE =================
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

        days_in_stage = max(0, (today - current_row["time"]).days)
        partial = min(days_in_stage / current_stage_days, 1) if current_stage_days > 0 else 0

        completed_days += int(current_stage_days * partial)
        progress = (completed_days / total_duration) * 100 if total_duration else 0

        # ================= PLANNED =================
        actual_elapsed = max(1, (today - start_date).days)
        planned_progress = (min(actual_elapsed, total_duration) / total_duration) * 100

        # ================= SMART PRODUCTIVITY (FIXED) =================
        recent_rates = []

        for i in range(len(house_df) - 1):
            t1 = house_df.loc[i, "time"]
            t2 = house_df.loc[i+1, "time"]

            if pd.notna(t1) and pd.notna(t2):
                actual_days = max(1, (t2 - t1).days)

                planned_days = activity_df[
                    activity_df["stage"] == house_df.loc[i, "stage"]
                ]["days"]

                planned_days = int(planned_days.values[0]) if not planned_days.empty else 1

                rate = actual_days / planned_days
                recent_rates.append(rate)

        # 🔥 ONLY LAST 2 STAGES (IMPORTANT)
        if len(recent_rates) >= 2:
            productivity_rate = sum(recent_rates[-2:]) / 2
        elif recent_rates:
            productivity_rate = recent_rates[-1]
        else:
            productivity_rate = 1

        # clamp
        productivity_rate = max(0.7, min(productivity_rate, 1.5))

        # ================= RESOURCE LEVELING =================
        consumption = consumption_data.get(house, 0)

        if total_stock < consumption:
            resource_delay = 5   # delay penalty
            resource_flag = "Shortage"
        else:
            resource_delay = 0
            resource_flag = "OK"

        remaining_days = total_duration - completed_days

        predicted_finish = today + timedelta(
            days=int((remaining_days * productivity_rate) + resource_delay)
        )

        expected_finish = start_date + timedelta(days=int(total_duration))
        delay = (predicted_finish - expected_finish).days

        # ================= BLOCKED =================
        days_since_update = (today - current_row["time"]).days
        if days_since_update > 3:
            early_warnings.append({
                "House": house,
                "Issue": f"Blocked at {current_stage}"
            })

        # ================= BOTTLENECK =================
        for i in range(len(house_df) - 1):
            t1 = house_df.loc[i, "time"]
            t2 = house_df.loc[i+1, "time"]

            if pd.notna(t1) and pd.notna(t2):
                actual_days = (t2 - t1).days

                planned_days = activity_df[
                    activity_df["stage"] == house_df.loc[i, "stage"]
                ]["days"]

                planned_days = int(planned_days.values[0]) if not planned_days.empty else 1

                stage_analysis.append({
                    "Stage": house_df.loc[i, "stage"],
                    "Delay": actual_days - planned_days
                })

        results.append({
            "House": house,
            "Stage": current_stage,
            "Progress %": round(progress, 1),
            "Planned %": round(planned_progress, 1),
            "Delay": delay,
            "Predicted Finish": predicted_finish.date(),
            "Resource": resource_flag
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
    col2.metric("Used", sum(consumption_data.values()))
    col3.metric("Remaining", total_stock - sum(consumption_data.values()))

    # ================= GRAPH =================
    st.subheader("📈 Progress vs Planned")
    st.line_chart(result_df[["Progress %", "Planned %"]])

    # ================= WARNINGS =================
    st.subheader("🚨 Early Warnings")
    if early_df.empty:
        st.success("All houses on track")
    else:
        st.dataframe(early_df)

    # ================= BOTTLENECK =================
    st.subheader("🔥 Bottleneck Stages")
    if not stage_df.empty:
        bottleneck = stage_df.groupby("Stage")["Delay"].mean().sort_values(ascending=False)
        st.bar_chart(bottleneck)

    # ================= FINAL =================
    st.subheader("🏠 House Intelligence")
    st.dataframe(result_df, use_container_width=True)
