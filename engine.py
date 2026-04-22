def run_engine(conn, cur):
    import streamlit as st
    import pandas as pd
    from datetime import datetime, timedelta

    st.title("🏭 Factory Intelligence System")
    st.header("⚙️ Scheduling Intelligence Engine")

    today = datetime.today().date()

    # ==============================
    # FETCH TRACKING DATA
    # ==============================
    cur.execute("""
        SELECT 
            h.house_no,
            p.project_name,
            u.unit_name,
            pm.product_code,
            pm.product_category,
            pm.orientation,
            s.stage_name,
            t.status,
            t.timestamp
        FROM tracking_log t
        JOIN stages s ON t.stage_id = s.stage_id
        JOIN products pr ON t.product_id = pr.product_id
        JOIN houses h ON pr.house_id = h.house_id
        JOIN units u ON h.unit_id = u.unit_id
        JOIN projects p ON u.project_id = p.project_id
        JOIN products_master pm ON pr.product_id = pm.product_id
        ORDER BY t.timestamp
    """)

    data = cur.fetchall()

    if not data:
        st.warning("No tracking data available")
        return

    df = pd.DataFrame(data, columns=[
        "house", "project", "unit",
        "product", "category", "orientation",
        "stage", "status", "time"
    ])

    df["time"] = pd.to_datetime(df["time"])

    # ==============================
    # STAGE SEQUENCE
    # ==============================
    cur.execute("SELECT stage_name, sequence FROM stages ORDER BY sequence")
    stage_seq = dict(cur.fetchall())

    # ==============================
    # ACTIVITY DURATIONS
    # ==============================
    cur.execute("SELECT activity_name, duration_days FROM activity_master")
    duration_map = dict(cur.fetchall())

    # ==============================
    # TOTAL PRODUCTS PER HOUSE
    # ==============================
    total_products = df.groupby("house")["product"].nunique().to_dict()

    # ==============================
    # LATEST RECORD PER PRODUCT
    # ==============================
    latest_df = df.sort_values("time").groupby(
        ["house", "product"]
    ).tail(1)

    houses = df["house"].unique()

    results = []

    for house in houses:

        h_data = latest_df[latest_df["house"] == house]

        if h_data.empty:
            continue

        # ✅ CURRENT STAGE (latest timestamp)
        h_data = h_data.sort_values("time")
        current_stage = h_data.iloc[-1]["stage"]

        # STAGE START
        stage_start = df[
            (df["house"] == house) &
            (df["stage"] == current_stage)
        ]["time"].min()

        if pd.isna(stage_start):
            stage_start = today

        stage_start = stage_start.date()

        stage_duration = duration_map.get(current_stage, 1)

        stage_elapsed = max((today - stage_start).days, 0)

        current_seq = stage_seq.get(current_stage, 1)

        remaining_future = sum([
            duration_map.get(s, 0)
            for s in stage_seq
            if stage_seq[s] > current_seq
        ])

        # ✅ CORRECT PREDICTION
        predicted_finish = stage_start + timedelta(
            days=int(stage_duration + remaining_future)
        )

        remaining_stage = max(stage_duration - stage_elapsed, 0)
        remaining_total = max((predicted_finish - today).days, 0)

        # ✅ PROGRESS
        stage_records = h_data[h_data["stage"] == current_stage]
        in_progress = len(stage_records)
        total = total_products.get(house, 1)
        progress = round((in_progress / total) * 100, 1)

        # ==========================
        # ✅ NEW: DELAY CALCULATION
        # ==========================
        delay_days = max((today - predicted_finish).days, 0)

        # ==========================
        # ✅ NEW: DELAY REASON
        # ==========================
        if delay_days == 0:
            delay_reason = "On Track"
        elif stage_elapsed > stage_duration:
            delay_reason = "Stage Delay"
        elif progress < 20:
            delay_reason = "Slow Progress"
        elif remaining_total > 10:
            delay_reason = "Upstream Bottleneck"
        else:
            delay_reason = "General Delay"

        # ACTUAL FINISH
        completed_check = df[
            (df["house"] == house) &
            (df["stage"] == "Dispatch") &
            (df["status"] == "Completed")
        ]

        if not completed_check.empty:
            actual_finish = completed_check["time"].max().date()
        else:
            actual_finish = "Not Finished"

        results.append({
            "House": house,
            "Stage": current_stage,
            "Progress %": progress,
            "Predicted Finish": predicted_finish,
            "Actual Finish": actual_finish,
            "Remaining (Stage)": f"{remaining_stage} days",
            "Remaining (Total)": f"{remaining_total} days",
            "Delay (Days)": delay_days,
            "Delay Reason": delay_reason
        })

    result_df = pd.DataFrame(results)

    # ==============================
    # DISPLAY
    # ==============================
    st.subheader("🏠 House Intelligence")
    st.dataframe(result_df)

    # ==============================
    # EARLY WARNING
    # ==============================
    st.subheader("🚨 Early Warning")

    risk_df = result_df[result_df["Delay (Days)"] > 0]

    if risk_df.empty:
        st.success("No early risks")
    else:
        st.error(f"{len(risk_df)} houses delayed")

    # ==============================
    # BOTTLENECK
    # ==============================
    st.subheader("⚠️ Bottleneck Detection")

    bottleneck = result_df["Stage"].value_counts().idxmax()
    st.warning(f"Bottleneck at: {bottleneck}")

    # ==============================
    # SLA (UNCHANGED)
    # ==============================
    st.subheader("📅 SLA Assignment")

    projects = df["project"].unique()
    selected_project = st.selectbox("Project", projects)

    units = df[df["project"] == selected_project]["unit"].unique()
    selected_unit = st.selectbox("Unit", units)

    houses_filtered = df[
        (df["project"] == selected_project) &
        (df["unit"] == selected_unit)
    ]["house"].unique()

    selected_house = st.selectbox("House", houses_filtered)

    sla_date = st.date_input("SLA Date")

    if st.button("Save SLA"):
        cur.execute("""
            INSERT INTO houses (house_no, predicted_finish)
            VALUES (%s, %s)
            ON CONFLICT (house_no)
            DO UPDATE SET predicted_finish = EXCLUDED.predicted_finish
        """, (selected_house, sla_date))

        conn.commit()
        st.success("SLA saved")
