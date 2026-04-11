def run_engine(conn, cur):
    import streamlit as st
    import pandas as pd
    from datetime import datetime, timedelta

    st.title("⚙️ Scheduling Intelligence Engine")

    TARGET_DAYS = 45
    today = datetime.now()

    # ================= LOAD MASTER =================
    cur.execute("""
        SELECT activity_name, sequence_order, duration_days
        FROM activity_master
        ORDER BY sequence_order
    """)
    act = cur.fetchall()
    activity_df = pd.DataFrame(act, columns=["stage", "seq", "days"])
    total_duration = activity_df["days"].sum()

    # ================= LOAD TRACKING =================
    cur.execute("""
        SELECT h.house_no, s.stage_name, s.sequence, t.timestamp
        FROM products p
        JOIN houses h ON p.house_id = h.house_id
        JOIN tracking_log t ON t.product_instance_id = p.id
        JOIN stages s ON t.stage_id = s.stage_id
    """)
    data = cur.fetchall()

    if not data:
        st.warning("No tracking data")
        return

    df = pd.DataFrame(data, columns=["house", "stage", "seq", "time"])
    df["time"] = pd.to_datetime(df["time"])

    results, stage_analysis, alerts = [], [], []

    for house in df["house"].unique():
        hdf = df[df["house"] == house].sort_values("seq")

        start = hdf["time"].min()
        current = hdf.iloc[-1]
        seq = current["seq"]

        completed = activity_df[activity_df["seq"] <= seq]["days"].sum()
        remaining = activity_df[activity_df["seq"] > seq]["days"].sum()

        progress = (completed / total_duration) * 100 if total_duration else 0

        elapsed = max(1, (today - start).days)
        planned = (elapsed / TARGET_DAYS) * 100

        perf = elapsed / completed if completed > 0 else 1

        predicted = today + timedelta(days=int(remaining * perf))
        expected = start + timedelta(days=TARGET_DAYS)

        delay = (predicted - expected).days

        # ALERT ENGINE
        if delay > 3:
            alerts.append((house, "CRITICAL", delay))
        elif progress < planned:
            alerts.append((house, "RISK", delay))

        # STAGE ANALYSIS
        for i in range(len(hdf) - 1):
            s = hdf.iloc[i]["stage"]
            t1 = hdf.iloc[i]["time"]
            t2 = hdf.iloc[i + 1]["time"]

            actual = (t2 - t1).days
            planned_d = activity_df[activity_df["stage"] == s]["days"].values[0]

            stage_analysis.append({
                "House": house,
                "Stage": s,
                "Planned": planned_d,
                "Actual": actual,
                "Delay": actual - planned_d
            })

        results.append({
            "House": house,
            "Stage": current["stage"],
            "Progress %": round(progress, 1),
            "Delay": delay,
            "Finish": predicted.date()
        })

    res_df = pd.DataFrame(results)
    stage_df = pd.DataFrame(stage_analysis)

    # ================= WOOD =================
    cur.execute("SELECT total_stock FROM wood_inventory ORDER BY id DESC LIMIT 1")
    stock = cur.fetchone()
    total_stock = stock[0] if stock else 0

    cur.execute("SELECT SUM(consumption) FROM wood_consumption")
    used = cur.fetchone()[0] or 0

    remaining = total_stock - used

    # ================= KPI ROW =================
    st.markdown("### 📊 Overview")
    c1, c2, c3, c4, c5 = st.columns(5)

    c1.metric("Stock", total_stock)
    c2.metric("Used", used)
    c3.metric("Remaining", remaining)
    c4.metric("Houses", len(res_df))
    c5.metric("Avg Delay", round(res_df["Delay"].mean(), 1))

    st.divider()

    # ================= OPERATION LAYER =================
    col1, col2 = st.columns([1, 1])

    # -------- WOOD INPUT --------
    with col1:
        st.markdown("### 🪵 Wood Control")

        stock_in = st.number_input("Update Total Stock", 0)
        if st.button("Update Stock"):
            cur.execute("INSERT INTO wood_inventory (total_stock) VALUES (%s)", (stock_in,))
            conn.commit()
            st.rerun()

        cons = st.number_input("Consumption", 0)
        house = st.text_input("House No")

        if st.button("Add Consumption"):
            cur.execute("INSERT INTO wood_consumption (house_no, consumption) VALUES (%s,%s)", (house, cons))
            conn.commit()
            st.rerun()

    # -------- ALERT ENGINE --------
    with col2:
        st.markdown("### 🚨 Early Warning Engine")

        if not alerts:
            st.success("All houses on track")
        else:
            for a in alerts:
                if a[1] == "CRITICAL":
                    st.error(f"{a[0]} delayed {a[2]} days")
                else:
                    st.warning(f"{a[0]} at risk")

    st.divider()

    # ================= INTELLIGENCE =================
    st.markdown("### 🏠 House Intelligence")
    st.dataframe(res_df, use_container_width=True)

    col3, col4 = st.columns(2)

    # -------- STAGE DELAY --------
    with col3:
        st.markdown("### ⏱ Stage Delay")
        st.dataframe(stage_df, use_container_width=True)

    # -------- BOTTLENECK --------
    with col4:
        st.markdown("### 🔥 Bottleneck")

        if not stage_df.empty:
            bottleneck = (
                stage_df.groupby("Stage")["Actual"]
                .mean()
                .reset_index()
                .sort_values(by="Actual", ascending=False)
            )
            st.dataframe(bottleneck, use_container_width=True)
