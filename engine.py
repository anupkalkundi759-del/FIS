def run_engine(conn, cur):
    import streamlit as st
    import pandas as pd
    from datetime import datetime, timedelta

    st.title("⚙️ Advanced Scheduling Intelligence Engine")

    # ================= LOAD MASTER ACTIVITIES =================
    cur.execute("""
        SELECT activity_name, sequence_order, duration_days
        FROM activity_master
        ORDER BY sequence_order
    """)
    activities = cur.fetchall()

    if not activities:
        st.error("No activity master found")
        return

    activity_df = pd.DataFrame(activities, columns=["stage", "seq", "days"])
    total_planned_days = activity_df["days"].sum()

    # ================= LOAD TRACKING DATA =================
    cur.execute("""
        SELECT 
            h.house_no,
            s.stage_name,
            t.timestamp

        FROM products p
        JOIN houses h ON p.house_id = h.house_id

        LEFT JOIN LATERAL (
            SELECT stage_id, timestamp
            FROM tracking_log
            WHERE product_instance_id = p.product_instance_id
            ORDER BY timestamp DESC
            LIMIT 1
        ) t ON TRUE

        LEFT JOIN stages s ON t.stage_id = s.stage_id
    """)

    data = cur.fetchall()

    if not data:
        st.warning("No tracking data found")
        return

    df = pd.DataFrame(data, columns=["house", "stage", "time"])

    # ================= CLEAN DATA =================
    df["stage"] = df["stage"].fillna("Not Started")

    df = df.merge(activity_df, left_on="stage", right_on="stage", how="left")

    # FIX: handle not started
    df["seq"] = df["seq"].fillna(0)
    df["days"] = df["days"].fillna(0)

    today = datetime.now()

    results = []

    # ================= CORE ENGINE =================
    for house in df["house"].unique():

        house_df = df[df["house"] == house]

        # FIX: correct current stage (latest, not mode)
        house_df = house_df.sort_values("seq")
        current_stage_row = house_df.iloc[-1]

        current_stage = current_stage_row["stage"]
        current_seq = current_stage_row["seq"]

        # ===== Remaining Work =====
        remaining_days = activity_df[activity_df["seq"] > current_seq]["days"].sum()

        # ===== Completed Work =====
        completed_days = activity_df[activity_df["seq"] <= current_seq]["days"].sum()

        # ===== Progress =====
        progress_percent = (completed_days / total_planned_days) * 100 if total_planned_days > 0 else 0

        # ===== Predicted Finish =====
        predicted_finish = today + timedelta(days=int(remaining_days))

        # ===== REAL DELAY CALCULATION =====
        start_date = today - timedelta(days=int(completed_days))
        expected_finish = start_date + timedelta(days=int(total_planned_days))

        delay_days = (predicted_finish - expected_finish).days
        delay_percent = (delay_days / total_planned_days) * 100 if total_planned_days > 0 else 0

        # ===== RISK DETECTION =====
        if delay_percent > 20:
            risk = "🔴 High Risk"
        elif delay_percent > 10:
            risk = "🟠 Medium Risk"
        else:
            risk = "🟢 Low Risk"

        results.append({
            "House": house,
            "Current Stage": current_stage,
            "Progress %": round(progress_percent, 1),
            "Remaining Days": int(remaining_days),
            "Predicted Finish": predicted_finish.date(),
            "Delay %": round(delay_percent, 1),
            "Risk": risk
        })

    result_df = pd.DataFrame(results)

    # ================= BOTTLENECK =================
    bottleneck = df["stage"].value_counts().reset_index()
    bottleneck.columns = ["Stage", "Count"]

    # ================= CRITICAL HOUSES =================
    critical = result_df.sort_values(by="Remaining Days", ascending=False).head(5)

    # ================= UI =================
    st.subheader("📊 House-Level Intelligence")
    st.dataframe(result_df, use_container_width=True)

    st.subheader("🔥 Bottleneck Detection")
    st.dataframe(bottleneck, use_container_width=True)

    st.subheader("🚨 Critical Houses (Priority)")
    st.dataframe(critical, use_container_width=True)

    # ================= KPI =================
    col1, col2, col3 = st.columns(3)

    col1.metric("Total Houses", len(result_df))
    col2.metric("Avg Progress %", round(result_df["Progress %"].mean(), 1))
    col3.metric("High Risk Houses", len(result_df[result_df["Risk"] == "🔴 High Risk"]))
