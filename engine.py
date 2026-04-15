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
            urgency INT DEFAULT 0,
            sla_date DATE
        )
    """)
    conn.commit()

    # ================= LOAD ACTIVITIES =================
    cur.execute("""
        SELECT activity_name, sequence_order, duration_days
        FROM activity_master
        ORDER BY sequence_order
    """)
    act = cur.fetchall()

    activity_df = pd.DataFrame(act, columns=["stage", "seq", "days"])
    activity_df["days"] = activity_df["days"].astype(int)
    total_duration = int(activity_df["days"].sum())

    st.subheader("⚙️ House Configuration (SLA + Urgency)")

    col1, col2, col3, col4, col5 = st.columns(5)

    cur.execute("SELECT project_id, project_name FROM projects ORDER BY project_name")
    projects = cur.fetchall()
    project_dict = {p[1]: p[0] for p in projects}
    selected_project = col1.selectbox("Project", list(project_dict.keys()))

    cur.execute("SELECT unit_id, unit_name FROM units WHERE project_id=%s", (project_dict[selected_project],))
    units = cur.fetchall()
    unit_dict = {u[1]: u[0] for u in units}
    selected_unit = col2.selectbox("Unit", list(unit_dict.keys()))

    cur.execute("SELECT house_no FROM houses WHERE unit_id=%s", (unit_dict[selected_unit],))
    houses = [h[0] for h in cur.fetchall()]
    selected_house = col3.selectbox("House", houses)

    urgency_map_ui = {"Low":0,"Medium":1,"High":2,"Critical":3}
    urgency_label = col4.selectbox("Urgency", list(urgency_map_ui.keys()))
    urgency = urgency_map_ui[urgency_label]

    sla_date = col5.date_input("SLA")

    if st.button("Save Configuration"):
        cur.execute("""
            INSERT INTO house_config (house_no, urgency, sla_date)
            VALUES (%s, %s, %s)
            ON CONFLICT (house_no)
            DO UPDATE SET urgency = EXCLUDED.urgency,
                          sla_date = EXCLUDED.sla_date
        """, (selected_house, urgency, sla_date))
        conn.commit()
        st.success("Saved")

    # ================= LOAD CONFIG =================
    cur.execute("SELECT house_no, urgency, sla_date FROM house_config")
    config_map = {row[0]: {"urgency": row[1], "sla": row[2]} for row in cur.fetchall()}

    # ================= LOAD TRACKING =================
    cur.execute("""
        SELECT h.house_no, s.stage_name, t.timestamp, s.sequence
        FROM products p
        JOIN houses h ON p.house_id = h.house_id
        JOIN tracking_log t ON t.product_instance_id = p.product_instance_id
        JOIN stages s ON t.stage_id = s.stage_id
        WHERE h.unit_id = %s
    """, (unit_dict[selected_unit],))

    df = pd.DataFrame(cur.fetchall(), columns=["house","stage","time","seq"])
    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    if df.empty:
        st.warning("No tracking data")
        return

    results = []

    def priority_color(score):
        if score >= 80: return "🔴 Critical"
        elif score >= 50: return "🟠 High"
        elif score >= 20: return "🟡 Medium"
        else: return "🟢 Low"

    for house in df["house"].unique():

        house_df = df[df["house"] == house]

        # ✅ START DATE (Measurement FIX PRESERVED)
        m_rows = house_df[house_df["stage"] == "Measurement"]
        if not m_rows.empty:
            start_date = m_rows["time"].min()
        else:
            start_date = house_df["time"].min()

        progress = 0 if start_date is None else min(100, ((today - start_date).days / total_duration) * 100)

        config = config_map.get(house, {})
        sla = config.get("sla")
        urgency_val = config.get("urgency", 0)

        # ✅ SLA FIX
        expected_finish = pd.to_datetime(sla) if sla else None

        # ✅ PREDICTION FIX
        if progress == 0:
            predicted_finish = None
            delay = None
        else:
            remaining_days = total_duration * (1 - progress / 100)
            predicted_finish = today + timedelta(days=int(remaining_days))
            delay = (predicted_finish - expected_finish).days if expected_finish else None

        # DELAY TEXT
        if progress == 0:
            delay_display = "Not started"
        elif delay is None:
            delay_display = "No SLA"
        elif delay < 0:
            delay_display = f"Ahead {abs(delay)} days"
        elif delay == 0:
            delay_display = "On time"
        else:
            delay_display = f"Delayed {delay} days"

        score = (100 - progress) + (urgency_val * 15)
        priority = priority_color(score)

        results.append({
            "House": house,
            "Progress %": round(progress,1),
            "Delay": delay_display,
            "SLA": expected_finish.date() if expected_finish else "Not Set",
            "Predicted Finish": predicted_finish.date() if predicted_finish else "N/A",
            "Urgency": urgency_label,
            "Priority": priority
        })

    result_df = pd.DataFrame(results)

    st.subheader("🏠 House Intelligence")
    st.dataframe(result_df, use_container_width=True)
