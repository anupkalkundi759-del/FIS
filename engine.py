def run_engine(conn, cur):
    import streamlit as st
    import pandas as pd
    from datetime import datetime, timedelta

    st.title("⚙️ Scheduling Intelligence Engine")

    today = datetime.now()

    # ================= CONFIG =================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS house_config (
            house_no TEXT PRIMARY KEY,
            urgency INT DEFAULT 0,
            sla_date DATE
        )
    """)
    conn.commit()

    # ================= ACTIVITIES =================
    cur.execute("""
        SELECT activity_name, sequence_order, duration_days
        FROM activity_master
        ORDER BY sequence_order
    """)
    act = cur.fetchall()

    activity_df = pd.DataFrame(act, columns=["stage", "seq", "days"])
    total_stages = len(activity_df)

    stage_map = dict(zip(activity_df["stage"], activity_df["seq"]))

    # ================= UI =================
    st.subheader("⚙️ House Configuration")

    c1, c2, c3, c4, c5 = st.columns(5)

    cur.execute("SELECT project_id, project_name FROM projects ORDER BY project_name")
    projects = cur.fetchall()
    project_dict = {p[1]: p[0] for p in projects}
    selected_project = c1.selectbox("Project", list(project_dict.keys()))

    cur.execute("SELECT unit_id, unit_name FROM units WHERE project_id=%s", (project_dict[selected_project],))
    units = cur.fetchall()
    unit_dict = {u[1]: u[0] for u in units}
    selected_unit = c2.selectbox("Unit", list(unit_dict.keys()))

    cur.execute("SELECT house_no FROM houses WHERE unit_id=%s", (unit_dict[selected_unit],))
    houses = [h[0] for h in cur.fetchall()]
    selected_house = c3.selectbox("House", houses)

    urgency_map = {"Low":0,"Medium":1,"High":2,"Critical":3}
    urgency_label = c4.selectbox("Urgency", list(urgency_map.keys()))
    urgency_val = urgency_map[urgency_label]

    sla_date = c5.date_input("SLA")

    if st.button("Save Configuration"):
        cur.execute("""
            INSERT INTO house_config (house_no, urgency, sla_date)
            VALUES (%s, %s, %s)
            ON CONFLICT (house_no)
            DO UPDATE SET urgency = EXCLUDED.urgency,
                          sla_date = EXCLUDED.sla_date
        """, (selected_house, urgency_val, sla_date))
        conn.commit()
        st.success("Saved")

    # ================= LOAD CONFIG =================
    cur.execute("SELECT house_no, urgency, sla_date FROM house_config")
    config_map = {r[0]: {"urgency": r[1], "sla": r[2]} for r in cur.fetchall()}

    # ================= TRACKING =================
    cur.execute("""
        SELECT h.house_no, p.product_instance_id, s.stage_name, s.sequence, t.timestamp
        FROM products p
        JOIN houses h ON p.house_id = h.house_id
        LEFT JOIN tracking_log t ON p.product_instance_id = t.product_instance_id
        LEFT JOIN stages s ON t.stage_id = s.stage_id
        WHERE h.unit_id = %s
    """, (unit_dict[selected_unit],))

    df = pd.DataFrame(cur.fetchall(), columns=["house","product","stage","seq","time"])
    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    if df.empty:
        st.warning("No tracking data")
        return

    # ================= BOTTLENECK =================
    stage_count = df.groupby("stage")["product"].count().reset_index()
    stage_count.columns = ["Stage", "Count"]
    bottleneck_stage = stage_count.sort_values("Count", ascending=False).iloc[0]

    # ================= CALC =================
    results = []
    early_warnings = []

    for house in df["house"].unique():

        house_df = df[df["house"] == house]

        # -------- HOUSE PROGRESS (FIXED) --------
        product_stage = house_df.groupby("product")["seq"].max().fillna(0)

        progress = (product_stage.sum() / (len(product_stage) * total_stages)) * 100 if len(product_stage) else 0

        # -------- START DATE (Measurement) --------
        m = house_df[house_df["stage"] == "Measurement"]
        start_date = m["time"].min() if not m.empty else None

        config = config_map.get(house, {})
        sla = config.get("sla")
        urgency = config.get("urgency", 0)

        expected_finish = pd.to_datetime(sla) if sla else None

        # -------- PREDICTION --------
        remaining = total_stages * (1 - progress / 100)
        predicted_finish = today + timedelta(days=int(remaining))

        delay = (predicted_finish - expected_finish).days if expected_finish else None

        # -------- EARLY WARNING --------
        if progress < 5:
            early_warnings.append({"House": house, "Issue": "Not started"})
        elif delay is not None and delay > 0:
            early_warnings.append({"House": house, "Issue": f"Delayed {delay} days"})

        # -------- PRIORITY --------
        score = (100 - progress) + (urgency * 15)

        if score >= 80:
            priority = "🔴 Critical"
        elif score >= 50:
            priority = "🟠 High"
        elif score >= 20:
            priority = "🟡 Medium"
        else:
            priority = "🟢 Low"

        # -------- DELAY TEXT --------
        if progress == 0:
            delay_txt = "Not started"
        elif delay is None:
            delay_txt = "No SLA"
        elif delay > 0:
            delay_txt = f"Delayed {delay} days"
        elif delay == 0:
            delay_txt = "On time"
        else:
            delay_txt = f"Ahead {abs(delay)} days"

        results.append({
            "House": house,
            "Progress %": round(progress,1),
            "Delay": delay_txt,
            "SLA": expected_finish.date() if expected_finish else "Not Set",
            "Predicted Finish": predicted_finish.date(),
            "Urgency": urgency_label,
            "Priority": priority
        })

    result_df = pd.DataFrame(results)

    # ================= OVERVIEW =================
    st.subheader("📊 Overview")
    a, b, c = st.columns(3)
    a.metric("Total Houses", len(result_df))
    b.metric("Delayed Houses", len(result_df[result_df["Delay"].str.contains("Delayed", na=False)]))
    c.metric("Avg Progress", round(result_df["Progress %"].mean(), 1))

    # ================= PRIORITY =================
    st.subheader("🚨 Priority Houses")
    st.dataframe(result_df.sort_values("Progress %").head(5), use_container_width=True)

    # ================= EARLY WARNING =================
    st.subheader("🚨 Early Warnings")
    if early_warnings:
        st.dataframe(pd.DataFrame(early_warnings), use_container_width=True)
    else:
        st.success("All houses on track")

    # ================= BOTTLENECK =================
    st.subheader("🚧 Bottleneck Analysis")
    st.dataframe(stage_count, use_container_width=True)
    st.error(f"Bottleneck Stage: {bottleneck_stage['Stage']} ({bottleneck_stage['Count']} products)")

    # ================= MAIN =================
    st.subheader("🏠 House Intelligence")
    st.dataframe(result_df, use_container_width=True)
