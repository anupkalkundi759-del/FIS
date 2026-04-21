def run_engine(conn, cur):
    import streamlit as st
    import pandas as pd
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    st.title("⚙️ Scheduling Intelligence Engine")

    today = datetime.now(ZoneInfo("Asia/Kolkata"))

    # ================= TABLES =================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS house_config (
            house_no TEXT PRIMARY KEY,
            sla_date DATE
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS delay_trend (
            date DATE,
            total_delay INT
        )
    """)
    conn.commit()

    # ================= ACTIVITIES =================
    cur.execute("""
        SELECT activity_name, sequence_order, duration_days
        FROM activity_master
        ORDER BY sequence_order
    """)
    activity_df = pd.DataFrame(cur.fetchall(), columns=["stage","seq","days"])
    activity_df["days"] = activity_df["days"].astype(int)

    # ================= SLA ASSIGN =================
    st.subheader("⚙️ SLA Assignment")

    c1, c2, c3, c4, c5 = st.columns([2,2,2,2,1])

    with c1:
        cur.execute("SELECT project_id, project_name FROM projects ORDER BY project_name")
        project_dict = {p[1]: p[0] for p in cur.fetchall()}
        selected_project = st.selectbox("Project", list(project_dict.keys()))

    with c2:
        cur.execute("SELECT unit_id, unit_name FROM units WHERE project_id=%s",
                    (project_dict[selected_project],))
        unit_dict = {u[1]: u[0] for u in cur.fetchall()}
        selected_unit = st.selectbox("Unit", list(unit_dict.keys()))

    with c3:
        cur.execute("SELECT house_no FROM houses WHERE unit_id=%s",
                    (unit_dict[selected_unit],))
        houses = [h[0] for h in cur.fetchall()]
        selected_house = st.selectbox("House", houses)

    with c4:
        sla_date = st.date_input("SLA Date")

    with c5:
        st.write("")
        if st.button("Save SLA"):
            if sla_date < today.date():
                st.error("SLA cannot be in the past")
            else:
                cur.execute("""
                    INSERT INTO house_config (house_no, sla_date)
                    VALUES (%s, %s)
                    ON CONFLICT (house_no)
                    DO UPDATE SET sla_date = EXCLUDED.sla_date
                """, (selected_house, sla_date))
                conn.commit()
                st.success("SLA Saved")

    cur.execute("SELECT house_no, sla_date FROM house_config")
    config_map = {r[0]: r[1] for r in cur.fetchall()}

    # ================= PRODUCT-LEVEL DATA =================
    cur.execute("""
        SELECT
            p.product_instance_id,
            h.house_no,
            s.stage_name,
            t.status,
            MIN(t.timestamp),
            MAX(t.timestamp)
        FROM tracking_log t
        JOIN products p ON t.product_instance_id = p.product_instance_id
        JOIN houses h ON p.house_id = h.house_id
        JOIN stages s ON s.stage_id = t.stage_id
        WHERE h.unit_id = %s
        GROUP BY p.product_instance_id, h.house_no, s.stage_name, t.status
    """, (unit_dict[selected_unit],))

    raw = pd.DataFrame(cur.fetchall(),
        columns=["pid","house","stage","status","start","end"])

    if not raw.empty:
        raw["start"] = pd.to_datetime(raw["start"]).dt.tz_localize("Asia/Kolkata")
        raw["end"] = pd.to_datetime(raw["end"]).dt.tz_localize("Asia/Kolkata")

    prod_stage = {}
    for _, r in raw.iterrows():
        key = (r["pid"], r["stage"])
        prod_stage.setdefault(key, {"completed":False,"in_progress":False,"start":None,"end":None})

        if r["status"] == "Completed":
            prod_stage[key]["completed"] = True
            prod_stage[key]["start"] = r["start"]
            prod_stage[key]["end"] = r["end"]

        elif r["status"] == "In Progress":
            prod_stage[key]["in_progress"] = True
            prod_stage[key]["start"] = r["start"]

    # Products per house
    cur.execute("""
        SELECT p.product_instance_id, h.house_no
        FROM products p
        JOIN houses h ON p.house_id = h.house_id
        WHERE h.unit_id = %s
    """, (unit_dict[selected_unit],))

    house_products = {}
    for pid, house in cur.fetchall():
        house_products.setdefault(house, []).append(pid)

    # ================= ENGINE =================
    results, sla_results, stage_delay_summary = [], [], {}

    for house in house_products:

        pids = house_products[house]
        total_products = len(pids)

        product_finish_times = []
        completed_pairs = 0
        stage_delays = []

        for pid in pids:
            current_pointer = today

            for _, row in activity_df.iterrows():
                stage = row["stage"]
                duration = row["days"]

                data = prod_stage.get((pid, stage), {})

                if data.get("completed"):
                    actual = max(1, (data["end"] - data["start"]).days)
                    delay = actual - duration
                    if delay > 0:
                        stage_delays.append((stage, delay))

                    current_pointer = data["end"]
                    completed_pairs += 1

                elif data.get("in_progress"):
                    current_pointer = data["start"] + timedelta(days=duration)

                else:
                    current_pointer += timedelta(days=duration)

            product_finish_times.append((pid, current_pointer))

        bottleneck_pid, predicted_finish = max(product_finish_times, key=lambda x: x[1])

        # Current stage
        current_stage = "Not Started"
        for _, row in activity_df.iterrows():
            s = row["stage"]
            d = prod_stage.get((bottleneck_pid, s), {})
            if not d.get("completed"):
                current_stage = f"{s} (In Progress)" if d.get("in_progress") else f"{s} (Pending)"
                break

        total_possible = total_products * len(activity_df)
        progress = round((completed_pairs / total_possible) * 100, 1) if total_possible else 0

        remaining_total = max(0, (predicted_finish - today).days)

        delay_reason = "On track"
        if stage_delays:
            worst = max(stage_delays, key=lambda x: x[1])
            delay_reason = f"{worst[0]} delay ({worst[1]}d)"

        # SLA
        sla = config_map.get(house)

        if sla:
            sla_ts = pd.to_datetime(sla).tz_localize("Asia/Kolkata")
            d = (predicted_finish - sla_ts).days

            status = "🟢 Early" if d < 0 else "🟡 On Time" if d == 0 else "🔴 Delay"
            impact = f"Ahead by {abs(d)} days" if d < 0 else "Exact" if d == 0 else f"Miss by {d} days"

            sla_results.append({
                "House": house,
                "Current Stage": current_stage,
                "Progress %": progress,
                "SLA Date": sla_ts.date(),
                "Predicted Finish": predicted_finish.date(),
                "Status": status,
                "Impact": impact
            })

        else:
            results.append({
                "House": house,
                "Current Stage": current_stage,
                "Progress %": progress,
                "Predicted Finish": predicted_finish.date(),
                "Remaining (Total)": f"{remaining_total} days",
                "Delay Reason": delay_reason
            })

        for s, d in stage_delays:
            stage_delay_summary.setdefault(s, {"delay":0,"count":0})
            stage_delay_summary[s]["delay"] += d
            stage_delay_summary[s]["count"] += 1

    # ================= DISPLAY =================
    st.subheader("🚨 SLA Priority Table")
    st.dataframe(pd.DataFrame(sla_results), use_container_width=True)

    st.subheader("🏠 House Intelligence (Non-SLA)")
    st.dataframe(pd.DataFrame(results), use_container_width=True)

    # EARLY WARNING
    st.subheader("🚨 Early Warning")
    early = [r for r in sla_results if "Miss by" in r["Impact"]]
    st.dataframe(pd.DataFrame(early), use_container_width=True) if early else st.success("No risks")

    # BOTTLENECK
    st.subheader("🧠 Bottleneck Insight")
    insight = [{
        "Stage": k,
        "Total Delay": v["delay"],
        "Affected Houses": v["count"]
    } for k,v in stage_delay_summary.items()]

    st.dataframe(pd.DataFrame(insight), use_container_width=True) if insight else st.info("No delays")

    # TREND
    total_delay_today = sum(v["delay"] for v in stage_delay_summary.values())

    cur.execute("DELETE FROM delay_trend WHERE date = CURRENT_DATE")
    cur.execute("INSERT INTO delay_trend VALUES (CURRENT_DATE, %s)", (int(total_delay_today),))
    conn.commit()

    trend_df = pd.read_sql("SELECT * FROM delay_trend ORDER BY date", conn)

    st.subheader("📈 Delay Trend")
    if not trend_df.empty:
        st.line_chart(trend_df.set_index("date"))
    else:
        st.info("No data yet")
