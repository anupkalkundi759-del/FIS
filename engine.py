def run_engine(conn, cur):
    import streamlit as st
    import pandas as pd
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    st.title("⚙️ OperaFlow Predictive Scheduling & EVM Intelligence Engine")

    tz = ZoneInfo("Asia/Kolkata")
    today = datetime.now(tz)

    # =========================================================
    # PROJECT / UNIT / HOUSE SELECTION WITH ALL OPTION
    # =========================================================
    top1, top2, top3 = st.columns(3)

    with top1:
        cur.execute("SELECT project_id, project_name FROM projects ORDER BY project_name")
        projects = cur.fetchall()
        project_dict = {p[1]: p[0] for p in projects}
        project_options = ["ALL"] + list(project_dict.keys())
        selected_project = st.selectbox("Select Project", project_options, key="eng_proj")

    with top2:
        if selected_project == "ALL":
            selected_unit = st.selectbox("Select Unit", ["ALL"], key="eng_unit")
            project_id = None
            unit_id = None
        else:
            project_id = project_dict[selected_project]
            cur.execute(
                "SELECT unit_id, unit_name FROM units WHERE project_id=%s ORDER BY unit_name",
                (project_id,)
            )
            units = cur.fetchall()
            unit_dict = {u[1]: u[0] for u in units}
            unit_options = ["ALL"] + list(unit_dict.keys())
            selected_unit = st.selectbox("Select Unit", unit_options, key="eng_unit")
            unit_id = None if selected_unit == "ALL" else unit_dict[selected_unit]

    if unit_id is not None:
        cur.execute("SELECT house_id, house_no FROM houses WHERE unit_id=%s ORDER BY house_no", (unit_id,))
    elif project_id is not None:
        cur.execute("""
            SELECT h.house_id, h.house_no
            FROM houses h
            JOIN units u ON h.unit_id = u.unit_id
            WHERE u.project_id=%s
            ORDER BY h.house_no
        """, (project_id,))
    else:
        cur.execute("SELECT house_id, house_no FROM houses ORDER BY house_no")

    master_house_rows = cur.fetchall()
    master_house_dict = {x[1]: x[0] for x in master_house_rows}
    master_house_list = [x[1] for x in master_house_rows]

    with top3:
        selected_house = st.selectbox("Select House", ["ALL"] + master_house_list, key="eng_house")

    st.markdown("---")
    st.subheader("💰 Project EVM Baseline / Actual Cost / SLA Monitor")

    r1c1, r1c2, r1c3, r1c4 = st.columns(4)

    with r1c1:
        if project_id is not None and unit_id is not None:
            cur.execute("""
                SELECT bac_amount
                FROM project_evm_baseline
                WHERE project_id=%s AND unit_id=%s
            """, (project_id, unit_id))
            b = cur.fetchone()
            existing_bac = float(b[0]) if b else 0.0
        else:
            existing_bac = 0.0

        bac_input = st.number_input("Total Planned Project Cost (BAC)", min_value=0.0, value=existing_bac, step=1000.0)

    with r1c2:
        if st.button("Save BAC"):
            if project_id is not None and unit_id is not None:
                cur.execute("""
                    INSERT INTO project_evm_baseline(project_id, unit_id, bac_amount)
                    VALUES(%s, %s, %s)
                    ON CONFLICT(project_id, unit_id)
                    DO UPDATE SET bac_amount = EXCLUDED.bac_amount
                """, (project_id, unit_id, bac_input))
                conn.commit()
                st.success("BAC Saved")
            else:
                st.warning("BAC can be saved only for specific project + unit")

    with r1c3:
        ac_date = st.date_input("Actual Cost Period Date", key="ac_date")

    with r1c4:
        ac_amt = st.number_input("Actual Cost This Period", min_value=0.0, step=1000.0, key="ac_amt")

    r2c1, r2c2, r2c3, r2c4 = st.columns(4)

    with r2c1:
        ac_remark = st.text_input("Remarks", key="ac_rem")

    with r2c2:
        if st.button("Save Actual Cost"):
            if project_id is not None and unit_id is not None:
                cur.execute("""
                    INSERT INTO evm_cost_log(project_id, unit_id, period_date, actual_cost, remarks)
                    VALUES(%s, %s, %s, %s, %s)
                """, (project_id, unit_id, ac_date, ac_amt, ac_remark))
                conn.commit()
                st.success("Actual Cost Logged")
            else:
                st.warning("Actual cost can be saved only for specific project + unit")

    with r2c3:
        hh = [(master_house_dict[h], h) for h in master_house_list]
        house_dict = {x[1]: x[0] for x in hh}
        sla_house = st.selectbox("SLA Monitor House", list(house_dict.keys()), key="sla_house")

    with r2c4:
        sla_date = st.date_input("SLA Date", key="sla_dt")

    r3c1, r3c2 = st.columns(2)

    with r3c1:
        sla_priority = st.selectbox("Priority", ["Normal", "High", "Critical"], key="sla_pri")

    with r3c2:
        if st.button("Save SLA House"):
            cur.execute("""
                INSERT INTO sla_monitor(house_id, sla_date, priority_level)
                VALUES(%s, %s, %s)
                ON CONFLICT(house_id)
                DO UPDATE SET
                    sla_date = EXCLUDED.sla_date,
                    priority_level = EXCLUDED.priority_level
            """, (house_dict[sla_house], sla_date, sla_priority))
            conn.commit()
            st.success("SLA Saved")

    st.markdown("---")

    cur.execute("""
        SELECT activity_name, sequence_order, duration_days
        FROM activity_master
        ORDER BY sequence_order
    """)
    act = cur.fetchall()
    activity_df = pd.DataFrame(act, columns=["stage", "seq", "days"])

    if activity_df.empty:
        st.error("Activity master empty")
        return

    total_duration = int(activity_df["days"].sum())
    activity_df["cum_days"] = activity_df["days"].cumsum()
    activity_df["earned_pct"] = round((activity_df["cum_days"] / total_duration) * 100, 2)

    seq_map = dict(zip(activity_df["stage"], activity_df["seq"]))
    earned_map = dict(zip(activity_df["stage"], activity_df["earned_pct"]))

    total_houses = len(master_house_list)

    if unit_id is not None:
        cur.execute("""
            SELECT COUNT(*)
            FROM products p
            JOIN houses h ON p.house_id = h.house_id
            WHERE h.unit_id=%s
        """, (unit_id,))
    elif project_id is not None:
        cur.execute("""
            SELECT COUNT(*)
            FROM products p
            JOIN houses h ON p.house_id = h.house_id
            JOIN units u ON h.unit_id = u.unit_id
            WHERE u.project_id=%s
        """, (project_id,))
    else:
        cur.execute("SELECT COUNT(*) FROM products")
    total_products_project = cur.fetchone()[0]

    live_sql = """
        WITH latest_log AS (
            SELECT
                t.product_instance_id,
                s.stage_name,
                t.status,
                t.timestamp,
                ROW_NUMBER() OVER(PARTITION BY t.product_instance_id ORDER BY t.timestamp DESC) rn
            FROM tracking_log t
            JOIN stages s ON t.stage_id = s.stage_id
        )
        SELECT
            h.house_no,
            h.house_id,
            p.product_instance_id,
            COALESCE(ll.stage_name, 'Not Started') AS stage,
            COALESCE(ll.status, 'Pending') AS status,
            ll.timestamp
        FROM houses h
        JOIN products p ON h.house_id = p.house_id
        LEFT JOIN latest_log ll
            ON p.product_instance_id = ll.product_instance_id
            AND ll.rn = 1
    """

    params = ()

    if unit_id is not None:
        live_sql += " WHERE h.unit_id=%s"
        params = (unit_id,)
    elif project_id is not None:
        live_sql += " WHERE h.unit_id IN (SELECT unit_id FROM units WHERE project_id=%s)"
        params = (project_id,)

    if selected_house != "ALL":
        if "WHERE" in live_sql:
            live_sql += " AND h.house_no=%s"
            params = params + (selected_house,)
        else:
            live_sql += " WHERE h.house_no=%s"
            params = (selected_house,)

    live_sql += " ORDER BY h.house_no"
    cur.execute(live_sql, params)

    live_df = pd.DataFrame(cur.fetchall(), columns=["house", "house_id", "product_instance_id", "stage", "status", "timestamp"])

    if live_df.empty:
        st.warning("No product data")
        return

    live_df["timestamp"] = pd.to_datetime(live_df["timestamp"], utc=True, errors="coerce").dt.tz_convert(tz)

    start_sql = """
        SELECT
            h.house_no,
            MIN(t.timestamp) AS measure_start
        FROM houses h
        JOIN products p ON h.house_id = p.house_id
        JOIN tracking_log t ON p.product_instance_id = t.product_instance_id
        JOIN stages s ON t.stage_id = s.stage_id
        WHERE s.stage_name='Measurement'
    """

    params = ()

    if unit_id is not None:
        start_sql += " AND h.unit_id=%s"
        params = (unit_id,)
    elif project_id is not None:
        start_sql += " AND h.unit_id IN (SELECT unit_id FROM units WHERE project_id=%s)"
        params = (project_id,)

    if selected_house != "ALL":
        start_sql += " AND h.house_no=%s"
        params = params + (selected_house,)

    start_sql += " GROUP BY h.house_no"
    cur.execute(start_sql, params)

    start_df = pd.DataFrame(cur.fetchall(), columns=["house", "measure_start"])
    start_df["measure_start"] = pd.to_datetime(start_df["measure_start"], utc=True, errors="coerce").dt.tz_convert(tz)
    start_map = dict(zip(start_df["house"], start_df["measure_start"]))

    finish_sql = """
        WITH latest_dispatch AS (
            SELECT
                h.house_no,
                p.product_instance_id,
                s.stage_name,
                t.timestamp,
                ROW_NUMBER() OVER(PARTITION BY p.product_instance_id ORDER BY t.timestamp DESC) rn
            FROM houses h
            JOIN products p ON h.house_id = p.house_id
            LEFT JOIN tracking_log t ON p.product_instance_id = t.product_instance_id
            LEFT JOIN stages s ON t.stage_id = s.stage_id
    """

    params = ()

    if unit_id is not None:
        finish_sql += " WHERE h.unit_id=%s"
        params = (unit_id,)
    elif project_id is not None:
        finish_sql += " WHERE h.unit_id IN (SELECT unit_id FROM units WHERE project_id=%s)"
        params = (project_id,)

    if selected_house != "ALL":
        if "WHERE" in finish_sql:
            finish_sql += " AND h.house_no=%s"
            params = params + (selected_house,)
        else:
            finish_sql += " WHERE h.house_no=%s"
            params = (selected_house,)

    finish_sql += """
        )
        SELECT
            house_no,
            COUNT(product_instance_id) AS total_products,
            COUNT(CASE WHEN stage_name='Dispatch' AND rn=1 THEN 1 END) AS dispatched_products,
            MAX(CASE WHEN stage_name='Dispatch' AND rn=1 THEN timestamp END) AS actual_finish
        FROM latest_dispatch
        WHERE rn=1
        GROUP BY house_no
    """

    cur.execute(finish_sql, params)
    finish_df = pd.DataFrame(cur.fetchall(), columns=["house", "total_products", "dispatched_products", "actual_finish"])
    finish_df["actual_finish"] = pd.to_datetime(finish_df["actual_finish"], utc=True, errors="coerce").dt.tz_convert(tz)
    finish_map = finish_df.set_index("house").to_dict("index")

    live_df["earned_pct"] = live_df["stage"].map(lambda x: earned_map.get(x, 0.0))
    project_actual_progress = round(live_df["earned_pct"].mean(), 2)

    planned_progress_list = []
    for house, start_dt in start_map.items():
        if pd.isna(start_dt):
            continue
        elapsed = max(0, (today - start_dt).days)
        pprog = min(100, round((elapsed / total_duration) * 100, 2))
        planned_progress_list.append(pprog)

    project_planned_progress = round(sum(planned_progress_list) / len(planned_progress_list), 2) if planned_progress_list else 0

    if project_id is not None and unit_id is not None:
        cur.execute("SELECT bac_amount FROM project_evm_baseline WHERE project_id=%s AND unit_id=%s", (project_id, unit_id))
        bb = cur.fetchone()
        BAC = float(bb[0]) if bb else 0.0
        cur.execute("SELECT COALESCE(SUM(actual_cost),0) FROM evm_cost_log WHERE project_id=%s AND unit_id=%s", (project_id, unit_id))
        AC = float(cur.fetchone()[0])
    elif project_id is not None:
        cur.execute("SELECT COALESCE(SUM(bac_amount),0) FROM project_evm_baseline WHERE project_id=%s", (project_id,))
        BAC = float(cur.fetchone()[0])
        cur.execute("SELECT COALESCE(SUM(actual_cost),0) FROM evm_cost_log WHERE project_id=%s", (project_id,))
        AC = float(cur.fetchone()[0])
    else:
        cur.execute("SELECT COALESCE(SUM(bac_amount),0) FROM project_evm_baseline")
        BAC = float(cur.fetchone()[0])
        cur.execute("SELECT COALESCE(SUM(actual_cost),0) FROM evm_cost_log")
        AC = float(cur.fetchone()[0])

    PV = round((project_planned_progress / 100) * BAC, 2)
    EV = round((project_actual_progress / 100) * BAC, 2)
    SV = round(EV - PV, 2)
    CV = round(EV - AC, 2)
    SPI = round(EV / PV, 2) if PV > 0 else 0
    CPI = round(EV / AC, 2) if AC > 0 else 0
    EAC = round(BAC / CPI, 2) if CPI > 0 else 0
    ETC_COST = round(EAC - AC, 2) if EAC > 0 else 0

    cur.execute("SELECT stage_name, capacity_per_day FROM stage_capacity")
    cap_rows = cur.fetchall()
    cap_map = {x[0]: x[1] for x in cap_rows}

    cur.execute("""
        SELECT
            s.stage_name,
            COUNT(*)::float / NULLIF(COUNT(DISTINCT DATE(t.timestamp)), 0) AS avg_exit_day
        FROM tracking_log t
        JOIN stages s ON t.stage_id = s.stage_id
        WHERE t.status='Completed'
        GROUP BY s.stage_name
    """)
    thr_rows = cur.fetchall()
    throughput_map = {x[0]: round(float(x[1]), 2) if x[1] else 0 for x in thr_rows}

    bottleneck_rows = []
    bottleneck_stage = None
    highest_pressure = -1

    for stage in activity_df["stage"].tolist():
        current_wip = int((live_df["stage"] == stage).sum())
        avg_exit = throughput_map.get(stage, 0)
        cap_day = cap_map.get(stage, 0)

        qdays = round(current_wip / avg_exit, 2) if avg_exit > 0 else 0
        pressure = round((current_wip / cap_day) * 100, 1) if cap_day > 0 else 0

        if pressure > highest_pressure and current_wip >= 5:
            highest_pressure = pressure
            bottleneck_stage = stage

        bottleneck_rows.append({
            "Stage": stage,
            "Current WIP": current_wip,
            "Avg Daily Exit": avg_exit,
            "Capacity/Day": cap_day,
            "Queue Load Days": qdays,
            "Pressure %": pressure
        })

    bottleneck_df = pd.DataFrame(bottleneck_rows)

    house_rows = []
    delayed_houses = 0
    completed_houses = 0
    started_houses = len(start_map)

    loop_houses = [selected_house] if selected_house != "ALL" else master_house_list

    for house in loop_houses:
        sub = live_df[live_df["house"] == house].copy()
        total_products = len(sub)
        actual_prog = round(sub["earned_pct"].mean(), 2) if total_products > 0 else 0

        if house in start_map:
            start_dt = start_map[house]
            elapsed = max(0, (today - start_dt).days)
            planned_prog = min(100, round((elapsed / total_duration) * 100, 2))
            planned_finish = (start_dt + timedelta(days=total_duration)).date()
        else:
            start_dt = None
            planned_prog = 0
            planned_finish = "Not Started"

        dispatch_info = finish_map.get(house, {})
        dispatched = dispatch_info.get("dispatched_products", 0)
        actual_finish_dt = dispatch_info.get("actual_finish", None)

        if total_products == 0:
            predicted_finish = "Awaiting Product Load"
            actual_finish = "Not Finished"
            ettc_days = "-"
            delay_days = "-"
            critical_stage = "No Products"
            health = "Awaiting"

        elif total_products > 0 and dispatched == total_products:
            completed_houses += 1
            predicted_finish = actual_finish_dt.date() if pd.notna(actual_finish_dt) else today.date()
            actual_finish = predicted_finish
            ettc_days = 0
            delay_days = max(0, (predicted_finish - planned_finish).days) if planned_finish != "Not Started" else 0
            critical_stage = "Completed"
            health = "Completed"

        elif start_dt is None:
            predicted_finish = "Awaiting Start"
            actual_finish = "Not Finished"
            ettc_days = total_duration
            delay_days = 0
            critical_stage = "Not Started"
            health = "Awaiting"

        else:
            grp = sub.groupby("stage").size().reset_index(name="cnt")
            grp["seq"] = grp["stage"].map(lambda x: seq_map.get(x, 0))
            grp = grp.sort_values(["cnt", "seq"], ascending=[False, False])

            dominant_stage = grp.iloc[0]["stage"]
            critical_stage = dominant_stage

            remaining_work_pct = max(0, 100 - actual_prog)
            base_remaining_days = (remaining_work_pct / 100) * total_duration

            q_penalty = 0
            if dominant_stage in bottleneck_df["Stage"].values:
                q_penalty = float(bottleneck_df[bottleneck_df["Stage"] == dominant_stage]["Queue Load Days"].iloc[0])

            spi_penalty = (1 / SPI) if SPI > 0 and SPI < 1 else 1
            ettc_days = int(round((base_remaining_days * spi_penalty) + q_penalty))

            predicted_finish_dt = today + timedelta(days=ettc_days)
            predicted_finish = predicted_finish_dt.date()
            actual_finish = "Not Finished"
            delay_days = max(0, (predicted_finish - planned_finish).days)

            if delay_days > 0:
                delayed_houses += 1

            stagnant_days = 0
            last_move = sub["timestamp"].max()
            if pd.notna(last_move):
                stagnant_days = (today - last_move).days

            if delay_days > 10:
                health = "Critical Delay"
            elif stagnant_days >= 3:
                health = "No Movement"
            elif dominant_stage == bottleneck_stage:
                health = "Bottleneck Hit"
            elif delay_days > 0:
                health = "Delayed"
            else:
                health = "On Track"

        house_rows.append({
            "House": house,
            "Total Products": total_products,
            "Actual Progress %": actual_prog,
            "Predicted Finish": predicted_finish,
            "Actual Finish": actual_finish,
            "ETTC Days": ettc_days,
            "Delay Days": delay_days,
            "Critical Stage": critical_stage,
            "Health": health
        })

    house_df = pd.DataFrame(house_rows)

    cur.execute("""
        SELECT sm.house_id, h.house_no, sm.sla_date, sm.priority_level
        FROM sla_monitor sm
        JOIN houses h ON sm.house_id = h.house_id
    """)
    all_sla = cur.fetchall()

    sla_priority_rows = []

    for r in all_sla:
        house_no = r[1]
        sla_dt = r[2]
        pri = r[3]

        rr = house_df[house_df["House"] == house_no]
        if rr.empty:
            continue

        pred_finish = rr.iloc[0]["Predicted Finish"]
        crit_stage = rr.iloc[0]["Critical Stage"]

        miss = 0 if isinstance(pred_finish, str) else (pred_finish - sla_dt).days

        if miss > 0:
            risk = "Miss Risk"
        elif miss == 0:
            risk = "Tight"
        else:
            risk = "Safe"

        sla_priority_rows.append({
            "House": house_no,
            "SLA Date": sla_dt,
            "Predicted Finish": pred_finish,
            "Expected Miss Days": miss,
            "Blocking Stage": crit_stage,
            "Priority": pri,
            "Risk": risk
        })

    sla_df = pd.DataFrame(sla_priority_rows)

    warning_rows = []

    if SPI < 1:
        warning_rows.append({"Alert": f"Project SPI below 1 ({SPI}) - Schedule Slipping"})
    if CPI > 0 and CPI < 1:
        warning_rows.append({"Alert": f"Project CPI below 1 ({CPI}) - Cost Burn Higher Than Earned"})
    if bottleneck_stage:
        warning_rows.append({"Alert": f"Critical Bottleneck Detected at {bottleneck_stage}"})

    stagnant = house_df[house_df["Health"] == "No Movement"]
    for _, rr in stagnant.iterrows():
        warning_rows.append({"Alert": f"House {rr['House']} has no recent movement"})

    critical_delay = house_df[house_df["Health"] == "Critical Delay"]
    for _, rr in critical_delay.iterrows():
        warning_rows.append({"Alert": f"House {rr['House']} critically delayed by {rr['Delay Days']} days"})

    warn_df = pd.DataFrame(warning_rows)

    cur.execute("""
        SELECT
            s.stage_name,
            COUNT(*) AS entered,
            COUNT(CASE WHEN t.status='Completed' THEN 1 END) AS completed
        FROM tracking_log t
        JOIN stages s ON t.stage_id = s.stage_id
        GROUP BY s.stage_name
        ORDER BY MIN(s.stage_id)
    """)
    flow = cur.fetchall()

    flow_rows = []

    for f in flow:
        entered = int(f[1]) if f[1] else 0
        completed = int(f[2]) if f[2] else 0
        pending_stage = entered - completed

        if pending_stage > 20:
            reason = "Severe Queue"
        elif pending_stage > 5:
            reason = "Pending Build-up"
        elif pending_stage > 0:
            reason = "Under Processing"
        else:
            reason = "Stable"

        flow_rows.append({
            "Stage": f[0],
            "Entered": entered,
            "Completed": completed,
            "Pending in Stage": pending_stage,
            "Reason": reason
        })

    flow_df = pd.DataFrame(flow_rows)

    st.subheader("📈 EVM Executive Control Panel")
    e1, e2, e3, e4, e5 = st.columns(5)
    e6, e7, e8, e9, e10 = st.columns(5)

    e1.metric("BAC", f"₹{BAC:,.0f}")
    e2.metric("PV", f"₹{PV:,.0f}")
    e3.metric("EV", f"₹{EV:,.0f}")
    e4.metric("AC", f"₹{AC:,.0f}")
    e5.metric("SV", f"₹{SV:,.0f}")
    e6.metric("CV", f"₹{CV:,.0f}")
    e7.metric("SPI", SPI)
    e8.metric("CPI", CPI)
    e9.metric("EAC", f"₹{EAC:,.0f}")
    e10.metric("ETC", f"₹{ETC_COST:,.0f}")

    st.subheader("🏭 Live Factory Health Snapshot")
    k1, k2, k3, k4, k5, k6 = st.columns(6)

    k1.metric("Total Houses", total_houses)
    k2.metric("Started Houses", started_houses)
    k3.metric("Completed Houses", completed_houses)
    k4.metric("Delayed Houses", delayed_houses)
    k5.metric("Active Products", total_products_project)
    k6.metric("Bottleneck", bottleneck_stage if bottleneck_stage else "-")

    st.subheader("🏠 House Predictive Intelligence")
    st.dataframe(house_df, use_container_width=True, height=420)

    st.subheader("🎯 SLA Priority Monitor")
    if not sla_df.empty:
        st.dataframe(sla_df.sort_values("Expected Miss Days", ascending=False), use_container_width=True, height=220)
    else:
        st.info("No SLA monitored houses")

    st.subheader("⚠️ Dynamic Early Warning Panel")
    if not warn_df.empty:
        st.dataframe(warn_df, use_container_width=True, height=220)
    else:
        st.success("No critical warnings")

    st.subheader("🔄 Flow Throughput Monitor")
    st.dataframe(flow_df, use_container_width=True, height=250)
