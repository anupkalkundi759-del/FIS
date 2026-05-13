def run_engine(conn, cur):
    import streamlit as st
    import pandas as pd
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    st.title("⚙️ OperaFlow Predictive Scheduling & EVM Intelligence Engine")

    tz = ZoneInfo("Asia/Kolkata")
    today = datetime.now(tz)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS evm_global_quarter_plan (
            id INTEGER PRIMARY KEY DEFAULT 1,
            baseline_start_date DATE,
            target_days INTEGER DEFAULT 0,
            buffer_days INTEGER DEFAULT 0,
            CHECK (id = 1)
        )
    """)
    conn.commit()

    top1, top2, top3 = st.columns(3)

    with top1:
        cur.execute("SELECT project_id, project_name FROM projects ORDER BY project_name")
        projects = cur.fetchall()
        project_dict = {p[1]: p[0] for p in projects}
        selected_project = st.selectbox("Select Project", ["ALL"] + list(project_dict.keys()), key="eng_proj")

    with top2:
        if selected_project == "ALL":
            selected_unit = st.selectbox("Select Unit Type", ["ALL"], key="eng_unit")
            project_id = None
            unit_id = None
        else:
            project_id = project_dict[selected_project]
            cur.execute("SELECT unit_id, unit_name FROM units WHERE project_id=%s ORDER BY unit_name", (project_id,))
            units = cur.fetchall()
            unit_dict = {u[1]: u[0] for u in units}
            selected_unit = st.selectbox("Select Unit", ["ALL"] + list(unit_dict.keys()), key="eng_unit")
            unit_id = None if selected_unit == "ALL" else unit_dict[selected_unit]

    if unit_id is not None:
        cur.execute("SELECT house_id, house_no FROM houses WHERE unit_id=%s ORDER BY house_no, house_id", (unit_id,))
    elif project_id is not None:
        cur.execute("""
            SELECT h.house_id, h.house_no
            FROM houses h
            JOIN units u ON h.unit_id=u.unit_id
            WHERE u.project_id=%s
            ORDER BY h.house_no, h.house_id
        """, (project_id,))
    else:
        cur.execute("SELECT house_id, house_no FROM houses ORDER BY house_no, house_id")

    master_house_rows = cur.fetchall()
    master_house_df = pd.DataFrame(master_house_rows, columns=["house_id", "house_no"])

    if master_house_df.empty:
        house_name_map = {}
    else:
        house_name_map = dict(zip(master_house_df["house_id"], master_house_df["house_no"]))

    with top3:
        house_options = [None] + list(house_name_map.keys())
        selected_house_id = st.selectbox(
            "Select Unit Number",
            house_options,
            format_func=lambda x: "ALL" if x is None else str(house_name_map.get(x, x)),
            key="eng_house_id_v2"
        )

    if selected_house_id == "ALL":
        selected_house_id = None
    elif selected_house_id is not None:
        selected_house_id = int(selected_house_id)

    st.markdown("---")
    st.subheader("💰 Project EVM Baseline / Actual Cost / SLA Monitor")

    left_col, right_col = st.columns(2)

    with left_col:
        st.markdown("**Budget at completion**")

        if project_id is not None and unit_id is not None:
            cur.execute("SELECT bac_amount FROM project_evm_baseline WHERE project_id=%s AND unit_id=%s", (project_id, unit_id))
            b = cur.fetchone()
            existing_bac = float(b[0]) if b else 0.0
        else:
            existing_bac = 0.0

        bac_input = st.number_input("Total Planned Project Cost (BAC)", min_value=0.0, value=existing_bac, step=1000.0)

        if st.button("Save BAC"):
            if project_id is not None and unit_id is not None:
                cur.execute("""
                    INSERT INTO project_evm_baseline(project_id, unit_id, bac_amount)
                    VALUES(%s,%s,%s)
                    ON CONFLICT(project_id, unit_id)
                    DO UPDATE SET bac_amount=EXCLUDED.bac_amount
                """, (project_id, unit_id, bac_input))
                conn.commit()
                st.success("BAC Saved")
            else:
                st.warning("BAC can be saved only for specific project + unit")

        st.markdown("**Actual Cost Updater**")

        ac_date = st.date_input("Actual Cost Period Date", key="ac_date")
        ac_amt = st.number_input("Actual Cost This Period", min_value=0.0, step=1000.0, key="ac_amt")
        ac_remark = st.text_input("Remarks", key="ac_rem")

        if st.button("Save Actual Cost"):
            if project_id is not None and unit_id is not None:
                cur.execute("""
                    INSERT INTO evm_cost_log(project_id, unit_id, period_date, actual_cost, remarks)
                    VALUES(%s,%s,%s,%s,%s)
                """, (project_id, unit_id, ac_date, ac_amt, ac_remark))
                conn.commit()
                st.success("Actual Cost Logged")
            else:
                st.warning("Actual cost can be saved only for specific project + unit")

    with right_col:
        st.markdown("**Service Level Agreement**")

        if master_house_df.empty:
            st.info("No houses available for SLA monitor")
        else:
            sla_house_id = st.selectbox(
                "SLA Monitor House",
                list(house_name_map.keys()),
                format_func=lambda x: str(house_name_map.get(x, x)),
                key="sla_house_id_v2"
            )
            sla_house_id = int(sla_house_id)

            sla_date = st.date_input("SLA Date", key="sla_dt")
            sla_priority = st.selectbox("Priority", ["Normal", "High", "Critical"], key="sla_pri")

            if st.button("Save SLA House"):
                cur.execute("""
                    INSERT INTO sla_monitor(house_id, sla_date, priority_level)
                    VALUES(%s,%s,%s)
                    ON CONFLICT(house_id)
                    DO UPDATE SET sla_date=EXCLUDED.sla_date,
                                  priority_level=EXCLUDED.priority_level
                """, (sla_house_id, sla_date, sla_priority))
                conn.commit()
                st.success("SLA Saved")

        st.markdown("---")
        st.markdown("**Quarter Baseline Planning**")

        cur.execute("""
            SELECT baseline_start_date, target_days, buffer_days
            FROM evm_global_quarter_plan
            WHERE id=1
        """)
        qp = cur.fetchone()

        ex_qdate = qp[0] if qp and qp[0] else today.date()
        ex_tdays = int(qp[1]) if qp and qp[1] else 0
        ex_bdays = int(qp[2]) if qp and qp[2] else 0

        quarter_base_date = st.date_input("Quarter Baseline Start Date", value=ex_qdate, key="q_base")
        target_days_input = st.number_input("Core Production Days", min_value=0, value=ex_tdays, step=1, key="q_target")
        buffer_days_input = st.number_input("Buffer / Float Days", min_value=0, value=ex_bdays, step=1, key="q_buffer")

        if st.button("Save Quarter Plan"):
            cur.execute("""
                INSERT INTO evm_global_quarter_plan(id, baseline_start_date, target_days, buffer_days)
                VALUES(1,%s,%s,%s)
                ON CONFLICT(id)
                DO UPDATE SET baseline_start_date=EXCLUDED.baseline_start_date,
                              target_days=EXCLUDED.target_days,
                              buffer_days=EXCLUDED.buffer_days
            """, (quarter_base_date, target_days_input, buffer_days_input))
            conn.commit()
            st.success("Quarter Plan Saved For All Projects / Units")

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

    live_sql = """
        WITH latest_log AS (
            SELECT t.product_instance_id,s.stage_name,t.status,t.timestamp,
                   ROW_NUMBER() OVER(PARTITION BY t.product_instance_id ORDER BY t.timestamp DESC) rn
            FROM tracking_log t
            JOIN stages s ON t.stage_id=s.stage_id
        )
        SELECT h.house_no,h.house_id,p.product_instance_id,
               COALESCE(ll.stage_name,'Not Started') AS stage,
               COALESCE(ll.status,'Pending') AS status,
               ll.timestamp
        FROM houses h
        JOIN products p ON h.house_id=p.house_id
        LEFT JOIN latest_log ll ON p.product_instance_id=ll.product_instance_id AND ll.rn=1
    """
    params = ()

    if unit_id is not None:
        live_sql += " WHERE h.unit_id=%s"
        params = (unit_id,)
    elif project_id is not None:
        live_sql += " WHERE h.unit_id IN (SELECT unit_id FROM units WHERE project_id=%s)"
        params = (project_id,)

    if selected_house_id is not None:
        live_sql += " AND h.house_id=%s" if "WHERE" in live_sql else " WHERE h.house_id=%s"
        params += (selected_house_id,)

    live_sql += " ORDER BY h.house_no, h.house_id"
    cur.execute(live_sql, params)

    live_df = pd.DataFrame(
        cur.fetchall(),
        columns=["house", "house_id", "product_instance_id", "stage", "status", "timestamp"]
    )

    if live_df.empty:
        st.warning("No product data")
        return

    live_df["timestamp"] = pd.to_datetime(live_df["timestamp"], utc=True, errors="coerce").dt.tz_convert(tz)

    start_sql = """
        SELECT h.house_id,h.house_no, MIN(t.timestamp) AS actual_start
        FROM houses h
        JOIN products p ON h.house_id=p.house_id
        JOIN tracking_log t ON p.product_instance_id=t.product_instance_id
    """
    params = ()

    if unit_id is not None:
        start_sql += " WHERE h.unit_id=%s"
        params = (unit_id,)
    elif project_id is not None:
        start_sql += " WHERE h.unit_id IN (SELECT unit_id FROM units WHERE project_id=%s)"
        params = (project_id,)

    if selected_house_id is not None:
        start_sql += " AND h.house_id=%s" if "WHERE" in start_sql else " WHERE h.house_id=%s"
        params += (selected_house_id,)

    start_sql += " GROUP BY h.house_id,h.house_no"
    cur.execute(start_sql, params)

    start_df = pd.DataFrame(cur.fetchall(), columns=["house_id", "house", "actual_start"])
    if not start_df.empty:
        start_df["actual_start"] = pd.to_datetime(start_df["actual_start"], utc=True, errors="coerce").dt.tz_convert(tz)
        start_map = start_df.set_index("house_id")["actual_start"].to_dict()
    else:
        start_map = {}

    finish_sql = """
        WITH latest_dispatch AS (
            SELECT h.house_id,h.house_no,p.product_instance_id,s.stage_name,t.timestamp,
                   ROW_NUMBER() OVER(PARTITION BY p.product_instance_id ORDER BY t.timestamp DESC NULLS LAST) rn
            FROM houses h
            JOIN products p ON h.house_id=p.house_id
            LEFT JOIN tracking_log t ON p.product_instance_id=t.product_instance_id
            LEFT JOIN stages s ON t.stage_id=s.stage_id
    """
    params = ()

    if unit_id is not None:
        finish_sql += " WHERE h.unit_id=%s"
        params = (unit_id,)
    elif project_id is not None:
        finish_sql += " WHERE h.unit_id IN (SELECT unit_id FROM units WHERE project_id=%s)"
        params = (project_id,)

    if selected_house_id is not None:
        finish_sql += " AND h.house_id=%s" if "WHERE" in finish_sql else " WHERE h.house_id=%s"
        params += (selected_house_id,)

    finish_sql += """
        )
        SELECT house_id,
               house_no,
               COUNT(DISTINCT product_instance_id) total_products,
               COUNT(DISTINCT CASE WHEN stage_name='Dispatch' AND rn=1 THEN product_instance_id END) dispatched_products,
               MAX(CASE WHEN stage_name='Dispatch' AND rn=1 THEN timestamp END) actual_finish
        FROM latest_dispatch
        WHERE rn=1
        GROUP BY house_id,house_no
    """
    cur.execute(finish_sql, params)

    finish_df = pd.DataFrame(
        cur.fetchall(),
        columns=["house_id", "house", "total_products", "dispatched_products", "actual_finish"]
    )

    if not finish_df.empty:
        finish_df["actual_finish"] = pd.to_datetime(finish_df["actual_finish"], utc=True, errors="coerce").dt.tz_convert(tz)
        finish_map = finish_df.set_index("house_id").to_dict("index")
    else:
        finish_map = {}

    live_df["earned_pct"] = live_df["stage"].map(lambda x: earned_map.get(x, 0.0))

    evm_live_sql = """
        WITH latest_log AS (
            SELECT t.product_instance_id,s.stage_name,t.status,t.timestamp,
                   ROW_NUMBER() OVER(PARTITION BY t.product_instance_id ORDER BY t.timestamp DESC) rn
            FROM tracking_log t
            JOIN stages s ON t.stage_id=s.stage_id
        )
        SELECT COALESCE(ll.stage_name,'Not Started') AS stage
        FROM houses h
        JOIN products p ON h.house_id=p.house_id
        LEFT JOIN latest_log ll ON p.product_instance_id=ll.product_instance_id AND ll.rn=1
    """
    evm_params = ()

    if unit_id is not None:
        evm_live_sql += " WHERE h.unit_id=%s"
        evm_params = (unit_id,)
    elif project_id is not None:
        evm_live_sql += " WHERE h.unit_id IN (SELECT unit_id FROM units WHERE project_id=%s)"
        evm_params = (project_id,)

    cur.execute(evm_live_sql, evm_params)
    evm_live_df = pd.DataFrame(cur.fetchall(), columns=["stage"])

    if evm_live_df.empty:
        project_actual_progress = 0
    else:
        evm_live_df["earned_pct"] = evm_live_df["stage"].map(lambda x: earned_map.get(x, 0.0))
        project_actual_progress = round(evm_live_df["earned_pct"].mean(), 2)

    cur.execute("""
        SELECT baseline_start_date, target_days, buffer_days
        FROM evm_global_quarter_plan
        WHERE id=1
    """)
    pvrow = cur.fetchone()

    if pvrow and pvrow[0] and pvrow[1] is not None and pvrow[2] is not None:
        base_start = datetime.combine(pvrow[0], datetime.min.time()).replace(tzinfo=tz)
        planned_horizon = int(pvrow[1]) + int(pvrow[2])
        elapsed_batch = max(0, (today - base_start).days)
        project_planned_progress = min(100, round((elapsed_batch / planned_horizon) * 100, 2)) if planned_horizon > 0 else 0
    else:
        project_planned_progress = 0

    if project_id is not None and unit_id is not None:
        cur.execute("SELECT bac_amount FROM project_evm_baseline WHERE project_id=%s AND unit_id=%s", (project_id, unit_id))
        bb = cur.fetchone()
        BAC = float(bb[0]) if bb else 0.0
        cur.execute("SELECT COALESCE(SUM(actual_cost),0) FROM evm_cost_log WHERE project_id=%s AND unit_id=%s", (project_id, unit_id))
        AC = float(cur.fetchone()[0])
    else:
        BAC = 0.0
        AC = 0.0

    PV = round((project_planned_progress / 100) * BAC, 2)
    EV = round((project_actual_progress / 100) * BAC, 2)
    SV = round(EV - PV, 2)
    CV = round(EV - AC, 2)
    SPI = round(EV / PV, 2) if PV > 0 else 0
    CPI = round(EV / AC, 2) if AC > 0 else 0
    EAC = round(BAC / CPI, 2) if CPI > 0 else 0
    ETC_COST = round(EAC - AC, 2) if EAC > 0 else 0

    cur.execute("SELECT stage_name, capacity_per_day FROM stage_capacity")
    cap_map = {x[0]: float(x[1]) for x in cur.fetchall()}

    cur.execute("""
        SELECT s.stage_name,
               COUNT(*)::float / NULLIF(COUNT(DISTINCT DATE(t.timestamp)),0)
        FROM tracking_log t
        JOIN stages s ON t.stage_id=s.stage_id
        WHERE t.status='Completed'
        GROUP BY s.stage_name
    """)
    throughput_map = {x[0]: round(float(x[1]), 2) if x[1] else 0 for x in cur.fetchall()}

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

        bottleneck_rows.append({"Stage": stage, "Queue Load Days": qdays})

    bottleneck_df = pd.DataFrame(bottleneck_rows)

    house_rows = []

    loop_house_ids = [selected_house_id] if selected_house_id is not None else live_df["house_id"].drop_duplicates().tolist()

    for house_id in loop_house_ids:
        sub = live_df[live_df["house_id"] == house_id].copy()

        if sub.empty:
            continue

        house = sub["house"].iloc[0]
        total_products = int(sub["product_instance_id"].nunique())
        actual_prog = round(sub["earned_pct"].mean(), 2) if total_products > 0 else 0

        start_dt = start_map.get(house_id)

        if start_dt is not None and pd.notna(start_dt):
            planned_finish_date = (start_dt + timedelta(days=total_duration)).date()
            planned_finish = planned_finish_date
        else:
            planned_finish_date = None
            planned_finish = "Not Started"

        dispatch_info = finish_map.get(house_id, {})
        dispatched = int(dispatch_info.get("dispatched_products", 0))
        actual_finish_dt = dispatch_info.get("actual_finish", None)

        if total_products == 0:
            predicted_finish = "Awaiting Product Load"
            actual_finish = "Not Finished"
            ettc_days = "-"
            delay_days = "-"
            critical_stage = "No Products"
            health = "Awaiting"

        elif dispatched == total_products:
            predicted_finish = actual_finish_dt.date() if pd.notna(actual_finish_dt) else today.date()
            actual_finish = predicted_finish
            ettc_days = 0
            delay_days = max(0, (predicted_finish - planned_finish_date).days) if planned_finish_date else 0
            critical_stage = "Completed"
            health = "Completed"

        elif start_dt is None or pd.isna(start_dt):
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
            work_remaining_days = max(1, int(round((base_remaining_days * spi_penalty) + q_penalty)))

            ettc_days = work_remaining_days
            predicted_finish_dt = today + timedelta(days=ettc_days)
            predicted_finish = predicted_finish_dt.date()
            actual_finish = "Not Finished"
            delay_days = max(0, (predicted_finish - planned_finish_date).days) if planned_finish_date else 0

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
            "_house_id": house_id,
            "House": house,
            "Total Products": total_products,
            "Earned Progress %": actual_prog,
            "Planned Finish": planned_finish,
            "Predicted Finish": predicted_finish,
            "Actual Finish": actual_finish,
            "ETTC Days": ettc_days,
            "Delay Days": delay_days,
            "Critical Stage": critical_stage,
            "Health": health
        })

    house_df = pd.DataFrame(house_rows)

    cur.execute("""
        SELECT sm.house_id,h.house_no,sm.sla_date,sm.priority_level
        FROM sla_monitor sm
        JOIN houses h ON sm.house_id=h.house_id
    """)
    all_sla = cur.fetchall()
    sla_priority_rows = []

    for r in all_sla:
        house_id = r[0]
        house_no = r[1]
        sla_dt = r[2]
        pri = r[3]

        if house_df.empty:
            continue

        rr = house_df[house_df["_house_id"] == house_id]
        if rr.empty:
            continue

        pred_finish = rr.iloc[0]["Predicted Finish"]
        crit_stage = rr.iloc[0]["Critical Stage"]
        miss = 0 if isinstance(pred_finish, str) else (pred_finish - sla_dt).days
        risk = "Miss Risk" if miss > 0 else ("Tight" if miss == 0 else "Safe")

        sla_priority_rows.append({
            "_house_id": house_id,
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

    if not house_df.empty:
        for _, rr in house_df[house_df["Health"] == "No Movement"].iterrows():
            warning_rows.append({"Alert": f"House {rr['House']} has no recent movement"})
        for _, rr in house_df[house_df["Health"] == "Critical Delay"].iterrows():
            warning_rows.append({"Alert": f"House {rr['House']} critically delayed by {rr['Delay Days']} days"})

    warn_df = pd.DataFrame(warning_rows)

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

    st.subheader("🏠 House Predictive Intelligence")
    display_house_df = house_df.drop(columns=["_house_id"], errors="ignore")
    st.dataframe(display_house_df, use_container_width=True, height=420)

    st.subheader("🎯 SLA Priority Monitor")
    if not sla_df.empty:
        display_sla_df = sla_df.drop(columns=["_house_id"], errors="ignore")
        st.dataframe(display_sla_df.sort_values("Expected Miss Days", ascending=False), use_container_width=True, height=220)
    else:
        st.info("No SLA monitored houses")

    st.subheader("⚠️ Dynamic Early Warning Panel")
    if not warn_df.empty:
        st.dataframe(warn_df, use_container_width=True, height=220)
    else:
        st.success("No critical warnings")
