def run_engine(conn, cur):
    import streamlit as st
    import pandas as pd
    from datetime import timedelta
    from zoneinfo import ZoneInfo
    from pandas import Timestamp

    st.title("⚙️ Scheduling Intelligence Engine")

    # Clear any failed transaction from a previous run
    try:
        conn.rollback()
    except Exception:
        pass

    IST = ZoneInfo("Asia/Kolkata")
    today = Timestamp.now(tz=IST)

    # ─────────────────────────────────────────────
    # HELPER — safe tz conversion
    # ─────────────────────────────────────────────
    def to_ist(series):
        """Convert a naive or UTC-aware datetime series to IST."""
        s = pd.to_datetime(series, utc=True)          # treats naive as UTC
        return s.dt.tz_convert(IST)

    # ─────────────────────────────────────────────
    # ENSURE TABLES EXIST
    # ─────────────────────────────────────────────
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS house_config (
                house_no TEXT PRIMARY KEY,
                sla_date DATE
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS delay_trend (
                date        DATE PRIMARY KEY,
                total_delay INT
            )
        """)
        conn.commit()
    except Exception as e:
        conn.rollback()
        st.error(f"Failed to create required tables: {e}")
        return

    # ─────────────────────────────────────────────
    # ACTIVITY MASTER
    # ─────────────────────────────────────────────
    cur.execute("""
        SELECT activity_name, sequence_order, duration_days
        FROM activity_master
        ORDER BY sequence_order
    """)
    activity_df = pd.DataFrame(
        cur.fetchall(), columns=["stage", "seq", "days"]
    )
    activity_df["days"] = activity_df["days"].astype(int)
    total_duration = int(activity_df["days"].sum())

    if total_duration == 0:
        st.error("Activity master has no stages or all durations are 0.")
        return

    # ─────────────────────────────────────────────
    # SLA ASSIGNMENT UI
    # ─────────────────────────────────────────────
    st.subheader("⚙️ SLA Assignment")

    c1, c2, c3, c4, c5 = st.columns([2, 2, 2, 2, 1])

    with c1:
        cur.execute("SELECT project_id, project_name FROM projects ORDER BY project_name")
        projects = cur.fetchall()
        if not projects:
            st.warning("No projects found.")
            return
        project_dict = {p[1]: p[0] for p in projects}
        selected_project = st.selectbox("Project", list(project_dict.keys()))

    with c2:
        cur.execute(
            "SELECT unit_id, unit_name FROM units WHERE project_id = %s",
            (project_dict[selected_project],),
        )
        units = cur.fetchall()
        if not units:
            st.warning("No units for this project.")
            return
        unit_dict = {u[1]: u[0] for u in units}
        selected_unit = st.selectbox("Unit", list(unit_dict.keys()))

    with c3:
        cur.execute(
            "SELECT house_no FROM houses WHERE unit_id = %s",
            (unit_dict[selected_unit],),
        )
        houses = [h[0] for h in cur.fetchall()]
        if not houses:
            st.warning("No houses for this unit.")
            return
        selected_house = st.selectbox("House", houses)

    with c4:
        sla_date = st.date_input("SLA Date")

    with c5:
        st.write("")
        if st.button("Save SLA"):
            if sla_date < today.date():
                st.error("SLA date cannot be in the past.")
            else:
                cur.execute(
                    """
                    INSERT INTO house_config (house_no, sla_date)
                    VALUES (%s, %s)
                    ON CONFLICT (house_no) DO UPDATE SET sla_date = EXCLUDED.sla_date
                    """,
                    (selected_house, sla_date),
                )
                conn.commit()
                st.success("SLA saved ✓")

    # Load all saved SLA configs
    cur.execute("SELECT house_no, sla_date FROM house_config")
    config_map = {r[0]: r[1] for r in cur.fetchall()}

    # ─────────────────────────────────────────────
    # FETCH ALL TRACKING EVENTS — both Completed & In Progress
    # We need In Progress start times to calculate current stage duration
    # ─────────────────────────────────────────────
    cur.execute("""
        SELECT
            h.house_no,
            s.stage_name,
            t.status,
            MIN(t.timestamp) AS first_event,
            MAX(t.timestamp) AS last_event
        FROM products p
        JOIN houses h       ON p.house_id             = h.house_id
        JOIN tracking_log t ON t.product_instance_id  = p.product_instance_id
        JOIN stages s       ON t.stage_id              = s.stage_id
        WHERE h.unit_id = %s
        GROUP BY h.house_no, s.stage_name, t.status
    """, (unit_dict[selected_unit],))

    raw_df = pd.DataFrame(
        cur.fetchall(), columns=["house", "stage", "status", "first_event", "last_event"]
    )
    if not raw_df.empty:
        raw_df["first_event"] = to_ist(raw_df["first_event"])
        raw_df["last_event"]  = to_ist(raw_df["last_event"])

    # Completed stages: start = first Completed event, end = last Completed event
    completed_df = (
        raw_df[raw_df["status"] == "Completed"]
        .rename(columns={"first_event": "start", "last_event": "end"})
        [["house", "stage", "start", "end"]]
        .copy()
        if not raw_df.empty
        else pd.DataFrame(columns=["house", "stage", "start", "end"])
    )

    # In Progress stages: when did this stage start on this house?
    inprogress_df = (
        raw_df[raw_df["status"] == "In Progress"]
        .rename(columns={"first_event": "ip_start"})
        [["house", "stage", "ip_start"]]
        .copy()
        if not raw_df.empty
        else pd.DataFrame(columns=["house", "stage", "ip_start"])
    )

    # ─────────────────────────────────────────────
    # LATEST EVENT PER HOUSE — for current stage label
    # ─────────────────────────────────────────────
    cur.execute("""
        SELECT DISTINCT ON (h.house_no)
            h.house_no,
            s.stage_name,
            t.status,
            t.timestamp
        FROM products p
        JOIN houses h       ON p.house_id             = h.house_id
        JOIN tracking_log t ON t.product_instance_id  = p.product_instance_id
        JOIN stages s       ON t.stage_id              = s.stage_id
        WHERE h.unit_id = %s
        ORDER BY h.house_no, t.timestamp DESC
    """, (unit_dict[selected_unit],))

    latest_df = pd.DataFrame(
        cur.fetchall(), columns=["house", "stage", "status", "time"]
    )
    if not latest_df.empty:
        latest_df["time"] = to_ist(latest_df["time"])
    latest_map = (
        {r["house"]: r for _, r in latest_df.iterrows()}
        if not latest_df.empty else {}
    )

    # ─────────────────────────────────────────────
    # TOTAL UNIQUE PRODUCTS PER HOUSE
    # ─────────────────────────────────────────────
    cur.execute("""
        SELECT h.house_no, COUNT(DISTINCT p.product_instance_id) AS total_products
        FROM houses h
        LEFT JOIN products p ON p.house_id = h.house_id
        WHERE h.unit_id = %s
        GROUP BY h.house_no
    """, (unit_dict[selected_unit],))
    total_products_map = {r[0]: r[1] for r in cur.fetchall()}

    # ─────────────────────────────────────────────
    # COMPLETED PRODUCT COUNT PER (house, stage)
    # ─────────────────────────────────────────────
    cur.execute("""
        SELECT
            h.house_no,
            s.stage_name,
            COUNT(DISTINCT CASE WHEN t.status = 'Completed'
                                THEN t.product_instance_id END) AS completed,
            COUNT(DISTINCT CASE WHEN t.status = 'In Progress'
                                THEN t.product_instance_id END) AS in_progress
        FROM houses h
        LEFT JOIN products p     ON p.house_id             = h.house_id
        LEFT JOIN tracking_log t ON t.product_instance_id  = p.product_instance_id
        LEFT JOIN stages s       ON t.stage_id              = s.stage_id
        WHERE h.unit_id = %s
        GROUP BY h.house_no, s.stage_name
    """, (unit_dict[selected_unit],))
    stage_count_map = {
        (r[0], r[1]): {"completed": r[2] or 0, "in_progress": r[3] or 0}
        for r in cur.fetchall()
    }

    # ─────────────────────────────────────────────
    # ALL HOUSES IN THE UNIT
    # ─────────────────────────────────────────────
    cur.execute(
        "SELECT house_no FROM houses WHERE unit_id = %s ORDER BY house_no",
        (unit_dict[selected_unit],),
    )
    all_houses = [r[0] for r in cur.fetchall()]

    if not all_houses:
        st.warning("No houses found for this unit.")
        return

    # ─────────────────────────────────────────────
    # ENGINE — compute per house
    # ─────────────────────────────────────────────
    sla_results, non_sla_results, stage_delay_summary = [], [], {}

    for house in all_houses:

        total_products = total_products_map.get(house, 0)

        # All completed stages for this house
        h_comp = (
            completed_df[completed_df["house"] == house].copy()
            if not completed_df.empty
            else pd.DataFrame(columns=["house", "stage", "start", "end"])
        )

        # In Progress stages for this house
        h_ip = (
            inprogress_df[inprogress_df["house"] == house].copy()
            if not inprogress_df.empty
            else pd.DataFrame(columns=["house", "stage", "ip_start"])
        )

        # ── Determine current stage correctly ──
        # The "current stage" is the EARLIEST incomplete stage in the activity
        # sequence that still has any products In Progress or not started.
        # This avoids showing a later stage just because one product ran ahead.
        current_stage = "Not Started"
        for _, act in activity_df.iterrows():
            s = act["stage"]
            counts = stage_count_map.get((house, s), {"completed": 0, "in_progress": 0})
            total_completed  = counts["completed"]
            total_inprogress = counts["in_progress"]

            # A stage is "fully done" only if ALL products completed it
            if total_products and total_completed >= total_products:
                continue  # all products done this stage — move to next
            elif total_inprogress > 0 or total_completed > 0:
                # Some products are here — this is the current bottleneck stage
                current_stage = f"{s} (In Progress)"
                break
            else:
                # No products reached this stage yet
                current_stage = f"{s} (Pending)"
                break

        # ── Progress % based on product × stage completion ──
        # Count how many (product, stage) pairs are fully completed
        # out of total possible (products × stages)
        total_possible  = total_products * len(activity_df)
        total_completed_pairs = sum(
            min(stage_count_map.get((house, s), {"completed": 0})["completed"], total_products)
            for s in activity_df["stage"]
        )
        if total_possible > 0:
            progress = min(99.0, round((total_completed_pairs / total_possible) * 100, 1))
        else:
            progress = 0.0

        all_stages_complete = (
            total_possible > 0 and total_completed_pairs >= total_possible
        )
        if all_stages_complete:
            progress = 100.0

        # ── Timeline: walk activity sequence using actual completion times ──
        candidates = []
        if not h_comp.empty:
            candidates.append(h_comp["start"].min())
        if not h_ip.empty:
            candidates.append(h_ip["ip_start"].min())
        project_start   = min(candidates) if candidates else today
        current_pointer = Timestamp(project_start)

        stage_delays = []

        for _, act in activity_df.iterrows():
            stage    = act["stage"]
            duration = int(act["days"])
            planned_finish = current_pointer + timedelta(days=duration)

            s_comp = h_comp[h_comp["stage"] == stage] if not h_comp.empty else pd.DataFrame()
            s_ip   = h_ip[h_ip["stage"] == stage]     if not h_ip.empty   else pd.DataFrame()

            counts        = stage_count_map.get((house, stage), {"completed": 0, "in_progress": 0})
            n_completed   = counts["completed"]
            n_in_progress = counts["in_progress"]

            if total_products and n_completed >= total_products:
                # ALL products completed this stage
                if not s_comp.empty:
                    actual_start = s_comp["start"].iloc[0]
                    actual_end   = s_comp["end"].iloc[0]
                    actual_days  = max(1, (actual_end - actual_start).days)
                    delay = actual_days - duration
                    if delay > 0:
                        stage_delays.append((stage, delay))
                        stage_delay_summary.setdefault(stage, {"delay": 0, "count": 0})
                        stage_delay_summary[stage]["delay"] += delay
                        stage_delay_summary[stage]["count"] += 1
                    current_pointer = actual_start + timedelta(days=actual_days)
                else:
                    current_pointer = planned_finish

            elif n_in_progress > 0 or n_completed > 0:
                # Stage is partially done / in progress — project from start of stage
                if not s_ip.empty:
                    ip_start = s_ip["ip_start"].iloc[0]
                elif not s_comp.empty:
                    ip_start = s_comp["start"].iloc[0]
                else:
                    ip_start = current_pointer

                predicted_stage_finish = ip_start + timedelta(days=duration)
                current_pointer = max(predicted_stage_finish, today)
                # Remaining stages planned from here — stop iterating forward
                break

            else:
                # Stage not yet started — plan forward from current pointer
                current_pointer = planned_finish

        predicted_finish     = current_pointer
        remaining_total_days = max(0, (predicted_finish - today).days)

        # Delay reason
        if stage_delays:
            worst_stage, worst_delay = max(stage_delays, key=lambda x: x[1])
            if worst_delay <= 1:
                delay_reason = f"{worst_stage} — slight delay"
            elif worst_delay <= 3:
                delay_reason = f"{worst_stage} — moderate delay"
            else:
                delay_reason = f"{worst_stage} — backlog ({worst_delay}d)"
        else:
            delay_reason = "On track"

        # ── SLA houses ──
        sla_date_raw = config_map.get(house)
        if sla_date_raw is not None:
            sla_ts = Timestamp(sla_date_raw).tz_localize(IST)
            delta  = (predicted_finish - sla_ts).days

            if delta < 0:
                status = "🟢 Early"
                impact = f"Ahead by {abs(delta)} days"
            elif delta == 0:
                status = "🟡 On Time"
                impact = "Exactly on SLA"
            else:
                status = "🔴 Delay"
                impact = f"Miss by {delta} days"

            sla_results.append({
                "House":             house,
                "Current Stage":     current_stage,
                "Progress %":        progress,
                "SLA Date":          sla_ts.date(),
                "Predicted Finish":  predicted_finish.date(),
                "Status":            status,
                "Impact":            impact,
            })

        # ── Non-SLA houses ──
        else:
            actual_finish_display = (
                h_comp["end"].max().date() if all_stages_complete
                else ("In Progress" if not h_comp.empty or not h_ip.empty else "Not Started")
            )

            # Remaining days for the current active stage
            current_stage_name = current_stage.split(" (")[0]
            act_row    = activity_df[activity_df["stage"] == current_stage_name]
            stage_remaining = "—"

            if not act_row.empty:
                s_ip_cur   = h_ip[h_ip["stage"] == current_stage_name]   if not h_ip.empty   else pd.DataFrame()
                s_comp_cur = h_comp[h_comp["stage"] == current_stage_name] if not h_comp.empty else pd.DataFrame()

                if all_stages_complete:
                    stage_remaining = "Completed"
                elif not s_ip_cur.empty:
                    ip_start    = s_ip_cur["ip_start"].iloc[0]
                    cs_duration = int(act_row["days"].values[0])
                    cs_expected = ip_start + timedelta(days=cs_duration)
                    rem         = (cs_expected - today).days
                    stage_remaining = (
                        "Due today"          if rem == 0
                        else f"{rem} days"   if rem > 0
                        else f"Overdue {abs(rem)}d"
                    )
                elif not s_comp_cur.empty:
                    stage_remaining = "Completed"
                else:
                    stage_remaining = "Pending"

            non_sla_results.append({
                "House":             house,
                "Current Stage":     current_stage,
                "Progress %":        progress,
                "Predicted Finish":  predicted_finish.date(),
                "Actual Finish":     actual_finish_display,
                "Remaining (Stage)": stage_remaining,
                "Remaining (Total)": f"{remaining_total_days} days",
                "Delay Reason":      delay_reason,
            })

    # ─────────────────────────────────────────────
    # DISPLAY — SLA PRIORITY TABLE
    # ─────────────────────────────────────────────
    st.subheader("🚨 SLA Priority Table")
    if sla_results:
        sla_df = pd.DataFrame(sla_results)
        # Sort: delayed first, then by delta days descending
        def sla_sort_key(row):
            if "Miss by" in row["Impact"]:
                return (0, -int(row["Impact"].split()[2]))
            elif "Ahead" in row["Impact"]:
                return (2, 0)
            else:
                return (1, 0)
        sla_df["_sort"] = sla_df.apply(sla_sort_key, axis=1)
        sla_df = sla_df.sort_values("_sort").drop(columns=["_sort"])
        st.dataframe(sla_df, use_container_width=True)
    else:
        st.info("No SLA-assigned houses in this unit.")

    # ─────────────────────────────────────────────
    # DISPLAY — NON-SLA HOUSE INTELLIGENCE
    # ─────────────────────────────────────────────
    st.subheader("🏠 House Intelligence (Non-SLA)")
    if non_sla_results:
        st.dataframe(pd.DataFrame(non_sla_results), use_container_width=True)
    else:
        st.info("All houses in this unit have an SLA assigned.")

    # ─────────────────────────────────────────────
    # DISPLAY — EARLY WARNING (SLA misses only)
    # ─────────────────────────────────────────────
    st.subheader("🚨 Early Warning")
    early = [
        {
            "House":      r["House"],
            "Issue":      "Will miss SLA",
            "Miss by (days)": int(r["Impact"].split()[2]),
            "Predicted":  r["Predicted Finish"],
            "SLA":        r["SLA Date"],
        }
        for r in sla_results
        if "Miss by" in r["Impact"]
    ]
    if early:
        early_df = pd.DataFrame(early).sort_values("Miss by (days)", ascending=False)
        st.dataframe(early_df, use_container_width=True)
    else:
        st.success("✅ No SLA risks detected.")

    # ─────────────────────────────────────────────
    # DISPLAY — STAGE DELAY INSIGHT (bottlenecks)
    # ─────────────────────────────────────────────
    st.subheader("🧠 Stage Delay Insight")
    if stage_delay_summary:
        insight_df = pd.DataFrame([
            {
                "Stage":            k,
                "Total Delay (days)": v["delay"],
                "Affected Houses":  v["count"],
                "Avg Delay (days)": round(v["delay"] / v["count"], 1),
            }
            for k, v in stage_delay_summary.items()
        ]).sort_values("Total Delay (days)", ascending=False)
        st.dataframe(insight_df, use_container_width=True)
    else:
        st.info("No stage delays detected yet.")

    # ─────────────────────────────────────────────
    # DELAY TREND — upsert today, trim to 90 days
    # ─────────────────────────────────────────────
    total_delay_today = sum(v["delay"] for v in stage_delay_summary.values())

    try:
        # Ensure the unique constraint exists (table may have been created
        # without PRIMARY KEY in a previous deployment)
        cur.execute("""
            ALTER TABLE delay_trend
            ADD CONSTRAINT delay_trend_date_pk PRIMARY KEY (date)
        """)
        conn.commit()
    except Exception:
        # Constraint already exists — that's fine
        conn.rollback()

    try:
        cur.execute("DELETE FROM delay_trend WHERE date = CURRENT_DATE")
        cur.execute(
            "INSERT INTO delay_trend (date, total_delay) VALUES (CURRENT_DATE, %s)",
            (int(total_delay_today),)
        )
        cur.execute("DELETE FROM delay_trend WHERE date < CURRENT_DATE - INTERVAL '90 days'")
        conn.commit()
    except Exception as e:
        conn.rollback()
        st.warning(f"Could not save delay trend: {e}")

    trend_df = pd.read_sql(
        "SELECT date, total_delay FROM delay_trend ORDER BY date", conn
    )

    st.subheader("📈 Delay Trend (last 90 days)")
    if not trend_df.empty:
        trend_df = trend_df.set_index("date")
        st.line_chart(trend_df)
    else:
        st.info("No trend data yet.")
