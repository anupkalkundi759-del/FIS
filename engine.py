def run_engine(conn, cur):
    import streamlit as st
    import pandas as pd
    from datetime import timedelta
    from zoneinfo import ZoneInfo
    from pandas import Timestamp

    st.title("⚙️ Scheduling Intelligence Engine")

    try:
        conn.rollback()
    except:
        pass

    IST = ZoneInfo("Asia/Kolkata")
    today = Timestamp.now(tz=IST)

    def to_ist(series):
        s = pd.to_datetime(series, utc=True)
        return s.dt.tz_convert(IST)

    # ─────────────────────────────
    # ACTIVITY MASTER
    # ─────────────────────────────
    cur.execute("""
        SELECT activity_name, sequence_order, duration_days
        FROM activity_master
        ORDER BY sequence_order
    """)
    activity_df = pd.DataFrame(
        cur.fetchall(), columns=["stage", "seq", "days"]
    )

    if activity_df.empty:
        st.error("No activity master found.")
        return

    activity_df["days"] = activity_df["days"].astype(int)

    # ─────────────────────────────
    # PROJECT / UNIT
    # ─────────────────────────────
    cur.execute("SELECT project_id, project_name FROM projects")
    proj = {p[1]: p[0] for p in cur.fetchall()}
    project = st.selectbox("Project", list(proj.keys()))

    cur.execute("SELECT unit_id, unit_name FROM units WHERE project_id=%s", (proj[project],))
    unit_map = {u[1]: u[0] for u in cur.fetchall()}
    unit = st.selectbox("Unit", list(unit_map.keys()))

    unit_id = unit_map[unit]

    # ─────────────────────────────
    # SLA CONFIG
    # ─────────────────────────────
    cur.execute("SELECT house_no, sla_date FROM house_config")
    config_map = {r[0]: r[1] for r in cur.fetchall()}

    # ─────────────────────────────
    # PRODUCTS
    # ─────────────────────────────
    cur.execute("""
        SELECT p.product_instance_id, h.house_no
        FROM products p
        JOIN houses h ON p.house_id = h.house_id
        WHERE h.unit_id = %s
    """, (unit_id,))
    
    prod_map = {}
    for pid, house in cur.fetchall():
        prod_map.setdefault(house, []).append(pid)

    all_houses = list(prod_map.keys())

    # ─────────────────────────────
    # PRODUCT STAGE DATA
    # ─────────────────────────────
    cur.execute("""
        SELECT
            p.product_instance_id,
            s.stage_name,
            t.status,
            MIN(t.timestamp),
            MAX(t.timestamp)
        FROM tracking_log t
        JOIN products p ON t.product_instance_id = p.product_instance_id
        JOIN stages s ON s.stage_id = t.stage_id
        JOIN houses h ON p.house_id = h.house_id
        WHERE h.unit_id = %s
        GROUP BY p.product_instance_id, s.stage_name, t.status
    """, (unit_id,))

    raw = pd.DataFrame(cur.fetchall(), columns=["pid","stage","status","start","end"])

    if not raw.empty:
        raw["start"] = to_ist(raw["start"])
        raw["end"] = to_ist(raw["end"])

    product_stage_map = {}

    for _, r in raw.iterrows():
        key = (r["pid"], r["stage"])
        if key not in product_stage_map:
            product_stage_map[key] = {"completed": False, "in_progress": False, "start": None, "end": None}

        if r["status"] == "Completed":
            product_stage_map[key]["completed"] = True
            product_stage_map[key]["start"] = r["start"]
            product_stage_map[key]["end"] = r["end"]

        elif r["status"] == "In Progress":
            product_stage_map[key]["in_progress"] = True
            product_stage_map[key]["start"] = r["start"]

    # ─────────────────────────────
    # ENGINE
    # ─────────────────────────────
    sla_results = []
    non_sla_results = []
    bottleneck_summary = {}

    for house in all_houses:

        product_ids = prod_map[house]

        product_finish_times = []
        product_stage_tracker = {}
        completed_pairs = 0

        for pid in product_ids:

            current_pointer = today

            for _, act in activity_df.iterrows():
                stage = act["stage"]
                duration = int(act["days"])

                data = product_stage_map.get((pid, stage), {})

                if data.get("completed"):
                    current_pointer = data["end"]
                    completed_pairs += 1

                elif data.get("in_progress"):
                    current_pointer = data["start"] + timedelta(days=duration)

                else:
                    current_pointer = current_pointer + timedelta(days=duration)

                product_stage_tracker[pid] = stage

            product_finish_times.append((pid, current_pointer))

        # TRUE FINISH
        bottleneck_pid, predicted_finish = max(product_finish_times, key=lambda x: x[1])

        # PROGRESS
        total_possible = len(product_ids) * len(activity_df)
        progress = round((completed_pairs / total_possible) * 100, 1) if total_possible else 0

        # CURRENT STAGE (bottleneck product)
        current_stage = "Not Started"
        for _, act in activity_df.iterrows():
            s = act["stage"]
            data = product_stage_map.get((bottleneck_pid, s), {})
            if not data.get("completed"):
                if data.get("in_progress"):
                    current_stage = f"{s} (In Progress)"
                else:
                    current_stage = f"{s} (Pending)"
                break

        # STAGE REMAINING
        stage_name = current_stage.split(" (")[0]
        act_row = activity_df[activity_df["stage"] == stage_name]

        stage_remaining = "—"
        if not act_row.empty:
            duration = int(act_row["days"].values[0])
            data = product_stage_map.get((bottleneck_pid, stage_name), {})

            if data.get("in_progress"):
                expected = data["start"] + timedelta(days=duration)
                rem = (expected - today).days

                stage_remaining = (
                    "Due today" if rem == 0 else
                    f"{rem} days" if rem > 0 else
                    f"Overdue {abs(rem)}d"
                )

            elif data.get("completed"):
                stage_remaining = "Completed"
            else:
                stage_remaining = "Pending"

        # TOTAL REMAINING
        remaining_total = max(0, (predicted_finish - today).days)

        # BOTTLENECK
        bottleneck_summary.setdefault(stage_name, 0)
        bottleneck_summary[stage_name] += 1

        delay_reason = f"Bottleneck at {stage_name}"

        # SLA
        sla_raw = config_map.get(house)

        if sla_raw:
            sla_ts = Timestamp(sla_raw).tz_localize(IST)
            delta = (predicted_finish - sla_ts).days

            if delta > 0:
                status = "🔴 Delay"
                impact = f"Miss by {delta} days"
            elif delta == 0:
                status = "🟡 On Time"
                impact = "Exact"
            else:
                status = "🟢 Early"
                impact = f"Ahead {abs(delta)}d"

            sla_results.append({
                "House": house,
                "Current Stage": current_stage,
                "Progress %": progress,
                "SLA Date": sla_ts.date(),
                "Predicted Finish": predicted_finish.date(),
                "Status": status,
                "Impact": impact,
            })

        else:
            non_sla_results.append({
                "House": house,
                "Current Stage": current_stage,
                "Progress %": progress,
                "Predicted Finish": predicted_finish.date(),
                "Actual Finish": "Completed" if progress == 100 else "In Progress",
                "Remaining (Stage)": stage_remaining,
                "Remaining (Total)": f"{remaining_total} days",
                "Delay Reason": delay_reason,
            })

    # ─────────────────────────────
    # DISPLAY
    # ─────────────────────────────
    st.subheader("🚨 SLA Priority")
    if sla_results:
        df = pd.DataFrame(sla_results)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("No SLA houses")

    st.subheader("🏠 House Intelligence")
    st.dataframe(pd.DataFrame(non_sla_results), use_container_width=True)

    # EARLY WARNING
    st.subheader("🚨 Early Warning")
    early = [r for r in sla_results if "Miss" in r["Impact"]]

    if early:
        st.dataframe(pd.DataFrame(early), use_container_width=True)
    else:
        st.success("No risks")

    # BOTTLENECK INSIGHT
    st.subheader("🧠 Bottleneck Insight")

    if bottleneck_summary:
        df = pd.DataFrame([
            {"Stage": k, "Bottleneck Count": v}
            for k, v in bottleneck_summary.items()
        ]).sort_values("Bottleneck Count", ascending=False)

        st.dataframe(df, use_container_width=True)
    else:
        st.info("No bottlenecks yet")
