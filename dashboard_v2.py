import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


def show_dashboard_v2(conn, cur):

    tz = ZoneInfo("Asia/Kolkata")
    today = datetime.now(tz)

    st.title("📊 Factory Intelligence Dashboard")

    # ================= MASTER COUNTS =================
    cur.execute("SELECT COUNT(*) FROM projects")
    total_projects = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM units")
    total_units = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM houses")
    total_houses = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM products")
    total_products = cur.fetchone()[0]

    # ================= LIVE PRODUCT STATUS ONLY =================
    cur.execute("""
        WITH latest_status AS (
            SELECT
                product_instance_id,
                status,
                ROW_NUMBER() OVER (
                    PARTITION BY product_instance_id
                    ORDER BY timestamp DESC
                ) AS rn
            FROM tracking_log
        )
        SELECT
            COUNT(*) FILTER (WHERE status = 'Completed'),
            COUNT(*) FILTER (WHERE status = 'Dispatched'),
            COUNT(*) FILTER (WHERE status = 'In Progress')
        FROM latest_status
        WHERE rn = 1
    """)
    completed, dispatched, in_progress = cur.fetchone()

    completed = completed or 0
    dispatched = dispatched or 0
    in_progress = in_progress or 0
    pending = max(0, total_products - (completed + dispatched + in_progress))

    # ================= ACTIVITY MASTER =================
    act = pd.read_sql("""
        SELECT activity_name, sequence_order, duration_days
        FROM activity_master
        ORDER BY sequence_order
    """, conn)

    if act.empty:
        st.warning("No activity master found")
        return

    act.columns = ["stage", "seq", "days"]

    # ================= SLA MAP =================
    try:
        cur.execute("SELECT house_no, sla_date FROM house_config")
        config_map = {r[0]: r[1] for r in cur.fetchall()}
    except:
        config_map = {}

    # ================= HOUSE START =================
    start_df = pd.read_sql("""
        SELECT h.house_no, MIN(t.timestamp) as start
        FROM houses h
        JOIN products p ON p.house_id = h.house_id
        JOIN tracking_log t ON t.product_instance_id = p.product_instance_id
        GROUP BY h.house_no
    """, conn)

    if not start_df.empty:
        start_df["start"] = pd.to_datetime(start_df["start"], utc=True).dt.tz_convert(tz)
    start_map = dict(zip(start_df["house_no"], start_df["start"])) if not start_df.empty else {}

    # ================= CURRENT LIVE STATUS =================
    progress_df = pd.read_sql("""
        WITH latest_stage AS (
            SELECT 
                t.product_instance_id,
                t.stage_id,
                t.status,
                t.timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY t.product_instance_id
                    ORDER BY t.timestamp DESC
                ) AS rn
            FROM tracking_log t
        ),
        current_products AS (
            SELECT
                h.house_no,
                p.product_instance_id,
                COALESCE(s.stage_name, 'Not Started') AS stage,
                ls.status,
                ls.timestamp
            FROM houses h
            LEFT JOIN products p ON p.house_id = h.house_id
            LEFT JOIN latest_stage ls
                ON p.product_instance_id = ls.product_instance_id
                AND ls.rn = 1
            LEFT JOIN stages s
                ON ls.stage_id = s.stage_id
        )
        SELECT
            house_no,
            stage,
            COUNT(product_instance_id) AS total,
            COUNT(CASE WHEN status='Completed' THEN 1 END) AS completed,
            MIN(timestamp) AS first_seen_stage
        FROM current_products
        GROUP BY house_no, stage
        ORDER BY house_no
    """, conn)

    if progress_df.empty:
        st.warning("No tracking data")
        return

    progress_df["first_seen_stage"] = pd.to_datetime(progress_df["first_seen_stage"], utc=True, errors="coerce")
    progress_df["first_seen_stage"] = progress_df["first_seen_stage"].dt.tz_convert(tz)

    # ================= BOTTLENECK EXCLUDING NOT STARTED =================
    stage_wip = progress_df[progress_df["stage"] != "Not Started"].groupby("stage")["total"].sum().to_dict()
    bottleneck_stage = max(stage_wip, key=stage_wip.get) if stage_wip else "No Active Stage"

    # ================= HOUSE INTELLIGENCE =================
    intel = []
    grouped = progress_df.groupby("house_no")

    for house, g in grouped:

        current_stage_row = g.sort_values(["first_seen_stage", "total"], ascending=[False, False]).iloc[0]
        current_stage = current_stage_row["stage"]

        start_date = start_map.get(house, today)
        if pd.isna(start_date):
            start_date = today

        days_elapsed = max(1, (today - start_date).days)

        stage_start = current_stage_row["first_seen_stage"]
        if pd.isna(stage_start):
            stage_start = start_date

        stage_days_elapsed = max(1, (today - stage_start).days)

        current_seq_row = act[act["stage"] == current_stage]
        current_seq = int(current_seq_row["seq"].values[0]) if not current_seq_row.empty else 1

        stage_total = g[g["stage"] == current_stage]["total"].sum()
        stage_completed = g[g["stage"] == current_stage]["completed"].sum()
        stage_ratio = (stage_completed / stage_total) if stage_total > 0 else 0

        stage_row = act[act["stage"] == current_stage]
        stage_duration = int(stage_row["days"].values[0]) if not stage_row.empty else 1

        remaining_stages = act[act["seq"] >= current_seq]
        downstream_duration = int(remaining_stages["days"].sum())

        progress = ((current_seq - 1) / len(act)) + (stage_ratio / len(act))
        progress_percent = round(progress * 100, 1)

        velocity = progress_percent / days_elapsed if days_elapsed > 0 else 0

        if velocity > 0:
            efficiency = min(1.8, max(0.4, velocity / 10))
            remaining_total_days = max(0, int((downstream_duration - stage_days_elapsed) / efficiency))
        else:
            remaining_total_days = max(0, downstream_duration - stage_days_elapsed)

        predicted_finish = today + timedelta(days=remaining_total_days)

        sla = config_map.get(house)
        delay_days = 0

        if sla:
            expected = pd.to_datetime(sla).tz_localize(tz)
            delay_days = (predicted_finish - expected).days

        issue = "On Track"

        if stage_days_elapsed > stage_duration and stage_ratio < 0.5:
            issue = "Stage Stagnation"
        elif current_stage == bottleneck_stage:
            issue = "Bottleneck Queue"
        elif velocity < 1:
            issue = "Low Production Rate"
        elif delay_days > 0:
            issue = "SLA Miss Risk"

        intel.append({
            "House": house,
            "Current Stage": current_stage,
            "Predicted Finish": predicted_finish.date(),
            "Delay Days": delay_days,
            "Issue": issue
        })

    intel_df = pd.DataFrame(intel)

    # ================= KPI ROW 1 =================
    k1,k2,k3,k4 = st.columns(4)
    k1.metric("Projects", total_projects)
    k2.metric("Units", total_units)
    k3.metric("Houses", total_houses)
    k4.metric("Products", total_products)

    # ================= KPI ROW 2 =================
    k5,k6,k7,k8 = st.columns(4)
    k5.metric("Completed", completed)
    k6.metric("Dispatched", dispatched)
    k7.metric("In Progress", in_progress)
    k8.metric("Pending", pending)

    # ================= KPI ROW 3 =================
    critical_houses = len(intel_df[intel_df["Issue"] != "On Track"])
    delayed_houses = len(intel_df[intel_df["Delay Days"] > 0])
    dispatch_ready = len(intel_df[pd.to_datetime(intel_df["Predicted Finish"]) <= pd.Timestamp(today.date() + timedelta(days=7))])

    k9,k10,k11,k12 = st.columns(4)
    k9.metric("Critical Houses", critical_houses)
    k10.metric("Delayed Houses", delayed_houses)
    k11.metric("Near Dispatch (7D)", dispatch_ready)
    k12.metric("Bottleneck", bottleneck_stage)

    st.markdown("---")

    # ================= STAGE SUMMARY =================
    st.subheader("🏭 Stage Summary")
    stage_summary = progress_df.groupby("stage")["total"].sum().reset_index()
    stage_summary.columns = ["Stage", "Products in Stage"]
    st.dataframe(stage_summary, use_container_width=True)

    # ================= CRITICAL HOUSE TABLE =================
    st.subheader("🚨 Critical Houses Requiring Attention")
    critical_table = intel_df[intel_df["Issue"] != "On Track"].sort_values(["Delay Days"], ascending=False)
    st.dataframe(critical_table, use_container_width=True)

    # ================= DISPATCH READY =================
    st.subheader("🚚 Houses Likely for Dispatch Soon")
    dispatch_table = intel_df[pd.to_datetime(intel_df["Predicted Finish"]) <= pd.Timestamp(today.date() + timedelta(days=7))]
    st.dataframe(dispatch_table, use_container_width=True)

    # ================= MANAGEMENT NOTES =================
    st.subheader("🧠 Management Notes")

    notes = []

    if bottleneck_stage != "No Active Stage":
        notes.append(f"🚧 Highest live work congestion currently at {bottleneck_stage}.")
    if critical_houses > 0:
        notes.append(f"⚠️ {critical_houses} houses require supervisory review.")
    if delayed_houses > 0:
        notes.append(f"🔴 {delayed_houses} houses are beyond SLA forecast.")
    if dispatch_ready < 5:
        notes.append("🚚 Dispatch pipeline is weak for upcoming 7 days.")
    if not notes:
        notes.append("🟢 Factory is operating within stable limits.")

    for n in notes:
        st.info(n)
