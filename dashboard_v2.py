import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


def show_dashboard_v2(conn, cur):

    tz = ZoneInfo("Asia/Kolkata")
    today = datetime.now(tz)

    st.markdown("""
    <h1 style='font-size:38px; font-weight:800;'>🧠 OPERAFLOW CPMO CONTROL TOWER</h1>
    <p style='color:gray; font-size:16px;'>Executive Factory Monitoring • Forecast Risk • Delivery Intelligence</p>
    """, unsafe_allow_html=True)

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

    # ================= CURRENT LIVE PRODUCT STATUS =================
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

    # ================= BOTTLENECK =================
    stage_wip = progress_df.groupby("stage")["total"].sum().to_dict()
    bottleneck_stage = max(stage_wip, key=stage_wip.get)

    # ================= BUILD HOUSE INTELLIGENCE =================
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

        progress = ((current_seq - 1) / len(act)) + (stage_ratio / len(act))
        progress_percent = round(progress * 100, 1)

        stage_row = act[act["stage"] == current_stage]
        stage_duration = int(stage_row["days"].values[0]) if not stage_row.empty else 1

        rem_stage = max(0, stage_duration - stage_days_elapsed)
        if stage_ratio > 0:
            rem_stage = max(0, int(rem_stage * (1 - stage_ratio)))

        remaining_stages = act[act["seq"] >= current_seq]
        downstream_duration = int(remaining_stages["days"].sum())

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

        reason = "On Track"
        severity = "Low"

        if stage_days_elapsed > stage_duration and stage_ratio < 0.5:
            reason = "Stage Stagnation"
            severity = "High"
        elif current_stage == bottleneck_stage:
            reason = "Bottleneck Queue"
            severity = "Medium"
        elif velocity < 1:
            reason = "Low Production Rate"
            severity = "Medium"
        elif delay_days > 0:
            reason = "SLA Miss Risk"
            severity = "High"

        intel.append({
            "House": house,
            "Current Stage": current_stage,
            "Progress %": progress_percent,
            "Predicted Finish": predicted_finish.date(),
            "Delay Days": delay_days,
            "Delay Reason": reason,
            "Severity": severity
        })

    intel_df = pd.DataFrame(intel)

    # ================= KPI STRIP =================
    total_houses = len(intel_df)
    on_track = len(intel_df[intel_df["Delay Reason"] == "On Track"])
    delayed = len(intel_df[intel_df["Delay Days"] > 0])
    at_risk = len(intel_df[intel_df["Severity"] != "Low"])
    dispatch_week = len(intel_df[pd.to_datetime(intel_df["Predicted Finish"]) <= pd.Timestamp(today.date() + timedelta(days=7))])
    avg_delay = round(intel_df["Delay Days"].clip(lower=0).mean(), 1)

    c1,c2,c3,c4,c5,c6,c7 = st.columns(7)
    c1.metric("🏠 Total Houses", total_houses)
    c2.metric("🟢 On Track", on_track)
    c3.metric("🔴 Delayed", delayed)
    c4.metric("⚠️ At Risk", at_risk)
    c5.metric("🚚 Dispatch 7D", dispatch_week)
    c6.metric("🚧 Bottleneck", bottleneck_stage)
    c7.metric("📉 Avg Delay", avg_delay)

    st.markdown("---")

    # ================= VISUAL ROW =================
    v1,v2,v3 = st.columns(3)

    with v1:
        sev = intel_df["Severity"].value_counts().reset_index()
        sev.columns = ["Severity","Count"]
        fig1 = px.pie(sev, names="Severity", values="Count", hole=0.55, title="Delay Severity")
        st.plotly_chart(fig1, use_container_width=True)

    with v2:
        cong = intel_df["Current Stage"].value_counts().reset_index()
        cong.columns = ["Stage","Count"]
        fig2 = px.bar(cong, x="Stage", y="Count", title="Stage Congestion")
        st.plotly_chart(fig2, use_container_width=True)

    with v3:
        trend = intel_df.groupby("Predicted Finish").size().reset_index(name="Count")
        fig3 = px.line(trend, x="Predicted Finish", y="Count", markers=True, title="Forecast Dispatch Trend")
        st.plotly_chart(fig3, use_container_width=True)

    # ================= CRITICAL HOUSES =================
    st.markdown("## 🚨 Critical Houses Requiring Action")
    critical = intel_df.sort_values(["Severity","Delay Days"], ascending=[False,False]).head(15)
    st.dataframe(critical, use_container_width=True)

    # ================= HEATMAP =================
    st.markdown("## 🌡️ Bottleneck Heatmap")
    heat = intel_df.groupby(["Current Stage","Severity"]).size().reset_index(name="Count")
    fig4 = px.density_heatmap(heat, x="Current Stage", y="Severity", z="Count", text_auto=True)
    st.plotly_chart(fig4, use_container_width=True)

    # ================= BASELINE VS FORECAST =================
    st.markdown("## 📅 Baseline vs Forecast Slippage")
    slip = intel_df[intel_df["Delay Days"] != 0].copy()
    if not slip.empty:
        fig5 = px.bar(slip.head(10), x="Delay Days", y="House", orientation="h",
                      title="Top Forecast Slippages")
        st.plotly_chart(fig5, use_container_width=True)
    else:
        st.success("No major slippages")

    # ================= MANAGEMENT INTERVENTION =================
    st.markdown("## 🧠 Today's Management Intervention")

    actions = []

    if bottleneck_stage == "Design & Engineering":
        actions.append("🚧 Increase Design & Engineering manpower immediately to release upstream congestion.")
    if delayed > 0:
        actions.append(f"🔴 {delayed} houses are already beyond SLA forecast. Escalation required.")
    if at_risk > 10:
        actions.append(f"⚠️ {at_risk} houses are operationally at risk. Supervisory intervention needed.")
    if dispatch_week < 5:
        actions.append("🚚 Dispatch pipeline weak for next 7 days. Review final assembly and dispatch readiness.")
    if avg_delay > 3:
        actions.append("📉 Average delay is rising above control threshold. Conduct production recovery review.")

    if not actions:
        actions.append("🟢 Factory operating within stable control limits.")

    for a in actions:
        st.info(a)
