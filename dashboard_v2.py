```python
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


def show_dashboard_v2(conn, cur):

    tz = ZoneInfo("Asia/Kolkata")
    today = datetime.now(tz)

    st.title("📊 Factory Intelligence Dashboard")

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
    total_duration = int(act["days"].sum())
    seq_map = dict(zip(act["stage"], act["seq"]))
    day_map = dict(zip(act["stage"], act["days"]))

    # ================= SLA MAP =================
    try:
        cur.execute("SELECT house_no, sla_date FROM house_config")
        config_map = {r[0]: r[1] for r in cur.fetchall()}
    except:
        config_map = {}

    # ================= MASTER LIVE SNAPSHOT =================
    live_df = pd.read_sql("""
        WITH latest_stage AS (
            SELECT
                t.product_instance_id,
                s.stage_name,
                t.timestamp,
                ROW_NUMBER() OVER(PARTITION BY t.product_instance_id ORDER BY t.timestamp DESC) rn
            FROM tracking_log t
            JOIN stages s ON t.stage_id = s.stage_id
        )
        SELECT
            pr.project_name,
            u.unit_name,
            h.house_no,
            p.product_instance_id,
            COALESCE(ls.stage_name,'Not Started') as stage,
            ls.timestamp
        FROM products p
        JOIN houses h ON p.house_id = h.house_id
        JOIN units u ON h.unit_id = u.unit_id
        JOIN projects pr ON u.project_id = pr.project_id
        LEFT JOIN latest_stage ls ON p.product_instance_id = ls.product_instance_id AND ls.rn=1
    """, conn)

    if live_df.empty:
        st.warning("No tracking data")
        return

    live_df["timestamp"] = pd.to_datetime(live_df["timestamp"], utc=True, errors="coerce").dt.tz_convert(tz)

    # ================= HOUSE START =================
    start_df = pd.read_sql("""
        SELECT h.house_no, MIN(t.timestamp) as start
        FROM houses h
        JOIN products p ON p.house_id = h.house_id
        JOIN tracking_log t ON t.product_instance_id = p.product_instance_id
        GROUP BY h.house_no
    """, conn)

    start_df["start"] = pd.to_datetime(start_df["start"], utc=True, errors="coerce").dt.tz_convert(tz)
    start_map = dict(zip(start_df["house_no"], start_df["start"])) if not start_df.empty else {}

    # ================= HOUSE LEVEL INTELLIGENCE =================
    house_intel = []

    grouped = live_df.groupby("house_no")

    for house, g in grouped:
        start_date = start_map.get(house, None)
        if pd.isna(start_date) or start_date is None:
            continue

        non_start = g[g["stage"] != "Not Started"].copy()
        if non_start.empty:
            continue

        non_start["seq"] = non_start["stage"].map(lambda x: seq_map.get(x, 0))
        current_seq = int(non_start["seq"].min())
        current_stage = non_start[non_start["seq"] == current_seq]["stage"].iloc[0]

        stage_rows = non_start[non_start["stage"] == current_stage]
        stage_age = max(1, (today - stage_rows["timestamp"].min()).days) if stage_rows["timestamp"].notna().any() else 1
        stagnant_days = max(0, (today - stage_rows["timestamp"].max()).days) if stage_rows["timestamp"].notna().any() else 0

        stage_duration = day_map.get(current_stage, 1)
        rem_stage = max(0, stage_duration - stage_age)
        downstream = int(act[act["seq"] > current_seq]["days"].sum())
        rem_total = rem_stage + downstream

        planned_finish = start_date + timedelta(days=total_duration)
        predicted_finish = today + timedelta(days=rem_total + max(0, stage_age-stage_duration))
        delay_days = max(0, (predicted_finish - planned_finish).days)

        sla_risk = False
        if house in config_map:
            sla_risk = predicted_finish.date() > config_map[house]
            delay_days = max(delay_days, (predicted_finish.date() - config_map[house]).days)

        risk = "On Track"
        if delay_days > 10:
            risk = "Critical Delay"
        elif sla_risk:
            risk = "SLA Risk"
        elif stagnant_days >= 3 and stage_age > stage_duration:
            risk = "Stagnant"
        elif stage_age > stage_duration:
            risk = "Slow"

        project = g["project_name"].iloc[0]
        unit = g["unit_name"].iloc[0]

        dispatched = True if (g["stage"] == "Dispatch").all() else False

        house_intel.append({
            "Project": project,
            "Unit": unit,
            "House": house,
            "Stage": current_stage,
            "Stage Age": stage_age,
            "Predicted Finish": predicted_finish.date(),
            "Delay": max(0, delay_days),
            "Risk": risk,
            "Dispatched": dispatched
        })

    intel_df = pd.DataFrame(house_intel)
    if intel_df.empty:
        st.warning("No active houses")
        return

    active_projects = intel_df["Project"].nunique()
    active_units = intel_df["Unit"].nunique()
    active_houses = len(intel_df[intel_df["Dispatched"] == False])
    dispatched_houses = len(intel_df[intel_df["Dispatched"] == True])
    active_products = len(live_df[live_df["stage"] != "Dispatch"])
    dispatched_products = len(live_df[live_df["stage"] == "Dispatch"])
    throughput = round((dispatched_products / len(live_df))*100,1) if len(live_df)>0 else 0
    near_dispatch = len(intel_df[pd.to_datetime(intel_df["Predicted Finish"]) <= pd.Timestamp(today.date()+timedelta(days=7))])
    critical_houses = len(intel_df[intel_df["Risk"] == "Critical Delay"])
    sla_houses = len(intel_df[intel_df["Risk"] == "SLA Risk"])
    stagnant_houses = len(intel_df[intel_df["Risk"] == "Stagnant"])
    avg_delay = round(intel_df["Delay"].mean(),1)

    # ================= BOTTLENECK =================
    stage_analysis = []
    bottleneck_stage = "No Active Stage"
    high_score = -1

    for stage in act["stage"]:
        sdf = live_df[live_df["stage"] == stage]
        if sdf.empty:
            continue
        queue = len(sdf)
        avg_age = round((today - sdf["timestamp"].min()).days,1) if sdf["timestamp"].notna().any() else 0
        crossed = len(live_df[live_df["stage"].map(lambda x: seq_map.get(x,0)) > seq_map.get(stage,0)])
        throughput_stage = round((crossed/(queue+crossed))*100,1) if (queue+crossed)>0 else 0
        score = queue + avg_age
        alert = "Stable"
        if avg_age > day_map.get(stage,1):
            alert = "Slow"
        if score > high_score:
            high_score = score
            bottleneck_stage = stage
        stage_analysis.append([stage, queue, avg_age, throughput_stage, alert])

    stage_df = pd.DataFrame(stage_analysis, columns=["Stage","Queue","Avg Aging","Throughput %","Alert"])

    # ================= KPI CARDS =================
    r1 = st.columns(4)
    r1[0].metric("Active Projects", active_projects)
    r1[1].metric("Active Units", active_units)
    r1[2].metric("Houses In Progress", active_houses)
    r1[3].metric("Houses Dispatched", dispatched_houses)

    r2 = st.columns(4)
    r2[0].metric("Active Products", active_products)
    r2[1].metric("Products Dispatched", dispatched_products)
    r2[2].metric("Factory Throughput %", throughput)
    r2[3].metric("Near Dispatch 7D", near_dispatch)

    r3 = st.columns(4)
    r3[0].metric("Critical Houses", critical_houses)
    r3[1].metric("SLA Risk Houses", sla_houses)
    r3[2].metric("Stagnant Houses", stagnant_houses)
    r3[3].metric("Avg Delay Days", avg_delay)

    st.error(f"🚨 Current Bottleneck Stage: {bottleneck_stage}")
    st.markdown("---")

    # ================= PROJECT HOTSPOT =================
    st.subheader("🔥 Project Hotspot Analysis")
    hotspot = intel_df.groupby("Project").agg({
        "Unit":"nunique",
        "House":"count",
        "Dispatched":"sum",
        "Delay":"mean"
    }).reset_index()
    hotspot.columns = ["Project","Units Active","Houses Active","Houses Dispatched","Avg Delay"]
    hotspot["Critical"] = hotspot["Project"].map(intel_df[intel_df["Risk"]!="On Track"].groupby("Project")["House"].count()).fillna(0).astype(int)
    st.dataframe(hotspot.sort_values("Critical", ascending=False), use_container_width=True)

    fig1 = px.bar(hotspot, x="Project", y="Critical", title="Project Critical House Load")
    st.plotly_chart(fig1, use_container_width=True)

    # ================= STAGE FLOW =================
    st.subheader("🏭 Stage Wise Flow Analysis")
    st.dataframe(stage_df, use_container_width=True)
    fig2 = px.bar(stage_df, x="Stage", y="Queue", title="Live Stage Queue")
    st.plotly_chart(fig2, use_container_width=True)

    # ================= TOP PRIORITY HOUSES =================
    st.subheader("🚨 Top Priority Houses")
    priority = intel_df[intel_df["Risk"] != "On Track"].sort_values(["Delay","Stage Age"], ascending=False).head(15)
    st.dataframe(priority[["Project","Unit","House","Stage","Stage Age","Predicted Finish","Delay","Risk"]], use_container_width=True)

    # ================= DISPATCH PIPELINE =================
    st.subheader("🚚 Dispatch Pipeline (Next 7 Days)")
    dispatch_table = intel_df[pd.to_datetime(intel_df["Predicted Finish"]) <= pd.Timestamp(today.date()+timedelta(days=7))]
    st.dataframe(dispatch_table[["Project","Unit","House","Stage","Predicted Finish","Delay"]], use_container_width=True)

    # ================= MANAGEMENT NOTES =================
    st.subheader("🧠 Management Notes")
    notes = []
    notes.append(f"🚧 Major production congestion detected at {bottleneck_stage}.")
    if critical_houses > 0:
        notes.append(f"🔴 {critical_houses} houses are under severe delay requiring intervention.")
    if stagnant_houses > 0:
        notes.append(f"⏸️ {stagnant_houses} houses show no effective movement beyond planned duration.")
    if near_dispatch < 5:
        notes.append("🚚 Dispatch pipeline is weak for upcoming 7 days.")
    if avg_delay > 5:
        notes.append(f"⚠️ Average house delay currently elevated at {avg_delay} days.")

    for n in notes:
        st.info(n)
```
