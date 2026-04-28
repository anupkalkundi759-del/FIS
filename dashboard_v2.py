import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


def show_dashboard_v2(conn, cur):

    tz = ZoneInfo("Asia/Kolkata")
    today = datetime.now(tz)

    st.title("📊 Factory War Room Dashboard")

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

    # ================= SLA =================
    try:
        cur.execute("SELECT house_no, sla_date FROM house_config")
        config_map = {r[0]: r[1] for r in cur.fetchall()}
    except:
        config_map = {}

    # ================= LIVE PRODUCT SNAPSHOT =================
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
        LEFT JOIN latest_stage ls
            ON p.product_instance_id = ls.product_instance_id
            AND ls.rn = 1
    """, conn)

    if live_df.empty:
        st.warning("No tracking data")
        return

    live_df["timestamp"] = pd.to_datetime(live_df["timestamp"], utc=True, errors="coerce").dt.tz_convert(tz)

    # ================= HOUSE START DATE =================
    start_df = pd.read_sql("""
        SELECT h.house_no, MIN(t.timestamp) as start
        FROM houses h
        JOIN products p ON p.house_id = h.house_id
        JOIN tracking_log t ON t.product_instance_id = p.product_instance_id
        GROUP BY h.house_no
    """, conn)

    start_df["start"] = pd.to_datetime(start_df["start"], utc=True, errors="coerce").dt.tz_convert(tz)
    start_map = dict(zip(start_df["house_no"], start_df["start"])) if not start_df.empty else {}

    # ================= HOUSE INTELLIGENCE =================
    house_rows = []

    for house, g in live_df.groupby("house_no"):

        start_date = start_map.get(house, None)
        if pd.isna(start_date) or start_date is None:
            continue

        total_products = len(g)

        stage_count = g.groupby("stage")["product_instance_id"].count().reset_index()
        dominant_stage = stage_count.sort_values("product_instance_id", ascending=False).iloc[0]["stage"]

        dispatch_count = int(stage_count[stage_count["stage"] == "Dispatch"]["product_instance_id"].sum()) if "Dispatch" in stage_count["stage"].values else 0
        dispatch_ratio = dispatch_count / total_products if total_products > 0 else 0

        if dispatch_ratio >= 0.90:
            current_stage = "Dispatch"
            predicted_finish = today.date()
            delay_days = 0
            risk = "Dispatched"
            stage_age = 0
            dispatched = True
        else:
            current_stage = dominant_stage
            current_seq = seq_map.get(current_stage, 1)

            stage_rows = g[g["stage"] == current_stage]

            if stage_rows["timestamp"].notna().any():
                stage_enter = stage_rows["timestamp"].min()
                stage_last = stage_rows["timestamp"].max()
            else:
                stage_enter = start_date
                stage_last = start_date

            stage_age = max(1, (today - stage_enter).days)
            stagnant_days = max(0, (today - stage_last).days)

            stage_duration = day_map.get(current_stage, 1)
            rem_stage = max(0, stage_duration - stage_age)
            downstream = int(act[act["seq"] > current_seq]["days"].sum())
            rem_total = rem_stage + downstream

            planned_finish = start_date + timedelta(days=total_duration)
            penalty = max(0, stage_age - stage_duration)

            predicted_finish_dt = today + timedelta(days=rem_total + penalty)
            predicted_finish = predicted_finish_dt.date()

            delay_days = max(0, (predicted_finish_dt - planned_finish).days)

            sla_risk = False
            if house in config_map:
                sla_risk = predicted_finish > config_map[house]
                delay_days = max(delay_days, (predicted_finish - config_map[house]).days)

            risk = "On Track"
            if delay_days > 7:
                risk = "Critical Delay"
            elif sla_risk:
                risk = "SLA Risk"
            elif stagnant_days >= 3 and stage_age > stage_duration:
                risk = "Stagnant"
            elif stage_age > stage_duration:
                risk = "Slow"

            dispatched = False

        project = g["project_name"].iloc[0]
        unit = g["unit_name"].iloc[0]

        house_rows.append({
            "Project": project,
            "Unit": unit,
            "House": house,
            "Stage": current_stage,
            "Stage Age": stage_age,
            "Predicted Finish": predicted_finish,
            "Delay": delay_days,
            "Risk": risk,
            "Dispatched": dispatched,
            "Total Products": total_products
        })

    intel_df = pd.DataFrame(house_rows)

    if intel_df.empty:
        st.warning("No active houses")
        return

    # ================= TRUE KPI VALUES =================
    active_projects = intel_df["Project"].nunique()
    active_units = intel_df["Unit"].nunique()
    active_houses = len(intel_df[intel_df["Dispatched"] == False])
    dispatched_houses = len(intel_df[intel_df["Dispatched"] == True])

    active_products = int(intel_df[intel_df["Dispatched"] == False]["Total Products"].sum())
    dispatched_products = int(intel_df[intel_df["Dispatched"] == True]["Total Products"].sum())

    throughput = round((dispatched_products / (active_products + dispatched_products)) * 100, 1) if (active_products + dispatched_products) > 0 else 0

    critical_houses = len(intel_df[intel_df["Risk"] == "Critical Delay"])
    sla_houses = len(intel_df[intel_df["Risk"] == "SLA Risk"])
    stagnant_houses = len(intel_df[intel_df["Risk"] == "Stagnant"])
    avg_delay = round(intel_df["Delay"].mean(), 1)

    # ================= STAGE ANALYSIS =================
    stage_analysis = []
    bottleneck_stage = "No Active Stage"
    high_score = -1

    for stage in act["stage"]:
        sdf = live_df[live_df["stage"] == stage]
        if sdf.empty:
            continue

        queue = len(sdf)
        avg_age = round((today - sdf["timestamp"].min()).days, 1) if sdf["timestamp"].notna().any() else 0
        score = queue + avg_age

        alert = "Stable"
        if avg_age > day_map.get(stage, 1):
            alert = "Slow"

        if score > high_score:
            high_score = score
            bottleneck_stage = stage

        stage_analysis.append([stage, queue, avg_age, alert])

    stage_df = pd.DataFrame(stage_analysis, columns=["Stage", "Queue", "Avg Aging", "Alert"])

    # ================= KPI CARDS =================
    r1 = st.columns(4)
    r1[0].metric("Active Projects", active_projects)
    r1[1].metric("Active Units", active_units)
    r1[2].metric("Houses In Progress", active_houses)
    r1[3].metric("Houses Dispatched", dispatched_houses)

    r2 = st.columns(4)
    r2[0].metric("Active Products", active_products)
    r2[1].metric("Products Dispatched", dispatched_products)
    r2[2].metric("Critical Houses", critical_houses)
    r2[3].metric("Avg Delay Days", avg_delay)

    r3 = st.columns(4)
    r3[0].metric("SLA Risk Houses", sla_houses)
    r3[1].metric("Stagnant Houses", stagnant_houses)
    r3[2].metric("Current Bottleneck", bottleneck_stage)
    r3[3].metric("Factory Throughput %", throughput)

    st.markdown("---")

    # ================= PROJECT RANKING =================
    st.subheader("🔥 Project Performance Ranking")
    project_rank = intel_df.groupby("Project").agg({
        "Unit": "nunique",
        "House": "count",
        "Dispatched": "sum",
        "Delay": "mean"
    }).reset_index()

    project_rank.columns = ["Project", "Units Active", "Houses Active", "Houses Dispatched", "Avg Delay"]
    project_rank["Critical Houses"] = project_rank["Project"].map(
        intel_df[intel_df["Risk"] != "On Track"].groupby("Project")["House"].count()
    ).fillna(0).astype(int)

    st.dataframe(project_rank.sort_values(["Critical Houses", "Avg Delay"], ascending=False), use_container_width=True)

    # ================= UNIT RANKING =================
    st.subheader("🏗️ Unit Performance Ranking")
    unit_rank = intel_df.groupby(["Project", "Unit"]).agg({
        "House": "count",
        "Dispatched": "sum",
        "Delay": "mean"
    }).reset_index()

    unit_rank.columns = ["Project", "Unit", "Houses Running", "Houses Dispatched", "Avg Delay"]
    unit_rank["Critical"] = unit_rank["Unit"].map(
        intel_df[intel_df["Risk"] != "On Track"].groupby("Unit")["House"].count()
    ).fillna(0).astype(int)

    st.dataframe(unit_rank.sort_values(["Critical", "Avg Delay"], ascending=False), use_container_width=True)

    # ================= STAGE BOTTLENECK =================
    st.subheader("🏭 Stage Bottleneck Analysis")
    st.dataframe(stage_df, use_container_width=True)
    st.plotly_chart(px.bar(stage_df, x="Stage", y="Queue", title="Live Factory Queue by Stage"), use_container_width=True)

    # ================= TOP INTERVENTION HOUSES =================
    st.subheader("🚨 Top Intervention Houses")
    priority = intel_df[intel_df["Risk"] != "On Track"].sort_values(["Delay", "Stage Age"], ascending=False).head(15)
    st.dataframe(priority[["Project", "Unit", "House", "Stage", "Stage Age", "Predicted Finish", "Delay", "Risk"]], use_container_width=True)

    # ================= ACTION NOTES =================
    st.subheader("🧠 Today's Action Zone")

    worst_project = project_rank.sort_values(["Critical Houses", "Avg Delay"], ascending=False).iloc[0]["Project"]
    worst_unit = unit_rank.sort_values(["Critical", "Avg Delay"], ascending=False).iloc[0]["Unit"]

    st.info(f"🚧 Highest intervention required in Project: {worst_project}")
    st.info(f"🏗️ Highest intervention required in Unit: {worst_unit}")
    st.info(f"⚠️ Current factory bottleneck concentrated at {bottleneck_stage}")
    st.info(f"📌 {critical_houses + stagnant_houses + sla_houses} houses need immediate supervisory attention")
