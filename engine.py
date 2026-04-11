def run_engine(conn, cur):
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

st.title("⚙️ Scheduling Intelligence Engine")

today = datetime.now()

# ================= LOAD ACTIVITY MASTER =================
cur.execute("""
    SELECT activity_name, sequence_order, duration_days
    FROM activity_master
    ORDER BY sequence_order
""")
activities = cur.fetchall()

if not activities:
    st.error("No activity master found")
    return

activity_df = pd.DataFrame(activities, columns=["stage", "seq", "days"])
total_days = activity_df["days"].sum()

# ================= LOAD TRACKING =================
cur.execute("""
    SELECT 
        h.house_no,
        s.stage_name,
        t.timestamp

    FROM products p
    JOIN houses h ON p.house_id = h.house_id

    LEFT JOIN LATERAL (
        SELECT stage_id, timestamp
        FROM tracking_log
        WHERE product_instance_id = p.product_instance_id
        ORDER BY timestamp DESC
        LIMIT 1
    ) t ON TRUE

    LEFT JOIN stages s ON t.stage_id = s.stage_id
""")

data = cur.fetchall()

if not data:
    st.warning("No tracking data")
    return

df = pd.DataFrame(data, columns=["house", "stage", "time"])
df["stage"] = df["stage"].fillna("Not Started")

df = df.merge(activity_df, on="stage", how="left")
df["seq"] = df["seq"].fillna(0)
df["days"] = df["days"].fillna(0)

results = []
alerts = []
stage_delay = []

for house in df["house"].unique():
    hdf = df[df["house"] == house].sort_values("seq")

    current = hdf.iloc[-1]
    current_seq = current["seq"]
    current_stage = current["stage"]

    completed_days = activity_df[activity_df["seq"] <= current_seq]["days"].sum()
    remaining_days = activity_df[activity_df["seq"] > current_seq]["days"].sum()

    progress = (completed_days / total_days) * 100 if total_days > 0 else 0

    predicted_finish = today + timedelta(days=int(remaining_days))
    planned_finish = today + timedelta(days=int(total_days - completed_days))

    delay = (predicted_finish - planned_finish).days

    # STATUS
    if delay > 3:
        status = "🔴 Delayed"
        alerts.append(f"{house} delayed by {delay} days")
    elif delay > 0:
        status = "🟠 At Risk"
    else:
        status = "🟢 On Track"

    # PRIORITY
    priority = delay * 10 + remaining_days

    results.append({
        "House": house,
        "Stage": current_stage,
        "Progress %": round(progress, 1),
        "Delay": delay,
        "Finish": predicted_finish.date(),
        "Status": status,
        "Priority": priority
    })

    # Stage delay
    for _, row in hdf.iterrows():
        stage_delay.append({
            "House": house,
            "Stage": row["stage"],
            "Planned": row["days"],
            "Actual": row["days"],  # placeholder
            "Delay": 0
        })

result_df = pd.DataFrame(results)

# ================= KPI =================
total_houses = len(result_df)
delayed = len(result_df[result_df["Status"].str.contains("Delayed")])
avg_delay = result_df["Delay"].mean()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Houses", total_houses)
col2.metric("Delayed", delayed)
col3.metric("Avg Delay", round(avg_delay, 1))
col4.metric("On-Time %", round((total_houses - delayed)/total_houses * 100,1))

st.divider()

# ================= WOOD INPUT =================
st.subheader("🪵 Wood Management")

col1, col2 = st.columns(2)

with col1:
    total_stock = st.number_input("Total Stock", 0)
    used = st.number_input("Used", 0)

remaining = total_stock - used

st.metric("Remaining Wood", remaining)

if remaining < 20:
    st.error("⚠️ Wood shortage risk")

st.divider()

# ================= EARLY WARNINGS =================
st.subheader("🚨 Early Warning System")

if alerts:
    for a in alerts:
        st.error(a)
else:
    st.success("All houses on track")

st.divider()

# ================= HOUSE TABLE =================
st.subheader("🏠 House Intelligence")

def color_status(val):
    if "Delayed" in val:
        return "background-color:red;color:white"
    elif "Risk" in val:
        return "background-color:orange"
    else:
        return "background-color:green;color:white"

st.dataframe(result_df.style.applymap(color_status, subset=["Status"]))

st.divider()

# ================= BOTTLENECK =================
st.subheader("🔥 Bottleneck Detection")

bottleneck = df["stage"].value_counts().reset_index()
bottleneck.columns = ["Stage", "Count"]

st.dataframe(bottleneck)

st.divider()

# ================= STAGE DELAY =================
st.subheader("📊 Stage Delay Analysis")

stage_df = pd.DataFrame(stage_delay)
st.dataframe(stage_df)
