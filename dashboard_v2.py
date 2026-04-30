import streamlit as st
import pandas as pd
import plotly.express as px


def show_dashboard_v2(conn, cur):

    try:
        conn.rollback()
    except:
        pass

    st.title("📊 Workflow Intelligence Monitor")

    stage_order = [
        "Not Started",
        "Measurement",
        "Cutting List",
        "Production",
        "Pre Assembly",
        "Polishing",
        "Final Assembly",
        "Dispatch"
    ]

    # ============================================================
    # LIVE MASTER QUERY WITH TRUE UNIT JOIN
    # ============================================================
    live_query = """
    WITH latest_tracking AS (
        SELECT DISTINCT ON (product_instance_id)
            product_instance_id,
            stage_id,
            timestamp
        FROM tracking_log
        ORDER BY product_instance_id, timestamp DESC
    )

    SELECT
        h.house_id,
        h.house_no,
        u.unit_name AS real_unit,
        p.product_instance_id,
        COALESCE(s.stage_name, 'Not Started') AS current_stage,
        lt.timestamp
    FROM products p
    JOIN houses h ON p.house_id = h.house_id
    LEFT JOIN units u ON h.unit_id = u.unit_id
    LEFT JOIN latest_tracking lt ON p.product_instance_id = lt.product_instance_id
    LEFT JOIN stages s ON lt.stage_id = s.stage_id
    """

    live_df = pd.read_sql_query(live_query, conn)

    if live_df.empty:
        st.warning("No dashboard data available.")
        return

    live_df["timestamp"] = pd.to_datetime(live_df["timestamp"], errors="coerce")

    total_products = len(live_df)
    total_houses = pd.read_sql_query("SELECT COUNT(DISTINCT house_id) AS cnt FROM houses", conn)["cnt"][0]

    # ============================================================
    # HOUSE COMPLETION
    # ============================================================
    house_stage = live_df.groupby("house_id")["current_stage"].apply(list)

    completed_houses = 0
    for hid, vals in house_stage.items():
        if all(v == "Dispatch" for v in vals):
            completed_houses += 1

    running_houses = total_houses - completed_houses

    # ============================================================
    # BUSINESS BACKLOG DEFINITIONS
    # ============================================================
    measurement_pending = len(live_df[live_df["current_stage"] == "Not Started"])
    production_pending = len(live_df[live_df["current_stage"] == "Production"])
    dispatch_pending = len(live_df[live_df["current_stage"] == "Dispatch"])

    backlog_counts = {
        "Measurement": measurement_pending,
        "Production": production_pending,
        "Dispatch": dispatch_pending
    }

    bottleneck_stage = max(backlog_counts, key=backlog_counts.get)
    highest_pending = backlog_counts[bottleneck_stage]

    overall_completion = round((dispatch_pending / total_products) * 100, 2)

    # ============================================================
    # KPI CARDS
    # ============================================================
    k1, k2, k3, k4, k5, k6, k7 = st.columns(7)

    k1.metric("Total Products", total_products)
    k2.metric("Total Houses", total_houses)
    k3.metric("Houses Completed", completed_houses)
    k4.metric("Houses Running", running_houses)
    k5.metric("Current Bottleneck", bottleneck_stage)
    k6.metric("Highest Pending", highest_pending)
    k7.metric("Overall Completion %", f"{overall_completion}%")

    st.markdown("---")

    # ============================================================
    # ROW 2
    # ============================================================
    r1, r2, r3 = st.columns(3)

    pending_house = (
        live_df[live_df["current_stage"] != "Dispatch"]
        .groupby(["house_id", "house_no"])
        .size()
        .reset_index(name="Pending Products")
        .sort_values("Pending Products", ascending=False)
        .head(8)
    )

    busy_units = (
        live_df[live_df["current_stage"] != "Dispatch"]
        .groupby("real_unit")
        .size()
        .reset_index(name="Pending Products")
        .sort_values("Pending Products", ascending=False)
        .head(8)
    )

    with r1:
        with st.container(border=True):
            st.subheader("🏠 Top Delayed Houses")
            for _, row in pending_house.iterrows():
                st.write(f"{row['house_no']} → {row['Pending Products']} pending")

    with r2:
        with st.container(border=True):
            st.subheader("🏗 Top Busy Units")
            for _, row in busy_units.iterrows():
                st.write(f"{row['real_unit']} → {row['Pending Products']} pending")

    with r3:
        with st.container(border=True):
            st.subheader("🚦 Stage Pressure")
            st.write(f"Measurement → {measurement_pending}")
            st.write(f"Production → {production_pending}")
            st.write(f"Dispatch → {dispatch_pending}")

    st.markdown("---")

    # ============================================================
    # ROW 3
    # ============================================================
    b1, b2 = st.columns([1.9, 1])

    with b1:
        with st.container(border=True):
            st.subheader("📈 Factory Pending Products Movement (Apr29 vs Apr30)")

            trend_df = live_df.dropna(subset=["timestamp"]).copy()

            if not trend_df.empty:
                trend_df["Date"] = trend_df["timestamp"].dt.date

                daily = trend_df.groupby(["Date", "current_stage"]).size().reset_index(name="Count")

                fig = px.line(
                    daily,
                    x="Date",
                    y="Count",
                    color="current_stage",
                    markers=True,
                    height=420
                )

                fig.update_layout(
                    margin=dict(l=5, r=5, t=5, b=5),
                    plot_bgcolor="white",
                    paper_bgcolor="white"
                )

                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No trend records available.")

    with b2:
        with st.container(border=True):
            st.subheader("📌 Immediate Actions Needed")

            top_unit = busy_units.iloc[0]["real_unit"] if not busy_units.empty else "N/A"

            st.error(f"Measurement pending too high ({measurement_pending})")
            st.warning(f"Production queue building ({production_pending})")
            st.error(f"Dispatch closure low ({dispatch_pending})")
            st.info(f"Unit {top_unit} needs monitoring")
            st.info(f"{running_houses} houses still open")
