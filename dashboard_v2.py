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
        "Measurement",
        "Cutting List",
        "Production",
        "Pre Assembly",
        "Polishing",
        "Final Assembly",
        "Dispatch"
    ]

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
        h.unit_name,
        h.unit,
        p.product_instance_id,
        COALESCE(s.stage_name, 'Not Started') AS current_stage,
        lt.timestamp
    FROM products p
    JOIN houses h ON p.house_id = h.house_id
    LEFT JOIN latest_tracking lt
        ON p.product_instance_id = lt.product_instance_id
    LEFT JOIN stages s
        ON lt.stage_id = s.stage_id
    """

    live_df = pd.read_sql_query(live_query, conn)

    if live_df.empty:
        st.warning("No dashboard data available.")
        return

    live_df["timestamp"] = pd.to_datetime(live_df["timestamp"], errors="coerce")
    live_df["unit_live"] = live_df["unit_name"].fillna(live_df["unit"]).fillna("N/A")

    total_products = len(live_df)
    total_houses = pd.read_sql_query("SELECT COUNT(DISTINCT house_id) AS cnt FROM houses", conn)["cnt"][0]

    # ==========================
    # COMPLETED / RUNNING HOUSES
    # ==========================
    house_stage = live_df.groupby("house_id")["current_stage"].apply(list)

    completed_houses = 0
    for hid, vals in house_stage.items():
        if all(v == "Dispatch" for v in vals):
            completed_houses += 1

    running_houses = total_houses - completed_houses

    # ==========================
    # STAGE BACKLOG
    # ==========================
    backlog_counts = {}

    for stg in stage_order:
        temp = live_df[live_df["current_stage"] == stg]
        backlog_counts[stg] = len(temp)

    bottleneck_stage = max(backlog_counts, key=backlog_counts.get)
    highest_pending = backlog_counts[bottleneck_stage]

    dispatch_products = len(live_df[live_df["current_stage"] == "Dispatch"])
    overall_completion = round((dispatch_products / total_products) * 100, 2)

    # ==========================
    # KPI ROW
    # ==========================
    k1, k2, k3, k4, k5, k6, k7 = st.columns(7)

    k1.metric("Total Products", total_products)
    k2.metric("Total Houses", total_houses)
    k3.metric("Houses Completed", completed_houses)
    k4.metric("Houses Running", running_houses)
    k5.metric("Current Bottleneck", bottleneck_stage)
    k6.metric("Highest Pending", highest_pending)
    k7.metric("Overall Completion %", f"{overall_completion}%")

    st.markdown("---")

    # ==========================
    # ROW 2
    # ==========================
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
        .groupby("unit_live")
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
                st.write(f"{row['unit_live']} → {row['Pending Products']} pending")

    with r3:
        with st.container(border=True):
            st.subheader("🚦 Stage Pressure")
            st.write(f"Measurement → {backlog_counts['Measurement']}")
            st.write(f"Production → {backlog_counts['Production']}")
            st.write(f"Dispatch → {backlog_counts['Dispatch']}")

    st.markdown("---")

    # ==========================
    # ROW 3
    # ==========================
    b1, b2 = st.columns([1.9, 1])

    with b1:
        with st.container(border=True):
            st.subheader("📈 Factory Pending Products Movement")

            trend_df = live_df.dropna(subset=["timestamp"]).copy()

            if not trend_df.empty:
                trend_df["Date"] = trend_df["timestamp"].dt.date

                records = []
                unique_dates = sorted(trend_df["Date"].unique())

                for dt in unique_dates:
                    upto = trend_df[trend_df["Date"] <= dt]

                    for stg in stage_order:
                        cnt = len(upto[upto["current_stage"] == stg])
                        records.append([dt, stg, cnt])

                daily_stage = pd.DataFrame(records, columns=["Date", "Stage", "Count"])

                fig = px.line(
                    daily_stage,
                    x="Date",
                    y="Count",
                    color="Stage",
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
                st.warning("No trend data available.")

    with b2:
        with st.container(border=True):
            st.subheader("📌 Immediate Actions Needed")

            measurement_backlog = backlog_counts["Measurement"]
            production_backlog = backlog_counts["Production"]
            dispatch_backlog = backlog_counts["Dispatch"]

            top_unit = busy_units.iloc[0]["unit_live"] if not busy_units.empty else "N/A"

            if measurement_backlog > 1000:
                st.error(f"Measurement pending too high ({measurement_backlog})")

            if dispatch_backlog > 100:
                st.error(f"Dispatch closure low ({dispatch_backlog})")

            if production_backlog > 500:
                st.warning(f"Production queue building ({production_backlog})")

            st.info(f"Unit {top_unit} needs monitoring")
            st.info(f"{running_houses} houses still open")
