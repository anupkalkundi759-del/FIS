import streamlit as st
import pandas as pd
import plotly.express as px

# ================= CONFIG =================
st.set_page_config(page_title="Executive Dashboard", layout="wide")

st.title("📊 Factory Intelligence Dashboard")

# ================= DATA QUERY =================
def get_dashboard_data(cur):
    query = """
    WITH latest_stage AS (
        SELECT DISTINCT ON (product_instance_id)
            product_instance_id, stage_id, status
        FROM tracking_log
        ORDER BY product_instance_id, timestamp DESC
    )
    SELECT 
        s.stage_name,
        COUNT(*) AS total,
        COUNT(CASE WHEN ls.status = 'Completed' THEN 1 END) AS completed,
        COUNT(CASE WHEN ls.status = 'In Progress' THEN 1 END) AS wip
    FROM latest_stage ls
    JOIN stages s ON ls.stage_id = s.stage_id
    GROUP BY s.stage_name
    ORDER BY s.stage_name;
    """
    cur.execute(query)
    data = cur.fetchall()
    df = pd.DataFrame(data, columns=["stage", "total", "completed", "wip"])
    return df


# ================= MAIN =================
def show_dashboard_v2(conn, cur):

    df = get_dashboard_data(cur)

    if df.empty:
        st.warning("No data available")
        return

    # ================= KPI CALC =================
    total_products = df["total"].sum()
    completed = df["completed"].sum()
    in_progress = df["wip"].sum()
    pending = total_products - completed

    # ================= KPI ROW =================
    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Total Products", total_products)
    col2.metric("In Progress", in_progress)
    col3.metric("Completed", completed)
    col4.metric("Pending", pending)

    # ================= BOTTLENECK =================
    bottleneck_row = df.sort_values("wip", ascending=False).iloc[0]
    bottleneck_stage = bottleneck_row["stage"]

    # ================= CHARTS =================
    c1, c2, c3 = st.columns(3)

    # ---- Stage Distribution ----
    with c1:
        fig1 = px.bar(
            df,
            x="stage",
            y="total",
            title="Stage Distribution"
        )
        fig1.update_layout(height=300)
        st.plotly_chart(fig1, use_container_width=True)

    # ---- WIP vs Completed ----
    with c2:
        df_melt = df.melt(id_vars="stage", value_vars=["wip", "completed"],
                          var_name="type", value_name="count")

        fig2 = px.bar(
            df_melt,
            x="stage",
            y="count",
            color="type",
            barmode="group",
            title="WIP vs Completed"
        )
        fig2.update_layout(height=300)
        st.plotly_chart(fig2, use_container_width=True)

    # ---- Bottleneck ----
    with c3:
        st.subheader("🚨 Bottleneck Stage")
        st.error(f"{bottleneck_stage}")

        st.subheader("Top WIP")
        top_wip = df.sort_values("wip", ascending=False).head(3)
        st.dataframe(top_wip, use_container_width=True)

    # ================= BOTTOM ROW =================
    c4, c5 = st.columns([2, 1])

    # ---- Summary Table ----
    with c4:
        st.subheader("Stage Summary")
        st.dataframe(df, use_container_width=True)

    # ---- Alerts ----
    with c5:
        st.subheader("⚠ Alerts")

        if bottleneck_row["wip"] > 50:
            st.error("High congestion in bottleneck stage")
        else:
            st.success("Flow is stable")

        if completed == 0:
            st.warning("No completed items yet")

        if in_progress > total_products * 0.7:
            st.warning("Too much WIP → risk of delay")

# ================= RUN =================
# call this from your app.py like:
# show_dashboard_v2(conn, cur)
