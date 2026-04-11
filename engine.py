# ================= KPI =================
total_houses = len(result_df)
delayed = len(result_df[result_df["Delay"] > 0])
avg_progress = result_df["Progress %"].mean()

# WOOD
cur.execute("SELECT total_stock FROM wood_inventory ORDER BY id DESC LIMIT 1")
stock = cur.fetchone()
total_stock = stock[0] if stock else 0

cur.execute("SELECT SUM(consumption) FROM wood_consumption")
used = cur.fetchone()[0] or 0
remaining = total_stock - used

st.markdown("## 📊 Overview")

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Houses", total_houses)
k2.metric("Delayed", delayed)
k3.metric("Avg Progress", round(avg_progress,1))
k4.metric("Wood Remaining", remaining)

st.divider()

# ================= ROW 2 =================
col1, col2 = st.columns(2)

with col1:
    st.subheader("📈 Progress vs Planned")
    st.line_chart(result_df[["Progress %", "Planned %"]])

with col2:
    st.subheader("🪵 Wood Status")
    st.metric("Total", total_stock)
    st.metric("Used", used)
    st.metric("Remaining", remaining)

st.divider()

# ================= ROW 3 =================
col3, col4 = st.columns(2)

with col3:
    st.subheader("🚨 Early Warnings")
    if early_df.empty:
        st.success("All houses on track")
    else:
        st.dataframe(early_df, use_container_width=True)

with col4:
    st.subheader("🔥 Bottleneck")
    if not stage_df.empty:
        bottleneck = stage_df.groupby("Stage")["Actual Days"].mean().sort_values(ascending=False)
        st.bar_chart(bottleneck)

st.divider()

# ================= FULL WIDTH =================
st.subheader("🏠 House Intelligence")
st.dataframe(result_df, use_container_width=True)

st.subheader("🚨 Priority Houses")
st.dataframe(result_df.sort_values("Priority", ascending=False).head(5), use_container_width=True)
