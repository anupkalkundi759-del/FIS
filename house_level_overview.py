import streamlit as st
import pandas as pd
from db import run_query

st.set_page_config(layout="wide")

st.markdown("<h1 style='margin-bottom:20px;'>📊 Workflow Intelligence Monitor</h1>", unsafe_allow_html=True)

# =========================================================
# MASTER DATA LOAD (HOUSE MASTER + PRODUCTS + TRACKING)
# =========================================================

query = """
WITH latest_tracking AS (
    SELECT DISTINCT ON (t.product_instance_id)
        t.product_instance_id,
        s.stage_name
    FROM tracking_log t
    LEFT JOIN stages s ON t.stage_id = s.stage_id
    ORDER BY t.product_instance_id, t.timestamp DESC
)

SELECT
    p.project_name,
    u.unit_name,
    h.house_no,
    pr.product_instance_id,
    pm.product_code,
    COALESCE(lt.stage_name, 'Not Started') AS current_stage
FROM houses h
JOIN units u ON h.unit_id = u.unit_id
JOIN projects p ON u.project_id = p.project_id
LEFT JOIN products pr ON h.house_id = pr.house_id
LEFT JOIN products_master pm ON pr.product_id = pm.product_id
LEFT JOIN latest_tracking lt ON pr.product_instance_id = lt.product_instance_id
ORDER BY p.project_name, u.unit_name, h.house_no;
"""

df = run_query(query)

df.columns = ["Project", "Unit", "House", "Product_Instance", "Product", "Current Stage"]

# =========================================================
# FILTERS
# =========================================================

c1, c2, c3 = st.columns(3)

project_list = ["All"] + sorted(df["Project"].dropna().unique().tolist())
selected_project = c1.selectbox("Select Project", project_list)

if selected_project != "All":
    df1 = df[df["Project"] == selected_project]
else:
    df1 = df.copy()

unit_list = ["All"] + sorted(df1["Unit"].dropna().unique().tolist())
selected_unit = c2.selectbox("Select Unit", unit_list)

if selected_unit != "All":
    df2 = df1[df1["Unit"] == selected_unit]
else:
    df2 = df1.copy()

house_list = sorted(df2["House"].dropna().unique().tolist())
selected_houses = c3.multiselect("Select Houses (Optional)", house_list)

if selected_houses:
    temp3 = df2[df2["House"].isin(selected_houses)]
else:
    temp3 = df2.copy()

# =========================================================
# KPI SUMMARY
# =========================================================

projects_count = temp3["Project"].nunique()
units_count = temp3["Unit"].nunique()
houses_count = temp3["House"].nunique()
products_count = temp3["Product_Instance"].nunique()

st.markdown("## 📈 Live Workflow Summary")
k1, k2, k3, k4 = st.columns(4)
k1.metric("Projects", projects_count)
k2.metric("Units", units_count)
k3.metric("Houses", houses_count)
k4.metric("Total Products", products_count)

# =========================================================
# HOUSE BOTTLENECK SAFE CALCULATION
# =========================================================

stage_rank = {
    "Not Started": 0,
    "Measurement": 1,
    "Cutting List": 2,
    "Production": 3,
    "Pre Assembly": 4,
    "Polishing": 5,
    "Final Assembly": 6,
    "Dispatch": 7
}

# master houses
master_house_df = temp3[["Project", "Unit", "House"]].drop_duplicates()

# bottleneck per house from all products
bottleneck_calc = temp3.groupby("House")["Current Stage"].apply(
    lambda x: sorted(list(set(x)), key=lambda y: stage_rank.get(y, 999))[0]
).reset_index(name="Bottleneck Stage")

# pending products count
pending_products = temp3[temp3["Current Stage"] != "Dispatch"].groupby("House")["Product_Instance"].count().reset_index(name="Pending Products")

house_bottleneck = master_house_df.merge(bottleneck_calc, on="House", how="left")
house_bottleneck = house_bottleneck.merge(pending_products, on="House", how="left")

house_bottleneck["Bottleneck Stage"] = house_bottleneck["Bottleneck Stage"].fillna("Not Started")
house_bottleneck["Pending Products"] = house_bottleneck["Pending Products"].fillna(0)

house_bottleneck = house_bottleneck[["House", "Bottleneck Stage", "Pending Products"]]

# =========================================================
# STAGE WISE HOUSE PENDING SUMMARY
# =========================================================

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

stage_summary = house_bottleneck.groupby("Bottleneck Stage")["House"].count().reset_index(name="Houses Pending")

stage_summary = pd.DataFrame({"Bottleneck Stage": stage_order}).merge(stage_summary, on="Bottleneck Stage", how="left").fillna(0)

st.markdown("## 🚦 Stage Wise House Pending Summary")
st.dataframe(stage_summary, use_container_width=True, height=320)

# =========================================================
# WHICH HOUSES ARE PENDING IN WHICH STAGE
# =========================================================

st.markdown("## 🏠 Which Houses Are Pending In Which Stage")
st.dataframe(
    house_bottleneck.sort_values(["Bottleneck Stage", "House"]),
    use_container_width=True,
    height=350
)

# =========================================================
# LEVEL 2 PRODUCT BREAKDOWN (WHEN UNIT SELECTED)
# =========================================================

if selected_unit != "All":
    st.markdown(f"## 🪟 Product Pending Breakdown Inside Unit : {selected_unit}")

    product_summary = temp3.groupby(["Product", "Current Stage"])["Product_Instance"].count().reset_index(name="Count")

    product_pivot = product_summary.pivot(index="Product", columns="Current Stage", values="Count").fillna(0).reset_index()

    for s in stage_order:
        if s not in product_pivot.columns:
            product_pivot[s] = 0

    product_pivot["Total Qty"] = product_pivot[stage_order].sum(axis=1)

    product_pivot = product_pivot[
        ["Product", "Total Qty", "Not Started", "Measurement", "Cutting List", "Production",
         "Pre Assembly", "Polishing", "Final Assembly", "Dispatch"]
    ]

    st.dataframe(product_pivot, use_container_width=True, height=450)
