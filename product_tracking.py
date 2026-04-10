def show_product_tracking(conn, cur):
    import streamlit as st
    import pandas as pd

    st.title("🔍 Product Tracking")

    query = """
    SELECT 
        hp.house_id,
        hp.product_code,
        p.product_category,
        p.orientation,
        h.measurement_date,
        h.predicted_finish
    FROM house_products hp
    JOIN products p ON hp.product_code = p.product_code
    JOIN houses h ON hp.house_id = h.house_id
    ORDER BY hp.house_id
    """

    cur.execute(query)
    data = cur.fetchall()

    df = pd.DataFrame(data, columns=[
        "House", "Product", "Category", "Orientation",
        "Measurement Date", "Predicted Finish"
    ])

    st.dataframe(df, use_container_width=True)