def show_upload(conn, cur):
    import streamlit as st
    import pandas as pd

    st.title("📤 Upload Excel")

    file = st.file_uploader("Upload Excel", type=["xlsx"])

    if file:
        df = pd.read_excel(file, engine="openpyxl")

        # ================= CLEAN HEADERS =================
        df.columns = [
            "project", "unit", "house_id",
            "category", "product_code", "orientation", "quantity"
        ]

        # ================= CLEAN DATA =================
        df = df.dropna()

        df["project"] = df["project"].astype(str).str.strip()
        df["unit"] = df["unit"].astype(str).str.strip()
        df["house_id"] = df["house_id"].astype(str).str.strip()
        df["product_code"] = df["product_code"].astype(str).str.strip()
        df["category"] = df["category"].astype(str).str.strip()
        df["orientation"] = df["orientation"].astype(str).str.strip()
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(1).astype(int)

        # ================= INSERT =================
        for _, row in df.iterrows():

            # PROJECT
            cur.execute("""
                INSERT INTO projects (project_name)
                VALUES (%s)
                ON CONFLICT DO NOTHING
            """, (row["project"],))

            cur.execute("SELECT project_id FROM projects WHERE project_name=%s", (row["project"],))
            project_id = cur.fetchone()[0]

            # UNIT
            cur.execute("""
                INSERT INTO units (project_id, unit_name)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (project_id, row["unit"]))

            cur.execute("""
                SELECT unit_id FROM units
                WHERE unit_name=%s AND project_id=%s
            """, (row["unit"], project_id))
            unit_id = cur.fetchone()[0]

            # HOUSE
            cur.execute("""
                INSERT INTO houses (house_id, unit_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (row["house_id"], unit_id))

            # PRODUCT
            cur.execute("""
                INSERT INTO products (product_code, product_category, orientation)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (row["product_code"], row["category"], row["orientation"]))

            # HOUSE-PRODUCT
            cur.execute("""
                INSERT INTO house_products (house_id, product_code)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (row["house_id"], row["product_code"]))

        conn.commit()

        st.success("✅ Upload Successful")

        st.write("Preview:")
        st.dataframe(df.head())
