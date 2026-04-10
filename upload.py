def show_upload(conn, cur):
    import streamlit as st
    import pandas as pd
    import time

    if st.session_state.role != "admin":
        st.error("Access denied")
        st.stop()

    st.subheader("📤 Upload Project Setup Excel")

    file = st.file_uploader("Upload Excel", type=["xlsx"])

    if file:

        start_time = time.time()

        status = st.empty()
        progress = st.progress(0)

        status.info("⏳ Uploading... Please wait")

        df = pd.read_excel(file, engine="openpyxl")

        # ================= CLEAN =================
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

        required_cols = ["project_name", "unit_name", "house_no", "product_code"]
        for col in required_cols:
            if col not in df.columns:
                st.error(f"Missing column: {col}")
                return

        df = df.dropna(subset=required_cols)

        df["project_name"] = df["project_name"].astype(str).str.strip()
        df["unit_name"] = df["unit_name"].astype(str).str.strip()
        df["house_no"] = df["house_no"].astype(str).str.strip()
        df["product_code"] = df["product_code"].astype(str).str.strip()

        df["product_category"] = df.get("product_category", None)
        df["orientation"] = df.get("orientation", None)
        df["quantity"] = pd.to_numeric(df.get("quantity", 1), errors="coerce").fillna(1).astype(int)

        df = df.drop_duplicates()

        total_rows = len(df)

        # ================= COUNTS =================
        project_set = set(df["project_name"])
        unit_set = set(zip(df["project_name"], df["unit_name"]))
        house_set = set(zip(df["project_name"], df["unit_name"], df["house_no"]))
        product_set = set(df["product_code"])

        # ================= INSERT PROJECTS =================
        for p in project_set:
            cur.execute("""
                INSERT INTO projects (project_name)
                VALUES (%s)
                ON CONFLICT (project_name) DO NOTHING
            """, (p,))
        conn.commit()

        progress.progress(10)

        cur.execute("SELECT project_id, project_name FROM projects")
        project_map = {name: pid for pid, name in cur.fetchall()}

        # ================= INSERT UNITS =================
        for project_name, unit_name in unit_set:
            cur.execute("""
                INSERT INTO units (project_id, unit_name)
                VALUES (%s, %s)
                ON CONFLICT (project_id, unit_name) DO NOTHING
            """, (project_map[project_name], unit_name))
        conn.commit()

        progress.progress(25)

        cur.execute("SELECT unit_id, unit_name, project_id FROM units")
        unit_map = {(u, p): uid for uid, u, p in cur.fetchall()}

        # ================= INSERT HOUSES =================
        for project_name, unit_name, house_no in house_set:
            unit_id = unit_map[(unit_name, project_map[project_name])]

            cur.execute("""
                INSERT INTO houses (unit_id, house_no)
                VALUES (%s, %s)
                ON CONFLICT (unit_id, house_no) DO NOTHING
            """, (unit_id, house_no))
        conn.commit()

        progress.progress(40)

        cur.execute("SELECT house_id, house_no, unit_id FROM houses")
        house_map = {(h, u): hid for hid, h, u in cur.fetchall()}

        # ================= PRODUCT MASTER =================
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO products_master (product_code, product_category, orientation)
                VALUES (%s, %s, %s)
                ON CONFLICT (product_code) DO NOTHING
            """, (row["product_code"], row["product_category"], row["orientation"]))
        conn.commit()

        progress.progress(55)

        cur.execute("SELECT product_id, product_code FROM products_master")
        product_map = {code: pid for pid, code in cur.fetchall()}

        # ================= PRODUCTS (EXPLODE QUANTITY) =================
        inserted_products = 0
        total_items = df["quantity"].sum()

        processed = 0

        for _, row in df.iterrows():

            unit_id = unit_map[(row["unit_name"], project_map[row["project_name"]])]
            house_id = house_map[(row["house_no"], unit_id)]
            product_id = product_map[row["product_code"]]

            for i in range(row["quantity"]):  # 🔥 EXPLOSION

                cur.execute("""
                    INSERT INTO products (house_id, product_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, (house_id, product_id))

                inserted_products += 1
                processed += 1

                # progress update (smooth)
                if processed % 50 == 0:
                    percent = 55 + int((processed / total_items) * 40)
                    progress.progress(min(percent, 95))

        conn.commit()

        progress.progress(100)

        total_time = round(time.time() - start_time, 2)

        status.empty()

        st.success(f"""
🚀 Upload Completed Successfully

⏱ Total Time: {total_time} sec  
📄 Excel Rows: {total_rows}  

📊 Data Summary:
- Projects: {len(project_set)}
- Units: {len(unit_set)}
- Houses: {len(house_set)}
- Product Types: {len(product_set)}
- Total Product Items (after quantity expansion): {inserted_products}
""")

        st.info(f"⚡ Avg Speed: {round(inserted_products / total_time, 2)} items/sec")
