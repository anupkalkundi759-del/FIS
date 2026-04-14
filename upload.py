def show_upload(conn, cur):
    import streamlit as st
    import pandas as pd
    import time

    if st.session_state.role != "admin":
        st.error("Access denied")
        st.stop()

    st.title("📤 Upload Project Setup Excel")

    file = st.file_uploader("Upload Excel", type=["xlsx"])

    if file:

        start_time = time.time()

        status = st.empty()
        progress = st.progress(0)
        eta_box = st.empty()

        status.info("⏳ Uploading... Please wait")

        df = pd.read_excel(file, engine="openpyxl")

        # ================= CLEAN =================
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

        required_cols = ["project_name", "unit_name", "house_no", "product_code"]
        for col in required_cols:
            if col not in df.columns:
                st.error(f"Missing column: {col}")
                st.stop()

        df = df.dropna(subset=required_cols)

        df["project_name"] = df["project_name"].astype(str).str.strip()
        df["unit_name"] = df["unit_name"].astype(str).str.strip()
        df["house_no"] = df["house_no"].astype(str).str.strip()
        df["product_code"] = df["product_code"].astype(str).str.strip()

        df["product_category"] = df.get("product_category", "").fillna("").astype(str).str.strip()
        df["orientation"] = df.get("orientation", "").fillna("").astype(str).str.strip()
        df["quantity"] = pd.to_numeric(df.get("quantity", 1), errors="coerce").fillna(1).astype(int)

        df["full_code"] = df.apply(
            lambda x: f"{x['product_code']} ({x['orientation']})" if x["orientation"] else x["product_code"],
            axis=1
        )

        df = df.drop_duplicates()

        total_rows = len(df)

        # ================= COUNTS =================
        project_set = set(df["project_name"])
        unit_set = set(zip(df["project_name"], df["unit_name"]))
        house_set = set(zip(df["project_name"], df["unit_name"], df["house_no"]))
        product_set = set(df["full_code"])

        total_items = int(df["quantity"].sum())

        st.info(f"📊 Estimated Items: {total_items}")

        # ================= PROJECTS =================
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

        # ================= UNITS =================
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

        # ================= HOUSES =================
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
        house_map = {(str(h).strip(), u): hid for hid, h, u in cur.fetchall()}

        # ================= PRODUCT MASTER =================
        product_master_df = df[["full_code", "product_category"]].drop_duplicates()

        for _, row in product_master_df.iterrows():
            cur.execute("""
                INSERT INTO products_master (product_code, product_category)
                VALUES (%s, %s)
                ON CONFLICT (product_code)
                DO UPDATE SET product_category = EXCLUDED.product_category
            """, (row["full_code"], row["product_category"]))
        conn.commit()

        progress.progress(60)

        cur.execute("SELECT product_id, product_code FROM products_master")
        product_map = {code: pid for pid, code in cur.fetchall()}

        # ================= DELETE EXISTING PRODUCTS =================
        cur.execute("""
            DELETE FROM products
            WHERE house_id IN (
                SELECT h.house_id
                FROM houses h
                JOIN units u ON h.unit_id = u.unit_id
                JOIN projects p ON u.project_id = p.project_id
                WHERE p.project_name = ANY(%s)
            )
        """, (list(project_set),))

        conn.commit()

        # ================= PRODUCTS (CORE LOGIC) =================
        inserted_products = 0
        processed = 0
        loop_start = time.time()

        for i, row in df.iterrows():

            try:
                unit_id = unit_map[(row["unit_name"], project_map[row["project_name"]])]
                key = (row["house_no"], unit_id)

                if key not in house_map:
                    st.error(f"❌ House mapping failed at row {i+1}")
                    st.stop()

                house_id = house_map[key]

                if row["full_code"] not in product_map:
                    st.error(f"❌ Product mapping failed at row {i+1}")
                    st.stop()

                product_id = product_map[row["full_code"]]

                for _ in range(row["quantity"]):

                    cur.execute("""
                        INSERT INTO products (house_id, product_id, orientation)
                        VALUES (%s, %s, %s)
                    """, (house_id, product_id, row["orientation"]))

                    inserted_products += 1
                    processed += 1

                    if processed % 20 == 0 or processed == total_items:

                        elapsed = time.time() - loop_start
                        speed = processed / elapsed if elapsed > 0 else 0

                        remaining = total_items - processed
                        eta = remaining / speed if speed > 0 else 0

                        percent = 60 + int((processed / total_items) * 35)
                        progress.progress(min(percent, 95))

                        eta_box.info(f"""
⏳ Processed: {processed}/{total_items}  
⚡ Speed: {round(speed, 2)} items/sec  
⌛ Estimated Time Remaining: {round(eta, 2)} sec
""")

            except Exception as e:
                st.error(f"""
❌ Error at row {i+1}

{str(e)}
""")
                st.stop()

        conn.commit()

        progress.progress(100)

        total_time = round(time.time() - start_time, 2)

        status.empty()

        st.success(f"""
🚀 Upload Completed Successfully

⏱ Time: {total_time} sec  
📄 Excel Rows: {total_rows}

📊 Summary:
- Projects: {len(project_set)}
- Units: {len(unit_set)}
- Houses: {len(house_set)}
- Product Types: {len(product_set)}
- Total Product Items: {inserted_products}
""")

        st.info(f"⚡ Speed: {round(inserted_products / total_time, 2)} items/sec")
