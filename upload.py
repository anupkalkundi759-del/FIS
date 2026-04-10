def show_upload(conn, cur):
    import streamlit as st
    import pandas as pd
    import time

    if st.session_state.role != "admin":
        st.error("Access denied")
        st.stop()

    st.subheader("Upload Project Setup Excel")

    uploaded_file = st.file_uploader("Upload Excel", type=["xlsx"])

    if uploaded_file:

        start_time = time.time()
        status = st.empty()
        status.info("⏳ Uploading... Please wait")

        df = pd.read_excel(uploaded_file)
        df.columns = df.columns.str.strip().str.lower()

        # ================= REQUIRED =================
        required = ["project_name", "unit_name", "house_no", "product_code", "orientation", "quantity"]
        for col in required:
            if col not in df.columns:
                st.error(f"Missing column: {col}")
                st.stop()

        # ================= CLEAN =================
        df["project_name"] = df["project_name"].astype(str).str.strip()
        df["unit_name"] = df["unit_name"].astype(str).str.strip()
        df["house_no"] = df["house_no"].astype(str).str.strip()
        df["product_code"] = df["product_code"].astype(str).str.strip()
        df["orientation"] = df["orientation"].astype(str).str.strip()

        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(1).astype(int)

        # ================= ESTIMATION =================
        total_items_est = df["quantity"].sum()
        estimated_time = round(total_items_est * 0.002, 2)

        st.info(f"📊 Estimated Items: {total_items_est} | Estimated Time: ~{estimated_time}s")

        # ================= PROJECT =================
        for p in df["project_name"].unique():
            cur.execute("INSERT INTO projects (project_name) VALUES (%s) ON CONFLICT DO NOTHING", (p,))
        conn.commit()

        cur.execute("SELECT project_id, project_name FROM projects")
        project_map = {name: pid for pid, name in cur.fetchall()}

        # ================= UNITS =================
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO units (project_id, unit_name)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (project_map[row["project_name"]], row["unit_name"]))
        conn.commit()

        cur.execute("SELECT unit_id, unit_name, project_id FROM units")
        unit_map = {(u, p): uid for uid, u, p in cur.fetchall()}

        # ================= HOUSES =================
        for _, row in df.iterrows():
            unit_id = unit_map[(row["unit_name"], project_map[row["project_name"]])]
            cur.execute("""
                INSERT INTO houses (unit_id, house_no)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (unit_id, row["house_no"]))
        conn.commit()

        cur.execute("SELECT house_id, house_no, unit_id FROM houses")
        house_map = {(str(h).strip(), u): hid for hid, h, u in cur.fetchall()}

        # ================= PRODUCT MASTER =================
        for p in df["product_code"].unique():
            cur.execute("""
                INSERT INTO products_master (product_code)
                VALUES (%s)
                ON CONFLICT DO NOTHING
            """, (p,))
        conn.commit()

        cur.execute("SELECT product_id, product_code FROM products_master")
        product_map = {code: pid for pid, code in cur.fetchall()}

        # ================= STRICT INSERT =================
        inserted_items = 0

        for i, row in df.iterrows():

            try:
                project_id = project_map[row["project_name"]]
            except:
                st.error(f"❌ Project not found at row {i+1}: {row['project_name']}")
                st.stop()

            try:
                unit_id = unit_map[(row["unit_name"], project_id)]
            except:
                st.error(f"❌ Unit mapping failed at row {i+1}: {row['unit_name']}")
                st.stop()

            key = (str(row["house_no"]).strip(), unit_id)

            if key not in house_map:
                st.error(f"""
❌ House mapping failed

Row: {i+1}
House: {row['house_no']}
Unit ID: {unit_id}

👉 Check Excel for spaces / mismatch
""")
                st.stop()

            house_id = house_map[key]

            try:
                product_id = product_map[row["product_code"]]
            except:
                st.error(f"❌ Product not found at row {i+1}: {row['product_code']}")
                st.stop()

            orientation = row["orientation"]

            for _ in range(row["quantity"]):
                cur.execute("""
                    INSERT INTO products (house_id, product_id, orientation)
                    VALUES (%s, %s, %s)
                """, (house_id, product_id, orientation))

                inserted_items += 1

        conn.commit()

        end_time = time.time()
        total_time = round(end_time - start_time, 2)

        status.empty()

        st.success(f"""
🚀 Upload Completed Successfully

⏱ Time Taken: {total_time}s  
📊 Estimated Time: ~{estimated_time}s  

🏗 Projects: {df["project_name"].nunique()}  
🏢 Units: {df["unit_name"].nunique()}  
🏠 Houses: {df["house_no"].nunique()}  
📦 Product Types: {df["product_code"].nunique()}  

🔩 Total Items Created: {inserted_items}

✅ Data Integrity Maintained (No rows skipped)
""")
