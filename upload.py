def show_upload(conn, cur):
    import streamlit as st
    import pandas as pd
    import time

    if st.session_state.role != "admin":
        st.error("Access denied")
        st.stop()

    st.subheader("Upload Project Setup Excel")

    file = st.file_uploader("Upload Excel", type=["xlsx"])

    if file:

        start_time = time.time()
        status = st.empty()
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

        # ================= COUNTERS =================
        project_count = 0
        unit_count = 0
        house_count = 0
        product_master_count = 0
        product_count = 0

        # ================= PROJECTS =================
        projects = df["project_name"].unique()
        for p in projects:
            cur.execute("""
                INSERT INTO projects (project_name)
                VALUES (%s)
                ON CONFLICT (project_name) DO NOTHING
            """, (p,))
        conn.commit()

        project_count = len(projects)

        cur.execute("SELECT project_id, project_name FROM projects")
        project_map = {name: pid for pid, name in cur.fetchall()}

        # ================= UNITS =================
        unit_set = set()
        for _, row in df.iterrows():
            key = (row["project_name"], row["unit_name"])
            unit_set.add(key)

        for project_name, unit_name in unit_set:
            cur.execute("""
                INSERT INTO units (project_id, unit_name)
                VALUES (%s, %s)
                ON CONFLICT (project_id, unit_name) DO NOTHING
            """, (project_map[project_name], unit_name))

        conn.commit()
        unit_count = len(unit_set)

        cur.execute("SELECT unit_id, unit_name, project_id FROM units")
        unit_map = {(u, p): uid for uid, u, p in cur.fetchall()}

        # ================= HOUSES =================
        house_set = set()
        for _, row in df.iterrows():
            key = (row["project_name"], row["unit_name"], row["house_no"])
            house_set.add(key)

        for project_name, unit_name, house_no in house_set:
            unit_id = unit_map[(unit_name, project_map[project_name])]

            cur.execute("""
                INSERT INTO houses (unit_id, house_no)
                VALUES (%s, %s)
                ON CONFLICT (unit_id, house_no) DO NOTHING
            """, (unit_id, house_no))

        conn.commit()
        house_count = len(house_set)

        cur.execute("SELECT house_id, house_no, unit_id FROM houses")
        house_map = {(h, u): hid for hid, h, u in cur.fetchall()}

        # ================= PRODUCT MASTER =================
        product_set = df["product_code"].unique()

        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO products_master (product_code, product_category, orientation)
                VALUES (%s, %s, %s)
                ON CONFLICT (product_code) DO NOTHING
            """, (row["product_code"], row["product_category"], row["orientation"]))

        conn.commit()
        product_master_count = len(product_set)

        cur.execute("SELECT product_id, product_code FROM products_master")
        product_map = {code: pid for pid, code in cur.fetchall()}

        # ================= PRODUCTS =================
        product_link_set = set()

        for _, row in df.iterrows():
            key = (row["project_name"], row["unit_name"], row["house_no"], row["product_code"])
            product_link_set.add(key)

        for project_name, unit_name, house_no, product_code in product_link_set:
            unit_id = unit_map[(unit_name, project_map[project_name])]
            house_id = house_map[(house_no, unit_id)]
            product_id = product_map[product_code]

            cur.execute("""
                INSERT INTO products (house_id, product_id, quantity)
                VALUES (%s, %s, %s)
                ON CONFLICT (house_id, product_id) DO NOTHING
            """, (house_id, product_id, 1))

        conn.commit()
        product_count = len(product_link_set)

        # ================= FINAL =================
        total_time = round(time.time() - start_time, 2)
        status.empty()

        st.success(f"""
🚀 Upload Completed!

⏱ Time Taken: {total_time} sec

📊 Summary:
- Projects Added: {project_count}
- Units Added: {unit_count}
- Houses Added: {house_count}
- Product Master Added: {product_master_count}
- Products Linked: {product_count}
""")

        st.dataframe(df.head())
