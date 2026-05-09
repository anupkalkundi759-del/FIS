def show_upload(conn, cur):
    import streamlit as st
    import pandas as pd
    import time

    if st.session_state.role != "admin":
        st.error("Access denied")
        st.stop()

    st.title("📤 Upload Project Setup Excel")

    # =========================================================
    # ORIGINAL UPLOAD LOGIC
    # =========================================================

    file = st.file_uploader("Upload Excel", type=["xlsx"])

    if file:

        start_time = time.time()

        status = st.empty()
        progress = st.progress(0)
        eta_box = st.empty()

        status.info("⏳ Uploading... Please wait")

        df = pd.read_excel(file, engine="openpyxl")

        # ================= CLEAN =================

        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_")
        )

        required_cols = [
            "project_name",
            "unit_name",
            "house_no",
            "product_code"
        ]

        for col in required_cols:

            if col not in df.columns:
                st.error(f"Missing column: {col}")
                st.stop()

        df = df.dropna(subset=required_cols)

        df["project_name"] = (
            df["project_name"]
            .astype(str)
            .str.strip()
        )

        df["unit_name"] = (
            df["unit_name"]
            .astype(str)
            .str.strip()
        )

        df["house_no"] = (
            df["house_no"]
            .astype(str)
            .str.strip()
        )

        df["product_code"] = (
            df["product_code"]
            .astype(str)
            .str.strip()
        )

        df["product_category"] = (
            df.get("product_category", "")
            .fillna("")
            .astype(str)
            .str.strip()
        )

        df["orientation"] = (
            df.get("orientation", "")
            .fillna("")
            .astype(str)
            .str.strip()
        )

        df["quantity"] = (
            pd.to_numeric(
                df.get("quantity", 1),
                errors="coerce"
            )
            .fillna(1)
            .astype(int)
        )

        df["full_code"] = df.apply(
            lambda x:
            f"{x['product_code']} ({x['orientation']})"
            if x["orientation"]
            else x["product_code"],
            axis=1
        )

        df = df.groupby(
            [
                "project_name",
                "unit_name",
                "house_no",
                "full_code",
                "orientation",
                "product_category"
            ],
            as_index=False
        )["quantity"].sum()

        total_rows = len(df)

        # ================= COUNTS =================

        project_set = set(df["project_name"])

        unit_set = set(
            zip(
                df["project_name"],
                df["unit_name"]
            )
        )

        house_set = set(
            zip(
                df["project_name"],
                df["unit_name"],
                df["house_no"]
            )
        )

        product_set = set(df["full_code"])

        total_items = int(df["quantity"].sum())

        st.info(f"📊 Estimated Items: {total_items}")

        # ================= PROJECTS =================

        for p in project_set:

            cur.execute("""
                INSERT INTO projects (project_name)
                VALUES (%s)
                ON CONFLICT (project_name)
                DO NOTHING
            """, (p,))

        conn.commit()

        progress.progress(10)

        cur.execute("""
            SELECT project_id, project_name
            FROM projects
        """)

        project_map = {
            name: pid
            for pid, name in cur.fetchall()
        }

        # ================= UNITS =================

        for project_name, unit_name in unit_set:

            cur.execute("""
                INSERT INTO units
                (project_id, unit_name)
                VALUES (%s, %s)
                ON CONFLICT (project_id, unit_name)
                DO NOTHING
            """, (
                project_map[project_name],
                unit_name
            ))

        conn.commit()

        progress.progress(25)

        cur.execute("""
            SELECT unit_id, unit_name, project_id
            FROM units
        """)

        unit_map = {
            (u, p): uid
            for uid, u, p in cur.fetchall()
        }

        # ================= HOUSES =================

        for project_name, unit_name, house_no in house_set:

            unit_id = unit_map[
                (unit_name, project_map[project_name])
            ]

            cur.execute("""
                INSERT INTO houses
                (unit_id, house_no)
                VALUES (%s, %s)
                ON CONFLICT (unit_id, house_no)
                DO NOTHING
            """, (
                unit_id,
                house_no
            ))

        conn.commit()

        progress.progress(40)

        cur.execute("""
            SELECT house_id, house_no, unit_id
            FROM houses
        """)

        house_map = {
            (str(h).strip(), u): hid
            for hid, h, u in cur.fetchall()
        }

        # ================= PRODUCT MASTER =================

        product_master_df = df[
            ["full_code", "product_category"]
        ].drop_duplicates()

        for _, row in product_master_df.iterrows():

            cur.execute("""
                INSERT INTO products_master
                (product_code, product_category)
                VALUES (%s, %s)
                ON CONFLICT (product_code)
                DO UPDATE SET
                product_category = EXCLUDED.product_category
            """, (
                row["full_code"],
                row["product_category"]
            ))

        conn.commit()

        progress.progress(60)

        cur.execute("""
            SELECT product_id, product_code
            FROM products_master
        """)

        product_map = {
            code: pid
            for pid, code in cur.fetchall()
        }

        # ================= PRODUCTS INSERT =================

        inserted_products = 0
        skipped_existing = 0
        processed = 0

        loop_start = time.time()

        for i, row in df.iterrows():

            try:

                unit_id = unit_map[
                    (
                        row["unit_name"],
                        project_map[row["project_name"]]
                    )
                ]

                house_id = house_map[
                    (
                        row["house_no"],
                        unit_id
                    )
                ]

                product_id = product_map[
                    row["full_code"]
                ]

                cur.execute("""
                    SELECT COUNT(*)
                    FROM products
                    WHERE house_id = %s
                    AND product_id = %s
                    AND COALESCE(orientation,'')
                    =
                    COALESCE(%s,'')
                """, (
                    house_id,
                    product_id,
                    row["orientation"]
                ))

                existing_qty = cur.fetchone()[0]

                upload_qty = int(row["quantity"])

                qty_to_insert = max(
                    upload_qty - existing_qty,
                    0
                )

                skipped_existing += min(
                    existing_qty,
                    upload_qty
                )

                for _ in range(qty_to_insert):

                    cur.execute("""
                        INSERT INTO products
                        (house_id, product_id, orientation)
                        VALUES (%s, %s, %s)
                    """, (
                        house_id,
                        product_id,
                        row["orientation"]
                    ))

                    inserted_products += 1

                processed += upload_qty

                if (
                    processed % 20 == 0
                    or processed >= total_items
                ):

                    elapsed = time.time() - loop_start

                    speed = (
                        processed / elapsed
                        if elapsed > 0 else 0
                    )

                    remaining = total_items - processed

                    eta = (
                        remaining / speed
                        if speed > 0 else 0
                    )

                    percent = (
                        60 +
                        int(
                            (processed / total_items) * 35
                        )
                    )

                    progress.progress(
                        min(percent, 95)
                    )

                    eta_box.info(f"""
⏳ Processed: {processed}/{total_items}
⚡ Speed: {round(speed, 2)} items/sec
⌛ Estimated Time Remaining: {round(eta, 2)} sec
""")

            except Exception as e:

                st.error(
                    f"❌ Error at row {i+1}\n{str(e)}"
                )

                st.stop()

        conn.commit()

        progress.progress(100)

        total_time = round(
            time.time() - start_time,
            2
        )

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
- Newly Added Product Items: {inserted_products}
- Existing Preserved Items: {skipped_existing}
""")

        st.info(f"""
⚡ Speed:
{round((inserted_products + skipped_existing) / total_time, 2)}
items/sec
""")

    st.divider()

    # =========================================================
    # ADD EXTRA PRODUCT
    # =========================================================

    st.subheader("➕ Add Extra Product")

    cur.execute("""
        SELECT project_id, project_name
        FROM projects
        ORDER BY project_name
    """)

    project_data = cur.fetchall()

    if project_data:

        project_map = {
            name: pid
            for pid, name in project_data
        }

        # ================= ROW 1 =================

        row1_col1, row1_col2, row1_col3 = st.columns(3)

        with row1_col1:

            selected_project = st.selectbox(
                "Project",
                options=list(project_map.keys()),
                key="quick_project"
            )

        cur.execute("""
            SELECT unit_id, unit_name
            FROM units
            WHERE project_id = %s
            ORDER BY unit_name
        """, (
            project_map[selected_project],
        ))

        unit_data = cur.fetchall()

        unit_map = {
            name: uid
            for uid, name in unit_data
        }

        with row1_col2:

            selected_unit = st.selectbox(
                "Unit",
                options=list(unit_map.keys()),
                key="quick_unit"
            )

        cur.execute("""
            SELECT house_id, house_no
            FROM houses
            WHERE unit_id = %s
            ORDER BY house_no
        """, (
            unit_map[selected_unit],
        ))

        house_data = cur.fetchall()

        house_map = {
            name: hid
            for hid, name in house_data
        }

        with row1_col3:

            selected_house = st.selectbox(
                "House",
                options=list(house_map.keys()),
                key="quick_house"
            )

        # ================= ROW 2 =================

        row2_col1, row2_col2, row2_col3 = st.columns(3)

        with row2_col1:

            quick_product_code = st.text_input(
                "Product Code",
                key="quick_product_code"
            )

        with row2_col2:

            quick_category = st.text_input(
                "Product Category",
                key="quick_category"
            )

        with row2_col3:

            quick_orientation = st.text_input(
                "Orientation (Optional)",
                key="quick_orientation"
            )

        # ================= ROW 3 =================

        row3_col1, row3_col2 = st.columns([1, 1])

        with row3_col1:

            quick_quantity = st.number_input(
                "Quantity",
                min_value=1,
                value=1,
                step=1,
                key="quick_quantity"
            )

        with row3_col2:

            st.markdown("<br>", unsafe_allow_html=True)

            add_btn = st.button("➕ Add Product Instantly")

        # ================= ADD LOGIC =================

        if add_btn:

            try:

                product_code = (
                    quick_product_code.strip()
                )

                orientation = (
                    quick_orientation.strip()
                )

                category = (
                    quick_category.strip()
                )

                quantity = int(quick_quantity)

                if not product_code:

                    st.warning(
                        "Product code required"
                    )

                    st.stop()

                full_code = (
                    f"{product_code} ({orientation})"
                    if orientation
                    else product_code
                )

                cur.execute("""
                    INSERT INTO products_master
                    (product_code, product_category)
                    VALUES (%s, %s)
                    ON CONFLICT (product_code)
                    DO UPDATE SET
                    product_category =
                    EXCLUDED.product_category
                """, (
                    full_code,
                    category
                ))

                conn.commit()

                cur.execute("""
                    SELECT product_id
                    FROM products_master
                    WHERE product_code = %s
                """, (
                    full_code,
                ))

                result = cur.fetchone()

                if not result:

                    st.error(
                        "Failed to create product"
                    )

                    st.stop()

                product_id = result[0]

                house_id = house_map[
                    selected_house
                ]

                cur.execute("""
                    SELECT COUNT(*)
                    FROM products
                    WHERE house_id = %s
                    AND product_id = %s
                    AND COALESCE(orientation,'')
                    =
                    COALESCE(%s,'')
                """, (
                    house_id,
                    product_id,
                    orientation
                ))

                existing_qty = cur.fetchone()[0]

                qty_to_insert = max(
                    quantity - existing_qty,
                    0
                )

                inserted = 0

                for _ in range(qty_to_insert):

                    cur.execute("""
                        INSERT INTO products
                        (house_id, product_id, orientation)
                        VALUES (%s, %s, %s)
                    """, (
                        house_id,
                        product_id,
                        orientation
                    ))

                    inserted += 1

                conn.commit()

                st.success(f"""
✅ Product Added Successfully

Project:
{selected_project}

Unit:
{selected_unit}

House:
{selected_house}

Product:
{full_code}

Inserted Quantity:
{inserted}

Existing Preserved:
{existing_qty}
""")

            except Exception as e:

                conn.rollback()

                st.error(
                    f"Add product failed: {str(e)}"
                )

    st.divider()

    # =========================================================
    # RENAME PRODUCT CODE
    # =========================================================

    st.subheader("✏ Rename / Correct Product Code")

    cur.execute("""
        SELECT project_id, project_name
        FROM projects
        ORDER BY project_name
    """)

    rename_project_data = cur.fetchall()

    if rename_project_data:

        rename_project_map = {
            name: pid
            for pid, name in rename_project_data
        }

        # ================= ROW 1 =================

        rename_row1_col1, rename_row1_col2, rename_row1_col3 = st.columns(3)

        with rename_row1_col1:

            rename_project = st.selectbox(
                "Select Project",
                options=list(rename_project_map.keys()),
                key="rename_project"
            )

        cur.execute("""
            SELECT unit_id, unit_name
            FROM units
            WHERE project_id = %s
            ORDER BY unit_name
        """, (
            rename_project_map[rename_project],
        ))

        rename_unit_data = cur.fetchall()

        rename_unit_map = {
            name: uid
            for uid, name in rename_unit_data
        }

        with rename_row1_col2:

            rename_unit = st.selectbox(
                "Select Unit",
                options=list(rename_unit_map.keys()),
                key="rename_unit"
            )

        cur.execute("""
            SELECT house_id, house_no
            FROM houses
            WHERE unit_id = %s
            ORDER BY house_no
        """, (
            rename_unit_map[rename_unit],
        ))

        rename_house_data = cur.fetchall()

        rename_house_map = {
            name: hid
            for hid, name in rename_house_data
        }

        with rename_row1_col3:

            rename_houses = st.multiselect(
                "Select Houses",
                options=list(rename_house_map.keys()),
                key="rename_house"
            )

        # ================= PRODUCTS =================

        product_codes = []

        if rename_houses:

            house_ids = [
                rename_house_map[h]
                for h in rename_houses
            ]

            placeholders = ",".join(
                ["%s"] * len(house_ids)
            )

            cur.execute(f"""
                SELECT DISTINCT pm.product_code
                FROM products p
                JOIN products_master pm
                ON p.product_id = pm.product_id
                WHERE p.house_id IN ({placeholders})
                ORDER BY pm.product_code
            """, tuple(house_ids))

            product_codes = [
                x[0]
                for x in cur.fetchall()
            ]

        # ================= ROW 2 =================

        rename_row2_col1, rename_row2_col2 = st.columns(2)

        with rename_row2_col1:

            old_code = st.multiselect(
                "Select Product Codes",
                options=product_codes,
                key="rename_old_code"
            )

        with rename_row2_col2:

            new_code = st.text_input(
                "Enter New Product Code",
                key="rename_new_code"
            )

        # ================= ROW 3 =================

        rename_btn = st.button("✅ Update Product Code")

        # ================= UPDATE LOGIC =================

        if rename_btn:

            if (
                not rename_houses
                or not old_code
                or not new_code.strip()
            ):

                st.warning(
                    "Please select houses, products and enter new code"
                )

            else:

                new_code = new_code.strip()

                try:

                    cur.execute("""
                        SELECT COUNT(*)
                        FROM products_master
                        WHERE product_code = %s
                    """, (
                        new_code,
                    ))

                    exists = cur.fetchone()[0]

                    if exists > 0:

                        st.error(
                            f"{new_code} already exists"
                        )

                        st.stop()

                    for code in old_code:

                        cur.execute("""
                            UPDATE products_master
                            SET product_code = %s
                            WHERE product_code = %s
                        """, (
                            new_code,
                            code
                        ))

                    conn.commit()

                    st.success(f"""
✅ Product Code Updated Successfully

Selected Houses:
{len(rename_houses)}

Updated Product(s):
{len(old_code)}

New Code:
{new_code}
""")

                    st.rerun()

                except Exception as e:

                    conn.rollback()

                    st.error(
                        f"Rename failed: {str(e)}"
                    )
