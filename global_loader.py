import pandas as pd
import streamlit as st
import time


@st.cache_data(ttl=20, show_spinner=False)
def load_all_data(_conn):

    # =====================================================
    # MASTER LIVE STATUS TABLE
    # =====================================================
    query = """
    WITH latest_log AS (
        SELECT DISTINCT ON (product_instance_id)
            product_instance_id,
            stage_id,
            status,
            timestamp
        FROM tracking_log
        ORDER BY product_instance_id, timestamp DESC
    )

    SELECT
        p.product_instance_id,
        pm.product_code,
        pm.product_category,
        p.orientation,

        pr.project_id,
        pr.project_name,

        u.unit_id,
        u.unit_name,

        h.house_id,
        h.house_no,

        COALESCE(s.stage_name, 'Not Started') AS stage_name,
        COALESCE(ll.status, 'Not Started') AS status,
        ll.timestamp

    FROM products p
    JOIN products_master pm ON p.product_id = pm.product_id
    JOIN houses h ON p.house_id = h.house_id
    JOIN units u ON h.unit_id = u.unit_id
    JOIN projects pr ON u.project_id = pr.project_id

    LEFT JOIN latest_log ll
        ON ll.product_instance_id = p.product_instance_id

    LEFT JOIN stages s
        ON ll.stage_id = s.stage_id

    ORDER BY h.house_no, pm.product_code
    """

    live_df = pd.read_sql(query, _conn)

    if not live_df.empty:
        live_df["timestamp"] = pd.to_datetime(live_df["timestamp"], errors="coerce", utc=True)

    # =====================================================
    # PROJECT SUMMARY
    # =====================================================
    if not live_df.empty:
        project_summary = live_df.groupby("project_name").agg(
            total_houses=("house_no", "nunique"),
            total_products=("product_instance_id", "count")
        ).reset_index()

        project_summary["completed"] = project_summary["project_name"].apply(
            lambda x: len(live_df[
                (live_df["project_name"] == x) &
                (live_df["stage_name"] == "Final Assembly") &
                (live_df["status"] == "Completed")
            ])
        )

        project_summary["dispatched"] = project_summary["project_name"].apply(
            lambda x: len(live_df[
                (live_df["project_name"] == x) &
                (live_df["stage_name"] == "Dispatch") &
                (live_df["status"] == "Completed")
            ])
        )

        project_summary["pending"] = project_summary["total_products"] - project_summary["dispatched"]
    else:
        project_summary = pd.DataFrame()

    # =====================================================
    # HOUSE SUMMARY
    # =====================================================
    if not live_df.empty:
        house_summary = live_df.groupby(["project_name", "unit_name", "house_no"]).agg(
            total_products=("product_instance_id", "count"),
            last_update=("timestamp", "max")
        ).reset_index()

        house_summary["dispatched"] = house_summary.apply(
            lambda x: len(live_df[
                (live_df["house_no"] == x["house_no"]) &
                (live_df["stage_name"] == "Dispatch") &
                (live_df["status"] == "Completed")
            ]),
            axis=1
        )

        house_summary["pending"] = house_summary["total_products"] - house_summary["dispatched"]
    else:
        house_summary = pd.DataFrame()

    # =====================================================
    # STAGE FLOW
    # =====================================================
    if not live_df.empty:
        stage_flow = live_df.groupby("stage_name").agg(
            wip=("product_instance_id", "count"),
            completed=("status", lambda x: (x == "Completed").sum())
        ).reset_index()

        stage_flow["efficiency"] = round((stage_flow["completed"] / stage_flow["wip"]) * 100, 1)
    else:
        stage_flow = pd.DataFrame()

    # =====================================================
    # HOUSE START DATES
    # =====================================================
    query2 = """
    SELECT
        h.house_no,
        MIN(t.timestamp) AS start_date
    FROM houses h
    JOIN products p ON p.house_id = h.house_id
    JOIN tracking_log t ON t.product_instance_id = p.product_instance_id
    GROUP BY h.house_no
    """

    start_df = pd.read_sql(query2, _conn)

    if not start_df.empty:
        start_df["start_date"] = pd.to_datetime(start_df["start_date"], errors="coerce", utc=True)

    return {
        "live_df": live_df,
        "project_summary": project_summary,
        "house_summary": house_summary,
        "stage_flow": stage_flow,
        "start_df": start_df,
        "loaded_at": time.time()
    }
