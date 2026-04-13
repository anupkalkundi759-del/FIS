import streamlit as st
import psycopg2

from tracking import show_tracking
from dashboard import show_dashboard
from product_tracking import show_product_tracking
from measurement import update_measurement
from engine import run_engine
from upload import show_upload
from delete import show_delete

# ================= LOGIN =================
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.role = None

def login():
    st.title("🔐 Login")

    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Login"):

        users = {
            "worker": {"password": "123", "role": "shopfloor"},
            "admin": {"password": "admin@123", "role": "admin"}
        }

        if username in users and users[username]["password"] == password:
            st.session_state.logged_in = True
            st.session_state.role = users[username]["role"]
            st.rerun()
        else:
            st.error("Invalid credentials")

if not st.session_state.logged_in:
    login()
    st.stop()

# ================= DB =================
conn = psycopg2.connect(
    host="aws-1-ap-south-1.pooler.supabase.com",
    port="6543",
    database="postgres",
    user="postgres.uigwsyjmcmmpsgshsxhc",
    password=",6r+rZUqHGnWide"
)
cur = conn.cursor()

# ================= NAVIGATION =================
st.sidebar.title("📂 Navigation")

menu = {
    "🏗 Execution": [
        "📍 Tracking",
        "📦 Product Tracking"
    ],
    "📊 Planning & Control": [
        "📊 Dashboard",
        "⚙️ Scheduling Engine"
    ],
    "🗂 Data Management": [
        "📤 Upload Excel",
        "🗑 Delete Data",
        "📏 Measurement Update"
    ]
}

# ================= ROLE FILTER =================
if st.session_state.role == "admin":
    allowed_pages = [
        "📍 Tracking",
        "📦 Product Tracking",
        "📊 Dashboard",
        "⚙️ Scheduling Engine",
        "📤 Upload Excel",
        "🗑 Delete Data",
        "📏 Measurement Update"
    ]
else:
    # SHOPFLOOR
    allowed_pages = [
        "📍 Tracking",
        "📦 Product Tracking"
    ]

# ================= BUILD MENU =================
all_options = []

for section, items in menu.items():
    st.sidebar.markdown(f"### {section}")
    for item in items:
        if item in allowed_pages:
            all_options.append(item)

page = st.sidebar.radio("", all_options)

# ================= LOGOUT =================
if st.sidebar.button("🚪 Logout"):
    st.session_state.logged_in = False
    st.session_state.role = None
    st.rerun()

st.title("🏭 Factory Intelligence System")

# ================= ROUTING =================
if page == "📍 Tracking":
    show_tracking(conn, cur)

elif page == "📊 Dashboard":
    show_dashboard(conn, cur)

elif page == "📦 Product Tracking":
    show_product_tracking(conn, cur)

elif page == "📏 Measurement Update":
    update_measurement(conn, cur)

elif page == "⚙️ Scheduling Engine":
    run_engine(conn, cur)

elif page == "📤 Upload Excel":
    show_upload(conn, cur)

elif page == "🗑 Delete Data":
    show_delete(conn, cur)
