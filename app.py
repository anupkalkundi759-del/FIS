import streamlit as st
import psycopg2

from tracking import show_tracking
from dashboard import show_dashboard
from product_tracking import show_product_tracking
from measurement import update_measurement
from engine import run_engine
from upload import show_upload
from delete import show_delete

# ================= SESSION INIT =================
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.role = None

# 🔥 persist login flag
if "auth" not in st.session_state:
    st.session_state.auth = False

# ================= LOGIN =================
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
            st.session_state.auth = True   # 🔥 persist
            st.rerun()
        else:
            st.error("Invalid credentials")

# 🔥 restore login after refresh
if st.session_state.get("auth", False):
    st.session_state.logged_in = True

# ================= LOGIN CHECK =================
if not st.session_state.logged_in:
    login()
    st.stop()

# ================= DB =================
conn = psycopg2.connect(
    host=st.secrets["DB_HOST"],
    port=st.secrets["DB_PORT"],
    database=st.secrets["DB_NAME"],
    user=st.secrets["DB_USER"],
    password=st.secrets["DB_PASSWORD"]
)
cur = conn.cursor()

# ================= SIDEBAR (CLEAN + HIGHLIGHT) =================
st.sidebar.title("📂 Navigation")

if st.session_state.role == "admin":
    pages = {
        "📍 Tracking": "Tracking",
        "📦 Product Tracking": "Product Tracking",
        "📊 Dashboard": "Dashboard",
        "⚙️ Scheduling Engine": "Scheduling Engine",
        "📤 Upload Excel": "Upload Excel",
        "🗑 Delete Data": "Delete Data",
        "📏 Measurement Update": "Measurement Update"
    }
else:
    # SHOPFLOOR
    pages = {
        "📍 Tracking": "Tracking",
        "📦 Product Tracking": "Product Tracking"
    }

page = st.sidebar.radio("", list(pages.keys()))
selected_page = pages[page]

# ================= LOGOUT =================
if st.sidebar.button("🚪 Logout"):
    st.session_state.logged_in = False
    st.session_state.role = None
    st.session_state.auth = False
    st.rerun()

# ================= MAIN TITLE =================
st.title("🏭 Factory Intelligence System")

# ================= ROUTING =================
if selected_page == "Tracking":
    show_tracking(conn, cur)

elif selected_page == "Dashboard":
    show_dashboard(conn, cur)

elif selected_page == "Product Tracking":
    show_product_tracking(conn, cur)

elif selected_page == "Measurement Update":
    update_measurement(conn, cur)

elif selected_page == "Scheduling Engine":
    run_engine(conn, cur)

elif selected_page == "Upload Excel":
    show_upload(conn, cur)

elif selected_page == "Delete Data":
    show_delete(conn, cur)
