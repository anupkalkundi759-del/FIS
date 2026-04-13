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

if "role" not in st.session_state:
    st.session_state.role = None

if "auth" not in st.session_state:
    st.session_state.auth = False

if "page" not in st.session_state:
    st.session_state.page = "Tracking"

# ================= LOGIN =================
def login():
    st.title("🔐 Login")

    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Login"):

        users = {
            "worker": {"password": "123", "role": "worker"},
            "admin": {"password": "admin@123", "role": "admin"}
        }

        if username in users and users[username]["password"] == password:
            st.session_state.logged_in = True
            st.session_state.role = users[username]["role"]
            st.session_state.auth = True
            st.session_state.user = username
            st.rerun()
        else:
            st.error("Invalid credentials")

# 🔥 PERSIST LOGIN AFTER REFRESH
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

# ================= SIDEBAR =================
st.sidebar.markdown("## 🏭 Factory System")

# USER INFO
st.sidebar.markdown(f"👤 **{st.session_state.get('user','User')}**")
st.sidebar.markdown(f"Role: `{st.session_state.role}`")
st.sidebar.markdown("---")

# ---------- OPERATIONS ----------
st.sidebar.markdown("### ⚙️ OPERATIONS")

if st.sidebar.button("🔴 Tracking", use_container_width=True):
    st.session_state.page = "Tracking"

if st.sidebar.button("📦 Product Tracking", use_container_width=True):
    st.session_state.page = "Product Tracking"

if st.sidebar.button("📊 Dashboard", use_container_width=True):
    st.session_state.page = "Dashboard"

# ---------- MANAGEMENT ----------
if st.session_state.role == "admin":

    st.sidebar.markdown("### 🛠 MANAGEMENT")

    if st.sidebar.button("🧠 Scheduling Engine", use_container_width=True):
        st.session_state.page = "Scheduling Engine"

    if st.sidebar.button("📤 Upload Excel", use_container_width=True):
        st.session_state.page = "Upload Excel"

    if st.sidebar.button("📏 Measurement Update", use_container_width=True):
        st.session_state.page = "Measurement Update"

    # ---------- SYSTEM ----------
    st.sidebar.markdown("### ⚠️ SYSTEM")

    if st.sidebar.button("🗑 Delete Data", use_container_width=True):
        st.session_state.page = "Delete Data"

# PUSH LOGOUT TO BOTTOM
st.sidebar.markdown("<br><br><br><br><br>", unsafe_allow_html=True)

# LOGOUT
if st.sidebar.button("🚪 Logout", use_container_width=True):
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.rerun()

# ================= MAIN =================
st.title("🏭 Factory Intelligence System")

page = st.session_state.page

# ================= ROUTING =================
if page == "Tracking":
    show_tracking(conn, cur)

elif page == "Dashboard":
    show_dashboard(conn, cur)

elif page == "Product Tracking":
    show_product_tracking(conn, cur)

elif page == "Measurement Update":
    update_measurement(conn, cur)

elif page == "Scheduling Engine":
    run_engine(conn, cur)

elif page == "Upload Excel":
    show_upload(conn, cur)

elif page == "Delete Data":
    show_delete(conn, cur)
