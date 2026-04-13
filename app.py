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
            st.rerun()
        else:
            st.error("Invalid credentials")

# restore session
if st.session_state.get("auth", False):
    st.session_state.logged_in = True

# ================= LOGIN CHECK =================
if not st.session_state.logged_in:
    login()
    st.stop()

# ================= DB =================
try:
    conn = psycopg2.connect(
        host=st.secrets["DB_HOST"],
        port=st.secrets["DB_PORT"],
        database=st.secrets["DB_NAME"],
        user=st.secrets["DB_USER"],
        password=st.secrets["DB_PASSWORD"]
    )
    cur = conn.cursor()
except:
    st.error("❌ Database connection failed")
    st.stop()

# ================= SIDEBAR STYLE =================
st.markdown("""
<style>

/* Sidebar color */
[data-testid="stSidebar"] {
    background-color: #1f4e79;
}

/* Text white */
[data-testid="stSidebar"] * {
    color: white !important;
}

/* Remove default button style */
[data-testid="stSidebar"] .stButton button {
    background: transparent !important;
    border: none !important;
    box-shadow: none !important;
    width: 100%;
    text-align: left;
    padding: 8px 10px;
    border-radius: 6px;
}

/* Hover */
[data-testid="stSidebar"] .stButton button:hover {
    background-color: rgba(255,255,255,0.15) !important;
}

/* Active */
.active-nav button {
    background-color: rgba(255,255,255,0.25) !important;
    font-weight: 600;
}

/* Section title */
.section-title {
    font-size: 11px;
    margin-top: 12px;
    margin-bottom: 4px;
    opacity: 0.7;
}

</style>
""", unsafe_allow_html=True)

# ================= SIDEBAR =================
with st.sidebar:

    st.markdown("### 🏢 OperaFlow")
    st.caption("Enterprise Suite")

    st.markdown("---")
    st.markdown(f"👤 {st.session_state.role.upper()}")

    st.markdown("---")

    def nav_item(label, page):
        if st.session_state.page == page:
            st.markdown('<div class="active-nav">', unsafe_allow_html=True)
        else:
            st.markdown('<div>', unsafe_allow_html=True)

        if st.button(label, key=page):
            st.session_state.page = page
            st.rerun()

        st.markdown('</div>', unsafe_allow_html=True)

    # OPERATIONS
    st.markdown('<div class="section-title">OPERATIONS</div>', unsafe_allow_html=True)
    nav_item("📍 Tracking", "Tracking")
    nav_item("📦 Product Tracking", "Product Tracking")
    nav_item("📊 Dashboard", "Dashboard")

    # MANAGEMENT
    if st.session_state.role == "admin":
        st.markdown('<div class="section-title">MANAGEMENT</div>', unsafe_allow_html=True)
        nav_item("⚙️ Scheduling Engine", "Scheduling Engine")
        nav_item("📤 Upload Excel", "Upload Excel")
        nav_item("📏 Measurement Update", "Measurement Update")

        st.markdown('<div class="section-title">SYSTEM</div>', unsafe_allow_html=True)
        nav_item("🗑 Delete Data", "Delete Data")

    st.markdown("---")

    if st.button("🚪 Logout"):
        st.session_state.clear()
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
