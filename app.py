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

# restore login (same session only)
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
except Exception as e:
    st.error("❌ Database connection failed")
    st.stop()

# ================= SIDEBAR STYLE =================
st.markdown("""
<style>
[data-testid="stSidebar"] {
    background-color: #1f4e79;
}
[data-testid="stSidebar"] * {
    color: white !important;
}
.section {
    font-size: 12px;
    margin-top: 18px;
    opacity: 0.7;
    font-weight: bold;
}
</style>
""", unsafe_allow_html=True)

# ================= SIDEBAR =================
with st.sidebar:

    # HEADER
    st.markdown("### 🏢 OperaFlow")
    st.caption("Enterprise Suite")

    st.markdown("---")

    # USER INFO
    st.markdown(f"**👤 {st.session_state.role.upper()}**")

    st.markdown("---")

    selected_page = None

    # OPERATIONS
    st.markdown('<div class="section">OPERATIONS</div>', unsafe_allow_html=True)
    if st.button("📍 Tracking"):
        selected_page = "Tracking"
    if st.button("📦 Product Tracking"):
        selected_page = "Product Tracking"
    if st.button("📊 Dashboard"):
        selected_page = "Dashboard"

    # MANAGEMENT
    if st.session_state.role == "admin":
        st.markdown('<div class="section">MANAGEMENT</div>', unsafe_allow_html=True)

        if st.button("⚙️ Scheduling Engine"):
            selected_page = "Scheduling Engine"
        if st.button("📤 Upload Excel"):
            selected_page = "Upload Excel"
        if st.button("📏 Measurement Update"):
            selected_page = "Measurement Update"

        st.markdown('<div class="section">SYSTEM</div>', unsafe_allow_html=True)

        if st.button("🗑 Delete Data"):
            selected_page = "Delete Data"

    st.markdown("---")

    # LOGOUT AT BOTTOM
    if st.button("🚪 Logout"):
        st.session_state.clear()
        st.rerun()

# ================= PAGE STATE FIX =================
if "page" not in st.session_state:
    st.session_state.page = "Tracking"

if selected_page:
    st.session_state.page = selected_page

selected_page = st.session_state.page

# ================= MAIN =================
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
