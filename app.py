import streamlit as st
import psycopg2
import streamlit_authenticator as stauth

from tracking import show_tracking
from dashboard import show_dashboard
from product_tracking import show_product_tracking
from measurement import update_measurement
from engine import run_engine
from upload import show_upload
from delete import show_delete

# ================= AUTH SETUP =================
names = ["Worker", "Admin"]
usernames = ["worker", "admin"]
passwords = ["123", "admin@123"]

hashed_passwords = stauth.Hasher(passwords).generate()

authenticator = stauth.Authenticate(
    names,
    usernames,
    hashed_passwords,
    "factory_app",
    "abcdef123",
    cookie_expiry_days=7
)

# ================= LOGIN =================
name, auth_status, username = authenticator.login("Login", "main")

if auth_status == False:
    st.error("Invalid credentials")
    st.stop()

if auth_status is None:
    st.warning("Enter login details")
    st.stop()

# ================= ROLE =================
role = "admin" if username == "admin" else "worker"

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
    st.error("Database connection failed")
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
    margin-top: 15px;
    opacity: 0.7;
}
</style>
""", unsafe_allow_html=True)

# ================= SIDEBAR =================
with st.sidebar:

    st.markdown("### 🏢 OperaFlow")
    st.caption("Enterprise Suite")

    st.markdown("---")
    st.markdown(f"**👤 {name}**")
    st.caption(role.capitalize())

    st.markdown("---")

    # OPERATIONS
    st.markdown('<div class="section">OPERATIONS</div>', unsafe_allow_html=True)
    op_page = st.radio("", ["Tracking", "Product Tracking", "Dashboard"], key="op")

    # MANAGEMENT (admin only)
    if role == "admin":
        st.markdown('<div class="section">MANAGEMENT</div>', unsafe_allow_html=True)
        mgmt_page = st.radio("", [
            "Scheduling Engine",
            "Upload Excel",
            "Measurement Update"
        ], key="mgmt")

        st.markdown('<div class="section">SYSTEM</div>', unsafe_allow_html=True)
        sys_page = st.radio("", ["Delete Data"], key="sys")

    st.markdown("---")

    # LOGOUT (proper)
    authenticator.logout("🚪 Logout", "sidebar")

# ================= PAGE LOGIC =================
selected_page = op_page

if role == "admin":
    if "mgmt_page" in st.session_state:
        selected_page = st.session_state.get("mgmt", op_page)
    if "sys_page" in st.session_state:
        selected_page = st.session_state.get("sys", selected_page)

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
