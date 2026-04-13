import streamlit as st
import psycopg2

from tracking import show_tracking
from dashboard import show_dashboard
from product_tracking import show_product_tracking
from measurement import update_measurement
from engine import run_engine
from upload import show_upload
from delete import show_delete

# ================= SESSION =================
if "page" not in st.session_state:
    st.session_state.page = "Tracking"

# ================= HANDLE NAV CLICK =================
query_params = st.query_params
if "page" in query_params:
    st.session_state.page = query_params["page"]

# ================= LOGIN =================
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "role" not in st.session_state:
    st.session_state.role = None

def login():
    st.title("🔐 Login")

    u = st.text_input("Username")
    p = st.text_input("Password", type="password")

    if st.button("Login"):
        users = {
            "worker": {"password": "123", "role": "worker"},
            "admin": {"password": "admin@123", "role": "admin"}
        }

        if u in users and users[u]["password"] == p:
            st.session_state.logged_in = True
            st.session_state.role = users[u]["role"]
            st.rerun()
        else:
            st.error("Invalid credentials")

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
    st.error("DB error")
    st.stop()

# ================= SIDEBAR (ZERO GAP HTML) =================
with st.sidebar:

    page = st.session_state.page

    def link(label, value):
        active = "active" if page == value else ""
        return f"""
        <a href="?page={value}" class="nav-item {active}">
            {label}
        </a>
        """

    html = f"""
    <style>
    .nav {{
        display: flex;
        flex-direction: column;
        gap: 0px;
    }}

    .nav-item {{
        padding: 6px 8px;
        text-decoration: none;
        color: white;
        font-size: 13px;
        border-radius: 4px;
    }}

    .nav-item:hover {{
        background: rgba(255,255,255,0.12);
    }}

    .active {{
        background: rgba(255,255,255,0.25);
        font-weight: 600;
    }}

    .section {{
        font-size: 10px;
        margin-top: 6px;
        margin-bottom: 2px;
        opacity: 0.6;
    }}
    </style>

    <div>
        <b>OperaFlow</b><br>
        <span style="opacity:0.7;font-size:12px;">Enterprise Suite</span><br><br>
        👤 {st.session_state.role.upper()}
    </div>

    <div class="nav">

        <div class="section">OPERATIONS</div>
        {link("📍 Tracking", "Tracking")}
        {link("📦 Product Tracking", "Product Tracking")}
        {link("📊 Dashboard", "Dashboard")}
    """

    if st.session_state.role == "admin":
        html += f"""
        <div class="section">MANAGEMENT</div>
        {link("⚙️ Scheduling Engine", "Scheduling Engine")}
        {link("📤 Upload Excel", "Upload Excel")}
        {link("📏 Measurement Update", "Measurement Update")}

        <div class="section">SYSTEM</div>
        {link("🗑 Delete Data", "Delete Data")}
        """

    html += "</div>"

    st.markdown(html, unsafe_allow_html=True)

    if st.button("🚪 Logout"):
        st.session_state.clear()
        st.rerun()

# ================= MAIN =================
st.title("🏭 Factory Intelligence System")

page = st.session_state.page

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
