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

    st.markdown("### 🏢 OperaFlow")
    st.caption("Enterprise Suite")

    st.markdown(f"👤 {st.session_state.role.upper()}")

    # ===== STYLE (TIGHT) =====
    st.markdown("""
    <style>
    /* kill vertical gaps properly */
    [data-testid="stSidebar"] .stButton {
        margin-bottom: -6px !important;
    }

    /* button look */
    [data-testid="stSidebar"] .stButton button {
        padding: 4px 6px !important;
        font-size: 13px;
        text-align: left;
        width: 100%;
        background: transparent;
        border: none;
    }

    /* hover */
    [data-testid="stSidebar"] .stButton button:hover {
        background: rgba(255,255,255,0.12);
    }

    /* active */
    .active button {
        background: rgba(255,255,255,0.25) !important;
        font-weight: 600;
    }

    /* section titles */
    .sec {
        font-size: 10px;
        margin: 4px 0 0 0;
        opacity: 0.6;
    }
    </style>
    """, unsafe_allow_html=True)

    def nav(label, page):
        if st.session_state.page == page:
            st.markdown('<div class="active">', unsafe_allow_html=True)
        else:
            st.markdown('<div>', unsafe_allow_html=True)

        if st.button(label, key=page):
            st.session_state.page = page
            st.rerun()

        st.markdown('</div>', unsafe_allow_html=True)

    # ===== NAV =====
    st.markdown('<div class="sec">OPERATIONS</div>', unsafe_allow_html=True)
    nav("📍 Tracking", "Tracking")
    nav("📦 Product Tracking", "Product Tracking")
    nav("📊 Dashboard", "Dashboard")

    if st.session_state.role == "admin":
        st.markdown('<div class="sec">MANAGEMENT</div>', unsafe_allow_html=True)
        nav("⚙️ Scheduling Engine", "Scheduling Engine")
        nav("📤 Upload Excel", "Upload Excel")
        nav("📏 Measurement Update", "Measurement Update")

        st.markdown('<div class="sec">SYSTEM</div>', unsafe_allow_html=True)
        nav("🗑 Delete Data", "Delete Data")

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
