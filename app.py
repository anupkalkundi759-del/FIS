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

if st.session_state.get("auth", False):
    st.session_state.logged_in = True

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

# ================= PRO SIDEBAR CSS =================
st.markdown("""
<style>

/* Sidebar */
[data-testid="stSidebar"] {
    background-color: #1f4e79;
}

/* Remove default radio circles */
[data-testid="stSidebar"] input[type="radio"] {
    display: none;
}

/* Sidebar text */
[data-testid="stSidebar"] * {
    color: white !important;
}

/* Section titles */
.sidebar-section {
    font-size: 11px;
    font-weight: 600;
    opacity: 0.6;
    margin-top: 20px;
    margin-bottom: 6px;
}

/* Menu item */
.menu-item {
    padding: 10px 12px;
    border-radius: 8px;
    margin-bottom: 4px;
    cursor: pointer;
}

/* Active item */
.menu-item-active {
    background-color: rgba(255,255,255,0.2);
    font-weight: 600;
}

/* Hover */
.menu-item:hover {
    background-color: rgba(255,255,255,0.1);
}

</style>
""", unsafe_allow_html=True)

# ================= SIDEBAR =================
with st.sidebar:

    st.markdown("### 🏢 OperaFlow")
    st.caption("Enterprise Suite")

    st.markdown("---")
    st.markdown(f"👤 {st.session_state.role.upper()}")

    # ===== MENU STRUCTURE =====
    menu_structure = {
        "OPERATIONS": [
            ("📍 Tracking", "Tracking"),
            ("📦 Product Tracking", "Product Tracking"),
            ("📊 Dashboard", "Dashboard")
        ]
    }

    if st.session_state.role == "admin":
        menu_structure["MANAGEMENT"] = [
            ("⚙️ Scheduling Engine", "Scheduling Engine"),
            ("📤 Upload Excel", "Upload Excel"),
            ("📏 Measurement Update", "Measurement Update")
        ]
        menu_structure["SYSTEM"] = [
            ("🗑 Delete Data", "Delete Data")
        ]

    # ===== BUILD MENU =====
    all_pages = []
    labels = []

    for section, items in menu_structure.items():

        st.markdown(f'<div class="sidebar-section">{section}</div>', unsafe_allow_html=True)

        for label, value in items:
            all_pages.append(value)
            labels.append(label)

    # ===== RADIO (HIDDEN LOGIC) =====
    selected = st.radio(
        "",
        all_pages,
        index=all_pages.index(st.session_state.page),
        key="nav",
        label_visibility="collapsed"
    )

    st.session_state.page = selected

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
