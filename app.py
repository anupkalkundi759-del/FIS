import streamlit as st
import psycopg2

# ================= SESSION =================
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

if "role" not in st.session_state:
    st.session_state.role = None

if "page" not in st.session_state:
    st.session_state.page = "Tracking"

# ================= LOGIN =================
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

# ================= DB (STABLE + AUTO RECONNECT) =================
def create_connection():
    try:
        return psycopg2.connect(
            host=st.secrets["DB_HOST"],
            port=st.secrets["DB_PORT"],
            database=st.secrets["DB_NAME"],
            user=st.secrets["DB_USER"],
            password=st.secrets["DB_PASSWORD"]
        )
    except Exception as e:
        st.error(f"DB connection failed: {e}")
        return None

def get_db():
    if "conn" not in st.session_state or st.session_state.conn.closed:
        st.session_state.conn = create_connection()

    if st.session_state.conn is None:
        return None, None

    try:
        cur = st.session_state.conn.cursor()
        return st.session_state.conn, cur
    except:
        st.session_state.conn = create_connection()
        if st.session_state.conn:
            return st.session_state.conn, st.session_state.conn.cursor()
        return None, None

conn, cur = get_db()

if conn is None:
    st.stop()

# ================= CSS (YOUR ORIGINAL STYLE RESTORED) =================
st.markdown("""
<style>

/* Sidebar color */
[data-testid="stSidebar"] {
    background-color: #1f4e79;
}

/* Text color */
[data-testid="stSidebar"] * {
    color: white !important;
}

/* Button style */
[data-testid="stSidebar"] .stButton button {
    background: transparent !important;
    border: none !important;
    padding: 6px 8px !important;
    text-align: left;
    width: 100%;
}

/* Hover */
[data-testid="stSidebar"] .stButton button:hover {
    background: rgba(255,255,255,0.15) !important;
}

/* Active highlight */
.active-btn button {
    background: rgba(255,255,255,0.3) !important;
    font-weight: 600;
}

</style>
""", unsafe_allow_html=True)

# ================= SIDEBAR =================
with st.sidebar:

    st.markdown("## OperaFlow")
    st.markdown(f"👤 {st.session_state.role.upper()}")

    def nav(label, page):
        is_active = st.session_state.page == page

        if is_active:
            st.markdown('<div class="active-btn">', unsafe_allow_html=True)
        else:
            st.markdown('<div>', unsafe_allow_html=True)

        if st.button(label, key=page):
            st.session_state.page = page

        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("### OPERATIONS")
    nav("📍 Tracking", "Tracking")
    nav("📦 Product Tracking", "Product Tracking")
    nav("📊 Dashboard", "Dashboard")

    if st.session_state.role == "admin":
        st.markdown("### MANAGEMENT")
        nav("⚙️ Scheduling Engine", "Scheduling Engine")
        nav("📤 Upload Excel", "Upload Excel")
        nav("📏 Measurement Update", "Measurement Update")

        st.markdown("### SYSTEM")
        nav("🗑 Delete Data", "Delete Data")

    if st.button("🚪 Logout"):
        st.session_state.clear()
        st.rerun()

# ================= MAIN =================
st.title("🏭 Factory Intelligence System")

page = st.session_state.page

# ================= LAZY LOAD (FASTER LOAD) =================
if page == "Tracking":
    from tracking import show_tracking
    show_tracking(conn, cur)

elif page == "Dashboard":
    from dashboard import show_dashboard
    show_dashboard(conn, cur)

elif page == "Product Tracking":
    from product_tracking import show_product_tracking
    show_product_tracking(conn, cur)

elif page == "Measurement Update":
    from measurement import update_measurement
    update_measurement(conn, cur)

elif page == "Scheduling Engine":
    from engine import run_engine
    run_engine(conn, cur)

elif page == "Upload Excel":
    from upload import show_upload
    show_upload(conn, cur)

elif page == "Delete Data":
    from delete import show_delete
    show_delete(conn, cur)
