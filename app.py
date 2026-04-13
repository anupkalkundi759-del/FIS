import streamlit as st
import psycopg2

# ================= PERFORMANCE: CACHE DB =================
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=st.secrets["DB_HOST"],
        port=st.secrets["DB_PORT"],
        database=st.secrets["DB_NAME"],
        user=st.secrets["DB_USER"],
        password=st.secrets["DB_PASSWORD"]
    )

@st.cache_resource
def get_cursor(conn):
    return conn.cursor()

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

# ================= DB =================
try:
    conn = get_connection()
    cur = get_cursor(conn)
except:
    st.error("DB error")
    st.stop()

# ================= SIDEBAR =================
with st.sidebar:
    st.markdown("**OperaFlow**")
    st.markdown(f"👤 {st.session_state.role.upper()}")

    page = st.radio(
        "Navigation",
        [
            "Tracking",
            "Product Tracking",
            "Dashboard",
            "Scheduling Engine",
            "Upload Excel",
            "Measurement Update",
            "Delete Data"
        ]
    )

    st.session_state.page = page

    if st.button("🚪 Logout"):
        st.session_state.clear()
        st.rerun()

# ================= MAIN =================
st.title("🏭 Factory Intelligence System")

page = st.session_state.page

# ================= LAZY LOAD PAGES =================
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
