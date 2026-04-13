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

# ================= DB (FIXED) =================
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
    # Create or reconnect if needed
    if "conn" not in st.session_state or st.session_state.conn.closed:
        st.session_state.conn = create_connection()

    if st.session_state.conn is None:
        return None, None

    try:
        cur = st.session_state.conn.cursor()
        return st.session_state.conn, cur
    except Exception as e:
        # reconnect if cursor fails
        st.session_state.conn = create_connection()
        if st.session_state.conn:
            return st.session_state.conn, st.session_state.conn.cursor()
        else:
            st.error(f"DB cursor error: {e}")
            return None, None

conn, cur = get_db()

if conn is None:
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
