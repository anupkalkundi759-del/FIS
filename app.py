import streamlit as st
import psycopg2

# ================= SESSION =================
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

if "role" not in st.session_state:
    st.session_state.role = None

if "page" not in st.session_state:
    st.session_state.page = "Tracking"

# ================= LOGIN (PRO UI) =================
def login():

    # ===== CSS =====
    st.markdown("""
        <style>
        .stApp {
            background: #f5f2eb;
        }

        /* LEFT PANEL */
        .left-panel {
            background: linear-gradient(135deg, #0f3d2e, #1b5e3c);
            color: white;
            padding: 60px;
            height: 100vh;
        }

        .left-title {
            font-size: 42px;
            margin-top: 100px;
            line-height: 1.2;
        }

        .highlight {
            color: #f5a623;
        }

        .stats {
            margin-top: 40px;
            font-size: 18px;
        }

        /* RIGHT PANEL */
        .right-panel {
            background: #f5f2eb;
            padding: 80px;
            height: 100vh;
        }

        .login-heading {
            font-size: 32px;
            font-weight: bold;
            margin-bottom: 10px;
        }

        .subtext {
            color: gray;
            margin-bottom: 30px;
        }

        .stTextInput>div>div>input {
            border-radius: 10px;
            height: 45px;
        }

        .stButton>button {
            width: 100%;
            height: 50px;
            border-radius: 10px;
            background-color: #f57c00;
            color: white;
            font-weight: bold;
            border: none;
        }
        </style>
    """, unsafe_allow_html=True)

    # ===== SPLIT SCREEN =====
    col1, col2 = st.columns([1, 1])

    # ===== LEFT SIDE =====
    with col1:
        st.markdown('<div class="left-panel">', unsafe_allow_html=True)

        st.image("logo.png", width=120)

        st.markdown("""
            <div class="left-title">
                Where nature meets <span class="highlight">design</span>
            </div>

            <div class="stats">
                32+ Years<br>
                120+ Projects<br>
                4,500+ Families
            </div>
        """, unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True)

    # ===== RIGHT SIDE =====
    with col2:
        st.markdown('<div class="right-panel">', unsafe_allow_html=True)

        st.markdown('<div class="login-heading">Sign in to your account</div>', unsafe_allow_html=True)
        st.markdown('<div class="subtext">Factory Intelligence System — Authorized access only</div>', unsafe_allow_html=True)

        u = st.text_input("Username")
        p = st.text_input("Password", type="password")

        if st.button("Sign In"):
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

        st.markdown('</div>', unsafe_allow_html=True)

# ===== LOGIN CHECK =====
if not st.session_state.logged_in:
    login()
    st.stop()

# ================= DB (UNCHANGED) =================
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
    except Exception as e:
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
            "Dashboard",
            "Product Tracking",
            "Measurement Update",
            "Scheduling Engine",
            "Upload Excel",
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
