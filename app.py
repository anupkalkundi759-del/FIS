import streamlit as st
import psycopg2
from PIL import Image

st.set_page_config(
    page_title="OperaFlow",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ================= SAFE EXECUTE =================
def safe_execute(conn, cur, query, params=None):
    try:
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
    except Exception as e:
        conn.rollback()
        raise e

# ================= IMAGE PROCESS =================
def remove_white_bg(image_path):
    img = Image.open(image_path).convert("RGBA")
    datas = img.getdata()

    newData = []
    for item in datas:
        if item[0] > 240 and item[1] > 240 and item[2] > 240:
            newData.append((255, 255, 255, 0))
        else:
            newData.append(item)

    img.putdata(newData)
    return img

# ================= SESSION =================
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

if "role" not in st.session_state:
    st.session_state.role = None

if "page" not in st.session_state:
    st.session_state.page = "Tracking"

# ================= LOGIN =================
def login():

    st.markdown("""
        <style>
        .stApp { background: #f5f2eb; font-family: 'Segoe UI', sans-serif; }
        .block-container { padding-top: 3rem; }

        .left-box { text-align: center; margin-top: 5px; }
        .title {
            font-size: 42px;
            margin-top: 3px;
            line-height: 1.2;
            color: #333;
        }
        .highlight { color: #f57c00; }

        .right-box { max-width: 420px; margin: auto; }
        .heading { font-size: 28px; font-weight: 700; }
        .subtext { color: #666; margin-bottom: 25px; }

        .stTextInput>div>div>input {
            border-radius: 10px;
            height: 45px;
        }

        .stButton>button {
            background-color: #f57c00;
            color: white;
            height: 45px;
            border-radius: 10px;
            width: 150px;
            font-weight: 600;
            border: none;
        }
        </style>
    """, unsafe_allow_html=True)

    col1, col2 = st.columns([1, 1])

    with col1:
        st.markdown('<div class="left-box">', unsafe_allow_html=True)
        logo = remove_white_bg("logo.png")
        st.image(logo, width=220)

        st.markdown("""
            <div class="title">
                Total Environment <span class="highlight">Machine Craft</span>
            </div>
        """, unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="right-box">', unsafe_allow_html=True)

        st.markdown('<div class="heading">Sign in to your account</div>', unsafe_allow_html=True)
        st.markdown('<div class="subtext">Factory Intelligence System — Authorized access only</div>', unsafe_allow_html=True)

        u = st.text_input("Username")
        p = st.text_input("Password", type="password")

        if st.button("Sign In"):

            users = {
                "production": {"password": "123", "role": "production"},
                "preassembly": {"password": "123", "role": "preassembly"},
                "polishing": {"password": "123", "role": "polishing"},
                "final": {"password": "123", "role": "final"},
                "dispatch": {"password": "123", "role": "dispatch"},
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

# ================= DB =================
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
        st.session_state.conn.rollback()
        cur = st.session_state.conn.cursor()
        return st.session_state.conn, cur

conn, cur = get_db()

if conn is None:
    st.stop()

# ================= SIDEBAR =================
with st.sidebar:
    st.markdown("**OperaFlow**")
    st.markdown(f"👤 {st.session_state.role.upper()}")

    if st.session_state.role == "admin":
        pages = [
            "Tracking",
            "Dashboard",
            "House Level Overview",
            "Product Tracking",
            "Scheduling Engine",
            "Upload Excel",
            "Delete Data"
        ]
    else:
        pages = [
            "Tracking",
            "Dashboard",
            "Product Tracking"
        ]

    page = st.radio("Navigation", pages)
    st.session_state.page = page

    if st.button("🚪 Logout"):
        st.session_state.clear()
        st.rerun()

# ================= MAIN =================
page = st.session_state.page

if st.session_state.role != "admin" and page in [
    "Scheduling Engine",
    "Upload Excel",
    "Delete Data"
]:
    st.error("⛔ Access Denied")
    st.stop()

# ================= PAGES =================
try:
    if page == "Tracking":
        from tracking import show_tracking
        show_tracking(conn, cur)

    elif page == "Dashboard":
        from dashboard_v2 import show_dashboard_v2
        show_dashboard_v2(conn, cur)

    elif page == "House Level Overview":
        from house_level_overview import show_dashboard
        show_dashboard(conn, cur)

    elif page == "Product Tracking":
        from product_tracking import show_product_tracking
        show_product_tracking(conn, cur)

    elif page == "Scheduling Engine":
        from engine import run_engine
        run_engine(conn, cur)

    elif page == "Upload Excel":
        from upload import show_upload
        show_upload(conn, cur)

    elif page == "Delete Data":
        from delete import show_delete
        show_delete(conn, cur)

except Exception as e:
    conn.rollback()
    st.error(f"Error occurred: {e}")

finally:
    if cur:
        cur.close()
