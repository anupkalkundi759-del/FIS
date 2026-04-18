import streamlit as st
import psycopg2
from PIL import Image

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
        .stApp {
            background: #f5f2eb;
            font-family: 'Segoe UI', sans-serif;
        }

        .block-container {
            padding-top: 3rem;
        }

        /* LEFT */
        .left-box {
            text-align: center;
            margin-top: 40px;
        }

        .logo-text {
            font-size: 18px;
            font-weight: 600;
            margin-top: 10px;
        }

        .title {
            font-size: 42px;
            margin-top: 40px;
            line-height: 1.2;
            color: #333;
        }

        .highlight {
            color: #f57c00;
        }

        /* RIGHT */
        .right-box {
            max-width: 420px;
            margin: auto;
        }

        .heading {
            font-size: 28px;
            font-weight: 700;
        }

        .subtext {
            color: #666;
            margin-bottom: 25px;
        }

        /* INPUT */
        .stTextInput>div>div>input {
            border-radius: 10px;
            height: 45px;
        }

        /* BUTTON */
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

    # ===== LEFT =====
    with col1:
        st.markdown('<div class="left-box">', unsafe_allow_html=True)

        logo = remove_white_bg("logo.png")
        st.image(logo, width=200)

        st.markdown("""
            <div class="title">
                Total Environment <span class="highlight">Machine Craft</span>
            </div>
        """, unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True)

    # ===== RIGHT =====
    with col2:
        st.markdown('<div class="right-box">', unsafe_allow_html=True)

        st.markdown('<div class="heading">Sign in to your account</div>', unsafe_allow_html=True)
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

    return st.session_state.conn, st.session_state.conn.cursor()

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

# ================= PAGES =================
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
