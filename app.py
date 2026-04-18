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

# ================= GLOBAL UI STYLE =================
st.markdown("""
<style>
.stApp {
    background: #f5f2eb;
    font-family: 'Segoe UI', sans-serif;
}

/* Divider */
.divider {
    border-left: 1px solid #e0ddd5;
    height: 70vh;
    margin: auto;
}

/* Card Shadow */
.card {
    background: white;
    padding: 20px;
    border-radius: 12px;
    box-shadow: 0px 4px 12px rgba(0,0,0,0.08);
}

/* Typography */
.heading {
    font-size: 28px;
    font-weight: 700;
}

.subtext {
    color: #666;
    margin-bottom: 20px;
}

/* Inputs */
.stTextInput>div>div>input {
    border-radius: 8px;
    height: 42px;
}

/* Button */
.stButton>button {
    background-color: #f57c00;
    color: white;
    border-radius: 8px;
    height: 42px;
    width: 100%;
    font-weight: 600;
    border: none;
}

/* KPI Cards */
.kpi {
    background: white;
    padding: 20px;
    border-radius: 12px;
    text-align: center;
    box-shadow: 0px 4px 10px rgba(0,0,0,0.06);
}
</style>
""", unsafe_allow_html=True)

# ================= LOGIN =================
def login():

    col1, col_mid, col2 = st.columns([1, 0.1, 1])

    # ===== LEFT =====
    with col1:
        st.markdown("<div style='display:flex;align-items:center;height:80vh;'>", unsafe_allow_html=True)
        st.markdown("<div>", unsafe_allow_html=True)

        logo = remove_white_bg("logo.png")
        st.image(logo, width=140)

        st.markdown("""
        <div style='font-size:16px;font-weight:600;margin-top:8px;margin-bottom:30px;'>
            Total Environment Machine Craft
        </div>
        """, unsafe_allow_html=True)

        st.markdown("""
        <div style='font-size:42px;line-height:1.2;'>
            Where nature meets <span style='color:#f57c00;'>design</span>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("</div></div>", unsafe_allow_html=True)

    # ===== DIVIDER =====
    with col_mid:
        st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

    # ===== RIGHT =====
    with col2:
        st.markdown("<div style='display:flex;align-items:center;height:80vh;'>", unsafe_allow_html=True)
        st.markdown("<div class='card'>", unsafe_allow_html=True)

        st.markdown("<div class='heading'>Sign in to your account</div>", unsafe_allow_html=True)
        st.markdown("<div class='subtext'>Factory Intelligence System — Authorized access only</div>", unsafe_allow_html=True)

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

        st.markdown("</div></div>", unsafe_allow_html=True)

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
    except:
        return None

def get_db():
    if "conn" not in st.session_state or st.session_state.conn.closed:
        st.session_state.conn = create_connection()
    return st.session_state.conn, st.session_state.conn.cursor()

conn, cur = get_db()

# ================= SIDEBAR =================
with st.sidebar:
    st.markdown("### OperaFlow")
    st.markdown(f"👤 {st.session_state.role.upper()}")

    page = st.radio("Navigation", [
        "Tracking", "Dashboard", "Product Tracking",
        "Measurement Update", "Scheduling Engine",
        "Upload Excel", "Delete Data"
    ])

    st.session_state.page = page

    if st.button("Logout"):
        st.session_state.clear()
        st.rerun()

# ================= DASHBOARD =================
if st.session_state.page == "Dashboard":
    st.markdown("## Dashboard Overview")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("<div class='kpi'><h3>120</h3><p>Orders</p></div>", unsafe_allow_html=True)
    with col2:
        st.markdown("<div class='kpi'><h3>85%</h3><p>Efficiency</p></div>", unsafe_allow_html=True)
    with col3:
        st.markdown("<div class='kpi'><h3>12</h3><p>Delayed</p></div>", unsafe_allow_html=True)

else:
    # ===== EXISTING PAGES =====
    page = st.session_state.page

    if page == "Tracking":
        from tracking import show_tracking
        show_tracking(conn, cur)

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
