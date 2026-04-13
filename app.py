with st.sidebar:

    st.markdown("**OperaFlow**")
    st.markdown("<small style='opacity:0.7'>Enterprise Suite</small>", unsafe_allow_html=True)
    st.markdown(f"👤 {st.session_state.role.upper()}")

    def nav(label, page):
        active = st.session_state.page == page

        btn = st.button(label, key=page, use_container_width=True)

        if btn:
            st.session_state.page = page

        if active:
            st.markdown(
                f"""
                <style>
                div[data-testid="stButton"][key="{page}"] button {{
                    background: rgba(255,255,255,0.25) !important;
                    font-weight: 600;
                }}
                </style>
                """,
                unsafe_allow_html=True
            )

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
