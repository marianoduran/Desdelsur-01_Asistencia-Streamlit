LOGO_URL = "https://www.desdelsur.com/wp-content/uploads/2019/08/dss545.png"

def header(st, title: str):
    c1, c2 = st.columns([1, 6])
    with c1: st.image(LOGO_URL, width=90)
    with c2:
        st.markdown(
            f"<h1 style='margin-bottom:0'>{title}</h1>"
            "<p style='margin-top:4px;color:#2b6cb0;font-weight:600'>DDS – Desdelsur</p>",
            unsafe_allow_html=True,
        )
