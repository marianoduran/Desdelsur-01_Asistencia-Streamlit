import streamlit as st

st.set_page_config(page_title="A definir", layout="wide")

# Oculta la navegación nativa de Streamlit en la sidebar
st.markdown("""
<style>
/* Sidebar native nav */
section[data-testid="stSidebarNav"] { display: none !important; }
div[data-testid="stSidebarNav"]     { display: none !important; } /* fallback */
</style>
""", unsafe_allow_html=True)

LOGO_URL = "https://www.desdelsur.com/wp-content/uploads/2019/08/dss545.png"
st.sidebar.image(LOGO_URL, width=70)
st.sidebar.page_link("TeamIT_Procesador_Asistencia_Streamlit_v08.py",
                     label="01 - Procesador de Asistencia Zarate")
st.sidebar.page_link("pages/02_a_definir.py",
                     label="02 - A definir")

st.title("Sección a definir")
st.info("Esta sección estará disponible en una próxima versión.")
