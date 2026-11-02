import streamlit as st

from api_client import get_health

st.set_page_config(
    page_title="Assignment Intelligence Platform",
    page_icon="📚",
    layout="centered",
)

st.title("Assignment Intelligence Platform")
st.write("Your AI-powered assignment assistant starts here.")

if st.button("Check API status"):
    try:
        result = get_health()

        if result.get("status") == "healthy":
            st.success("Backend API is healthy!")
        else:
            st.warning(f"Unexpected response: {result}")

    except Exception as exc:
        st.error(
            "Could not connect to the backend API. "
            "Make sure FastAPI is running."
        )
        st.caption(f"Technical details: {exc}")
