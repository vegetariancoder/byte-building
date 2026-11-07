import streamlit as st
from api_client import get_health, stream_chat_events, upload_document

st.set_page_config(
    page_title="Assignment Intelligence Platform",
    page_icon="📚",
    layout="centered",
)

st.title("Assignment Intelligence Platform")
st.write("Upload a document, then ask questions grounded in its contents.")

if "document_id" not in st.session_state:
    st.session_state.document_id = ""
if "document_name" not in st.session_state:
    st.session_state.document_name = ""
if "messages" not in st.session_state:
    st.session_state.messages = []

uploaded_file = st.file_uploader(
    "Choose a PDF or DOCX document",
    type=["pdf", "docx"],
)

if uploaded_file is not None:
    selected_file = (uploaded_file.name, uploaded_file.size)
    previous_file = st.session_state.get("selected_file")
    if previous_file is not None and previous_file != selected_file:
        st.session_state.document_id = ""
        st.session_state.document_name = ""
        st.session_state.messages = []
    st.session_state.selected_file = selected_file

if st.button(
    "Upload and index document",
    disabled=uploaded_file is None,
):
    try:
        with st.spinner("Uploading and indexing document..."):
            result = upload_document(
                uploaded_file.name,
                uploaded_file.getvalue(),
                uploaded_file.type or "application/octet-stream",
            )
        st.session_state.document_id = result["document_id"]
        st.session_state.document_name = result["filename"]
        st.session_state.messages = []
        st.success(result["message"])
    except Exception as exc:
        st.error(f"Document upload failed: {exc}")

document_id = st.session_state.document_id
if document_id:
    st.caption(
        f"Ready to chat about **{st.session_state.document_name}**."
    )

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if message.get("sources"):
            with st.expander("View sources"):
                for source in message["sources"]:
                    st.write(f"**{source['file_name']}**")
                    if source["page_number"] is not None:
                        st.write(f"Page: {source['page_number']}")
                    if source["paragraph_number"] is not None:
                        st.write(
                            f"Paragraph: {source['paragraph_number']}"
                        )

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

question = st.chat_input(
    "Ask a question about your document",
    disabled=not document_id,
)

if question:
    if not document_id.strip():
        st.warning("Upload a document before asking a question.")
    else:
        st.session_state.messages.append(
            {"role": "user", "content": question}
        )
        with st.chat_message("user"):
            st.markdown(question)

        with st.chat_message("assistant"):
            answer_placeholder = st.empty()
            answer_text = ""
            sources = []

            try:
                for event in stream_chat_events(
                    question,
                    document_id,
                ):
                    if event["type"] == "token":
                        answer_text += event["text"]
                        answer_placeholder.markdown(answer_text)

                    elif event["type"] == "answer":
                        answer_text = event["text"]
                        answer_placeholder.markdown(answer_text)

                    elif event["type"] == "sources":
                        sources = event["items"]

                    elif event["type"] == "error":
                        st.error(event["message"])
                        break

                    elif event["type"] == "done":
                        break

                if answer_text:
                    st.session_state.messages.append(
                        {
                            "role": "assistant",
                            "content": answer_text,
                            "sources": sources,
                        }
                    )

                if sources:
                    with st.expander("View sources"):
                        for source in sources:
                            st.write(
                                f"**{source['file_name']}**"
                            )

                            if source["page_number"] is not None:
                                st.write(
                                    f"Page: {source['page_number']}"
                                )

                            if source["paragraph_number"] is not None:
                                st.write(
                                    "Paragraph: "
                                    f"{source['paragraph_number']}"
                                )

            except Exception as exc:
                st.error(f"Chat request failed: {exc}")
