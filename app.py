import streamlit as st
from supabase import create_client
import agent

# ── Supabase auth client ──────────────────────────────────────────────────────
supabase = create_client(
    st.secrets["SUPABASE_URL"],
    st.secrets["SUPABASE_ANON_KEY"],
)

st.set_page_config(page_title="Waterfall AI Agent", page_icon="🤖", layout="centered")

# ── Auth state ────────────────────────────────────────────────────────────────
if "user" not in st.session_state:
    st.session_state.user = None

# ── Login page ────────────────────────────────────────────────────────────────
def show_login():
    st.title("🔐 Waterfall AI Agent")
    st.caption("Please log in to continue.")

    email    = st.text_input("Email")
    password = st.text_input("Password", type="password")

    if st.button("Log in", use_container_width=True):
        if not email or not password:
            st.error("Please enter both email and password.")
            return
        try:
            response = supabase.auth.sign_in_with_password({
                "email":    email,
                "password": password,
            })
            st.session_state.user = response.user
            st.rerun()
        except Exception as e:
            st.error("❌ Invalid email or password. Please try again.")

# ── Main app ──────────────────────────────────────────────────────────────────
def show_app():
    # Header with logout
    col1, col2 = st.columns([4, 1])
    with col1:
        st.title("🤖 Waterfall AI Agent")
        st.caption("Ask me to generate waterfalls or answer questions about your data.")
    with col2:
        st.write("")
        st.write("")
        if st.button("Log out", use_container_width=True):
            supabase.auth.sign_out()
            st.session_state.user = None
            st.session_state.messages = []
            st.session_state.history  = []
            st.rerun()

    # Chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "history" not in st.session_state:
        st.session_state.history = []

    # Display past messages
    for i, msg in enumerate(st.session_state.messages):
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if "file" in msg:
                st.download_button(
                    "📥 Download Waterfall",
                    data      = msg["file"],
                    file_name = msg["filename"],
                    mime      = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key       = f"dl_{i}",
                )

    # Chat input
    if prompt := st.chat_input("e.g. Give me a detail waterfall for weeks 11 to 13"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                text, file_buf, filename = agent.run_agent(
                    prompt, st.session_state.history
                )
            st.markdown(text)

            msg_entry = {"role": "assistant", "content": text}

            if file_buf:
                file_bytes = file_buf.read()
                st.download_button(
                    "📥 Download Waterfall",
                    data      = file_bytes,
                    file_name = filename,
                    mime      = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key       = f"dl_{len(st.session_state.messages)}",
                )
                msg_entry["file"]     = file_bytes
                msg_entry["filename"] = filename

            st.session_state.messages.append(msg_entry)

        st.session_state.history.append({"role": "user",      "content": prompt})
        st.session_state.history.append({"role": "assistant", "content": text})


# ── Router ────────────────────────────────────────────────────────────────────
if st.session_state.user is None:
    show_login()
else:
    show_app()
