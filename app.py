import streamlit as st
import agent

st.set_page_config(page_title="Waterfall AI Agent", page_icon="🤖", layout="centered")
st.title("🤖 Waterfall AI Agent")
st.caption("Ask me to generate waterfalls or answer questions about your data.")

# Chat history
if "messages" not in st.session_state:
    st.session_state.messages = []
if "history" not in st.session_state:
    st.session_state.history = []

# Display past messages
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if "file" in msg:
            st.download_button(
                "📥 Download Waterfall",
                data      = msg["file"],
                file_name = msg["filename"],
                mime      = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

# Chat input
if prompt := st.chat_input("e.g. Give me a detail waterfall for weeks 11 to 13"):
    # Show user message
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Run agent
    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            text, file_buf, filename = agent.run_agent(prompt, st.session_state.history)

        st.markdown(text)

        msg_entry = {"role": "assistant", "content": text}

        if file_buf:
            file_bytes = file_buf.read()
            st.download_button(
                "📥 Download Waterfall",
                data      = file_bytes,
                file_name = filename,
                mime      = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            msg_entry["file"]     = file_bytes
            msg_entry["filename"] = filename

        st.session_state.messages.append(msg_entry)

    # Update history for context
    st.session_state.history.append({"role": "user",      "content": prompt})
    st.session_state.history.append({"role": "assistant", "content": text})
