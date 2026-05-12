import streamlit as st
from supabase import create_client
from datetime import datetime, timezone
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

# ── Rate limit config ─────────────────────────────────────────────────────────
MAX_REQUESTS  = 10
WINDOW_HOURS  = 1

# ── Rate limit check ──────────────────────────────────────────────────────────
def check_rate_limit(user_id: str) -> tuple:
    """
    Returns (allowed: bool, remaining: int, reset_in_minutes: int)
    """
    try:
        now = datetime.now(timezone.utc)

        response = supabase.table("rate_limits") \
            .select("*") \
            .eq("user_id", user_id) \
            .execute()

        if not response.data:
            # First request ever — create record
            supabase.table("rate_limits").insert({
                "user_id":       user_id,
                "request_count": 1,
                "window_start":  now.isoformat(),
            }).execute()
            return True, MAX_REQUESTS - 1, 60

        record       = response.data[0]
        window_start = datetime.fromisoformat(record["window_start"])
        elapsed      = (now - window_start).total_seconds() / 3600

        if elapsed >= WINDOW_HOURS:
            # Window expired — reset
            supabase.table("rate_limits").update({
                "request_count": 1,
                "window_start":  now.isoformat(),
            }).eq("user_id", user_id).execute()
            return True, MAX_REQUESTS - 1, 60

        count = record["request_count"]

        if count >= MAX_REQUESTS:
            # Limit hit
            reset_in = int((WINDOW_HOURS * 60) - (elapsed * 60))
            return False, 0, reset_in

        # Increment count
        supabase.table("rate_limits").update({
            "request_count": count + 1,
        }).eq("user_id", user_id).execute()

        return True, MAX_REQUESTS - (count + 1), reset_in_minutes(window_start, now)

    except Exception as e:
        # If rate limit check fails, allow the request
        return True, MAX_REQUESTS, 60


def reset_in_minutes(window_start, now) -> int:
    elapsed = (now - window_start).total_seconds() / 60
    return max(0, int(60 - elapsed))


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
            st.session_state.user    = None
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

            # ── Rate limit check ──────────────────────────────────────────────
            user_id = st.session_state.user.id
            allowed, remaining, reset_in = check_rate_limit(user_id)

            if not allowed:
                text = (
                    f"⚠️ You've reached the limit of {MAX_REQUESTS} requests per hour. "
                    f"Please wait **{reset_in} minute(s)** before trying again."
                )
                st.warning(text)
                st.session_state.messages.append({
                    "role": "assistant", "content": text
                })

            else:
                # Show remaining requests
                if remaining <= 3:
                    st.warning(f"⚠️ You have {remaining} request(s) remaining this hour.")

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
