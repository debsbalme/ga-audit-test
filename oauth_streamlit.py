from __future__ import annotations

import os
from typing import Optional

import streamlit as st
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow


def get_streamlit_base_url() -> str:
    """
    Best-effort base URL for redirect URI.
    For local dev: http://localhost:8502
    For deployed: set STREAMLIT_BASE_URL env var (recommended).
    """
    return os.environ.get("STREAMLIT_BASE_URL", "http://localhost:8502")


def build_flow(client_secret_path: str, scopes: list[str]) -> Flow:
    redirect_uri = f"{get_streamlit_base_url()}/"
    flow = Flow.from_client_secrets_file(
        client_secret_path,
        scopes=scopes,
        redirect_uri=redirect_uri,
    )
    return flow


def get_user_credentials_via_oauth(
    *,
    client_secret_path: str,
    scopes: list[str],
    token_key: str = "google_oauth_token",
) -> Optional[Credentials]:
    """
    Streamlit-friendly OAuth:
    - If token exists in session_state, reuse it
    - If we have ?code= in URL, exchange it for tokens and store in session_state
    - Otherwise, display an auth link and return None
    """
    # 1) Already authenticated?
    token_dict = st.session_state.get(token_key)
    if token_dict:
        creds = Credentials.from_authorized_user_info(token_dict, scopes=scopes)
        if creds and creds.valid:
            return creds

    flow = build_flow(client_secret_path, scopes)

    # 2) Coming back from Google with a code?
    params = st.query_params
    code = params.get("code", None)
    if code:
        try:
            flow.fetch_token(code=code)
            creds = flow.credentials
            st.session_state[token_key] = {
                "token": creds.token,
                "refresh_token": creds.refresh_token,
                "token_uri": creds.token_uri,
                "client_id": creds.client_id,
                "client_secret": creds.client_secret,
                "scopes": creds.scopes,
            }
            # Clear URL params for cleanliness
            st.query_params.clear()
            st.success("Google OAuth complete.")
            return creds
        except Exception as e:
            st.error(f"OAuth token exchange failed: {type(e).__name__}: {e}")
            return None

    # 3) Not authenticated yet: show auth link
    auth_url, _ = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",  # ensures refresh_token the first time
    )

    st.info("To continue, authenticate with Google:")
    st.link_button("Sign in with Google", auth_url)

    return None


def oauth_logout(token_key: str = "google_oauth_token"):
    if token_key in st.session_state:
        del st.session_state[token_key]
    st.success("Logged out (local session cleared).")
