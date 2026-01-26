from __future__ import annotations

import os
from typing import Optional, Dict, Any

import streamlit as st
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow


def _get_base_url() -> str:
    # Prefer Streamlit Cloud Secrets; fallback to env; then localhost
    if "base_url" in st.secrets:
        return str(st.secrets["base_url"])
    return os.environ.get("STREAMLIT_BASE_URL", "http://localhost:8501")


def _get_query_params() -> Dict[str, Any]:
    # Streamlit API compatibility
    try:
        # Newer Streamlit
        return dict(st.query_params)
    except Exception:
        # Older Streamlit
        return st.experimental_get_query_params()


def _clear_query_params():
    try:
        st.query_params.clear()
    except Exception:
        st.experimental_set_query_params()


def get_user_credentials_via_oauth(
    *,
    client_secret_path: str,
    scopes: list[str],
    token_key: str = "google_oauth_token",
) -> Optional[Credentials]:
    base_url = _get_base_url().rstrip("/")
    redirect_uri = f"{base_url}/"

    # 1) Reuse token if available
    token_dict = st.session_state.get(token_key)
    if token_dict:
        creds = Credentials.from_authorized_user_info(token_dict, scopes=scopes)
        if creds and creds.valid:
            return creds

    # 2) Build flow with explicit redirect URI
    flow = Flow.from_client_secrets_file(
        client_secret_path,
        scopes=scopes,
        redirect_uri=redirect_uri,
    )

    # 3) Handle callback
    params = _get_query_params()
    code = params.get("code")
    if isinstance(code, list):
        code = code[0] if code else None

    if code:
        try:
            # IMPORTANT: flow must have the same redirect_uri used to generate auth_url
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

            _clear_query_params()
            st.success("Google OAuth complete.")
            st.rerun()
        except Exception as e:
            st.error(f"OAuth token exchange failed: {type(e).__name__}: {e}")
            return None

    # 4) Not authenticated yet → show auth link
    auth_url, _ = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
    )

    st.info("Authenticate with Google to continue:")
    st.link_button("Sign in with Google", auth_url)
    return None

def oauth_logout(token_key: str = "google_oauth_token"):
    if token_key in st.session_state:
        del st.session_state[token_key]
    st.success("Logged out (local session cleared).")


from __future__ import annotations

from typing import Optional
import streamlit as st

def render_auth_status(creds, *, required_scopes: list[str] | None = None):
    """
    UI helper to display whether OAuth is working.
    Safe to call even when creds is None.
    """
    with st.expander("Auth Debug", expanded=True):
        if creds is None:
            st.error("Not authenticated (creds is None).")
            st.write("Session keys:", list(st.session_state.keys()))
            return

        st.success("Authenticated (creds object present).")

        # Basic flags
        st.write("Valid:", getattr(creds, "valid", None))
        st.write("Expired:", getattr(creds, "expired", None))
        st.write("Has refresh token:", bool(getattr(creds, "refresh_token", None)))

        scopes = list(getattr(creds, "scopes", []) or [])
        st.write("Scopes on creds:", scopes)

        if required_scopes:
            missing = [s for s in required_scopes if s not in scopes]
            if missing:
                st.warning(f"Missing required scopes: {missing}")
            else:
                st.success("All required scopes are present.")


from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def test_drive_access(creds):
    with st.expander("Auth API Test (Drive)", expanded=False):
        try:
            drive = build("drive", "v3", credentials=creds)
            about = drive.about().get(fields="user,storageQuota").execute()
            st.success("Drive API call succeeded.")
            st.json(about)
        except HttpError as e:
            st.error(f"Drive API call failed: {e}")
        except Exception as e:
            st.error(f"Drive API call failed: {type(e).__name__}: {e}")
