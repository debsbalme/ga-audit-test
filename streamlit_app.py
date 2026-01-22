import streamlit as st

"""
app.py

Streamlit shell that calls audit_core.run_audit().
Auth seam:
- For now: service account JSON upload OR ADC.
- Later: replace get_credentials_* with web OAuth.

Enhancement:
- Input mode toggle: single property_id OR CSV upload.
"""

import json
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st
import google.auth
from google.oauth2 import service_account

from audit_core import run_audit

st.set_page_config(page_title="GA4 / GTM Audit MVP", layout="wide")
st.title("GA4 / GTM Audit MVP")

# ---- Scopes (adjust as needed) ----
SCOPES = [
    "https://www.googleapis.com/auth/analytics.readonly",
    "https://www.googleapis.com/auth/tagmanager.readonly",
]

# ----------------------------
# Auth seam (replace later with web OAuth)
# ----------------------------

def get_credentials_from_service_account_json(sa_info: Dict[str, Any]):
    return service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)

def get_credentials_from_adc():
    creds, _ = google.auth.default(scopes=SCOPES)
    return creds

# ----------------------------
# Sidebar
# ----------------------------

with st.sidebar:
    st.header("Authentication (temporary)")
    auth_mode = st.radio(
        "Choose auth mode",
        options=["Service Account JSON (recommended)", "Application Default Credentials (ADC)"],
        index=0,
        help="For testing in locked-down orgs, Service Account is usually easiest. Replace this later with web OAuth."
    )

    creds = None

    if auth_mode == "Service Account JSON (recommended)":
        sa_file = st.file_uploader("Upload Service Account JSON", type=["json"])
        if sa_file is not None:
            sa_info = json.load(sa_file)
            try:
                creds = get_credentials_from_service_account_json(sa_info)
                st.success("Service Account credentials loaded.")
            except Exception as e:
                st.error(f"Failed to load service account creds: {e}")

        st.caption(
            "GA4 requirement: add the service account email as a Viewer on each GA4 property. "
            "GTM access via service account may not be available in many orgs; GTM checks may warn/skip."
        )

    else:
        st.caption("ADC requires the environment running Streamlit to already be authenticated (e.g., gcloud auth).")
        if st.button("Load ADC credentials"):
            try:
                creds = get_credentials_from_adc()
                st.success("ADC credentials loaded.")
            except Exception as e:
                st.error(f"Failed to load ADC creds: {e}")

    st.divider()
    days_lookback = st.number_input("Lookback days", min_value=7, max_value=365, value=30, step=1)

# ----------------------------
# Main: input mode
# ----------------------------

st.subheader("Audit target input")

input_mode = st.radio(
    "Choose input mode",
    options=["Single Property ID", "CSV Upload (multiple properties)"],
    index=0,
    horizontal=True,
)

clients: List[Dict[str, Any]] = []

if input_mode == "Single Property ID":
    col1, col2 = st.columns([2, 1])

    with col1:
        client_name = st.text_input("Client / Property label (optional)", value="Single Property")
        property_id = st.text_input("GA4 Property ID (numeric)", placeholder="e.g., 302663863")
    with col2:
        gtm_account_id = st.text_input("GTM Account ID (optional)", placeholder="e.g., 1234567")

    if property_id.strip():
        clients = [{
            "client_name": client_name.strip() or "Single Property",
            "property_id": property_id.strip(),
            "gtm_account_id": gtm_account_id.strip(),
        }]

    st.caption("Note: GA4 Property ID is numeric (not the Measurement ID that starts with G-).")

else:
    st.write("Upload a CSV with columns: `client_name, property_id, gtm_account_id` (gtm_account_id optional).")
    clients_file = st.file_uploader("Upload clients CSV", type=["csv"])

    if clients_file is not None:
        clients_df = pd.read_csv(clients_file).fillna("")
        st.dataframe(clients_df, use_container_width=True)
        clients = clients_df.to_dict("records")
    else:
        st.info("No CSV uploaded yet.")

st.divider()

run_btn = st.button(
    "Run Audit",
    type="primary",
    disabled=(creds is None or len(clients) == 0),
    help="Provide credentials and at least one property input to run."
)

if run_btn:
    with st.spinner("Running audit..."):
        results_df = run_audit(clients, creds=creds, days_lookback=int(days_lookback))

    st.success("Audit complete.")
    st.subheader("Findings")
    st.dataframe(results_df, use_container_width=True)

    # Optional: show extracted property profile from the P-01 evidence
    st.subheader("Property Profile (extracted fields)")
    try:
        p01 = results_df[results_df["control_id"] == "P-01"].iloc[0]
        profile = p01["evidence"].get("profile", {})
        # Render a simple key/value table
        prof_df = pd.DataFrame([{"field": k, "value": v} for k, v in profile.items() if k not in ("change_history_events_sample",)])
        st.dataframe(prof_df, use_container_width=True)

        with st.expander("Change history sample (up to 25 events)"):
            ch = profile.get("change_history_events_sample", [])
            st.json(ch)
    except Exception:
        st.info("Property profile not available in results.")


    st.subheader("Download")
    csv_bytes = results_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download findings CSV",
        data=csv_bytes,
        file_name="ga_audit_findings.csv",
        mime="text/csv",
    )
