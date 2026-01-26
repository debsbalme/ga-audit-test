"""
streamlit_app.py

Streamlit shell that calls audit_core.run_audit().
Auth seam:
- For now: service account JSON upload OR ADC.
- Later: replace get_credentials_* with web OAuth.

Enhancement:
- Input mode toggle: single property_id OR CSV upload.
"""

import json
from typing import Any, Dict, List

import tempfile
import pandas as pd
import streamlit as st
import google.auth
from google.oauth2 import service_account

from audit_core import (
    run_audit,
    generate_property_audit_deck_from_results,
)

from datetime import date
from oauth_streamlit import get_user_credentials_via_oauth, oauth_logout
from deliverables_slides import create_google_slides_presentation


st.set_page_config(page_title="GA4 / GTM Audit MVP", layout="wide")
st.title("GA4 / GTM Audit MVP")

# ---- Scopes (adjust as needed) ----
# ----------------------------
SCOPES = [
    # GA4 + GTM
    "https://www.googleapis.com/auth/analytics.readonly",
    "https://www.googleapis.com/auth/tagmanager.readonly",

    # Drive (copy/rename/move files)
    "https://www.googleapis.com/auth/drive",

    # Slides (batchUpdate / replaceAllText)
    "https://www.googleapis.com/auth/presentations",
]

def write_oauth_client_secret_to_tempfile() -> str:
    """
    Writes the OAuth client JSON from Streamlit Secrets to a temp file.
    Returns the temp file path.
    """
    raw = st.secrets["client_secret_web_json"]
    # Validate it is valid JSON (helps catch formatting issues in secrets)
    _ = json.loads(raw)

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
    tmp.write(raw.encode("utf-8"))
    tmp.flush()
    return tmp.name

# ----------------------------
# Auth seam (replace later with web OAuth)
# ----------------------------

def get_credentials_from_service_account_json(sa_info: Dict[str, Any]):
    return service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)

def get_credentials_from_adc():
    creds, _ = google.auth.default(scopes=SCOPES)
    return creds

# Store results across reruns
if "results_df" not in st.session_state:
    st.session_state["results_df"] = None

# ----------------------------
# Sidebar
# ----------------------------
with st.sidebar:
    st.header("Authentication (Google OAuth)")

client_secret_path = write_oauth_client_secret_to_tempfile()
creds = get_user_credentials_via_oauth(
    client_secret_path=client_secret_path,
    scopes=SCOPES,
)
#    client_secret_path = "client_secret.json"  # ensure this file exists in your app directory

creds = get_user_credentials_via_oauth(
        client_secret_path=client_secret_path,
        scopes=SCOPES,
    )

if creds:
        st.success("Signed in with Google.")
        if st.button("Logout"):
            oauth_logout()
            st.rerun()
else:
        st.stop()




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
    help="Provide credentials and at least one property input to run.",
)

if run_btn:
    with st.spinner("Running audit..."):
        st.session_state["results_df"] = run_audit(clients, creds=creds, days_lookback=int(days_lookback))

# Pull results from session state
results_df = st.session_state["results_df"]

# ----------------------------
# Results + downstream sections
# ----------------------------

if results_df is not None and not results_df.empty:
    st.success("Audit complete.")
    st.subheader("Findings")
    st.dataframe(results_df, use_container_width=True)

    # ---- Property Profile (P-01) ----
    st.subheader("Property Profile (extracted fields)")
    try:
        p01 = results_df[results_df["control_id"] == "P-01"].iloc[0]
        profile = p01["evidence"].get("profile", {})
        prof_df = pd.DataFrame(
            [{"field": k, "value": v} for k, v in profile.items() if k not in ("change_history_events_sample",)]
        )
        st.dataframe(prof_df, use_container_width=True)

        with st.expander("Change history sample (up to 25 events)"):
            st.json(profile.get("change_history_events_sample", []))
    except Exception as e:
        st.info(f"Property profile not available in results. ({type(e).__name__})")

    # ---- Custom Definitions (CMCD-01) ----
    st.subheader("Custom Definitions (GA4)")
    cmcd_df = results_df[results_df["control_id"] == "CMCD-01"]

    if cmcd_df.empty:
        st.info("No custom definitions inventory available.")
    else:
        row = cmcd_df.iloc[0]
        evidence = row["evidence"]

        st.markdown("### Custom Dimensions")
        custom_dims = evidence.get("custom_dimensions", [])
        if custom_dims:
            dims_df = pd.DataFrame(custom_dims)
            preferred_dim_cols = [
                "parameter_name", "display_name", "scope", "description",
                "disallow_ads_personalization", "name",
            ]
            dims_df = dims_df[[c for c in preferred_dim_cols if c in dims_df.columns]]
            st.dataframe(dims_df, use_container_width=True)
            st.caption(f"Total custom dimensions: {len(dims_df)}")
        else:
            st.info("No custom dimensions found for this property.")

        st.markdown("### Custom Metrics")
        custom_mets = evidence.get("custom_metrics", [])
        if custom_mets:
            mets_df = pd.DataFrame(custom_mets)
            preferred_met_cols = [
                "parameter_name", "display_name", "scope",
                "measurement_unit", "restricted_metric_type",
                "description", "name",
            ]
            mets_df = mets_df[[c for c in preferred_met_cols if c in mets_df.columns]]
            st.dataframe(mets_df, use_container_width=True)
            st.caption(f"Total custom metrics: {len(mets_df)}")
        else:
            st.info("No custom metrics found for this property.")

        errors = evidence.get("errors", {})
        if isinstance(errors, dict) and any(errors.values()):
            with st.expander("Custom definitions diagnostics"):
                st.json(errors)

    # ---- Download (always available when results exist) ----
    st.subheader("Download")
    csv_bytes = results_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download findings CSV",
        data=csv_bytes,
        file_name="ga_audit_findings.csv",
        mime="text/csv",
    )

    # ---- Deliverable (Slides) ----
st.subheader("Deliverable (Google Slides)")

TEMPLATE_ID = st.text_input("Google Slides Template ID", placeholder="Template presentation ID")
FOLDER_ID = st.text_input("Destination Shared Drive Folder ID", placeholder="Folder ID (inside Shared Drive)")

can_generate = bool(TEMPLATE_ID.strip()) and bool(FOLDER_ID.strip()) and (results_df is not None)

if st.button("Generate Slides Deck", disabled=not can_generate):
    # Pull P-01 profile
    p01 = results_df[results_df["control_id"] == "P-01"].iloc[0]
    profile = p01["evidence"].get("profile", {})

    account_name = profile.get("account_name", "Unknown Account")
    property_name = profile.get("property_name", "Unknown Property")
    property_id = str(profile.get("property_id", clients[0]["property_id"]))
    today = date.today().isoformat()

    new_name = f"GA4 Audit Property {property_id} {today}"

    pres_id, link = create_google_slides_presentation(
        template_id=TEMPLATE_ID.strip(),
        account_name=account_name,
        property_id=property_id,
        date_str=today,
        drive_folder_id=FOLDER_ID.strip(),
        new_presentation_name=new_name,
        creds=creds,
    )

    st.success(f"Deck created: {new_name}")
    st.markdown(f"[Open deck]({link})")
