"""
audit_core.py

Core GA4 + GTM audit logic with a normalized findings model.
- No Streamlit dependencies.
- Auth-agnostic: expects a google.auth.credentials.Credentials object.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# GA Admin + Data
from google.analytics.admin_v1beta import AnalyticsAdminServiceClient
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest,
    RunRealtimeReportRequest,
    DateRange,
    Dimension,
    Metric,
)

# GTM (discovery)
from googleapiclient.discovery import build


# ----------------------------
# Normalized output model
# ----------------------------

@dataclass
class Finding:
    client_name: str
    property_id: str
    control_id: str
    control_name: str
    severity: str      # critical/high/medium/low/info
    status: str        # pass/fail/warn/skip
    evidence: Dict[str, Any]
    recommendation: str


def findings_to_df(findings: List[Finding]) -> pd.DataFrame:
    return pd.DataFrame([asdict(f) for f in findings])


# ----------------------------
# Client builders (auth-agnostic)
# ----------------------------

def build_clients(creds) -> Tuple[AnalyticsAdminServiceClient, BetaAnalyticsDataClient, object]:
    """
    Build GA Admin, GA Data, and GTM clients from any valid Google Credentials object.
    """
    admin_client = AnalyticsAdminServiceClient(credentials=creds)
    data_client = BetaAnalyticsDataClient(credentials=creds)
    gtm = build("tagmanager", "v2", credentials=creds, cache_discovery=False)
    return admin_client, data_client, gtm


# ----------------------------
# GA Admin helpers
# ----------------------------

def admin_get_property(admin: AnalyticsAdminServiceClient, property_id: str):
    return admin.get_property(name=f"properties/{property_id}")


def admin_list_web_streams(admin: AnalyticsAdminServiceClient, property_id: str):
    """
    Return list of WEB_DATA_STREAM dataStreams for a GA4 property.
    Uses keyword-arg style to avoid request-constructor incompatibilities.
    """
    parent = f"properties/{property_id}"
    streams = list(admin.list_data_streams(parent=parent))
    return [s for s in streams if getattr(s.type_, "name", "") == "WEB_DATA_STREAM"]


def admin_get_global_site_tag_snippet(admin: AnalyticsAdminServiceClient, stream_name: str) -> Optional[str]:
    """
    stream_name: "properties/{property_id}/dataStreams/{stream_id}"
    """
    try:
        gst = admin.get_global_site_tag(name=f"{stream_name}/globalSiteTag")
        return gst.snippet
    except Exception:
        return None


def admin_get_enhanced_measurement(admin: AnalyticsAdminServiceClient, stream_name: str) -> Optional[Dict[str, Any]]:
    try:
        ems = admin.get_enhanced_measurement_settings(name=f"{stream_name}/enhancedMeasurementSettings")
        return {
            "stream_enabled": ems.stream_enabled,
            "scrolls_enabled": getattr(ems, "scrolls_enabled", None),
            "outbound_clicks_enabled": getattr(ems, "outbound_clicks_enabled", None),
            "site_search_enabled": getattr(ems, "site_search_enabled", None),
            "video_engagement_enabled": getattr(ems, "video_engagement_enabled", None),
            "file_downloads_enabled": getattr(ems, "file_downloads_enabled", None),
            "page_changes_enabled": getattr(ems, "page_changes_enabled", None),
            "form_interactions_enabled": getattr(ems, "form_interactions_enabled", None),
        }
    except Exception:
        return None


def admin_list_key_events(admin: AnalyticsAdminServiceClient, property_id: str) -> List[Dict[str, Any]]:
    """
    GA4 “key events” are represented as ConversionEvents in the Admin API.
    """
    parent = f"properties/{property_id}"
    events = list(admin.list_conversion_events(parent=parent))
    return [{
        "event_name": e.event_name,
        "resource_name": e.name,
        "create_time": str(e.create_time),
        "deletable": e.deletable,
    } for e in events]


# ----------------------------
# GA Data helpers
# ----------------------------

def data_top_events(
    data_client: BetaAnalyticsDataClient,
    property_id: str,
    start: date,
    end: date,
    limit: int = 50
) -> List[Dict[str, Any]]:
    req = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=str(start), end_date=str(end))],
        dimensions=[Dimension(name="eventName")],
        metrics=[Metric(name="eventCount")],
        limit=limit,
    )
    resp = data_client.run_report(req)
    rows: List[Dict[str, Any]] = []
    for r in resp.rows:
        rows.append({
            "eventName": r.dimension_values[0].value,
            "eventCount": int(r.metric_values[0].value or 0)
        })
    return rows


def data_realtime_events(
    data_client: BetaAnalyticsDataClient,
    property_id: str,
    limit: int = 25
) -> List[Dict[str, Any]]:
    req = RunRealtimeReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="eventName")],
        metrics=[Metric(name="eventCount")],
        limit=limit,
    )
    resp = data_client.run_realtime_report(req)
    rows: List[Dict[str, Any]] = []
    for r in (resp.rows or []):
        rows.append({
            "eventName": r.dimension_values[0].value,
            "eventCount": int(r.metric_values[0].value or 0)
        })
    return rows


# ----------------------------
# GTM helpers (MVP access check)
# ----------------------------

def gtm_list_containers(gtm, account_id: str) -> List[Dict[str, Any]]:
    parent = f"accounts/{account_id}"
    resp = gtm.accounts().containers().list(parent=parent).execute()
    return resp.get("container", [])


# ----------------------------
# Controls (MVP set from your Colab flow)
# ----------------------------

def audit_ga4_property_mvp(
    *,
    client_name: str,
    property_id: str,
    admin: AnalyticsAdminServiceClient,
    data_client: BetaAnalyticsDataClient,
    days_lookback: int = 30,
) -> List[Finding]:
    findings: List[Finding] = []
    end = date.today()
    start = end - timedelta(days=days_lookback)

    # A-00 Property reachable (helps distinguish scope/access vs missing stream)
    try:
        prop = admin_get_property(admin, property_id)
        findings.append(Finding(
            client_name, property_id,
            "A-00", "Property reachable via Admin API",
            "info", "pass",
            {"property_name": prop.name, "display_name": getattr(prop, "display_name", None)},
            "None."
        ))
    except Exception as e:
        findings.append(Finding(
            client_name, property_id,
            "A-00", "Property reachable via Admin API",
            "critical", "fail",
            {"exception_type": type(e).__name__, "exception_message": str(e)},
            "Fix authentication scopes and/or property access. Ensure you are using the numeric GA4 Property ID."
        ))
        return findings

    # A-02 Web streams present
    try:
        web_streams = admin_list_web_streams(admin, property_id)
    except Exception as e:
        findings.append(Finding(
            client_name, property_id,
            "A-02", "Web streams present",
            "critical", "fail",
            {"exception_type": type(e).__name__, "exception_message": str(e)},
            "Confirm property access and correct GA4 Property ID."
        ))
        return findings

    if not web_streams:
        findings.append(Finding(
            client_name, property_id,
            "A-02", "Web streams present",
            "critical", "fail",
            {"web_stream_count": 0},
            "Create at least one GA4 Web data stream for the site and implement the Google tag."
        ))
        return findings

    findings.append(Finding(
        client_name, property_id,
        "A-02", "Web streams present",
        "info", "pass",
        {"web_stream_count": len(web_streams), "web_streams": [s.name for s in web_streams]},
        "None."
    ))

    # A-03 Global site tag snippet retrievable + A-05 Enhanced measurement retrievable (per stream)
    for s in web_streams:
        snippet = admin_get_global_site_tag_snippet(admin, s.name)
        findings.append(Finding(
            client_name, property_id,
            "A-03", "Global site tag retrievable (per web stream)",
            "medium" if not snippet else "info",
            "warn" if not snippet else "pass",
            {"stream_name": s.name, "has_snippet": bool(snippet)},
            "If missing, confirm this is a web stream and that your auth token includes Analytics Admin-compatible scopes."
        ))

        ems = admin_get_enhanced_measurement(admin, s.name)
        findings.append(Finding(
            client_name, property_id,
            "A-05", "Enhanced measurement settings retrievable (per web stream)",
            "medium" if ems is None else "info",
            "warn" if ems is None else "pass",
            {"stream_name": s.name, "enhanced_measurement": ems},
            "Review enhanced measurement toggles against measurement strategy (and confirm consent impacts)."
        ))

    # A-08 Key events configured
    try:
        key_events = admin_list_key_events(admin, property_id)
        findings.append(Finding(
            client_name, property_id,
            "A-08", "Key events configured",
            "high" if len(key_events) == 0 else "info",
            "warn" if len(key_events) == 0 else "pass",
            {"key_event_count": len(key_events), "key_events": key_events},
            "Define key events for priority actions (or document rationale if none are needed)."
        ))
    except Exception as e:
        findings.append(Finding(
            client_name, property_id,
            "A-08", "Key events configured",
            "high", "warn",
            {"exception_type": type(e).__name__, "exception_message": str(e)},
            "Verify GA Admin permissions for conversion events."
        ))

    # D-10 Core events present (Data API)
    try:
        top_events = data_top_events(data_client, property_id, start, end, limit=50)
        observed = {r["eventName"] for r in top_events}
        expected_core = {"page_view", "session_start"}
        missing = sorted(list(expected_core - observed))

        findings.append(Finding(
            client_name, property_id,
            "D-10", "Core events present (last N days)",
            "high" if missing else "info",
            "fail" if missing else "pass",
            {"window_days": days_lookback, "missing_core": missing, "top_events": top_events[:25]},
            "If core events are missing, verify tag deployment, consent behavior, filters, and stream selection."
        ))
    except Exception as e:
        findings.append(Finding(
            client_name, property_id,
            "D-10", "Core events present (last N days)",
            "high", "warn",
            {"exception_type": type(e).__name__, "exception_message": str(e)},
            "Verify GA Data API access to the property."
        ))

    # RT-28 Realtime activity observable
    try:
        rt = data_realtime_events(data_client, property_id, limit=25)
        total = sum(r["eventCount"] for r in rt)
        findings.append(Finding(
            client_name, property_id,
            "RT-28", "Realtime activity observable",
            "medium",
            "pass" if total > 0 else "warn",
            {"realtime_events": rt, "total_eventCount": total},
            "If empty, run a controlled test and re-check; review consent blocking."
        ))
    except Exception as e:
        findings.append(Finding(
            client_name, property_id,
            "RT-28", "Realtime activity observable",
            "medium", "warn",
            {"exception_type": type(e).__name__, "exception_message": str(e)},
            "Verify realtime reporting is available and permissions are sufficient."
        ))

    return findings


def gtm_access_check(
    *,
    client_name: str,
    property_id: str,
    gtm,
    gtm_account_id: str,
) -> List[Finding]:
    findings: List[Finding] = []
    try:
        containers = gtm_list_containers(gtm, gtm_account_id)
        findings.append(Finding(
            client_name, property_id,
            "GTM-01", "GTM access and container inventory",
            "info", "pass",
            {
                "gtm_account_id": gtm_account_id,
                "container_count": len(containers),
                "containers": [{"name": c.get("name"), "publicId": c.get("publicId"), "path": c.get("path")} for c in containers],
            },
            "None."
        ))
    except Exception as e:
        findings.append(Finding(
            client_name, property_id,
            "GTM-01", "GTM access and container inventory",
            "medium", "warn",
            {"gtm_account_id": gtm_account_id, "exception_type": type(e).__name__, "exception_message": str(e)},
            "Grant read access to GTM account/container or provide the correct GTM account ID."
        ))
    return findings


# ----------------------------
# Batch runner
# ----------------------------

def run_audit(
    clients: List[Dict[str, Any]],
    *,
    creds,
    days_lookback: int = 30,
) -> pd.DataFrame:
    """
    clients: list of dicts with keys:
      - client_name (str)
      - property_id (str or int)
      - gtm_account_id (optional str)
    """
    admin_client, data_client, gtm = build_clients(creds)

    all_findings: List[Finding] = []

    for c in clients:
        client_name = (c.get("client_name") or "Unknown").strip()
        property_id = str(c.get("property_id") or "").strip()
        gtm_account_id = str(c.get("gtm_account_id") or "").strip()

        if not property_id:
            continue

        all_findings.extend(
            audit_ga4_property_mvp(
                client_name=client_name,
                property_id=property_id,
                admin=admin_client,
                data_client=data_client,
                days_lookback=days_lookback,
            )
        )

        if gtm_account_id:
            all_findings.extend(
                gtm_access_check(
                    client_name=client_name,
                    property_id=property_id,
                    gtm=gtm,
                    gtm_account_id=gtm_account_id,
                )
            )

    return findings_to_df(all_findings)
