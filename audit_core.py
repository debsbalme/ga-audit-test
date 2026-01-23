"""
audit_core.py

Core GA4 + GTM audit logic with a normalized findings model.
- No Streamlit dependencies.
- Auth-agnostic: expects a google.auth.credentials.Credentials object.
"""
from __future__ import annotations

from datetime import date
from googleapiclient.discovery import build

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

from google.analytics.admin_v1alpha import AnalyticsAdminServiceClient as AnalyticsAdminServiceClientV1Alpha
from google.analytics.admin_v1alpha.types import (
    GetGoogleSignalsSettingsRequest,
    GetAttributionSettingsRequest,
    ListRollupPropertySourceLinksRequest,
    ListSubpropertyEventFiltersRequest,
    SearchChangeHistoryEventsRequest,
)
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

def build_clients(creds):
    """
    Build GA Admin v1beta, GA Admin v1alpha (for advanced settings), GA Data, and GTM.
    """
    admin_beta = AnalyticsAdminServiceClient(credentials=creds)
    admin_alpha = AnalyticsAdminServiceClientV1Alpha(credentials=creds)
    data_client = BetaAnalyticsDataClient(credentials=creds)
    gtm = build("tagmanager", "v2", credentials=creds, cache_discovery=False)
    return admin_beta, admin_alpha, data_client, gtm


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

def _safe_call(fn, *, default=None, **kwargs):
    try:
        return fn(**kwargs), None
    except Exception as e:
        return default, f"{type(e).__name__}: {e}"

def _safe_list(fn, *, default=None, **kwargs):
    try:
        return list(fn(**kwargs)), None
    except Exception as e:
        return default if default is not None else [], f"{type(e).__name__}: {e}"


def _safe_list(admin, method_name: str, *, parent: str) -> Dict[str, Any]:
    """
    Calls admin.<method_name>(parent=...) if available.
    Returns {available, count, items, error}
    """
    if not hasattr(admin, method_name):
        return {"available": False, "count": None, "items": None, "error": f"Method not available: {method_name}"}

    try:
        method = getattr(admin, method_name)
        items = list(method(parent=parent))
        return {"available": True, "count": len(items), "items": items, "error": None}
    except Exception as e:
        return {"available": True, "count": None, "items": None, "error": f"{type(e).__name__}: {e}"}

def get_property_profile(
    *,
    property_id: str,
    admin_beta: AnalyticsAdminServiceClient,
    admin_alpha: AnalyticsAdminServiceClientV1Alpha,
    data_client: BetaAnalyticsDataClient,
    days_events_lookback: int = 7,
    change_history_days_lookback: int = 90,
) -> Dict[str, Any]:
    """
    Build a property-level profile capturing as many of the requested fields as possible.

    Returns a dict with:
      - profile: extracted fields
      - availability/errors: per-surface diagnostics
    """
    profile: Dict[str, Any] = {"property_id": property_id}
    diag: Dict[str, Any] = {"errors": {}, "availability": {}}

    # ---- Property (v1beta) ----
    prop, err = _safe_call(admin_beta.get_property, name=f"properties/{property_id}")
    if err:
        diag["errors"]["get_property_v1beta"] = err
        return {"profile": profile, "diagnostics": diag}

    # Core property fields
    profile.update({
        "property_name": getattr(prop, "display_name", None),
        "property_resource_name": getattr(prop, "name", None),
        "property_type": getattr(getattr(prop, "property_type", None), "name", None),
        "parent": getattr(prop, "parent", None),
        "create_time": str(getattr(prop, "create_time", "")) if getattr(prop, "create_time", None) else None,
        "update_time": str(getattr(prop, "update_time", "")) if getattr(prop, "update_time", None) else None,
        "industry_category": getattr(getattr(prop, "industry_category", None), "name", None),
        "time_zone": getattr(prop, "time_zone", None),
        "currency_code": getattr(prop, "currency_code", None),
        "service_level": getattr(getattr(prop, "service_level", None), "name", None),
    })

    # ---- Account (v1beta) via parent ----
    account_id = None
    account_name = None
    if profile.get("parent", "").startswith("accounts/"):
        account_id = profile["parent"].split("/", 1)[1]
        acct, aerr = _safe_call(admin_beta.get_account, name=profile["parent"])
        if aerr:
            diag["errors"]["get_account_v1beta"] = aerr
        else:
            account_name = getattr(acct, "display_name", None)

    profile.update({
        "account_id": account_id,
        "account_name": account_name,
    })

    # ---- Data Retention Settings (v1beta) ----
    # Resource name: properties/{property_id}/dataRetentionSettings
    dr, derr = _safe_call(admin_beta.get_data_retention_settings, name=f"properties/{property_id}/dataRetentionSettings")
    if derr:
        diag["errors"]["get_data_retention_settings_v1beta"] = derr
    else:
        profile.update({
            "data_retention_duration": getattr(getattr(dr, "retention_duration", None), "name", None),
            "reset_user_data_on_new_activity": getattr(dr, "reset_user_data_on_new_activity", None),
            # Some orgs colloquially call this "user retention"; API surface is data retention settings.
            "user_retention_duration": getattr(getattr(dr, "retention_duration", None), "name", None),
        })

    # ---- Google Signals Settings (v1alpha) ----
    gs, gserr = _safe_call(
        admin_alpha.get_google_signals_settings,
        request=GetGoogleSignalsSettingsRequest(name=f"properties/{property_id}/googleSignalsSettings"),
    )
    if gserr:
        diag["errors"]["get_google_signals_settings_v1alpha"] = gserr
    else:
        profile.update({
            "google_signals_state": getattr(getattr(gs, "state", None), "name", None),
            "google_signals_consent": getattr(getattr(gs, "consent", None), "name", None),
        })

    # ---- Attribution Settings (v1alpha) ----
    attr, aerr = _safe_call(
        admin_alpha.get_attribution_settings,
        request=GetAttributionSettingsRequest(name=f"properties/{property_id}/attributionSettings"),
    )
    if aerr:
        diag["errors"]["get_attribution_settings_v1alpha"] = aerr
    else:
        profile.update({
            "acquisition_event_lookback_window": getattr(getattr(attr, "acquisition_conversion_event_lookback_window", None), "name", None),
            "other_conversion_event_lookback_window": getattr(getattr(attr, "other_conversion_event_lookback_window", None), "name", None),
            "reporting_attribution_model": getattr(getattr(attr, "reporting_attribution_model", None), "name", None),
        })

    # ---- Rollup Source Properties (v1alpha, only if property_type is ROLLUP) ----
    if profile.get("property_type") == "ROLLUP":
        rollups, rerr = _safe_list(
            admin_alpha.list_rollup_property_source_links,
            request=ListRollupPropertySourceLinksRequest(parent=f"properties/{property_id}"),
        )
        if rerr:
            diag["errors"]["list_rollup_property_source_links_v1alpha"] = rerr
        else:
            # Each link typically references a source property
            sources = []
            for x in rollups:
                sources.append({
                    "name": getattr(x, "name", None),
                    "source_property": getattr(x, "source_property", None),
                })
            profile["rollup_source_properties"] = sources

    # ---- Subproperty Event Filters (v1alpha, only if property_type is SUBPROPERTY) ----
    if profile.get("property_type") == "SUBPROPERTY":
        filters, ferr = _safe_list(
            admin_alpha.list_subproperty_event_filters,
            request=ListSubpropertyEventFiltersRequest(parent=f"properties/{property_id}"),
        )
        if ferr:
            diag["errors"]["list_subproperty_event_filters_v1alpha"] = ferr
        else:
            sub_filters = []
            for f in filters:
                sub_filters.append({
                    "name": getattr(f, "name", None),
                    "apply_to_property": getattr(f, "apply_to_property", None),
                    "filter_expression": str(getattr(f, "filter_expression", "")) if getattr(f, "filter_expression", None) else None,
                })
            profile["subproperty_event_filters"] = sub_filters

    # ---- Number of Events over past N days (Data API) ----
    end = date.today()
    start = end - timedelta(days=days_events_lookback)
    try:
        req = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date=str(start), end_date=str(end))],
            metrics=[Metric(name="eventCount")],
            limit=1,
        )
        resp = data_client.run_report(req)
        total_events = 0
        if resp.rows:
            total_events = int(resp.rows[0].metric_values[0].value or 0)
        profile["number_of_events_past_7_days"] = total_events
    except Exception as e:
        diag["errors"]["data_api_eventCount_7d"] = f"{type(e).__name__}: {e}"

    # ---- Change history (Create/Update/Delete/Action Taken) (v1alpha) ----
    # This requires account context. If we don't have account_id, skip.
    if account_id:
        try:
            # Window
            ch_end = date.today()
            ch_start = ch_end - timedelta(days=change_history_days_lookback)

            # SearchChangeHistoryEvents is account-scoped
            req = SearchChangeHistoryEventsRequest(
                account=f"accounts/{account_id}",
                # Filter to property resource name prefix.
                # The API supports filtering; here we keep it simple and post-filter in code.
                # (If you want stricter server-side filters, we can add them.)
                start_time=f"{ch_start.isoformat()}T00:00:00Z",
                end_time=f"{ch_end.isoformat()}T23:59:59Z",
            )
            events = list(admin_alpha.search_change_history_events(request=req))

            # Post-filter to only changes where any changed resource name contains this property
            filtered = []
            for ev in events:
                # Each event has one or more changes. We capture high-level action info.
                # Fields vary; we store what is generally present.
                # Some events include actor_type, action, resource name(s).
                ev_dict = {
                    "event_time": str(getattr(ev, "event_time", "")) if getattr(ev, "event_time", None) else None,
                    "actor_type": getattr(getattr(ev, "actor_type", None), "name", None),
                    "action": getattr(getattr(ev, "action", None), "name", None),
                    "changes": [],
                }

                changes = getattr(ev, "changes", None) or []
                include = False
                for c in changes:
                    changed_resource = getattr(c, "resource", None)
                    changed_resource_name = getattr(changed_resource, "name", None) if changed_resource else None
                    if changed_resource_name and f"properties/{property_id}" in changed_resource_name:
                        include = True
                    ev_dict["changes"].append({
                        "resource_name": changed_resource_name,
                        "change_type": getattr(getattr(c, "change_type", None), "name", None),
                        "old_value": str(getattr(c, "old_value", "")) if getattr(c, "old_value", None) else None,
                        "new_value": str(getattr(c, "new_value", "")) if getattr(c, "new_value", None) else None,
                    })

                if include:
                    filtered.append(ev_dict)

            profile["change_history_window_days"] = change_history_days_lookback
            profile["change_history_event_count"] = len(filtered)
            profile["change_history_events_sample"] = filtered[:25]  # keep evidence bounded
        except Exception as e:
            diag["errors"]["search_change_history_events_v1alpha"] = f"{type(e).__name__}: {e}"
    else:
        profile["change_history_event_count"] = None
        diag["availability"]["change_history"] = "skipped_no_account_parent"

    return {"profile": profile, "diagnostics": diag}



def admin_get_product_links_snapshot(admin: AnalyticsAdminServiceClient, property_id: str) -> Dict[str, Any]:
    """
    Returns counts and basic info for key Product Links.

    Notes:
      - Availability varies by GA Admin API version/channel and org enablement.
      - We use hasattr + try/except to keep the audit robust.

    Link types correspond to GA4 Admin → Product links surfaces. :contentReference[oaicite:1]{index=1}
    """
    parent = f"properties/{property_id}"

    # Common Product Links
    checks = {
        # Google Ads links
        "google_ads_links": "list_google_ads_links",

        # BigQuery links
        "bigquery_links": "list_big_query_links",

        # Firebase links
        "firebase_links": "list_firebase_links",

        # Search Ads 360 links
        "search_ads360_links": "list_search_ads360_links",

        # Display & Video 360 advertiser links
        "dv360_advertiser_links": "list_display_video360_advertiser_links",
    }

    snapshot: Dict[str, Any] = {"parent": parent, "links": {}}
    for label, method in checks.items():
        res = _safe_list(admin, method, parent=parent)

        # To keep evidence lightweight, store only names/ids when items exist
        item_summaries = None
        if res["items"]:
            item_summaries = []
            for x in res["items"]:
                # Most link resources have .name; some include platform IDs on specific fields.
                item_summaries.append({
                    "name": getattr(x, "name", None),
                    "display_name": getattr(x, "display_name", None),
                    "link_id": getattr(x, "link_id", None),
                })

        snapshot["links"][label] = {
            "method": method,
            "available": res["available"],
            "count": res["count"],
            "error": res["error"],
            "items": item_summaries,
        }

    return snapshot

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

def control_property_profile(
    *,
    client_name: str,
    property_id: str,
    admin_beta: AnalyticsAdminServiceClient,
    admin_alpha: AnalyticsAdminServiceClientV1Alpha,
    data_client: BetaAnalyticsDataClient,
) -> List[Finding]:
    out = get_property_profile(
        property_id=property_id,
        admin_beta=admin_beta,
        admin_alpha=admin_alpha,
        data_client=data_client,
        days_events_lookback=7,
        change_history_days_lookback=90,
    )

    profile = out["profile"]
    diag = out["diagnostics"]

    # Define "core" fields we expect for a healthy profile pull
    core_fields = [
        "account_id", "account_name",
        "property_name", "property_id",
        "property_type", "parent",
        "create_time", "update_time",
        "industry_category", "time_zone", "currency_code", "service_level",
        "number_of_events_past_7_days",
        "google_signals_state",
        "data_retention_duration", "reset_user_data_on_new_activity",
        "acquisition_event_lookback_window", "other_conversion_event_lookback_window",
        "reporting_attribution_model",
    ]

    present = [f for f in core_fields if profile.get(f) not in (None, "", [], {})]
    missing = [f for f in core_fields if f not in present]

    # Severity logic:
    # - If property itself couldn’t be retrieved, that would have been caught earlier as fatal.
    # - Here, warn if too many missing due to permissions/method availability.
    status = "pass" if len(missing) <= 3 else "warn"
    severity = "info" if status == "pass" else "medium"

    return [Finding(
        client_name, property_id,
        "P-01", "Property profile available (metadata + retention + signals + attribution + usage + change history)",
        severity, status,
        {
            "present_fields": present,
            "missing_fields": missing,
            "profile": profile,
            "diagnostics": diag,
        },
        "If fields are missing, confirm Admin API access, required API enablement, and that the service account has Viewer access on the property (and Account for change history)."
    )]

def admin_list_custom_dimensions(admin_beta: AnalyticsAdminServiceClient, property_id: str) -> List[Dict[str, Any]]:
    """
    Lists GA4 custom dimensions for the property via Admin API v1beta.
    """
    parent = f"properties/{property_id}"
    dims = list(admin_beta.list_custom_dimensions(parent=parent))
    out: List[Dict[str, Any]] = []
    for d in dims:
        out.append({
            "name": getattr(d, "name", None),  # resource name
            "parameter_name": getattr(d, "parameter_name", None),
            "display_name": getattr(d, "display_name", None),
            "description": getattr(d, "description", None),
            "scope": getattr(getattr(d, "scope", None), "name", None),  # EVENT / USER / ITEM
            "disallow_ads_personalization": getattr(d, "disallow_ads_personalization", None),
        })
    return out


def admin_list_custom_metrics(admin_beta: AnalyticsAdminServiceClient, property_id: str) -> List[Dict[str, Any]]:
    """
    Lists GA4 custom metrics for the property via Admin API v1beta.
    """
    parent = f"properties/{property_id}"
    mets = list(admin_beta.list_custom_metrics(parent=parent))
    out: List[Dict[str, Any]] = []
    for m in mets:
        out.append({
            "name": getattr(m, "name", None),  # resource name
            "parameter_name": getattr(m, "parameter_name", None),
            "display_name": getattr(m, "display_name", None),
            "description": getattr(m, "description", None),
            "scope": getattr(getattr(m, "scope", None), "name", None),  # EVENT / USER / ITEM
            "measurement_unit": getattr(getattr(m, "measurement_unit", None), "name", None),
            "restricted_metric_type": getattr(getattr(m, "restricted_metric_type", None), "name", None),
        })
    return out

def control_custom_definitions_inventory(
    *,
    client_name: str,
    property_id: str,
    admin_beta: AnalyticsAdminServiceClient,
) -> List[Finding]:
    # Custom Dimensions
    custom_dims = []
    dims_err = None
    try:
        custom_dims = admin_list_custom_dimensions(admin_beta, property_id)
    except Exception as e:
        dims_err = f"{type(e).__name__}: {e}"

    # Custom Metrics
    custom_mets = []
    mets_err = None
    try:
        custom_mets = admin_list_custom_metrics(admin_beta, property_id)
    except Exception as e:
        mets_err = f"{type(e).__name__}: {e}"

    # Determine status/severity
    if dims_err and mets_err:
        status = "warn"
        severity = "medium"
        rec = "Unable to enumerate custom definitions. Confirm Admin API access and that the service account has Viewer on the property."
    else:
        status = "pass"
        severity = "info"
        rec = "None."

    return [Finding(
        client_name, property_id,
        "CMCD-01", "Custom definitions inventory (custom dimensions + custom metrics)",
        severity, status,
        {
            "custom_dimensions_count": None if dims_err else len(custom_dims),
            "custom_metrics_count": None if mets_err else len(custom_mets),
            "custom_dimensions": custom_dims,
            "custom_metrics": custom_mets,
            "errors": {
                "custom_dimensions": dims_err,
                "custom_metrics": mets_err,
            },
        },
        rec
    )]


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


    # PL-01 Product links present
    try:
        pl = admin_get_product_links_snapshot(admin, property_id)

        # Count links that were successfully enumerated and have count > 0
        link_counts = {
            k: v.get("count") for k, v in pl["links"].items()
            if v.get("count") is not None
        }
        total_links = sum(c for c in link_counts.values() if isinstance(c, int))

        # If *all* link types errored or are unavailable, treat as WARN (not FAIL)
        any_enumerated = any(v.get("count") is not None for v in pl["links"].values())

        if not any_enumerated:
            findings.append(Finding(
                client_name, property_id,
                "PL-01", "Product links present (Google Ads / BigQuery / SA360 / DV360 / Firebase)",
                "medium", "warn",
                {"product_links": pl},
                "Unable to enumerate product links via Admin API in this environment. Verify API enablement, permissions, and library version."
            ))
        else:
            # Decide pass/warn based on whether any product links exist
            status = "pass" if total_links > 0 else "warn"
            severity = "info" if total_links > 0 else "medium"

            findings.append(Finding(
                client_name, property_id,
                "PL-01", "Product links present (Google Ads / BigQuery / SA360 / DV360 / Firebase)",
                severity, status,
                {
                    "total_links": total_links,
                    "link_counts": link_counts,
                    "details": pl,
                },
                "If no product links are present, consider linking relevant platforms (e.g., Google Ads, BigQuery, DV360/SA360, Firebase) to improve activation and governance."
            ))
    except Exception as e:
        findings.append(Finding(
            client_name, property_id,
            "PL-01", "Product links present (Google Ads / BigQuery / SA360 / DV360 / Firebase)",
            "medium", "warn",
            {"exception_type": type(e).__name__, "exception_message": str(e)},
            "Failed to evaluate product links. Validate Admin API access and retry."
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
    Run GA4/GTM audit across one or more clients/properties.

    clients: list of dicts with keys:
      - client_name (str)
      - property_id (str or int)  [GA4 numeric property id]
      - gtm_account_id (optional str)

    Returns:
      Findings DataFrame (one row per control finding).
    """
    # Updated: build both Admin clients + Data + GTM
    admin_beta, admin_alpha, data_client, gtm = build_clients(creds)

    all_findings: List[Finding] = []

    for c in clients:
        client_name = (c.get("client_name") or "Unknown").strip()
        property_id = str(c.get("property_id") or "").strip()
        gtm_account_id = str(c.get("gtm_account_id") or "").strip()

        if not property_id:
            continue

        # New: Property Profile control (v1beta + v1alpha + Data API + Change History)
        all_findings.extend(
            control_property_profile(
                client_name=client_name,
                property_id=property_id,
                admin_beta=admin_beta,
                admin_alpha=admin_alpha,
                data_client=data_client,
            )
        )

        all_findings.extend(
            control_custom_definitions_inventory(
                client_name=client_name,
                property_id=property_id,
                admin_beta=admin_beta,
            )
        )
        
        # Existing: GA4 MVP audit (uses v1beta Admin client + Data API)
        all_findings.extend(
            audit_ga4_property_mvp(
                client_name=client_name,
                property_id=property_id,
                admin=admin_beta,
                data_client=data_client,
                days_lookback=days_lookback,
            )
        )

        # Existing: optional GTM access check (only runs if gtm_account_id provided)
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



def get_profile_from_p01(results_df: pd.DataFrame, property_id: str) -> Dict[str, Any]:
    """
    Extract the P-01 profile evidence for a given property_id.
    Returns the 'profile' dict stored in evidence.
    """
    subset = results_df[
        (results_df["control_id"] == "P-01") &
        (results_df["property_id"].astype(str) == str(property_id))
    ]

    if subset.empty:
        raise ValueError(f"No P-01 row found for property_id={property_id}. Ensure control_property_profile runs before deck generation.")

    evidence = subset.iloc[0]["evidence"]
    profile = evidence.get("profile", {})
    if not profile:
        raise ValueError("P-01 evidence.profile is empty. Check that get_property_profile() succeeded.")

    return profile


def copy_slides_template(
    *,
    creds,
    template_presentation_id: str,
    new_name: str,
    destination_folder_id: str | None = None,
) -> str:
    drive = build("drive", "v3", credentials=creds)

    body = {"name": new_name}
    if destination_folder_id:
        body["parents"] = [destination_folder_id]

    copied = drive.files().copy(
        fileId=template_presentation_id,
        body=body,
    ).execute()

    return copied["id"]


def replace_placeholders_in_slides(
    *,
    creds,
    presentation_id: str,
    replacements: dict,
) -> None:
    slides = build("slides", "v1", credentials=creds)

    requests = []
    for placeholder, value in replacements.items():
        requests.append({
            "replaceAllText": {
                "containsText": {"text": placeholder, "matchCase": True},
                "replaceText": str(value),
            }
        })

    slides.presentations().batchUpdate(
        presentationId=presentation_id,
        body={"requests": requests},
    ).execute()


def generate_property_audit_deck_from_results(
    *,
    creds,
    results_df: pd.DataFrame,
    property_id: str,
    template_presentation_id: str,
    destination_folder_id: str | None = None,
) -> dict:
    """
    Uses P-01 (Property Profile) control evidence to populate placeholders:
      {{ACCOUNT_NAME}}, {{PROPERTY_NAME}}, {{PROPERTY_ID}}

    Names the output deck:
      GA4 Audit Property <PROPERTY_ID> <YYYY-MM-DD>
    """
    profile = get_profile_from_p01(results_df, property_id=str(property_id))

    account_name = profile.get("account_name") or "Unknown Account"
    prop_name = profile.get("property_name") or "Unknown Property"
    prop_id = profile.get("property_id") or str(property_id)

    today = date.today().isoformat()
    new_name = f"GA4 Audit Property {prop_id} {today}"

    # 1) Copy template with new name
    new_presentation_id = copy_slides_template(
        creds=creds,
        template_presentation_id=template_presentation_id,
        new_name=new_name,
        destination_folder_id=destination_folder_id,
    )

    # 2) Replace placeholders from P-01 profile
    replace_placeholders_in_slides(
        creds=creds,
        presentation_id=new_presentation_id,
        replacements={
            "{{ACCOUNT_NAME}}": account_name,
            "{{PROPERTY_NAME}}": prop_name,
            "{{PROPERTY_ID}}": prop_id,
        },
    )

    url = f"https://docs.google.com/presentation/d/{new_presentation_id}/edit"

    return {
        "presentation_id": new_presentation_id,
        "presentation_name": new_name,
        "url": url,
        "placeholders_used": {
            "{{ACCOUNT_NAME}}": account_name,
            "{{PROPERTY_NAME}}": prop_name,
            "{{PROPERTY_ID}}": prop_id,
        }
    }