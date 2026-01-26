from __future__ import annotations

from typing import Optional, Tuple

from googleapiclient.discovery import build


def create_google_slides_presentation(
    *,
    template_id: str,
    account_name: str,
    property_id: str,
    date_str: str,
    drive_folder_id: str,
    new_presentation_name: str,
    creds,
) -> Tuple[str, str]:
    """
    Copies a Slides template into a destination folder (Shared Drive folder supported),
    replaces placeholders, and returns (presentation_id, webViewLink).

    Placeholders expected in template:
      {{ACCOUNT_NAME}}
      {{PROPERTY_ID}}
      {{DATE}}
    """
    slides_service = build("slides", "v1", credentials=creds)
    drive_service = build("drive", "v3", credentials=creds)

    copy_body = {
        "name": new_presentation_name,
        "parents": [drive_folder_id],
    }

    # IMPORTANT for Shared Drives
    new_presentation = drive_service.files().copy(
        fileId=template_id,
        body=copy_body,
        supportsAllDrives=True,
    ).execute()

    new_presentation_id = new_presentation["id"]

    requests = [
        {
            "replaceAllText": {
                "containsText": {"text": "{{ACCOUNT_NAME}}", "matchCase": True},
                "replaceText": str(account_name),
            }
        },
        {
            "replaceAllText": {
                "containsText": {"text": "{{PROPERTY_ID}}", "matchCase": True},
                "replaceText": str(property_id),
            }
        },
        {
            "replaceAllText": {
                "containsText": {"text": "{{DATE}}", "matchCase": True},
                "replaceText": str(date_str),
            }
        },
    ]

    slides_service.presentations().batchUpdate(
        presentationId=new_presentation_id,
        body={"requests": requests},
    ).execute()

    meta = drive_service.files().get(
        fileId=new_presentation_id,
        fields="webViewLink",
        supportsAllDrives=True,
    ).execute()

    return new_presentation_id, meta.get("webViewLink", "")
