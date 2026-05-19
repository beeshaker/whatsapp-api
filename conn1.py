# conn1.py — ODOO API ADAPTER
# Replaces the old AWS/MySQL writes with calls to the Apricot Ticketing Odoo module.
# Keep this filename as conn1.py in Flask so whatsapp.py imports do not break.

from __future__ import annotations

import base64
import json
import logging
import mimetypes
import os
import time
from datetime import datetime
from typing import Any
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

load_dotenv()

KENYA_TZ = ZoneInfo("Africa/Nairobi")


def kenya_now() -> datetime:
    return datetime.now(KENYA_TZ)


def kenya_now_db() -> datetime:
    return datetime.now(KENYA_TZ).replace(tzinfo=None)


# -----------------------------------------------------------------------------
# Odoo API config
# -----------------------------------------------------------------------------
ODOO_BASE_URL = (os.getenv("ODOO_BASE_URL") or "http://localhost:8069").rstrip("/")
ODOO_DB = os.getenv("ODOO_DB") or "crm"
ODOO_API_TOKEN = os.getenv("ODOO_API_TOKEN") or os.getenv("ODOO_TICKETING_API_TOKEN")
ODOO_TIMEOUT = int(os.getenv("ODOO_TIMEOUT", "30"))


class OdooAPIError(RuntimeError):
    pass


def _headers() -> dict[str, str]:
    if not ODOO_API_TOKEN:
        raise OdooAPIError("ODOO_API_TOKEN is missing from environment variables")
    # X-API-Key is what worked reliably in your Odoo local tests.
    return {
        "X-API-Key": ODOO_API_TOKEN,
        "Content-Type": "application/json",
    }


def _url(path: str) -> str:
    if not path.startswith("/"):
        path = "/" + path
    sep = "&" if "?" in path else "?"
    if ODOO_DB:
        return f"{ODOO_BASE_URL}{path}{sep}{urlencode({'db': ODOO_DB})}"
    return f"{ODOO_BASE_URL}{path}"


def odoo_post(path: str, payload: dict[str, Any] | None = None, *, retries: int = 2) -> dict[str, Any]:
    payload = payload or {}
    last_error: Exception | None = None

    for attempt in range(retries + 1):
        try:
            response = requests.post(
                _url(path),
                headers=_headers(),
                json=payload,
                timeout=ODOO_TIMEOUT,
            )
            try:
                data = response.json()
            except Exception:
                data = {"raw": response.text}

            if response.status_code >= 400:
                raise OdooAPIError(
                    f"Odoo API HTTP {response.status_code} at {path}: {data}"
                )
            if isinstance(data, dict) and data.get("ok") is False:
                raise OdooAPIError(f"Odoo API rejected {path}: {data}")
            return data

        except Exception as exc:
            last_error = exc
            logging.error("Odoo API call failed attempt %s path=%s error=%s", attempt + 1, path, exc)
            if attempt < retries:
                time.sleep(0.8 * (attempt + 1))

    raise OdooAPIError(str(last_error))


def odoo_health() -> dict[str, Any]:
    response = requests.get(_url("/apricot_ticketing/api/health"), timeout=ODOO_TIMEOUT)
    response.raise_for_status()
    return response.json()


# -----------------------------------------------------------------------------
# Compatibility shim: old code expected a DB engine. Do not use in new code.
# -----------------------------------------------------------------------------
def get_db_connection1():
    raise RuntimeError(
        "MySQL has been disabled. Use the Odoo API helper functions in conn1.py instead."
    )


# -----------------------------------------------------------------------------
# Odoo requester/user helpers
# -----------------------------------------------------------------------------
def check_user(whatsapp_number: str) -> dict[str, Any]:
    return odoo_post("/api/apricot_ticketing/check_user", {"whatsapp_number": whatsapp_number})


def is_registered_user_odoo(whatsapp_number: str) -> bool:
    try:
        data = check_user(whatsapp_number)
        return bool(data.get("exists"))
    except Exception:
        logging.error("Failed to check Odoo user %s", whatsapp_number, exc_info=True)
        return False


def register_user(
    whatsapp_number: str,
    name: str | None = None,
    property_id: int | str | None = None,
    property_name: str | None = None,
    unit_number: str | None = None,
    terms_accepted: bool = False,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "whatsapp_number": whatsapp_number,
        "name": name or whatsapp_number,
        "unit_number": unit_number,
        "terms_accepted": terms_accepted,
    }
    if property_id not in (None, ""):
        payload["property_id"] = property_id
    if property_name:
        payload["property_name"] = property_name
    return odoo_post("/api/apricot_ticketing/register_user", payload)


def accept_terms(whatsapp_number: str) -> dict[str, Any]:
    return odoo_post("/api/apricot_ticketing/accept_terms", {"whatsapp_number": whatsapp_number})


def mark_user_accepted_via_temp_table(whatsapp_number: str):
    """
    Old MySQL function name kept for compatibility.
    The actual registration payload is handled in whatsapp.py using temp_opt_in_data.
    This fallback only marks terms accepted if the requester already exists in Odoo.
    """
    return accept_terms(whatsapp_number)


# -----------------------------------------------------------------------------
# Tickets and media
# -----------------------------------------------------------------------------
def create_ticket(
    whatsapp_number: str,
    description: str,
    category: str | None = None,
    property_id: int | str | None = None,
    property_name: str | None = None,
    unit_number: str | None = None,
    source: str = "whatsapp",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "whatsapp_number": whatsapp_number,
        "issue_description": description,
        "category_name": category or "Other",
        "unit_number": unit_number,
        "source": source,
    }
    if property_id not in (None, ""):
        payload["property_id"] = property_id
    if property_name:
        payload["property_name"] = property_name
    return odoo_post("/api/apricot_ticketing/create_ticket", payload)


def insert_ticket_and_get_id(user_id, description, category, property_id):
    """
    Old signature kept.
    In the converted Flask flow, user_id should be the WhatsApp number.
    Returns Odoo ticket_id.
    """
    whatsapp_number = str(user_id)
    data = create_ticket(
        whatsapp_number=whatsapp_number,
        description=description,
        category=category,
        property_id=property_id,
    )
    return data.get("ticket_id")


def _guess_mimetype(file_path: str, media_type: str | None = None) -> str | None:
    mimetype, _ = mimetypes.guess_type(file_path)
    if mimetype:
        return mimetype

    media_type = (media_type or "").lower()
    if media_type == "image":
        return "image/jpeg"
    if media_type == "video":
        return "video/mp4"
    if media_type == "document":
        return "application/octet-stream"
    return None


def save_ticket_media(ticket_id, media_type, file_path, mimetype: str | None = None, filename: str | None = None):
    try:
        with open(file_path, "rb") as f:
            encoded = base64.b64encode(f.read()).decode("ascii")

        clean_filename = filename or os.path.basename(file_path)
        clean_mimetype = mimetype or _guess_mimetype(file_path, media_type)

        payload = {
            "ticket_id": int(ticket_id),
            "filename": clean_filename,
            "mimetype": clean_mimetype,
            "media_type": media_type,
            "base64": encoded,
        }
        odoo_post("/api/apricot_ticketing/upload_ticket_media", payload)
        logging.info(
            "✅ Uploaded media to Odoo ticket=%s file=%s filename=%s mimetype=%s",
            ticket_id,
            file_path,
            clean_filename,
            clean_mimetype,
        )
        return True
    except Exception as e:
        logging.error("❌ Failed to upload media to Odoo ticket=%s file=%s error=%s", ticket_id, file_path, e, exc_info=True)
        return False


# This is not used for final persistence anymore. whatsapp.py keeps temp media in-memory.
def save_temp_media_to_db(sender_id, media_type, media_path, caption):
    logging.warning(
        "save_temp_media_to_db called, but MySQL is disabled. "
        "Temp media should be handled by whatsapp.py in-memory store."
    )
    return False


# -----------------------------------------------------------------------------
# WhatsApp message logging
# -----------------------------------------------------------------------------
def _normalise_direction(direction: str | None) -> str:
    direction = (direction or "out").lower()
    if direction in ("inbound", "incoming", "in"):
        return "in"
    return "out"


def log_whatsapp_message(
    wa_number: str,
    direction: str,
    message_type: str,
    body_text: str | None = None,
    message_id: str | None = None,
    template_name: str | None = None,
    status: str | None = None,
    error_text: str | None = None,
    meta_json: str | dict | None = None,
    ticket_id: int | None = None,
    job_card_id: int | None = None,
):
    if isinstance(meta_json, (dict, list)):
        meta_json = json.dumps(meta_json, ensure_ascii=False)

    payload: dict[str, Any] = {
        "wa_number": str(wa_number),
        "direction": _normalise_direction(direction),
        "message_type": message_type,
        "body_text": body_text,
        "message_id": message_id,
        "template_name": template_name,
        "status": status,
        "error_text": error_text,
        "meta_json": meta_json,
    }
    if ticket_id:
        payload["ticket_id"] = int(ticket_id)
    if job_card_id:
        payload["job_card_id"] = int(job_card_id)

    try:
        return odoo_post("/api/apricot_ticketing/log_message", payload)
    except Exception as e:
        logging.error("❌ Failed to log WhatsApp message in Odoo: %s", e, exc_info=True)
        return {"ok": False, "error": str(e)}


def update_whatsapp_message_status(message_id: str, status: str, error_text: str | None = None):
    # Current Odoo endpoint logs status events. It does not update existing rows yet.
    return log_whatsapp_message(
        wa_number="unknown",
        direction="outbound",
        message_type="status",
        message_id=message_id,
        status=status,
        error_text=error_text,
    )


def mark_message_as_processed_odoo(message_id: str) -> dict[str, Any]:
    return odoo_post("/api/apricot_ticketing/mark_processed_message", {"message_id": message_id})


def get_open_tickets(whatsapp_number: str) -> list[dict[str, Any]]:
    """Return active tickets for a WhatsApp number from Odoo.

    Requires the Odoo controller endpoint:
      POST /api/apricot_ticketing/get_tickets
    """
    try:
        data = odoo_post(
            "/api/apricot_ticketing/get_tickets",
            {"whatsapp_number": whatsapp_number, "active_only": True},
        )
        return data.get("tickets") or []
    except Exception as e:
        logging.error("Failed to get Odoo open tickets for %s: %s", whatsapp_number, e, exc_info=True)
        return []
