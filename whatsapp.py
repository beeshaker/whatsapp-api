import os
import json
import time
import requests
import logging
import mimetypes
import re
import threading
import tempfile
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from conn1 import (
    save_ticket_media,
    insert_ticket_and_get_id,
    log_whatsapp_message,
    update_whatsapp_message_status,
    is_registered_user_odoo,
    register_user,
    accept_terms,
    mark_message_as_processed_odoo,
    get_open_tickets,
)
from threading import Timer
from concurrent.futures import ThreadPoolExecutor
from logging.handlers import RotatingFileHandler
from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from datetime import timezone, timedelta

# -----------------------------------------------------------------------------
# Timezone (Kenya)
# -----------------------------------------------------------------------------
try:
    KENYA_TZ = ZoneInfo("Africa/Nairobi")
except ZoneInfoNotFoundError:
    # Windows fallback if tzdata is not installed. Kenya is UTC+3 year-round.
    KENYA_TZ = timezone(timedelta(hours=3))


def kenya_now() -> datetime:
    return datetime.now(KENYA_TZ)


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
os.makedirs("logs", exist_ok=True)
log_file = "logs/app.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=5),
        logging.StreamHandler(),
    ],
)

# -----------------------------------------------------------------------------
# Flask + Executor
# -----------------------------------------------------------------------------
app = Flask(__name__)
executor = ThreadPoolExecutor(max_workers=10)

# -----------------------------------------------------------------------------
# Env
# -----------------------------------------------------------------------------
load_dotenv()
WHATSAPP_ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
WHATSAPP_PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")
# MySQL is no longer used. Odoo API config is handled in conn1.py.

# Local folder for temporary WhatsApp downloads before they are attached to Odoo.
# On Windows, /tmp may not exist, so use tempfile.gettempdir() by default.
UPLOAD_DIR = os.getenv("UPLOAD_DIR") or os.path.join(tempfile.gettempdir(), "apricot_whatsapp_uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)


def _safe_name(value: str | None, fallback: str = "whatsapp_media") -> str:
    value = (value or fallback).strip()
    value = re.sub(r"[^A-Za-z0-9_.-]+", "_", value)
    return value.strip("._") or fallback


def _extension_from_mime(mime_type: str | None, media_type: str | None = None) -> str:
    mime_type = (mime_type or "").split(";")[0].strip().lower()

    explicit = {
        "image/jpeg": ".jpg",
        "image/jpg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
        "image/gif": ".gif",
        "video/mp4": ".mp4",
        "video/quicktime": ".mov",
        "application/pdf": ".pdf",
    }
    if mime_type in explicit:
        return explicit[mime_type]

    guessed = mimetypes.guess_extension(mime_type) if mime_type else None
    if guessed:
        return ".jpg" if guessed == ".jpe" else guessed

    media_type = (media_type or "").lower()
    if media_type == "image":
        return ".jpg"
    if media_type == "video":
        return ".mp4"
    return ".bin"


def _build_whatsapp_media_filename(media_type: str, media_id: str, media_obj: dict) -> tuple[str, str | None, str | None]:
    """Return a proper filename, mimetype and optional direct media URL for Odoo attachments."""
    media_obj = media_obj or {}
    mime_type = media_obj.get("mime_type")
    media_url = media_obj.get("url")

    original = media_obj.get("filename")
    ext = _extension_from_mime(mime_type, media_type)

    if original:
        base, original_ext = os.path.splitext(original)
        base = _safe_name(base, fallback=f"{media_type}_{media_id}")
        if not original_ext:
            original_ext = ext
        filename = f"{base}{original_ext}"
    else:
        filename = f"{_safe_name(media_type)}_{_safe_name(media_id)}{ext}"

    return filename, mime_type, media_url

# -----------------------------------------------------------------------------
# Locks
# -----------------------------------------------------------------------------
media_buffer_lock = threading.Lock()
user_timers_lock = threading.Lock()
terms_pending_lock = threading.Lock()
accept_lock = threading.Lock()
attachment_timer_lock = threading.Lock()

# ✅ NEW: per-user upload state lock (prevents race between upload and button press)
upload_state_lock = threading.Lock()

# -----------------------------------------------------------------------------
# In-memory state
# -----------------------------------------------------------------------------
processed_message_ids = set()
last_messages = {}          # { sender_id: (message_text, timestamp_epoch) }
media_buffer = {}           # legacy buffer (kept, but you now store uploads in DB)
upload_state = {}           # { sender_id: { uploading: bool } }
terms_pending_users = {}    # { sender_id: timestamp_epoch }
temp_opt_in_data = {}       # { sender_id: { name, property_id, unit_number } }
accept_retry_state = {}     # { sender_id: { 'attempt': int, 'timer': Timer } }

user_timers = {}            # { sender_id: datetime_kenya }
upload_prompt_timers = {}   # (unused, but kept if referenced elsewhere)

MEDIA_TTL_SECONDS = 900  # legacy buffer TTL
MAX_ATTACHMENTS = 5      # ✅ attachment limit
DESCRIPTION_TTL_SECONDS = 5 * 60  # ✅ delete uploads if no description in 5 mins

# Per-user description timers (in-memory)
description_timers = {}  # { sender_id: Timer }

# Odoo migration state:
# Temporary attachments and WhatsApp flow state are kept in memory until ticket creation.
# The final requester, ticket, attachments, and message logs are persisted in Odoo.
temp_media_store = {}      # { sender_id: [ {id, media_type, media_path, caption, uploaded_at} ] }
temp_media_next_id = 1
user_flow_state = {}       # { sender_id: {last_action, temp_category, property_id, unit_number} }
state_lock = threading.Lock()

# -----------------------------------------------------------------------------
# Button / List IDs
# -----------------------------------------------------------------------------
BTN_CREATE_TICKET = "create_ticket"
BTN_CHECK_TICKET = "check_ticket"

BTN_ATTACH_ADD_MORE = "attach_add_more"
BTN_ATTACH_DESCRIBE = "attach_describe"
BTN_ATTACH_MANAGE = "attach_manage"

LIST_ATTACH_PREVIEW = "attach_preview"
LIST_ATTACH_REMOVE_LAST = "attach_remove_last"
LIST_ATTACH_CLEAR_ALL = "attach_clear_all"

# -----------------------------------------------------------------------------
# Legacy DB helper compatibility layer
# -----------------------------------------------------------------------------
def _get_state(sender_id: str) -> dict:
    with state_lock:
        return user_flow_state.setdefault(
            sender_id,
            {
                "last_action": None,
                "temp_category": None,
                "property_id": None,
                "unit_number": None,
            },
        )


def _set_state(sender_id: str, **values):
    with state_lock:
        state = user_flow_state.setdefault(
            sender_id,
            {
                "last_action": None,
                "temp_category": None,
                "property_id": None,
                "unit_number": None,
            },
        )
        state.update(values)


def query_database(query, params=(), commit=False):
    """
    Compatibility shim while moving from AWS MySQL to Odoo.

    The original webhook code called query_database() for session state, temporary
    attachments and processed-message checks. Those are now handled in memory here.
    Final data is persisted in Odoo through conn1.py API helpers.
    """
    global temp_media_next_id

    q = " ".join((query or "").lower().split())
    params = params or ()

    try:
        # ------------------------------------------------------------------
        # Temp attachment store
        # ------------------------------------------------------------------
        if "from temp_ticket_media" in q:
            sender_id = str(params[0]) if params else ""
            rows = list(temp_media_store.get(sender_id, []))

            if "count(*)" in q:
                return [{"c": len(rows)}]

            if "select caption" in q:
                return [{"caption": r.get("caption")} for r in rows]

            if "order by uploaded_at desc" in q and "limit 1" in q:
                return [rows[-1]] if rows else []

            if "select media_type, media_path" in q:
                return [
                    {
                        "media_type": r.get("media_type"),
                        "media_path": r.get("media_path"),
                        "mimetype": r.get("mimetype"),
                        "filename": r.get("filename"),
                    }
                    for r in rows
                ]

            return rows

        if q.startswith("delete from temp_ticket_media where sender_id"):
            sender_id = str(params[0]) if params else ""
            temp_media_store.pop(sender_id, None)
            return True

        if q.startswith("delete from temp_ticket_media where id"):
            media_id = int(params[0])
            for sender_id, rows in list(temp_media_store.items()):
                temp_media_store[sender_id] = [r for r in rows if int(r.get("id")) != media_id]
            return True

        # ------------------------------------------------------------------
        # Processed messages
        # ------------------------------------------------------------------
        if "from processed_messages" in q:
            message_id = str(params[0]) if params else ""
            return [{"id": message_id}] if message_id in processed_message_ids else []

        if q.startswith("insert ignore into processed_messages"):
            message_id = str(params[0]) if params else ""
            processed_message_ids.add(message_id)
            try:
                mark_message_as_processed_odoo(message_id)
            except Exception:
                logging.error("Failed to mark processed message in Odoo", exc_info=True)
            return True

        # ------------------------------------------------------------------
        # User/requester state
        # ------------------------------------------------------------------
        if "from users" in q and "where whatsapp_number" in q:
            sender_id = str(params[0]) if params else ""
            state = _get_state(sender_id)

            if "select id, temp_category" in q:
                return [{"id": sender_id, "temp_category": state.get("temp_category")} ]

            if "select last_action, temp_category" in q:
                return [{
                    "last_action": state.get("last_action"),
                    "temp_category": state.get("temp_category"),
                }]

            if "select last_action" in q:
                return [{"last_action": state.get("last_action")}]

            if "select property_id" in q:
                return [{"property_id": state.get("property_id")}]

            if "select id" in q:
                return [{"id": sender_id}] if is_registered_user_odoo(sender_id) else []

        if q.startswith("update users set") and "where whatsapp_number" in q:
            sender_id = str(params[-1]) if params else ""

            if "last_action = null" in q:
                _set_state(sender_id, last_action=None, temp_category=None)
                return True

            if "last_action = 'awaiting_category'" in q:
                _set_state(sender_id, last_action="awaiting_category")
                return True

            if "last_action = 'awaiting_issue_description', temp_category =" in q:
                category = params[0] if params else None
                _set_state(sender_id, last_action="awaiting_issue_description", temp_category=category)
                return True

            if "last_action = 'awaiting_issue_description'" in q:
                _set_state(sender_id, last_action="awaiting_issue_description")
                return True

        # ------------------------------------------------------------------
        # Active tickets / check status
        # ------------------------------------------------------------------
        if "from tickets" in q:
            sender_id = str(params[0]) if params else ""
            tickets = get_open_tickets(sender_id)
            return tickets or []

        logging.warning("query_database shim did not handle query: %s params=%s", q, params)
        return True if commit else []

    except Exception as err:
        logging.error("query_database compatibility error: %s", err, exc_info=True)
        return None


def save_temp_media_to_db(sender_id, media_type, media_path, caption, mimetype=None, filename=None):
    """Store pending media in memory until a ticket is created in Odoo."""
    global temp_media_next_id
    row = {
        "id": temp_media_next_id,
        "media_type": media_type,
        "media_path": media_path,
        "caption": caption,
        "mimetype": mimetype,
        "filename": filename or os.path.basename(media_path),
        "uploaded_at": kenya_now(),
    }
    temp_media_next_id += 1
    temp_media_store.setdefault(str(sender_id), []).append(row)
    logging.info(
        "✅ Temp media stored in memory for %s: %s -> %s filename=%s mimetype=%s",
        sender_id,
        media_type,
        media_path,
        row["filename"],
        mimetype,
    )
    return True


# -----------------------------------------------------------------------------
# Upload state helpers (prevents “Describe issue” while upload still processing)
# -----------------------------------------------------------------------------
def set_uploading(sender_id: str, uploading: bool):
    with upload_state_lock:
        upload_state.setdefault(sender_id, {})
        upload_state[sender_id]["uploading"] = bool(uploading)


def is_uploading(sender_id: str) -> bool:
    with upload_state_lock:
        return bool(upload_state.get(sender_id, {}).get("uploading", False))


# -----------------------------------------------------------------------------
# Attachments (DB) helpers
# -----------------------------------------------------------------------------
def get_temp_media_rows(sender_id: str):
    return query_database(
        """
        SELECT id, media_type, media_path, caption, uploaded_at
        FROM temp_ticket_media
        WHERE sender_id = %s
        ORDER BY uploaded_at ASC
        """,
        (sender_id,),
    ) or []


def get_temp_media_count(sender_id: str) -> int:
    res = query_database(
        "SELECT COUNT(*) AS c FROM temp_ticket_media WHERE sender_id = %s",
        (sender_id,),
    )
    return int(res[0]["c"]) if res else 0


def _safe_delete_file(path: str):
    if not path:
        return
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


def clear_all_attachments(sender_id: str, notify: bool = True):
    rows = get_temp_media_rows(sender_id)
    query_database(
        "DELETE FROM temp_ticket_media WHERE sender_id = %s",
        (sender_id,),
        commit=True,
    )
    for r in rows:
        _safe_delete_file(r.get("media_path"))

    if notify:
        send_whatsapp_message(sender_id, f"🗑️ Cleared {len(rows)} attachment(s).")


def remove_last_attachment(sender_id: str):
    row = query_database(
        """
        SELECT id, media_type, media_path
        FROM temp_ticket_media
        WHERE sender_id = %s
        ORDER BY uploaded_at DESC
        LIMIT 1
        """,
        (sender_id,),
    )
    if not row:
        send_whatsapp_message(sender_id, "📎 No attachments to remove.")
        return

    row = row[0]
    query_database("DELETE FROM temp_ticket_media WHERE id = %s", (row["id"],), commit=True)
    _safe_delete_file(row.get("media_path"))

    remaining = get_temp_media_count(sender_id)
    send_whatsapp_message(sender_id, f"🗑️ Removed last attachment. Remaining: {remaining}/{MAX_ATTACHMENTS}")


def list_attachments(sender_id: str):
    rows = get_temp_media_rows(sender_id)
    if not rows:
        send_whatsapp_message(sender_id, "📎 You have no pending attachments.")
        return

    msg = f"📎 Your pending attachments ({len(rows)}/{MAX_ATTACHMENTS}):\n\n"
    for i, r in enumerate(rows, 1):
        cap = (r.get("caption") or "No caption").strip()
        if len(cap) > 40:
            cap = cap[:40] + "..."
        msg += f"{i}. {r.get('media_type','file').capitalize()} — {cap}\n"

    send_whatsapp_message(sender_id, msg)


# -----------------------------------------------------------------------------
# Attachment description timeout
# -----------------------------------------------------------------------------
def cancel_description_timer(sender_id: str):
    with attachment_timer_lock:
        t = description_timers.pop(sender_id, None)
        if t:
            t.cancel()


def start_or_reset_description_timer(sender_id: str):
    """
    If user uploaded files but never describes the issue, we purge files after TTL.
    Timer resets on each upload and when we prompt "Describe issue".
    """
    cancel_description_timer(sender_id)

    def _expire():
        count = get_temp_media_count(sender_id)
        if count <= 0:
            return

        st = query_database(
            "SELECT last_action FROM users WHERE whatsapp_number = %s",
            (sender_id,),
        )
        last_action = st[0]["last_action"] if st else None

        if last_action == "awaiting_issue_description":
            clear_all_attachments(sender_id, notify=False)
            query_database(
                "UPDATE users SET last_action = NULL, temp_category = NULL WHERE whatsapp_number = %s",
                (sender_id,),
                commit=True,
            )
            send_menu_prompt(
                sender_id,
                f"⏳ Time limit reached ({DESCRIPTION_TTL_SECONDS//60} minutes). "
                "Your pending attachments were deleted because no description was received.\n\n"
                "Please start again:",
            )

    t = Timer(DESCRIPTION_TTL_SECONDS, _expire)
    t.daemon = True
    with attachment_timer_lock:
        description_timers[sender_id] = t
    t.start()


# -----------------------------------------------------------------------------
# Category prompt + timeout
# -----------------------------------------------------------------------------
def send_category_prompt(to):
    message = (
        "Please select a category:\n"
        "1️⃣ Accounts\n"
        "2️⃣ Maintenance\n"
        "3️⃣ Security\n"
        "4️⃣ Other\n\n"
        "Reply with the number."
    )
    executor.submit(send_whatsapp_message, to, message)

    with user_timers_lock:
        user_timers[to] = kenya_now()

    threading.Thread(target=reset_category_selection, args=(to,), daemon=True).start()


def reset_category_selection(to: str):
    time.sleep(300)  # 5 minutes
    with user_timers_lock:
        last_attempt_time = user_timers.get(to)
        if not last_attempt_time:
            return
        elapsed_time = (kenya_now() - last_attempt_time).total_seconds()
        if elapsed_time < 300:
            return
        del user_timers[to]

    user_info = query_database(
        "SELECT last_action FROM users WHERE whatsapp_number = %s", (to,)
    )
    if user_info and user_info[0]["last_action"] != "awaiting_category":
        logging.info(f"Skipping reset for {to}: last_action={user_info[0]['last_action']}")
        return

    logging.info(f"⏳ Resetting category selection for {to} due to timeout.")
    query_database(
        "UPDATE users SET last_action = NULL WHERE whatsapp_number = %s",
        (to,),
        commit=True,
    )
    send_menu_prompt(
        to,
        "⏳ Your category selection request has expired. Please start again:",
    )


# -----------------------------------------------------------------------------
# Terms prompt / opt-in route
# -----------------------------------------------------------------------------
def send_terms_prompt(sender_id):
    terms_url = os.getenv("TERMS_URL", "https://digiagekenya.com/apricot/TermsofService.html")
    privacy_url = os.getenv("PRIVACY_URL", "https://digiagekenya.com/apricot/policy.html")

    template_name = "registration_welcome"
    template_parameters = [terms_url, privacy_url]

    response = send_template_message(sender_id, template_name, template_parameters)
    if response.get("messages"):
        terms_pending_users[sender_id] = time.time()
        logging.info(f"Sent terms template to {sender_id}: {response}")
    else:
        logging.error(f"Failed to send terms template to {sender_id}: {response}")


@app.route("/opt_in_user", methods=["POST"])
def opt_in_user_route():
    if request.headers.get("X-API-KEY") != os.getenv("INTERNAL_API_KEY"):
        return jsonify({"error": "Unauthorized"}), 403

    data = request.json or {}
    name = data.get("name")
    whatsapp_number = data.get("whatsapp_number")
    property_id = data.get("property_id")
    unit_number = data.get("unit_number")

    if not all([name, whatsapp_number, property_id, unit_number]):
        logging.error("Missing fields in opt-in request.")
        return jsonify({"error": "Missing fields"}), 400

    logging.info(f"Storing opt-in data for {whatsapp_number}: {name}, {property_id}, {unit_number}")

    temp_opt_in_data[whatsapp_number] = {
        "name": name,
        "property_id": property_id,
        "unit_number": unit_number,
    }
    _set_state(
        whatsapp_number,
        property_id=property_id,
        unit_number=unit_number,
        last_action=None,
        temp_category=None,
    )
    terms_pending_users[whatsapp_number] = time.time()

    send_terms_prompt(whatsapp_number)
    return jsonify({"status": "terms_sent"}), 200


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def get_category_name(category_number):
    categories = {"1": "Accounts", "2": "Maintenance", "3": "Security", "4": "Other"}
    return categories.get(category_number, None)


def is_message_processed(message_id):
    if message_id in processed_message_ids:
        return True
    result = query_database("SELECT id FROM processed_messages WHERE id = %s", (message_id,))
    return bool(result)


def mark_message_as_processed(message_id):
    processed_message_ids.add(message_id)
    query_database("INSERT IGNORE INTO processed_messages (id) VALUES (%s)", (message_id,), commit=True)


def should_process_message(sender_id, message_text):
    current_time = time.time()
    if sender_id in last_messages:
        last_text, last_time = last_messages[sender_id]
        if last_text == message_text and (current_time - last_time) < 3:
            logging.info(f"⚠️ Ignoring duplicate message from {sender_id} within 3 seconds.")
            return False
    last_messages[sender_id] = (message_text, current_time)
    return True


def is_registered_user(whatsapp_number):
    return is_registered_user_odoo(str(whatsapp_number))


# -----------------------------------------------------------------------------
# WhatsApp messaging
# -----------------------------------------------------------------------------
def send_menu_prompt(to: str, body_text: str):
    """
    ✅ Sends Create Ticket / Check Status buttons (used for unknown messages too).
    """
    url = f"https://graph.facebook.com/v22.0/{WHATSAPP_PHONE_NUMBER_ID}/messages"
    headers = {"Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}", "Content-Type": "application/json"}
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": body_text},
            "action": {
                "buttons": [
                    {"type": "reply", "reply": {"id": BTN_CREATE_TICKET, "title": "📝 Create Ticket"}},
                    {"type": "reply", "reply": {"id": BTN_CHECK_TICKET, "title": "📌 Check Status"}},
                ]
            },
        },
    }
    response = requests.post(url, headers=headers, json=payload)
    logging.info(f"Sent menu prompt buttons: {response.json()}")
    return response.json()


def send_whatsapp_buttons(to):
    return send_menu_prompt(to, "What would you like to do?")


def send_whatsapp_message(to, message):
    url = f"https://graph.facebook.com/v22.0/{WHATSAPP_PHONE_NUMBER_ID}/messages"
    headers = {"Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}", "Content-Type": "application/json"}
    payload = {"messaging_product": "whatsapp", "to": to, "type": "text", "text": {"body": message}}

    response = requests.post(url, headers=headers, json=payload)
    data = response.json()
    logging.info(f"Sent WhatsApp message: {data}")

    # ✅ Log outbound with wamid if present
    try:
        wamid = None
        if isinstance(data, dict) and data.get("messages"):
            wamid = data["messages"][0].get("id")

        log_whatsapp_message(
            wa_number=to,
            direction="outbound",
            message_type="text",
            body_text=message,
            message_id=wamid,
            status="sent" if response.status_code in (200, 201) else "failed",
            error_text=(json.dumps(data) if response.status_code not in (200, 201) else None),
        )
    except Exception as e:
        logging.error(f"Failed to log outbound text: {e}", exc_info=True)

    return data



def send_whatsapp_tickets(to):
    tickets = get_open_tickets(str(to)) or []

    if not tickets:
        send_whatsapp_message(to, "You have no open or in-progress tickets at the moment.")
        return

    message = "Your active tickets (Open / In Progress):\n\n"
    for ticket in tickets[:20]:
        ticket_id = ticket.get("id") or ticket.get("ticket_id") or ticket.get("name")
        status = ticket.get("status") or ticket.get("stage") or "Open"
        desc = ticket.get("short_description") or ticket.get("description") or ""
        last_update = ticket.get("last_update") or ticket.get("updated_at") or ""
        message += (
            f"Ticket ID: {ticket_id}\n"
            f"Status: {status}\n"
            f"Description: {desc}\n"
            f"Last Update on: {last_update}\n\n"
        )

    send_whatsapp_message(to, message)



def send_attachment_action_buttons(sender_id: str, note: str | None = None):
    """
    3-button max on WhatsApp.
    Buttons:
      - Add more attachments
      - Describe issue
      - Manage (list: preview/remove last/clear all)

    ✅ FIX: caller can pass `note` so we don't send an extra text message.
    """
    count = get_temp_media_count(sender_id)

    extra = f"\n\n{note}" if note else ""
    body_text = (
        f"📎 Attachments: *{count}/{MAX_ATTACHMENTS}*"
        f"{extra}\n\n"
        f"You have *{DESCRIPTION_TTL_SECONDS//60} minutes* to send the issue description "
        "or your uploads will be deleted.\n\n"
        "📎 If you wish to upload a file, please do so *before describing your issue*.\n\n"
        "Choose an option:"
    )

    url = f"https://graph.facebook.com/v22.0/{WHATSAPP_PHONE_NUMBER_ID}/messages"
    headers = {"Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}", "Content-Type": "application/json"}
    payload = {
        "messaging_product": "whatsapp",
        "to": sender_id,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": body_text},
            "action": {
                "buttons": [
                    {"type": "reply", "reply": {"id": BTN_ATTACH_ADD_MORE, "title": "➕ Add more"}},
                    {"type": "reply", "reply": {"id": BTN_ATTACH_DESCRIBE, "title": "✍️ Describe issue"}},
                    {"type": "reply", "reply": {"id": BTN_ATTACH_MANAGE, "title": "📎 Manage"}},
                ]
            },
        },
    }
    response = requests.post(url, headers=headers, json=payload)
    logging.info(f"Sent attachment action buttons: {response.json()}")
    return response.json()


def send_manage_attachments_list(sender_id: str):
    count = get_temp_media_count(sender_id)
    body_text = f"Manage attachments (*{count}/{MAX_ATTACHMENTS}*):"

    url = f"https://graph.facebook.com/v22.0/{WHATSAPP_PHONE_NUMBER_ID}/messages"
    headers = {"Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}", "Content-Type": "application/json"}
    payload = {
        "messaging_product": "whatsapp",
        "to": sender_id,
        "type": "interactive",
        "interactive": {
            "type": "list",
            "body": {"text": body_text},
            "action": {
                "button": "Options",
                "sections": [
                    {
                        "title": "Attachments",
                        "rows": [
                          
                            {"id": LIST_ATTACH_REMOVE_LAST, "title": "Remove last"},
                            {"id": LIST_ATTACH_CLEAR_ALL, "title": "🧹 Clear all"},
                        ],
                    }
                ],
            },
        },
    }
    response = requests.post(url, headers=headers, json=payload)
    logging.info(f"Sent manage attachments list: {response.json()}")
    return response.json()


# -----------------------------------------------------------------------------
# Webhook routes
# -----------------------------------------------------------------------------
@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        verify_token = "12345"
        if request.args.get("hub.verify_token") == verify_token:
            return request.args.get("hub.challenge"), 200
        return "Invalid verification token", 403

    data = request.get_json()
    logging.info(f"Incoming webhook data: {json.dumps(data, indent=2)}")
    executor.submit(_safe_process_webhook, data)
    return jsonify({"status": "received"}), 200


@app.route("/send_message", methods=["POST"])
def external_send_message():
    # ✅ PROOF the endpoint was hit (even if auth fails)
    logging.info("✅ /send_message HIT")

    data = request.get_json(silent=True) or {}
    api_key = request.headers.get("X-API-KEY")
    expected_key = os.getenv("INTERNAL_API_KEY")

    # ✅ Log auth status (so “nothing happens” can’t be silent)
    if api_key != expected_key:
        logging.warning(
            "⛔ Unauthorized /send_message. "
            f"got={'<missing>' if not api_key else api_key} "
            f"expected={'<missing>' if not expected_key else expected_key}"
        )
        return jsonify({"error": "Unauthorized"}), 401

    # ✅ Log payload summary (don’t spam full content)
    to = data.get("to")
    message = data.get("message")
    template_name = data.get("template_name")
    template_parameters = data.get("template_parameters", [])

    logging.info(
        f"/send_message OK auth | to={to} "
        f"template_name={template_name} "
        f"has_message={bool(message)} "
        f"param_count={len(template_parameters) if isinstance(template_parameters, list) else 'n/a'}"
    )

    if not to or (not message and not template_name):
        logging.warning(f"⚠️ /send_message Missing fields. keys={list(data.keys())}")
        return jsonify({"error": "Missing required fields"}), 400

    try:
        if template_name:
            executor.submit(send_template_message, to, template_name, template_parameters)
        else:
            executor.submit(send_whatsapp_message, to, message)

        logging.info("✅ /send_message queued successfully")
        return jsonify({"status": "queued"}), 200

    except Exception as e:
        logging.error(f"❌ Error sending WhatsApp message: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500



def send_template_message(to, template_name, parameters):
    url = f"https://graph.facebook.com/v22.0/{WHATSAPP_PHONE_NUMBER_ID}/messages"
    headers = {"Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}", "Content-Type": "application/json"}
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "template",
        "template": {
            "name": template_name,
            "language": {"code": "en"},
            "components": [{"type": "body", "parameters": [{"type": "text", "text": p} for p in parameters]}],
        },
    }

    response = requests.post(url, headers=headers, json=payload)
    data = response.json()
    logging.info(f"Sent WhatsApp template message: {data}")

    # ✅ Log outbound template
    try:
        wamid = None
        if isinstance(data, dict) and data.get("messages"):
            wamid = data["messages"][0].get("id")

        log_whatsapp_message(
            wa_number=to,
            direction="outbound",
            message_type="template",
            body_text=None,
            template_name=template_name,
            message_id=wamid,
            status="sent" if response.status_code in (200, 201) else "failed",
            error_text=(json.dumps(data) if response.status_code not in (200, 201) else None),
            meta_json=json.dumps({"params": parameters}, ensure_ascii=False),
        )
    except Exception as e:
        logging.error(f"Failed to log outbound template: {e}", exc_info=True)

    return data



# -----------------------------------------------------------------------------
# Media handling
# -----------------------------------------------------------------------------
def download_media(media_id, filename=None, media_url=None, mimetype=None, media_type=None):
    """
    Download WhatsApp media to a local temp folder.

    Fixes Windows issue where /tmp may not exist. Also catches/logs failures so
    background webhook threads do not fail silently.
    """
    try:
        headers = {"Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}"}

        # Some webhook payloads include a direct media URL. Use it if present;
        # otherwise fetch the media metadata URL from Graph.
        if not media_url:
            meta_url = f"https://graph.facebook.com/v22.0/{media_id}"
            media_response = requests.get(meta_url, headers=headers, timeout=30)
            if media_response.status_code != 200:
                logging.error(
                    "❌ Failed to fetch media URL media_id=%s status=%s body=%s",
                    media_id,
                    media_response.status_code,
                    media_response.text,
                )
                return {"error": "Failed to fetch media URL", "details": media_response.text}

            media_url = media_response.json().get("url")
            if not media_url:
                logging.error("❌ Media URL missing in metadata response for media_id=%s", media_id)
                return {"error": "Media URL not found"}

        media_file_response = requests.get(media_url, headers=headers, timeout=60)
        if media_file_response.status_code != 200:
            logging.error(
                "❌ Failed to download media media_id=%s status=%s body=%s",
                media_id,
                media_file_response.status_code,
                media_file_response.text,
            )
            return {"error": "Failed to download media", "details": media_file_response.text}

        timestamp = kenya_now().strftime("%Y%m%d_%H%M%S")
        if not filename:
            filename = f"{media_id}_{timestamp}{_extension_from_mime(mimetype, media_type)}"
        else:
            name, ext = os.path.splitext(filename)
            if not ext or ext.lower() in (".ima", ".vid", ".doc"):
                ext = _extension_from_mime(mimetype, media_type)
            filename = f"{name}_{timestamp}{ext}"

        os.makedirs(UPLOAD_DIR, exist_ok=True)
        save_path = os.path.join(UPLOAD_DIR, filename)
        with open(save_path, "wb") as f:
            f.write(media_file_response.content)

        logging.info(
            "✅ Downloaded WhatsApp media media_id=%s bytes=%s path=%s",
            media_id,
            len(media_file_response.content),
            save_path,
        )
        return {"success": True, "path": save_path}

    except Exception as e:
        logging.error("❌ Exception while downloading media_id=%s: %s", media_id, e, exc_info=True)
        return {"error": str(e)}


def purge_expired_items():
    now = time.time()

    # Legacy in-memory purge (still fine to keep)
    with media_buffer_lock:
        for wa_id, media_list in list(media_buffer.items()):
            fresh_media = []
            for entry in media_list:
                age = now - entry["timestamp"]
                if age < MEDIA_TTL_SECONDS:
                    fresh_media.append(entry)

            if fresh_media:
                media_buffer[wa_id] = fresh_media
            else:
                del media_buffer[wa_id]
                send_whatsapp_message(wa_id, "⏳ Your uploaded files have expired. Please start again.")

    with terms_pending_lock:
        expired = [uid for uid, ts in terms_pending_users.items() if now - ts > 1800]
        for uid in expired:
            terms_pending_users.pop(uid, None)
            temp_opt_in_data.pop(uid, None)
            send_whatsapp_message(uid, "⏳ Your session to accept Terms expired. Please try again.")


def is_valid_message(sender_id, message_id, message_text):
    # Allow messages from users pending terms acceptance
    if sender_id in terms_pending_users:
        logging.info(f"Allowing message from pending user {sender_id}")
        return True

    if is_message_processed(message_id) or not should_process_message(sender_id, message_text):
        return False

    if not is_registered_user(sender_id):
        send_whatsapp_message(sender_id, "You are not registered. Please register first.")
        return False

    mark_message_as_processed(message_id)
    return True


def process_media_upload(media_id, filename, sender_id, media_type, caption_text, mimetype=None, media_url=None):
    # ✅ Mark this sender as “uploading” to block Describe/Ticket creation until complete
    set_uploading(sender_id, True)

    try:
        logging.info("📎 Processing media upload sender=%s media_id=%s type=%s filename=%s", sender_id, media_id, media_type, filename)

        # Attachment limit
        current_count = get_temp_media_count(sender_id)
        if current_count >= MAX_ATTACHMENTS:
            send_whatsapp_message(sender_id, f"⚠️ Max attachments reached ({MAX_ATTACHMENTS}). Please tap *Describe issue*.")
            send_attachment_action_buttons(sender_id)
            return

        user_status = query_database("SELECT last_action FROM users WHERE whatsapp_number = %s", (sender_id,))
        if not user_status:
            send_whatsapp_message(sender_id, "⚠️ You're not registered. Please register first.")
            return

        last_action = user_status[0]["last_action"]
        if last_action not in ["awaiting_category", "awaiting_issue_description"]:
            query_database(
                "UPDATE users SET last_action = 'awaiting_category' WHERE whatsapp_number = %s",
                (sender_id,),
                commit=True,
            )
            send_whatsapp_message(sender_id, "⚠️ Please start by selecting a category first.")
            send_category_prompt(sender_id)
            return

        download_result = download_media(media_id, filename, media_url=media_url, mimetype=mimetype, media_type=media_type)
        if "success" not in download_result:
            logging.error(f"❌ Download failed for {sender_id}: {download_result}")
            send_whatsapp_message(sender_id, f"❌ Failed to upload {media_type}. Please try again.")
            return

        caption = (caption_text or "").strip() or "No Caption"

        logging.info("📎 Saving temp media sender=%s type=%s path=%s", sender_id, media_type, download_result["path"])
        ok = save_temp_media_to_db(sender_id, media_type, download_result["path"], caption, mimetype=mimetype, filename=os.path.basename(download_result["path"]))
        if not ok:
            send_whatsapp_message(sender_id, "❌ Failed to save attachment. Please try again.")
            return

        # After first attachment we definitely want the user in awaiting_issue_description state
        query_database(
            "UPDATE users SET last_action = 'awaiting_issue_description' WHERE whatsapp_number = %s",
            (sender_id,),
            commit=True,
        )

        # Reset timer on each upload
        start_or_reset_description_timer(sender_id)

        # ✅ Buttons (instead of /done)
        send_attachment_action_buttons(sender_id)

    finally:
        # ✅ Upload complete
        set_uploading(sender_id, False)


def handle_ticket_creation(sender_id, message_text, property_id):
    # ✅ If upload still processing, do NOT create ticket yet
    if is_uploading(sender_id):
        send_whatsapp_message(sender_id, "⏳ Please wait — your attachment is still uploading/processing.")
        send_attachment_action_buttons(sender_id)
        return

    # If they finally typed description, cancel the expiry timer
    cancel_description_timer(sender_id)

    user_info = query_database(
        "SELECT id, temp_category FROM users WHERE whatsapp_number = %s",
        (sender_id,),
    )
    if not user_info:
        send_whatsapp_message(sender_id, "❌ Error creating ticket. Please try again.")
        return

    user_id = user_info[0]["id"]
    category = user_info[0]["temp_category"]

    description = (message_text or "").strip()

    # If empty, try captions fallback
    if not description:
        media_captions = query_database(
            "SELECT caption FROM temp_ticket_media WHERE sender_id = %s",
            (sender_id,),
        )
        captions = [entry["caption"] for entry in (media_captions or []) if entry["caption"] != "No Caption"]
        if captions:
            description = "AUTO-FILLED ISSUE DESCRIPTION:\n\n" + "\n\n".join(captions)
        elif media_captions:
            description = "No description provided. Media uploaded only."
        else:
            send_whatsapp_message(sender_id, "✏️ Please describe your issue or upload a file.")
            return

    # Clear state first (prevents double ticket creation)
    query_database(
        "UPDATE users SET last_action = NULL, temp_category = NULL WHERE whatsapp_number = %s",
        (sender_id,),
        commit=True,
    )
    with user_timers_lock:
        user_timers.pop(sender_id, None)

    # Insert ticket
    ticket_id = insert_ticket_and_get_id(user_id, description, category, property_id)

    # Attach media
    recent_media = query_database(
        "SELECT media_type, media_path FROM temp_ticket_media WHERE sender_id = %s",
        (sender_id,),
    ) or []

    logging.info("📎 Found %s pending attachment(s) for sender=%s before upload to Odoo ticket=%s", len(recent_media), sender_id, ticket_id)

    attached = 0
    for entry in recent_media:
        if save_ticket_media(ticket_id, entry["media_type"], entry["media_path"], mimetype=entry.get("mimetype"), filename=entry.get("filename")):
            attached += 1

    # Cleanup temp media + local files
    for entry in recent_media:
        _safe_delete_file(entry.get("media_path"))

    query_database(
        "DELETE FROM temp_ticket_media WHERE sender_id = %s",
        (sender_id,),
        commit=True,
    )

    send_whatsapp_message(
        sender_id,
        f"✅ Your ticket #{ticket_id} has been created under *{category}* with {attached} attachment(s). Our team will get back to you soon!",
    )


# -----------------------------------------------------------------------------
# Button replies + list replies
# -----------------------------------------------------------------------------
def handle_button_reply(message, sender_id):
    button_id = message["interactive"]["button_reply"]["id"]
    logging.info(f"🔘 Button clicked: {button_id} by {sender_id}")

    # Main menu
    if button_id == BTN_CREATE_TICKET:
        query_database(
            "UPDATE users SET last_action = 'awaiting_category' WHERE whatsapp_number = %s",
            (sender_id,),
            commit=True,
        )
        executor.submit(send_category_prompt, sender_id)
        return

    if button_id == BTN_CHECK_TICKET:
        executor.submit(send_whatsapp_tickets, sender_id)
        return

    # Attachment flow
    if button_id == BTN_ATTACH_ADD_MORE:
        if is_uploading(sender_id):
            send_whatsapp_message(sender_id, "⏳ Please wait — your previous attachment is still processing.")
            send_attachment_action_buttons(sender_id)
            return

        count = get_temp_media_count(sender_id)
        start_or_reset_description_timer(sender_id)

        # ✅ FIX: DO NOT send a separate text message here.
        # Just refresh the buttons and show an instruction inside the buttons body.
        if count >= MAX_ATTACHMENTS:
            send_attachment_action_buttons(
                sender_id,
                note=f"⚠️ Max attachments reached ({MAX_ATTACHMENTS}). Tap *Describe issue* to continue.",
            )
        else:
            send_attachment_action_buttons(
                sender_id,
                note="➕ Send the next attachment now (one at a time).",
            )
        return

    if button_id == BTN_ATTACH_DESCRIBE:
        if is_uploading(sender_id):
            send_whatsapp_message(sender_id, "⏳ Please wait — your attachment is still uploading/processing.")
            send_attachment_action_buttons(sender_id)
            return

        query_database(
            "UPDATE users SET last_action = 'awaiting_issue_description' WHERE whatsapp_number = %s",
            (sender_id,),
            commit=True,
        )
        send_whatsapp_message(
            sender_id,
            "✍️ Please describe your issue now.\n\n"
            "📎 If you wish to upload a file, please do so *before describing your issue*.\n\n"
            f"⏳ You have {DESCRIPTION_TTL_SECONDS//60} minutes from your last upload."
        )
        start_or_reset_description_timer(sender_id)
        return

    if button_id == BTN_ATTACH_MANAGE:
        if is_uploading(sender_id):
            send_whatsapp_message(sender_id, "⏳ Please wait — your attachment is still uploading/processing.")
            send_attachment_action_buttons(sender_id)
            return

        send_manage_attachments_list(sender_id)
        start_or_reset_description_timer(sender_id)
        return


def handle_list_reply(message, sender_id):
    item_id = message["interactive"]["list_reply"]["id"]
    logging.info(f"📋 List item selected: {item_id} by {sender_id}")

    if is_uploading(sender_id):
        send_whatsapp_message(sender_id, "⏳ Please wait — your attachment is still uploading/processing.")
        send_attachment_action_buttons(sender_id)
        return

    if item_id == LIST_ATTACH_PREVIEW:
        list_attachments(sender_id)
        send_attachment_action_buttons(sender_id)
        start_or_reset_description_timer(sender_id)
        return

    if item_id == LIST_ATTACH_REMOVE_LAST:
        remove_last_attachment(sender_id)
        send_attachment_action_buttons(sender_id)
        start_or_reset_description_timer(sender_id)
        return

    if item_id == LIST_ATTACH_CLEAR_ALL:
        clear_all_attachments(sender_id, notify=True)
        send_attachment_action_buttons(sender_id)
        if get_temp_media_count(sender_id) <= 0:
            cancel_description_timer(sender_id)
        return


# -----------------------------------------------------------------------------
# Upload handling (incoming media)
# -----------------------------------------------------------------------------
def handle_media_upload(message, sender_id, message_text):
    media_type = message.get("type")
    if media_type not in ["document", "image", "video"]:
        return False

    logging.info("📎 Incoming media detected sender=%s type=%s message_id=%s", sender_id, media_type, message.get("id"))

    # If an upload is already in flight, tell them to wait (prevents piling up)
    if is_uploading(sender_id):
        send_whatsapp_message(sender_id, "⏳ Please wait — your previous attachment is still processing.")
        send_attachment_action_buttons(sender_id)
        return True  # we handled it

    media_obj = message.get(media_type, {}) or {}
    media_id = media_obj["id"]
    filename, mimetype, direct_media_url = _build_whatsapp_media_filename(media_type, media_id, media_obj)

    # ✅ Pass caption through so it’s saved properly
    caption = (media_obj.get("caption") or "").strip() if isinstance(media_obj, dict) else ""

    logging.info(
        "📎 Media filename resolved sender=%s type=%s media_id=%s filename=%s mimetype=%s",
        sender_id,
        media_type,
        media_id,
        filename,
        mimetype,
    )

    # process upload async but wait (keeps current behaviour deterministic)
    future = executor.submit(process_media_upload, media_id, filename, sender_id, media_type, caption, mimetype, direct_media_url)
    try:
        future.result()
    except Exception as e:
        logging.error("❌ Media upload worker crashed sender=%s media_id=%s: %s", sender_id, media_id, e, exc_info=True)
        send_whatsapp_message(sender_id, "❌ Failed to process the attachment. Please try uploading it again.")
    return True


def handle_clear_attachments(sender_id):
    clear_all_attachments(sender_id, notify=True)
    cancel_description_timer(sender_id)


def handle_category_selection(sender_id: str, message_text: str):
    category_name = get_category_name(message_text)
    if category_name:
        query_database(
            "UPDATE users SET last_action = 'awaiting_issue_description', temp_category = %s WHERE whatsapp_number = %s",
            (category_name, sender_id),
            commit=True,
        )
        with user_timers_lock:
            if sender_id in user_timers:
                del user_timers[sender_id]

        send_whatsapp_message(
            sender_id,
            "✍️ Please describe your issue.\n\n"
            "📎 If you wish to upload a file, please do so *before describing your issue*.\n\n"
            "If you want to attach files, send them now (one at a time).\n"
            f"✅ Max {MAX_ATTACHMENTS} attachments.\n"
            f"⏳ After uploading, you have {DESCRIPTION_TTL_SECONDS//60} minutes to send the issue description "
            "or uploads will be deleted.",
        )

        if get_temp_media_count(sender_id) > 0:
            start_or_reset_description_timer(sender_id)
    else:
        send_whatsapp_message(sender_id, "⚠️ Invalid selection. Please reply with 1️⃣, 2️⃣, 3️⃣, or 4️⃣.")
        send_category_prompt(sender_id)


# -----------------------------------------------------------------------------
# ✅ TIME FIX HERE: remove NOW() and store Kenya time
# -----------------------------------------------------------------------------
def mark_user_accepted(whatsapp_number):
    return accept_terms(str(whatsapp_number))


def handle_accept(sender_id):
    with accept_lock:
        logging.info(f"Processing accept for {sender_id}")
        send_whatsapp_message(sender_id, "⏳ We're getting things sorted, this may take a minute or two...")

        if is_registered_user(sender_id):
            with terms_pending_lock:
                terms_pending_users.pop(sender_id, None)
            try:
                accept_terms(sender_id)
            except Exception:
                logging.error("Failed to mark existing requester terms accepted in Odoo", exc_info=True)
            send_whatsapp_message(sender_id, "🎉 You are already registered!")
            return

        payload = temp_opt_in_data.get(sender_id) or {}
        if not payload:
            with terms_pending_lock:
                terms_pending_users.pop(sender_id, None)
            send_whatsapp_message(
                sender_id,
                "⚠️ We couldn't find your registration details. Please contact support to resend the opt-in.",
            )
            return

        try:
            register_user(
                whatsapp_number=sender_id,
                name=payload.get("name") or sender_id,
                property_id=payload.get("property_id"),
                unit_number=payload.get("unit_number"),
                terms_accepted=True,
            )
            accept_terms(sender_id)
            _set_state(
                sender_id,
                property_id=payload.get("property_id"),
                unit_number=payload.get("unit_number"),
                last_action=None,
                temp_category=None,
            )
            temp_opt_in_data.pop(sender_id, None)
            with terms_pending_lock:
                terms_pending_users.pop(sender_id, None)
            send_whatsapp_message(sender_id, "🎉 You've been registered successfully!")
        except Exception as e:
            logging.error(f"❌ Odoo registration failed for {sender_id}: {e}", exc_info=True)
            with terms_pending_lock:
                terms_pending_users.pop(sender_id, None)
            send_whatsapp_message(
                sender_id,
                "⚠️ We couldn't finalize your registration. Please try again or contact support.",
            )


# -----------------------------------------------------------------------------
# Message parsing
# -----------------------------------------------------------------------------
def extract_message_info(message):
    message_id = message.get("id")
    sender_id = message["from"]
    message_text = ""

    if "text" in message:
        message_text = message.get("text", {}).get("body", "").strip()
    elif message.get("type") in ["image", "video", "document"]:
        media_type = message["type"]
        message_text = message[media_type].get("caption", "").strip()

    return message_id, sender_id, message_text


def handle_cancel_command(sender_id):
    clear_all_attachments(sender_id, notify=False)
    cancel_description_timer(sender_id)
    query_database(
        "UPDATE users SET last_action = NULL, temp_category = NULL WHERE whatsapp_number = %s",
        (sender_id,),
        commit=True,
    )
    send_menu_prompt(
        sender_id,
        "🚫 Cancelled. Your uploads were deleted and your progress was reset.\n\nChoose an option:",
    )


# -----------------------------------------------------------------------------
# Webhook processor
# -----------------------------------------------------------------------------
def _safe_process_webhook(data):
    try:
        process_webhook(data)
    except Exception as e:
        logging.error("❌ Unhandled exception in process_webhook: %s", e, exc_info=True)


def process_webhook(data):
    logging.info(f"Processing webhook data:\n{json.dumps(data, indent=2)}")

    if "entry" not in data:
        logging.warning("No 'entry' found in webhook data.")
        return

    for entry in data["entry"]:
        for change in entry.get("changes", []):
            value = change.get("value", {})
            if "statuses" in value:
                for stt in value.get("statuses", []):
                    try:
                        wamid = stt.get("id")
                        status = stt.get("status")  # sent/delivered/read/failed
                        err_txt = None
                        if stt.get("errors"):
                            err_txt = json.dumps(stt.get("errors"), ensure_ascii=False)

                        # Update row if exists, otherwise insert a "status" event
                        try:
                            update_whatsapp_message_status(wamid, status=status, error_text=err_txt)
                        except Exception:
                            pass

                        # Optional: also store status event as its own line
                        log_whatsapp_message(
                            wa_number=(stt.get("recipient_id") or "unknown"),
                            direction="outbound",
                            message_type="status",
                            body_text=None,
                            message_id=wamid,
                            status=status,
                            error_text=err_txt,
                            meta_json=json.dumps(stt, ensure_ascii=False),
                        )
                    except Exception as e:
                        logging.error(f"Failed to log status: {e}", exc_info=True)
                continue


            for message in value.get("messages", []):
                message_id, sender_id, message_text = extract_message_info(message)
                logging.info(f"Message from {sender_id} ({message_id}): {message_text}")

                # ✅ Log inbound (raw)
                try:
                    mtype = message.get("type", "text")
                    log_whatsapp_message(
                        wa_number=sender_id,
                        direction="inbound",
                        message_type=mtype,
                        body_text=message_text or None,
                        message_id=message_id,
                        meta_json=json.dumps({"type": mtype}, ensure_ascii=False),
                    )
                except Exception as e:
                    logging.error(f"Failed to log inbound message: {e}", exc_info=True)


                # Handle button replies
                if "interactive" in message and "button_reply" in message["interactive"]:
                    handle_button_reply(message, sender_id)
                    continue

                # Handle list replies
                if "interactive" in message and "list_reply" in message["interactive"]:
                    handle_list_reply(message, sender_id)
                    continue

                normalized = message_text.strip().lower() if message_text else ""

                if normalized in ["accept", "reject"]:
                    with terms_pending_lock:
                        if normalized == "reject":
                            temp_opt_in_data.pop(sender_id, None)
                            terms_pending_users.pop(sender_id, None)
                            executor.submit(
                                send_whatsapp_message,
                                sender_id,
                                "❌ You must accept the Terms to use this service.",
                            )
                        else:
                            executor.submit(handle_accept, sender_id)
                    continue

                if not is_valid_message(sender_id, message_id, message_text):
                    continue

                # Media upload
                if handle_media_upload(message, sender_id, message_text):
                    continue

                # Commands
                if normalized == "/cancel":
                    handle_cancel_command(sender_id)
                    return

                # (kept for compatibility)
                if normalized == "/clear_attachments":
                    handle_clear_attachments(sender_id)
                    continue

                # Fetch user last_action
                user_status = query_database(
                    "SELECT last_action, temp_category FROM users WHERE whatsapp_number = %s",
                    (sender_id,),
                )
                user_info = query_database(
                    "SELECT property_id FROM users WHERE whatsapp_number = %s",
                    (sender_id,),
                )
                if not user_status or not user_info:
                    send_menu_prompt(sender_id, "⚠️ You are not registered. Please contact support.\n\nChoose an option:")
                    continue

                last_action = user_status[0]["last_action"]
                property_id = user_info[0]["property_id"]

                if last_action == "awaiting_category":
                    handle_category_selection(sender_id, message_text)
                    continue

                if last_action == "awaiting_issue_description":
                    handle_ticket_creation(sender_id, message_text, property_id)
                    continue

                if normalized in ["hi", "hello", "help", "menu"]:
                    send_whatsapp_buttons(sender_id)
                    continue

                # ✅ Requested: whenever bot says "Sorry...", include buttons
                send_menu_prompt(
                    sender_id,
                    "🤖 Sorry, I didn't understand that. Please choose an option from the menu.",
                )

    purge_expired_items()
