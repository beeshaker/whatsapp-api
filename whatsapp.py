import os
import json
import time
import requests
import mysql.connector
import logging
import threading
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from conn1 import (
    get_db_connection1,
    save_ticket_media,
    insert_ticket_and_get_id,
    mark_user_accepted_via_temp_table,
    save_temp_media_to_db,   # ✅ IMPORTANT (so we don’t re-import inside)
)
from sqlalchemy.sql import text
from threading import Timer
from concurrent.futures import ThreadPoolExecutor
from logging.handlers import RotatingFileHandler
from datetime import datetime
from zoneinfo import ZoneInfo

# -----------------------------------------------------------------------------
# Timezone (Kenya)
# -----------------------------------------------------------------------------
KENYA_TZ = ZoneInfo("Africa/Nairobi")


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
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

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
# DB helper
# -----------------------------------------------------------------------------
def query_database(query, params=(), commit=False):
    try:
        conn = mysql.connector.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
        )
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, params)
        if commit:
            conn.commit()
            cursor.close()
            conn.close()
            return True
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result
    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
        return None


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
    engine = get_db_connection1()
    with engine.connect() as conn:
        user_check = conn.execute(
            text("SELECT id FROM users WHERE whatsapp_number = :whatsapp_number"),
            {"whatsapp_number": whatsapp_number},
        ).fetchone()
        admin_check = conn.execute(
            text("SELECT id FROM admin_users WHERE whatsapp_number = :whatsapp_number"),
            {"whatsapp_number": whatsapp_number},
        ).fetchone()
    return user_check is not None or admin_check is not None


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
    logging.info(f"Sent WhatsApp message: {response.json()}")
    return response.json()


def send_whatsapp_tickets(to):
    message = ""
    query = """
        SELECT 
            id,
            LEFT(issue_description, 50) AS short_description,
            status,
            COALESCE(updated_at, created_at) AS last_update
        FROM tickets
        WHERE user_id = (SELECT id FROM users WHERE whatsapp_number = %s)
          AND LOWER(status) IN ('open', 'in progress')
        ORDER BY COALESCE(updated_at, created_at) DESC
        LIMIT 20
    """
    tickets = query_database(query, (to,))

    if not tickets:
        message = "You have no open or in-progress tickets at the moment."
    else:
        message = "Your active tickets (Open / In Progress):\n\n"
        for ticket in tickets:
            message += (
                f"Ticket ID: {ticket['id']}\n"
                f"Status: {ticket['status']}\n"
                f"Description: {ticket['short_description']}\n"
                f"Last Update on: {ticket['last_update']}\n\n"
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
    executor.submit(process_webhook, data)
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
            "components": [
                {
                    "type": "body",
                    "parameters": [{"type": "text", "text": p} for p in parameters],
                }
            ],
        },
    }

    response = requests.post(url, headers=headers, json=payload)
    logging.info(f"Sent WhatsApp template message: {response.json()}")
    return response.json()


# -----------------------------------------------------------------------------
# Media handling
# -----------------------------------------------------------------------------
def download_media(media_id, filename=None):
    meta_url = f"https://graph.facebook.com/v22.0/{media_id}"
    headers = {"Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}"}

    media_response = requests.get(meta_url, headers=headers)
    if media_response.status_code != 200:
        return {"error": "Failed to fetch media URL", "details": media_response.text}

    media_url = media_response.json().get("url")
    if not media_url:
        return {"error": "Media URL not found"}

    media_file_response = requests.get(media_url, headers=headers)
    if media_file_response.status_code != 200:
        return {"error": "Failed to download media", "details": media_file_response.text}

    timestamp = kenya_now().strftime("%Y%m%d_%H%M%S")
    if not filename:
        filename = f"{media_id}_{timestamp}.bin"
    else:
        name, ext = os.path.splitext(filename)
        filename = f"{name}_{timestamp}{ext}"

    save_path = os.path.join("/tmp", filename)
    with open(save_path, "wb") as f:
        f.write(media_file_response.content)

    return {"success": True, "path": save_path}


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


def process_media_upload(media_id, filename, sender_id, media_type, caption_text):
    # ✅ Mark this sender as “uploading” to block Describe/Ticket creation until complete
    set_uploading(sender_id, True)

    try:
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

        download_result = download_media(media_id, filename)
        if "success" not in download_result:
            logging.error(f"❌ Download failed for {sender_id}: {download_result}")
            send_whatsapp_message(sender_id, f"❌ Failed to upload {media_type}. Please try again.")
            return

        caption = (caption_text or "").strip() or "No Caption"

        ok = save_temp_media_to_db(sender_id, media_type, download_result["path"], caption)
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

    attached = 0
    for entry in recent_media:
        if save_ticket_media(ticket_id, entry["media_type"], entry["media_path"]):
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

    # If an upload is already in flight, tell them to wait (prevents piling up)
    if is_uploading(sender_id):
        send_whatsapp_message(sender_id, "⏳ Please wait — your previous attachment is still processing.")
        send_attachment_action_buttons(sender_id)
        return True  # we handled it

    media_id = message[media_type]["id"]
    base_filename = message[media_type].get("filename", f"{media_id}.{media_type[:3]}")
    name, ext = os.path.splitext(base_filename)
    timestamp = kenya_now().strftime("%Y%m%d_%H%M%S")
    filename = f"{name}_{timestamp}{ext}"

    # ✅ Pass caption through so it’s saved properly
    caption = ""
    if media_type in message and isinstance(message[media_type], dict):
        caption = (message[media_type].get("caption") or "").strip()

    # process upload async but wait (keeps current behaviour deterministic)
    future = executor.submit(process_media_upload, media_id, filename, sender_id, media_type, caption)
    future.result()
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
    engine = get_db_connection1()
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT id FROM users WHERE whatsapp_number = :num"),
            {"num": whatsapp_number},
        ).fetchone()
        if not result:
            raise Exception("User not found in DB")

        conn.execute(
            text(
                """
                UPDATE users
                SET terms_accepted = 1,
                    terms_accepted_at = :ts
                WHERE whatsapp_number = :num
                """
            ),
            {"num": whatsapp_number, "ts": kenya_now()},
        )
        conn.commit()


def handle_accept(sender_id):
    with accept_lock:
        logging.info(f"Processing accept for {sender_id}")
        send_whatsapp_message(sender_id, "⏳ We're getting things sorted, this may take a minute or two...")

        if sender_id in accept_retry_state:
            retry_info = accept_retry_state.pop(sender_id)
            if retry_info and retry_info["timer"]:
                retry_info["timer"].cancel()

        if is_registered_user(sender_id):
            with terms_pending_lock:
                terms_pending_users.pop(sender_id, None)
            send_whatsapp_message(sender_id, "🎉 You are already registered!")
            return

        def try_register(attempt):
            with accept_lock:
                if is_registered_user(sender_id):
                    accept_retry_state.pop(sender_id, None)
                    with terms_pending_lock:
                        terms_pending_users.pop(sender_id, None)
                    send_whatsapp_message(sender_id, "🎉 You are already registered!")
                    return

                try:
                    mark_user_accepted_via_temp_table(sender_id)

                    accept_retry_state.pop(sender_id, None)
                    with terms_pending_lock:
                        terms_pending_users.pop(sender_id, None)

                    send_whatsapp_message(sender_id, "🎉 You've been registered successfully!")
                except Exception as e:
                    logging.error(f"❌ Attempt {attempt} failed for {sender_id}: {e}")
                    if attempt < 3:
                        timer = Timer(15, try_register, args=[attempt + 1])
                        accept_retry_state[sender_id] = {"attempt": attempt + 1, "timer": timer}
                        timer.start()
                    else:
                        accept_retry_state.pop(sender_id, None)
                        with terms_pending_lock:
                            terms_pending_users.pop(sender_id, None)
                        send_whatsapp_message(
                            sender_id,
                            "⚠️ We couldn't finalize your registration. Please try again or contact support.",
                        )

        timer = Timer(1, try_register, args=[1])
        accept_retry_state[sender_id] = {"attempt": 1, "timer": timer}
        timer.start()


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
def process_webhook(data):
    logging.info(f"Processing webhook data:\n{json.dumps(data, indent=2)}")

    if "entry" not in data:
        logging.warning("No 'entry' found in webhook data.")
        return

    for entry in data["entry"]:
        for change in entry.get("changes", []):
            value = change.get("value", {})
            if "statuses" in value:
                continue

            for message in value.get("messages", []):
                message_id, sender_id, message_text = extract_message_info(message)
                logging.info(f"Message from {sender_id} ({message_id}): {message_text}")

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
