import os
import json
import time
import requests
import logging
import threading
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from conn1 import get_db_connection1, save_ticket_media, insert_ticket_and_get_id
from sqlalchemy.sql import text
from threading import Timer
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# Initialize Flask app and executor
app = Flask(__name__)
executor = ThreadPoolExecutor(max_workers=10)
logging.basicConfig(level=logging.INFO)

# Load environment variables
load_dotenv()
WHATSAPP_ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
WHATSAPP_PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")

# Threading locks
media_buffer_lock = threading.Lock()
user_timers_lock = threading.Lock()

# In-memory storage
processed_message_ids = set()
last_messages = {}  # { sender_id: (message_text, timestamp) }
media_buffer = {}  # { sender_id: { media_id: { media_type, media_path, caption, timestamp, confirmed } } }
upload_state = {}  # { sender_id: { timer, last_upload_time, media_count } }
user_timers = {}

# Media buffer TTL (1 hour to prevent premature expiration)
MEDIA_TTL_SECONDS = 3600

# Function to check if a message has been processed
def is_message_processed(message_id):
    if message_id in processed_message_ids:
        return True
    engine = get_db_connection1()
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT id FROM processed_messages WHERE id = :message_id"),
            {"message_id": message_id}
        ).fetchone()
    return bool(result)

def mark_message_as_processed(message_id):
    processed_message_ids.add(message_id)
    engine = get_db_connection1()
    with engine.connect() as conn:
        conn.execute(
            text("INSERT IGNORE INTO processed_messages (id) VALUES (:message_id)"),
            {"message_id": message_id}
        )
        conn.commit()

def should_process_message(sender_id, message_text):
    global last_messages
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
            {"whatsapp_number": whatsapp_number}
        ).fetchone()
        admin_check = conn.execute(
            text("SELECT id FROM admin_users WHERE whatsapp_number = :whatsapp_number"),
            {"whatsapp_number": whatsapp_number}
        ).fetchone()
    return bool(user_check) or bool(admin_check)

def send_category_prompt(to):
    """Asks the user to select a category for the ticket."""
    message = "Please select a category:\n1️⃣ Accounts\n2️⃣ Maintenance\n3️⃣ Security\n4️⃣ Other\n\nReply with the number."
    executor.submit(send_whatsapp_message, to, message)

    with user_timers_lock:
        user_timers[to] = datetime.now()

    threading.Thread(target=reset_category_selection, args=(to,), daemon=True).start()

def reset_category_selection(to):
    """Resets the category selection if the user takes more than 5 minutes to respond."""
    time.sleep(300)  # Wait for 5 minutes

    with user_timers_lock:
        last_attempt_time = user_timers.get(to)
        if last_attempt_time:
            elapsed_time = (datetime.now() - last_attempt_time).total_seconds()
            if elapsed_time >= 300:
                del user_timers[to]
            else:
                return
        else:
            return

    logging.info(f"⏳ Resetting category selection for {to} due to timeout.")
    engine = get_db_connection1()
    with engine.connect() as conn:
        conn.execute(
            text("UPDATE users SET last_action = NULL WHERE whatsapp_number = :whatsapp_number"),
            {"whatsapp_number": to}
        )
        conn.commit()
    send_whatsapp_message(to, "⏳ Your category selection request has expired. Please start again by selecting '📝 Create Ticket'.")

@app.route('/opt_in_user', methods=['POST'])
def opt_in_user_route():
    if request.headers.get("X-API-KEY") != os.getenv("INTERNAL_API_KEY"):
        return jsonify({"error": "Unauthorized"}), 403

    data = request.json
    whatsapp_number = data.get("whatsapp_number")
    
    if not whatsapp_number:
        return jsonify({"error": "Missing whatsapp_number"}), 400

    url = f"https://graph.facebook.com/v22.0/{os.getenv('WHATSAPP_PHONE_NUMBER_ID')}/messages"
    headers = {
        "Authorization": f"Bearer {os.getenv('WHATSAPP_ACCESS_TOKEN')}",
        "Content-Type": "application/json",
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": whatsapp_number,
        "type": "template",
        "template": {
            "name": "registration_welcome",
            "language": {"code": "en"},
            "components": [
                {
                    "type": "body",
                    "parameters": [
                        {"type": "text", "text": "Welcome to our service! You are now registered."}
                    ]
                }
            ]
        }
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        return jsonify({"status": "success", "details": response.json()}), response.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def get_category_name(category_number):
    categories = {
        "1": "Accounts",
        "2": "Maintenance",
        "3": "Security",
        "4": "Other"
    }
    return categories.get(category_number, None)

def send_whatsapp_buttons(to):
    url = f"https://graph.facebook.com/v22.0/{WHATSAPP_PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": "What would you like to do?"},
            "action": {
                "buttons": [
                    {
                        "type": "reply",
                        "reply": {
                            "id": "create_ticket",
                            "title": "📝 Create Ticket",
                        },
                    },
                    {
                        "type": "reply",
                        "reply": {
                            "id": "check_ticket",
                            "title": "📌 Check Status",
                        },
                    },
                ]
            },
        },
    }
    response = requests.post(url, headers=headers, json=payload)
    logging.info(f"Sent WhatsApp interactive buttons: {response.json()}")
    return response.json()

def send_whatsapp_message(to, message):
    url = f"https://graph.facebook.com/v22.0/{WHATSAPP_PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"body": message},
    }
    response = requests.post(url, headers=headers, json=payload)
    logging.info(f"Sent WhatsApp message: {response.json()}")
    return response.json()

def send_whatsapp_tickets(to):
    message = ""
    engine = get_db_connection1()
    with engine.connect() as conn:
        tickets = conn.execute(
            text("""
                SELECT id, LEFT(issue_description, 50) AS short_description, updated_at as last_update
                FROM tickets 
                WHERE user_id = (SELECT id FROM users WHERE whatsapp_number = :whatsapp_number) 
                AND status = 'Open'
            """),
            {"whatsapp_number": to}
        ).fetchall()

    if not tickets:
        message = "You have no open tickets at the moment."
    else:
        message = "Your open tickets:\n"
        for ticket in tickets:
            message += f"Ticket ID: {ticket['id']}\nDescription: {ticket['short_description']}\nLast Update on: {ticket['last_update']}\n\n"
    
    executor.submit(send_whatsapp_message, to, message)

@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        verify_token = "12345"
        if request.args.get("hub.verify_token") == verify_token:
            return request.args.get("hub.challenge"), 200
        return "Invalid verification token", 403

    try:
        data = request.get_json()
        if not data:
            logging.error("No JSON data received in webhook")
            return jsonify({"error": "Invalid request data"}), 400
        logging.info(f"Incoming webhook data: {json.dumps(data, indent=2)}")
        process_webhook(data)
        return jsonify({"status": "received"}), 200
    except Exception as e:
        logging.error(f"Error processing webhook: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500

@app.route("/send_message", methods=["POST"])
def external_send_message():
    data = request.get_json()
    api_key = request.headers.get("X-API-KEY")

    if api_key != os.getenv("INTERNAL_API_KEY"):
        return jsonify({"error": "Unauthorized"}), 401

    to = data.get("to")
    message = data.get("message")
    template_name = data.get("template_name")
    template_parameters = data.get("template_parameters", [])

    if not to or (not message and not template_name):
        return jsonify({"error": "Missing required fields"}), 400

    try:
        if template_name:
            result = executor.submit(send_template_message, to, template_name, template_parameters)
        else:
            result = executor.submit(send_whatsapp_message, to, message)

        return jsonify(result), 200
    except Exception as e:
        logging.error(f"Error sending WhatsApp message: {str(e)}")
        return jsonify({"error": str(e)}), 500

def send_template_message(to, template_name, parameters):
    url = f"https://graph.facebook.com/v22.0/{WHATSAPP_PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "template",
        "template": {
            "name": template_name,
            "language": { "code": "en" },
            "components": [{
                "type": "body",
                "parameters": [{"type": "text", "text": p} for p in parameters]
            }]
        }
    }

    response = requests.post(url, headers=headers, json=payload)
    logging.info(f"Sent WhatsApp template message: {response.json()}")
    return response.json()

def download_media(media_id, filename=None):
    meta_url = f"https://graph.facebook.com/v22.0/{media_id}"
    headers = {"Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}"}
    try:
        media_response = requests.get(meta_url, headers=headers)
        if media_response.status_code != 200:
            return {"error": "Failed to fetch media URL", "details": media_response.text}
        media_url = media_response.json().get("url")
        if not media_url:
            return {"error": "Media URL not found"}
        media_file_response = requests.get(media_url, headers=headers)
        if media_file_response.status_code != 200:
            return {"error": "Failed to download media", "details": media_file_response.text}
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if not filename:
            filename = f"{media_id}_{timestamp}.bin"
        else:
            name, ext = os.path.splitext(filename)
            filename = f"{name}_{timestamp}{ext}"
        save_path = os.path.join("/tmp", filename)
        with open(save_path, "wb") as f:
            f.write(media_file_response.content)
        return {"success": True, "path": save_path}
    except Exception as e:
        return {"error": f"Exception while downloading media: {str(e)}"}

def purge_expired_media():
    now = time.time()
    with media_buffer_lock:
        for sender_id in list(media_buffer.keys()):
            engine = get_db_connection1()
            with engine.connect() as conn:
                user_status = conn.execute(
                    text("SELECT last_action FROM users WHERE whatsapp_number = :whatsapp_number"),
                    {"whatsapp_number": sender_id}
                ).fetchone()
            if user_status and user_status["last_action"] in ["awaiting_issue_description", "awaiting_category"]:
                continue
            media_buffer[sender_id] = {
                mid: entry
                for mid, entry in media_buffer[sender_id].items()
                if now - entry["timestamp"] < MEDIA_TTL_SECONDS
            }
            if not media_buffer[sender_id]:
                del media_buffer[sender_id]
                send_whatsapp_message(sender_id, "⏳ Your uploaded files have expired. Please start again.")

def schedule_purge():
    purge_expired_media()
    threading.Timer(60, schedule_purge).start()

schedule_purge()

def flush_user_media_after_ticket(sender_id, ticket_id, delay=30):
    time.sleep(delay)
    with media_buffer_lock:
        media_list = list(media_buffer.get(sender_id, {}).values())
        if sender_id in media_buffer:
            del media_buffer[sender_id]
    for entry in media_list:
        if entry["confirmed"]:
            save_ticket_media(ticket_id, entry["media_type"], entry["media_path"])
            logging.info(f"📁 (Post-ticket) Linked {entry['media_type']} to ticket #{ticket_id}")

def handle_auto_submit_ticket(sender_id):
    with media_buffer_lock:
        non_empty_captions = [
            entry["caption"]
            for entry in media_buffer.get(sender_id, {}).values()
            if entry.get("caption") and entry["confirmed"]
        ]

    if not non_empty_captions:
        send_whatsapp_message(sender_id, "❌ No valid captions found. Please describe your issue.")
        return

    send_whatsapp_message(sender_id, "📝 Auto-submitting ticket with your uploaded media and captions.")

    engine = get_db_connection1()
    with engine.connect() as conn:
        conn.execute(
            text("UPDATE users SET last_action = 'awaiting_issue_description', temp_category = :category WHERE whatsapp_number = :whatsapp_number"),
            {"category": "Other", "whatsapp_number": sender_id}
        )
        conn.commit()

    auto_description = "AUTO-FILLED ISSUE DESCRIPTION:\n\n" + "\n\n".join(non_empty_captions)

    engine = get_db_connection1()
    with engine.connect() as conn:
        user_info = conn.execute(
            text("SELECT id, property_id FROM users WHERE whatsapp_number = :whatsapp_number"),
            {"whatsapp_number": sender_id}
        ).fetchone()
    if not user_info:
        send_whatsapp_message(sender_id, "❌ Error creating ticket. Please try again.")
        return

    user_id = user_info["id"]
    property = user_info["property_id"]

    create_ticket_with_media(sender_id, user_id, "Other", property, auto_description)

    with user_timers_lock:
        if sender_id in user_timers:
            del user_timers[sender_id]

def send_caption_confirmation(phone_number, captions, media_id):
    url = f"https://graph.facebook.com/v22.0/{WHATSAPP_PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    caption_text = '\n'.join([f"{i+1}. \"{c.strip()}\"" for i, c in enumerate(captions)])
    payload = {
        "messaging_product": "whatsapp",
        "to": phone_number,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {
                "text": f"📝 Caption for media:\n\n{caption_text}\n\nDoes this look correct?"
            },
            "action": {
                "buttons": [
                    {"type": "reply", "reply": {"id": f"caption_confirm_yes_{media_id}", "title": "✅ Yes"}},
                    {"type": "reply", "reply": {"id": f"caption_confirm_no_{media_id}", "title": "❌ No"}}
                ]
            }
        }
    }
    response = requests.post(url, headers=headers, json=payload)
    logging.info(f"Sent caption confirmation for {phone_number}, media_id: {media_id}")
    return response.json()

def is_valid_message(sender_id, message_id, message_text):
    if not is_registered_user(sender_id):
        logging.info(f"Blocked unregistered user: {sender_id}")
        send_whatsapp_message(sender_id, "You are not registered. Please register first.")
        return False

    if is_message_processed(message_id) or not should_process_message(sender_id, message_text):
        logging.info(f"⚠️ Skipping duplicate message {message_id}")
        return False

    mark_message_as_processed(message_id)
    return True

def process_media_upload(media_id, filename, sender_id, media_type, message_text):
    with user_timers_lock:
        if sender_id in upload_state:
            if time.time() - upload_state[sender_id]["last_upload_time"] >= 60:
                upload_state[sender_id]["media_count"] = 0
            elif upload_state[sender_id]["media_count"] >= 10:
                send_whatsapp_message(sender_id, "⚠️ Too many uploads. Please wait a minute before uploading more.")
                return

    engine = get_db_connection1()
    with engine.connect() as conn:
        user_status = conn.execute(
            text("SELECT last_action, temp_category FROM users WHERE whatsapp_number = :whatsapp_number"),
            {"whatsapp_number": sender_id}
        ).fetchone()
    if not user_status or user_status["last_action"] != "awaiting_issue_description":
        send_whatsapp_message(sender_id, "⚠️ Please select a category first. Reply with 1️⃣, 2️⃣, 3️⃣, or 4️⃣.")
        send_category_prompt(sender_id)
        return

    download_result = download_media(media_id, filename)
    if "success" in download_result:
        with media_buffer_lock:
            if sender_id not in media_buffer:
                media_buffer[sender_id] = {}
            media_buffer[sender_id][media_id] = {
                "media_type": media_type,
                "media_path": download_result["path"],
                "caption": message_text.strip() if message_text else None,
                "timestamp": time.time(),
                "confirmed": not message_text.strip()
            }
            media_count = len(media_buffer[sender_id])
            logging.info(f"📤 Added media {media_id} for {sender_id}. Buffer: {media_buffer[sender_id]}, Count: {media_count}")

        with user_timers_lock:
            upload_state[sender_id] = upload_state.get(sender_id, {"media_count": 0, "timer": None, "last_upload_time": 0})
            upload_state[sender_id]["media_count"] = media_count
            upload_state[sender_id]["last_upload_time"] = time.time()
            logging.info(f"Updated upload_state for {sender_id}. State: {upload_state[sender_id]}")

        if message_text.strip():
            send_caption_confirmation(sender_id, [message_text.strip()], media_id)
        else:
            manage_upload_timer(sender_id)
    else:
        send_whatsapp_message(sender_id, f"❌ Failed to upload {media_type}. Please try again.")
        logging.error(f"Failed to save {media_type} for {sender_id}: {download_result}")

def handle_button_reply(message, sender_id):
    button_id = message["interactive"]["button_reply"]["id"]
    if button_id.startswith("caption_confirm_yes_") or button_id.startswith("caption_confirm_no_"):
        media_id = button_id.split("_", 3)[-1]
        with user_timers_lock:
            if sender_id in upload_state and upload_state[sender_id]["timer"]:
                upload_state[sender_id]["timer"].cancel()
                upload_state[sender_id]["timer"] = None
        with media_buffer_lock:
            if sender_id in media_buffer and media_id in media_buffer[sender_id]:
                if button_id.startswith("caption_confirm_yes_"):
                    media_buffer[sender_id][media_id]["confirmed"] = True
                    media_count = len(media_buffer[sender_id])
                    logging.info(f"✅ Caption confirmed for {sender_id}, media_id: {media_id}. Buffer: {media_buffer[sender_id]}")
                    with user_timers_lock:
                        upload_state[sender_id]["media_count"] = media_count
                        upload_state[sender_id]["last_upload_time"] = time.time()
                    send_whatsapp_message(sender_id, f"✅ Caption confirmed for media. You've uploaded {media_count} file(s). Send more or reply /done to proceed.")
                    manage_upload_timer(sender_id)
                else:
                    removed = media_buffer[sender_id].pop(media_id, None)
                    if not media_buffer[sender_id]:
                        del media_buffer[sender_id]
                    logging.info(f"❌ Caption rejected for {sender_id}, media_id: {media_id}. Removed: {removed}")
                    with user_timers_lock:
                        upload_state[sender_id]["media_count"] = len(media_buffer.get(sender_id, {}))
                        upload_state[sender_id]["last_upload_time"] = time.time() if upload_state[sender_id]["media_count"] > 0 else 0
                    send_whatsapp_message(sender_id, "📝 Caption rejected. Please upload that file again with a corrected caption.")
            else:
                send_whatsapp_message(sender_id, "⚠️ Media not found. Please upload again.")
    elif button_id in ["upload_done", "upload_not_done"]:
        with user_timers_lock:
            if sender_id in upload_state and upload_state[sender_id]["timer"]:
                upload_state[sender_id]["timer"].cancel()
                upload_state[sender_id]["timer"] = None
        if button_id == "upload_done":
            engine = get_db_connection1()
            with engine.connect() as conn:
                user_data = conn.execute(
                    text("SELECT temp_category FROM users WHERE whatsapp_number = :whatsapp_number"),
                    {"whatsapp_number": sender_id}
                ).fetchone()
            if user_data and user_data["temp_category"]:
                engine = get_db_connection1()
                with engine.connect() as conn:
                    conn.execute(
                        text("UPDATE users SET last_action = 'awaiting_issue_description' WHERE whatsapp_number = :whatsapp_number"),
                        {"whatsapp_number": sender_id}
                    )
                    conn.commit()
                send_whatsapp_message(sender_id, "✏️ Great! Please describe your issue.")
            else:
                engine = get_db_connection1()
                with engine.connect() as conn:
                    conn.execute(
                        text("UPDATE users SET last_action = 'awaiting_category' WHERE whatsapp_number = :whatsapp_number"),
                        {"whatsapp_number": sender_id}
                    )
                    conn.commit()
                send_category_prompt(sender_id)
        elif button_id == "upload_not_done":
            send_whatsapp_message(sender_id, "👍 Okay, send more files when you're ready.")
    elif button_id == "create_ticket":
        engine = get_db_connection1()
        with engine.connect() as conn:
            conn.execute(
                text("UPDATE users SET last_action = 'awaiting_category' WHERE whatsapp_number = :whatsapp_number"),
                {"whatsapp_number": sender_id}
            )
            conn.commit()
        send_category_prompt(sender_id)
    elif button_id == "check_ticket":
        send_whatsapp_tickets(sender_id)

def handle_media_upload(message, sender_id, message_text):
    media_type = message.get("type")
    if media_type in ["document", "image", "video"]:
        media_id = message[media_type]["id"]
        base_filename = message[media_type].get("filename", f"{media_id}.{media_type[:3]}")
        name, ext = os.path.splitext(base_filename)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{name}_{timestamp}{ext}"
        executor.submit(process_media_upload, media_id, filename, sender_id, media_type, message_text)
        return True
    return False

def handle_list_uploads(sender_id):
    with media_buffer_lock:
        media_list = media_buffer.get(sender_id, {})
    if not media_list:
        send_whatsapp_message(sender_id, "📎 You have no pending uploads.")
        return
    message = f"📎 Your pending uploads ({len(media_list)}):\n"
    for i, (media_id, entry) in enumerate(media_list.items(), 1):
        caption = entry.get("caption", "No caption")
        status = "Confirmed" if entry["confirmed"] else "Pending confirmation"
        message += f"{i}. {entry['media_type'].capitalize()}: {caption[:30]}... ({status})\n"
    message += "\nReply /remove_upload <number> to delete a specific file or /clear_attachments to clear all."
    send_whatsapp_message(sender_id, message)

def handle_remove_upload(sender_id, upload_index):
    try:
        index = int(upload_index) - 1
        with media_buffer_lock:
            media_list = list(media_buffer.get(sender_id, {}).items())
            if 0 <= index < len(media_list):
                media_id, removed = media_list[index]
                del media_buffer[sender_id][media_id]
                send_whatsapp_message(sender_id, f"🗑️ Removed {removed['media_type'].capitalize()} from your uploads.")
                if not media_buffer[sender_id]:
                    del media_buffer[sender_id]
                with user_timers_lock:
                    upload_state[sender_id]["media_count"] = len(media_buffer.get(sender_id, {}))
                    upload_state[sender_id]["last_upload_time"] = time.time() if upload_state[sender_id]["media_count"] > 0 else 0
            else:
                send_whatsapp_message(sender_id, "⚠️ Invalid upload number.")
    except ValueError:
        send_whatsapp_message(sender_id, "⚠️ Please provide a valid upload number (e.g., /remove_upload 1).")

def send_done_upload_prompt(sender_id):
    with media_buffer_lock:
        media_count = len(media_buffer.get(sender_id, {}))
    url = f"https://graph.facebook.com/v22.0/{WHATSAPP_PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    body_text = f"📎 You've uploaded *{media_count}* file(s).\nAre you done uploading attachments? Reply /done to confirm or send more files."
    payload = {
        "messaging_product": "whatsapp",
        "to": sender_id,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": body_text},
            "action": {
                "buttons": [
                    {"type": "reply", "reply": {"id": "upload_done", "title": "✅ Done"}},
                    {"type": "reply", "reply": {"id": "upload_not_done", "title": "➕ Add More"}},
                ],
            },
        },
    }
    response = requests.post(url, headers=headers, json=payload)
    logging.info(f"Sent done upload prompt to {sender_id}: {response.json()}")

def create_ticket_with_media(sender_id, user_id, category, property_id, description):
    engine = get_db_connection1()
    with engine.connect() as conn:
        try:
            conn.execution_options(autocommit=False)
            ticket_id = insert_ticket_and_get_id(user_id, description, category, property_id, conn=conn)
            if not ticket_id:
                raise Exception("Failed to create ticket")

            with media_buffer_lock:
                media_list = [
                    media_buffer[sender_id][mid]
                    for mid in media_buffer.get(sender_id, {})
                    if media_buffer[sender_id][mid]["confirmed"]
                ]
                logging.info(f"Media to attach for ticket #{ticket_id} (user {sender_id}): {media_list}")
                if sender_id in media_buffer:
                    del media_buffer[sender_id]

            for entry in media_list:
                save_result = save_ticket_media(ticket_id, entry["media_type"], entry["media_path"], conn=conn)
                if not save_result:
                    logging.error(f"Failed to link {entry['media_type']} to ticket #{ticket_id}")

            conn.execute(
                text("UPDATE users SET last_action = NULL, temp_category = NULL WHERE whatsapp_number = :whatsapp_number"),
                {"whatsapp_number": sender_id}
            )
            conn.commit()
            send_whatsapp_message(
                sender_id,
                f"✅ Your ticket #{ticket_id} has been created under *{category}* with {len(media_list)} attachment(s)."
            )
            executor.submit(flush_user_media_after_ticket, sender_id, ticket_id)
        except Exception as e:
            conn.rollback()
            logging.error(f"Failed to create ticket for {sender_id}: {e}")
            send_whatsapp_message(sender_id, "❌ Error creating ticket. Please try again.")
        finally:
            conn.execution_options(autocommit=True)

def handle_clear_attachments(sender_id):
    with media_buffer_lock:
        if sender_id in media_buffer:
            count = len(media_buffer[sender_id])
            del media_buffer[sender_id]
            send_whatsapp_message(sender_id, f"🗑️ Cleared {count} pending attachment(s).")
        else:
            send_whatsapp_message(sender_id, "📎 You have no pending attachments.")

def handle_category_selection(sender_id, message_text):
    category_name = get_category_name(message_text)
    if category_name:
        engine = get_db_connection1()
        with engine.connect() as conn:
            conn.execute(
                text("UPDATE users SET last_action = 'awaiting_issue_description', temp_category = :category WHERE whatsapp_number = :whatsapp_number"),
                {"category": category_name, "whatsapp_number": sender_id}
            )
            conn.commit()
        send_whatsapp_message(sender_id, "Please describe your issue, or upload a supporting file.")
        if sender_id in user_timers:
            del user_timers[sender_id]
    else:
        send_whatsapp_message(sender_id, "⚠️ Invalid selection. Please reply with 1️⃣, 2️⃣, 3️⃣ or 4️⃣.")
        send_category_prompt(sender_id)

def handle_ticket_creation(sender_id, message_text, property):
    engine = get_db_connection1()
    with engine.connect() as conn:
        user_info = conn.execute(
            text("SELECT id, temp_category FROM users WHERE whatsapp_number = :whatsapp_number"),
            {"whatsapp_number": sender_id}
        ).fetchone()
    if not user_info:
        send_whatsapp_message(sender_id, "❌ Error creating ticket. Please try again.")
        return

    user_id = user_info["id"]
    category = user_info["temp_category"]

    engine = get_db_connection1()
    with engine.connect() as conn:
        ticket_check = conn.execute(
            text("""
                SELECT created_at FROM tickets
                WHERE user_id = :user_id
                ORDER BY created_at DESC
                LIMIT 1
            """),
            {"user_id": user_id}
        ).fetchone()

    if ticket_check:
        last_created = ticket_check["created_at"]
        if (datetime.now() - last_created).total_seconds() < 60:
            logging.info(f"🛑 Ticket already created recently for user {sender_id}. Skipping.")
            return

    if not message_text:
        with media_buffer_lock:
            captions = [
                entry["caption"]
                for entry in media_buffer.get(sender_id, {}).values()
                if entry.get("caption") and entry["confirmed"]
            ]

        if captions:
            message_text = "AUTO-FILLED ISSUE DESCRIPTION:\n\n" + "\n\n".join(captions)

    if not message_text:
        send_whatsapp_message(sender_id, "✏️ Please describe your issue or confirm the above captions.")
        return

    create_ticket_with_media(sender_id, user_id, category, property, message_text.strip())

    with user_timers_lock:
        if sender_id in user_timers:
            del user_timers[sender_id]

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

def manage_upload_timer(sender_id):
    with user_timers_lock:
        if sender_id in upload_state and upload_state[sender_id]["timer"]:
            upload_state[sender_id]["timer"].cancel()
        def send_prompt():
            with user_timers_lock:
                if sender_id in upload_state:
                    with media_buffer_lock:
                        media_count = len(media_buffer.get(sender_id, {}))
                        all_confirmed = all(
                            media_buffer[sender_id][mid]["confirmed"] or not media_buffer[sender_id][mid]["caption"]
                            for mid in media_buffer.get(sender_id, {})
                        )
                        all_confirmed_with_captions = all(
                            media_buffer[sender_id][mid]["confirmed"] and media_buffer[sender_id][mid]["caption"]
                            for mid in media_buffer.get(sender_id, {})
                        )
                    if media_count > 0 and all_confirmed_with_captions:
                        executor.submit(handle_auto_submit_ticket, sender_id)
                        upload_state[sender_id]["timer"] = None
                    elif media_count > 0 and all_confirmed:
                        send_done_upload_prompt(sender_id)
                        upload_state[sender_id]["timer"] = None
                    elif media_count > 0:
                        t = Timer(60, send_prompt)
                        upload_state[sender_id]["timer"] = t
                        t.start()
                    else:
                        upload_state[sender_id]["timer"] = None
        with media_buffer_lock:
            media_count = len(media_buffer.get(sender_id, {}))
            if media_count == 0:
                return
            all_confirmed = all(
                media_buffer[sender_id][mid]["confirmed"] or not media_buffer[sender_id][mid]["caption"]
                for mid in media_buffer.get(sender_id, {})
            )
            all_confirmed_with_captions = all(
                media_buffer[sender_id][mid]["confirmed"] and media_buffer[sender_id][mid]["caption"]
                for mid in media_buffer.get(sender_id, {})
            )
        if all_confirmed_with_captions:
            t = Timer(60, send_prompt)
        elif all_confirmed:
            t = Timer(10, send_prompt)
        else:
            return
        upload_state[sender_id] = upload_state.get(sender_id, {"media_count": 0, "last_upload_time": 0, "timer": None})
        upload_state[sender_id]["timer"] = t
        t.start()

def process_webhook(data):
    purge_expired_media()
    logging.info(f"Processing webhook data: {json.dumps(data, indent=2)}")
    if "entry" in data:
        for entry in data["entry"]:
            for change in entry.get("changes", []):
                if "statuses" in change["value"]:
                    continue
                if "messages" in change["value"]:
                    for message in change["value"]["messages"]:
                        message_id, sender_id, message_text = extract_message_info(message)
                        if not is_valid_message(sender_id, message_id, message_text):
                            continue
                        if handle_media_upload(message, sender_id, message_text):
                            with media_buffer_lock:
                                logging.info(f"After handle_media_upload for {sender_id}. Buffer: {media_buffer.get(sender_id, {})}")
                            continue
                        if "interactive" in message and "button_reply" in message["interactive"]:
                            handle_button_reply(message, sender_id)
                            with media_buffer_lock:
                                logging.info(f"After handle_button_reply for {sender_id}. Buffer: {media_buffer.get(sender_id, {})}")
                            continue
                        if message_text.lower() == "/clear_attachments":
                            handle_clear_attachments(sender_id)
                            with media_buffer_lock:
                                logging.info(f"After clear_attachments for {sender_id}. Buffer: {media_buffer.get(sender_id, {})}")
                            continue
                        if message_text.lower() == "/done":
                            engine = get_db_connection1()
                            with engine.connect() as conn:
                                user_data = conn.execute(
                                    text("SELECT temp_category FROM users WHERE whatsapp_number = :whatsapp_number"),
                                    {"whatsapp_number": sender_id}
                                ).fetchone()
                            if user_data and user_data["temp_category"]:
                                engine = get_db_connection1()
                                with engine.connect() as conn:
                                    conn.execute(
                                        text("UPDATE users SET last_action = 'awaiting_issue_description' WHERE whatsapp_number = :whatsapp_number"),
                                        {"whatsapp_number": sender_id}
                                    )
                                    conn.commit()
                                send_whatsapp_message(sender_id, "✏️ Great! Please describe your issue.")
                            else:
                                engine = get_db_connection1()
                                with engine.connect() as conn:
                                    conn.execute(
                                        text("UPDATE users SET last_action = 'awaiting_category' WHERE whatsapp_number = :whatsapp_number"),
                                        {"whatsapp_number": sender_id}
                                    )
                                    conn.commit()
                                send_category_prompt(sender_id)
                            continue
                        if message_text.lower() == "/list_uploads":
                            handle_list_uploads(sender_id)
                            continue
                        if message_text.lower().startswith("/remove_upload"):
                            try:
                                upload_index = message_text.split()[1]
                                handle_remove_upload(sender_id, upload_index)
                                with media_buffer_lock:
                                    logging.info(f"After remove_upload for {sender_id}. Buffer: {media_buffer.get(sender_id, {})}")
                            except IndexError:
                                send_whatsapp_message(sender_id, "⚠️ Please provide an upload number (e.g., /remove_upload 1).")
                            continue
                        engine = get_db_connection1()
                        with engine.connect() as conn:
                            user_status = conn.execute(
                                text("SELECT last_action FROM users WHERE whatsapp_number = :whatsapp_number"),
                                {"whatsapp_number": sender_id}
                            ).fetchone()
                            user_info = conn.execute(
                                text("SELECT property_id FROM users WHERE whatsapp_number = :whatsapp_number"),
                                {"whatsapp_number": sender_id}
                            ).fetchone()
                        if not user_info or not user_status:
                            send_whatsapp_message(sender_id, "⚠️ User not found. Please register.")
                            continue
                        property = user_info["property_id"]
                        if user_status["last_action"] == "awaiting_category":
                            handle_category_selection(sender_id, message_text)
                            continue
                        if user_status["last_action"] == "awaiting_issue_description":
                            handle_ticket_creation(sender_id, message_text, property)
                            continue
                        if message_text.lower() in ["hi", "hello", "help", "menu"]:
                            send_whatsapp_buttons(sender_id)
                            logging.info(f"Worked: Sent menu to {sender_id}")
                            continue

