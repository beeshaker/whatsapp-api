import os
import json
import time
import requests
import mysql.connector
import logging
import threading
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from conn1 import get_db_connection1, save_ticket_media, insert_ticket_and_get_id
from sqlalchemy.sql import text
from threading import Timer
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import re

# Initialize Flask app and executor
app = Flask(__name__)
executor = ThreadPoolExecutor(max_workers=10)
logging.basicConfig(level=logging.INFO)

# Load environment variables
load_dotenv()
WHATSAPP_ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
WHATSAPP_PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Threading locks
media_buffer_lock = threading.Lock()
user_timers_lock = threading.Lock()
terms_pending_lock = threading.Lock()

# In-memory storage
processed_message_ids = set()
last_messages = {}  # { sender_id: (message_text, timestamp) }
media_buffer = {}  # { sender_id: [{ media_type, media_path, caption, timestamp }] }
upload_state = {}  # { sender_id: { timer, last_upload_time, media_count } }
terms_pending_users = {}  # sender_id: timestamp
temp_opt_in_data = {}
# Tracks retry attempts and timer objects per user
accept_retry_state = {}  # { sender_id: { 'attempt': int, 'timer': Timer } }
accept_lock = threading.Lock()







# In-memory storage to track processed messages
user_timers = {}
upload_prompt_timers = {}  # key: sender_id, value: Timer

# Media buffer TTL (15 minutes)
MEDIA_TTL_SECONDS = 900


# Function to connect to MySQL and execute queries
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
    
    
def send_category_prompt(to):
    """Asks the user to select a category for the ticket."""
    message = "Please select a category:\n1️⃣ Accounts\n2️⃣ Maintenance\n3️⃣ Security\n4️⃣ Other\n\nReply with the number."
    executor.submit(send_whatsapp_message, to, message)


    # Safely record the timestamp
    with user_timers_lock:
        user_timers[to] = datetime.now()

    threading.Thread(target=reset_category_selection, args=(to,), daemon=True).start()


    
def reset_category_selection(to: str):
    time.sleep(300)  # 5 minutes
    with user_timers_lock:
        last_attempt_time = user_timers.get(to)
        if not last_attempt_time:
            return
        elapsed_time = (datetime.now() - last_attempt_time).total_seconds()
        if elapsed_time < 300:
            return
        del user_timers[to]

    user_info = query_database("SELECT last_action FROM users WHERE whatsapp_number = %s", (to,))
    if user_info and user_info[0]["last_action"] != "awaiting_category":
        logging.info(f"Skipping reset for {to}: last_action={user_info[0]['last_action']}")
        return

    logging.info(f"⏳ Resetting category selection for {to} due to timeout.")
    query_database("UPDATE users SET last_action = NULL WHERE whatsapp_number = %s", (to,), commit=True)
    send_whatsapp_message(to, "⏳ Your category selection request has expired. Please start again by selecting '📝 Create Ticket'.")
    
def send_terms_prompt(sender_id):
    terms_url = os.getenv("TERMS_URL", "https://digiagekenya.com/apricot/TermsofService.html")
    privacy_url = os.getenv("PRIVACY_URL", "https://digiagekenya.com/apricot/policy.html")

    # Use a pre-approved WhatsApp template for initial contact
    template_name = "registration_welcome"  # Ensure this template is approved in WhatsApp Business API
    template_parameters = [terms_url, privacy_url]

    response = send_template_message(sender_id, template_name, template_parameters)
    if response.get("messages"):
        terms_pending_users[sender_id] = time.time()
        logging.info(f"Sent terms template to {sender_id}: {response}")
        logging.info(terms_pending_users)
    else:
        logging.error(f"Failed to send terms template to {sender_id}: {response}")
        # Optionally, notify admin or retry
        


@app.route('/opt_in_user', methods=['POST'])
def opt_in_user_route():
    if request.headers.get("X-API-KEY") != os.getenv("INTERNAL_API_KEY"):
        return jsonify({"error": "Unauthorized"}), 403

    data = request.json
    name = data.get("name")
    whatsapp_number = data.get("whatsapp_number")
    property_id = data.get("property_id")
    unit_number = data.get("unit_number")

    if not all([name, whatsapp_number, property_id, unit_number]):
        logging.error("Missing fields in opt-in request.")
        return jsonify({"error": "Missing fields"}), 400

    logging.info(f"Storing opt-in data for {whatsapp_number}: {name}, {property_id}, {unit_number}")

    # ✅ Add both temp data and pending terms state
    temp_opt_in_data[whatsapp_number] = {
        "name": name,
        "property_id": property_id,
        "unit_number": unit_number
    }
    terms_pending_users[whatsapp_number] = time.time()

    send_terms_prompt(whatsapp_number)
    return jsonify({"status": "terms_sent"}), 200






def get_category_name(category_number):
    categories = {
        "1": "Accounts",
        "2": "Maintenance",
        "3": "Security",
        "4": "Other"  # ✅ Added new option
    }
    return categories.get(category_number, None)


# Prevent duplicate message processing
def is_message_processed(message_id):
    """Check if a message ID has already been processed."""
    if message_id in processed_message_ids:
        return True
    query = "SELECT id FROM processed_messages WHERE id = %s"
    result = query_database(query, (message_id,))
    return bool(result)

def mark_message_as_processed(message_id):
    """Mark a message as processed (in-memory & database)."""
    processed_message_ids.add(message_id)  # ✅ Immediate in-memory tracking
    query = "INSERT IGNORE INTO processed_messages (id) VALUES (%s)"
    query_database(query, (message_id,), commit=True)

def should_process_message(sender_id, message_text):
    """Check if the last message was identical within 3 seconds."""
    global last_messages
    current_time = time.time()

    if sender_id in last_messages:
        last_text, last_time = last_messages[sender_id]
        
        # Ignore duplicate messages within 3 seconds
        if last_text == message_text and (current_time - last_time) < 3:
            logging.info(f"⚠️ Ignoring duplicate message from {sender_id} within 3 seconds.")
            return False

    # ✅ Store this message as the last message
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
    return user_check is not None or admin_check is not None
    
    



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

# Send a WhatsApp message
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
    """Fetches and sends open tickets for the client via WhatsApp."""
    message = ""
    # Fetch open tickets for the given WhatsApp number
    query = """
        SELECT id, LEFT(issue_description, 50) AS short_description, updated_at as last_update
        FROM tickets 
        WHERE user_id = (SELECT id FROM users WHERE whatsapp_number = %s) 
        AND status = 'Open'
    """
    tickets = query_database(query, (to,))

    # If no open tickets found
    if not tickets:
        message = "You have no open tickets at the moment."
        
    else:
        message = "Your open tickets:\n"
        for ticket in tickets:
            message += f"Ticket ID: {ticket['id']}\nDescription: {ticket['short_description']}\nLast Update on: {ticket['last_update']}\n\n"
    
    executor.submit(send_whatsapp_message, to, message)
 
    


# Webhook route to handle incoming messages
@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        verify_token = "12345"  # Make sure this matches your Meta settings
        if request.args.get("hub.verify_token") == verify_token:
            return request.args.get("hub.challenge"), 200
        return "Invalid verification token", 403

    # POST: Handle webhook events
    data = request.get_json()
    logging.info(f"Incoming webhook data: {json.dumps(data, indent=2)}")

    # ✅ Process inline to prevent duplicate processing
    executor.submit(process_webhook(data))

    return jsonify({"status": "received"}), 200


@app.route("/send_message", methods=["POST"])
def external_send_message():
    data = request.get_json()
    api_key = request.headers.get("X-API-KEY")  # Optional security

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
        print("❌ Error sending WhatsApp message:", e)
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



def purge_expired_media():
    now = time.time()
    with media_buffer_lock:
        for wa_id, media_list in list(media_buffer.items()):
            fresh_media = [entry for entry in media_list if now - entry["timestamp"] < MEDIA_TTL_SECONDS]
            if fresh_media:
                media_buffer[wa_id] = fresh_media
            else:
                del media_buffer[wa_id]
                send_whatsapp_message(wa_id, "⏳ Your uploaded files have expired. Please start again.")
                
                
def purge_expired_items():
    now = time.time()

    # Purge expired media uploads (5-minute TTL)
    with media_buffer_lock:
        for wa_id, media_list in list(media_buffer.items()):
            fresh_media = [entry for entry in media_list if now - entry["timestamp"] < 300]
            if fresh_media:
                media_buffer[wa_id] = fresh_media
            else:
                logging.info(f"Purging expired media for {wa_id}")
                del media_buffer[wa_id]
                send_whatsapp_message(wa_id, "⏳ Your uploaded files have expired. Please start again.")

    # Purge expired terms prompts (30-minute expiry)
    with terms_pending_lock:
        expired = [uid for uid, ts in terms_pending_users.items() if now - ts > 1800]
        for uid in expired:
            logging.info(f"Purging expired terms prompt for {uid}")
            del terms_pending_users[uid]
            if uid in temp_opt_in_data:
                del temp_opt_in_data[uid]
            send_whatsapp_message(uid, "⏳ Your session to accept Terms expired. Please try again.")


        
        
def flush_user_media_after_ticket(sender_id, ticket_id, delay=30):
    """Flushes any media uploaded shortly after ticket creation."""
    time.sleep(delay)

    with media_buffer_lock:
        media_list = media_buffer.pop(sender_id, [])

    for entry in media_list:
        media = entry["media"]
        save_ticket_media(ticket_id, media["media_type"], media["media_path"])
        logging.info(f"📁 (Post-ticket) Linked {media['media_type']} to ticket #{ticket_id}")

        
        


def send_caption_confirmation(phone_number, captions, access_token, phone_number_id):
    url = f"https://graph.facebook.com/v22.0/{phone_number_id}/messages"
    headers = {
        "Authorization": f"Bearer {access_token}",
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
                "text": f"📝 Captions extracted:\n\n{caption_text}\n\nDoes this look correct?"
            },
            "action": {
                "buttons": [
                    {
                        "type": "reply",
                        "reply": {
                            "id": "caption_confirm_yes",
                            "title": "✅ Yes"
                        }
                    },
                    {
                        "type": "reply",
                        "reply": {
                            "id": "caption_confirm_no",
                            "title": "❌ No"
                        }
                    }
                ]
            }
        }
    }

    response = requests.post(url, headers=headers, json=payload)
    return response.json()


def is_valid_message(sender_id, message_id, message_text):
    # Allow messages from users pending terms acceptance
    if sender_id in terms_pending_users:
        logging.info(f"Pending terms: {terms_pending_users}")
        logging.info(f"Allowing message from pending user {sender_id}")
        return True


    # Skip duplicate/rapid messages
    if is_message_processed(message_id) or not should_process_message(sender_id, message_text):
        logging.info(f"Message {message_id} is duplicate or rapid for {sender_id}")
        return False
    
    if not is_registered_user(sender_id):
        logging.info(f"Blocked unregistered user: {sender_id}")
        send_whatsapp_message(sender_id, "You are not registered. Please register first.")
        return False


    mark_message_as_processed(message_id)
    logging.info(f"Message {message_id} marked as processed for {sender_id}")
    return True


def process_media_upload(media_id, filename, sender_id, media_type, message_text):
    user_status = query_database("SELECT last_action, temp_category FROM users WHERE whatsapp_number = %s", (sender_id,))
    
    if not user_status or user_status[0]["last_action"] != "awaiting_issue_description":
        send_whatsapp_message(sender_id, "⚠️ Please select a category first. Reply with 1️⃣, 2️⃣, 3️⃣, or 4️⃣.")
        send_category_prompt(sender_id)
        return

    download_result = download_media(media_id, filename)
    if "success" in download_result:
        with media_buffer_lock:
            if sender_id not in media_buffer:
                media_buffer[sender_id] = []
            media_buffer[sender_id].append({
                "media_type": media_type,
                "media_path": download_result["path"],
                "caption": None,
                "timestamp": time.time(),
            })
            media_count = len(media_buffer[sender_id])

        with user_timers_lock:
            upload_state[sender_id] = upload_state.get(sender_id, {"media_count": 0, "timer": None})
            upload_state[sender_id]["media_count"] = media_count
            upload_state[sender_id]["last_upload_time"] = time.time()

        # ✅ After first upload, ask user to describe the issue (don't create ticket yet)
        if media_count == 1:
            query_database(
                "UPDATE users SET last_action = 'awaiting_issue_description' WHERE whatsapp_number = %s",
                (sender_id,), commit=True
            )
            send_whatsapp_message(sender_id, "✅ File received! Please describe your issue to continue.")

            # Optional reminder after 2 minutes
            def prompt_for_description_reminder(sid):
                time.sleep(120)
                user_status = query_database("SELECT last_action FROM users WHERE whatsapp_number = %s", (sid,))
                if user_status and user_status[0]["last_action"] == "awaiting_issue_description":
                    send_whatsapp_message(sid, "⏳ Please describe your issue so we can proceed with the ticket.")
            threading.Thread(target=prompt_for_description_reminder, args=(sender_id,), daemon=True).start()

        else:
            send_whatsapp_message(sender_id, f"✅ {media_type.capitalize()} received! You've uploaded {media_count} file(s).")
            manage_upload_timer(sender_id)

    else:
        send_whatsapp_message(sender_id, f"❌ Failed to upload {media_type}. Please try again.")
        logging.error(f"Failed to save {media_type}: {download_result}")


def handle_button_reply(message, sender_id):
    button_id = message["interactive"]["button_reply"]["id"]
    
    logging.info(f"🔘 Button clicked: {button_id} by {sender_id}")

    if button_id in ["upload_done", "upload_not_done", "caption_confirm_yes", "caption_confirm_no"]:
        with user_timers_lock:
            if sender_id in upload_state and upload_state[sender_id]["timer"]:
                upload_state[sender_id]["timer"].cancel()
                upload_state[sender_id]["timer"] = None

    elif button_id == "accept_terms":
        try:
            user = temp_opt_in_data.get(sender_id)
            if user:
                executor.submit(handle_accept, sender_id)
            else:
                logging.warning(f"⚠️ No temp data found for {sender_id} in temp_opt_in_data.")
                executor.submit(send_whatsapp_message, sender_id, "⚠️ Something went wrong. Please try again.")
        except Exception as e:
            logging.error(f"❌ Exception during user registration for {sender_id}: {e}", exc_info=True)
            executor.submit(send_whatsapp_message, sender_id, "❌ An error occurred during registration. Please contact support.")

    elif button_id == "reject_terms":
        if sender_id in terms_pending_users:
            del terms_pending_users[sender_id]
        if sender_id in temp_opt_in_data:
            del temp_opt_in_data[sender_id]
        executor.submit(send_whatsapp_message, sender_id, "❌ You must accept the Terms to use this service.")

    elif button_id == "create_ticket":
        query_database("UPDATE users SET last_action = 'awaiting_category' WHERE whatsapp_number = %s", (sender_id,), commit=True)
        executor.submit(send_category_prompt, sender_id)

    elif button_id == "check_ticket":
        executor.submit(send_whatsapp_tickets, sender_id)

    elif button_id == "upload_done":
        user_data = query_database("SELECT temp_category FROM users WHERE whatsapp_number = %s", (sender_id,))
        if user_data and user_data[0]["temp_category"]:
            query_database("UPDATE users SET last_action = 'awaiting_issue_description' WHERE whatsapp_number = %s", (sender_id,), commit=True)
            executor.submit(send_whatsapp_message, sender_id, "✏️ Great! Please describe your issue.")
        else:
            query_database("UPDATE users SET last_action = 'awaiting_category' WHERE whatsapp_number = %s", (sender_id,), commit=True)
            executor.submit(send_category_prompt, sender_id)

    elif button_id == "upload_not_done":
        executor.submit(send_whatsapp_message, sender_id, "👍 Okay, send more files when you're ready.")

    elif button_id == "caption_confirm_yes":
        user_data = query_database("SELECT temp_category FROM users WHERE whatsapp_number = %s", (sender_id,))
        if user_data and user_data[0]["temp_category"]:
            query_database("UPDATE users SET last_action = 'awaiting_issue_description' WHERE whatsapp_number = %s", (sender_id,), commit=True)
            with media_buffer_lock:
                media_count = len(media_buffer.get(sender_id, []))
            executor.submit(send_whatsapp_message, sender_id, f"✅ Captions confirmed! You've uploaded {media_count} file(s). Send more or reply /done to proceed.")
            manage_upload_timer(sender_id)
        else:
            query_database("UPDATE users SET last_action = 'awaiting_category' WHERE whatsapp_number = %s", (sender_id,), commit=True)
            executor.submit(send_category_prompt, sender_id)

    elif button_id == "caption_confirm_no":
        executor.submit(send_whatsapp_message, sender_id, "📝 Please upload the files again with corrected captions.")
        with media_buffer_lock:
            if sender_id in media_buffer:
                del media_buffer[sender_id]
        with user_timers_lock:
            if sender_id in upload_state:
                upload_state[sender_id]["media_count"] = 0
                upload_state[sender_id]["last_upload_time"] = 0

        
        

def handle_media_upload(message, sender_id, message_text):
    media_type = message.get("type")
    if media_type in ["document", "image", "video"]:
        media_id = message[media_type]["id"]
        base_filename = message[media_type].get("filename", f"{media_id}.{media_type[:3]}")
        name, ext = os.path.splitext(base_filename)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{name}_{timestamp}{ext}"
        executor.submit(process_media_upload, media_id, filename, sender_id, media_type, None)
        return True
    return False

def handle_list_uploads(sender_id):
    with media_buffer_lock:
        media_list = media_buffer.get(sender_id, [])
    if not media_list:
        send_whatsapp_message(sender_id, "📎 You have no pending uploads.")
        return
    message = f"📎 Your pending uploads ({len(media_list)}):\n"
    for i, entry in enumerate(media_list, 1):
        caption = entry.get("caption", "No caption")
        message += f"{i}. {entry['media_type'].capitalize()}: {caption[:30]}...\n"
    message += "\nReply /remove_upload <number> to delete a specific file or /clear_attachments to clear all."
    send_whatsapp_message(sender_id, message)


def handle_remove_upload(sender_id, upload_index):
    try:
        index = int(upload_index) - 1
        with media_buffer_lock:
            if sender_id in media_buffer and 0 <= index < len(media_buffer[sender_id]):
                removed = media_buffer[sender_id].pop(index)
                executor.submit(send_whatsapp_message, sender_id, f"🗑️ Removed {removed['media_type'].capitalize()} from your uploads.")
                if not media_buffer[sender_id]:
                    del media_buffer[sender_id]
            else:
                executor.submit(send_whatsapp_message, sender_id, "⚠️ Invalid upload number.")
    except ValueError:
        executor.submit(send_whatsapp_message, sender_id, "⚠️ Please provide a valid upload number (e.g., /remove_upload 1).")
        
        


        
        
def send_done_upload_prompt(sender_id):
    with media_buffer_lock:
        media_count = len(media_buffer.get(sender_id, []))
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


        
        
def handle_auto_submit_ticket(sender_id):
    with media_buffer_lock:
        non_empty_captions = [
            entry["media"].get("caption")
            for entry in media_buffer.get(sender_id, [])
            if entry["media"].get("caption")
        ]

    if not non_empty_captions:
        send_whatsapp_message(sender_id, "❌ No valid captions found. Please describe your issue.")
        return

    # Set default category and mark awaiting description
    query_database(
        "UPDATE users SET last_action = 'awaiting_issue_description', temp_category = %s WHERE whatsapp_number = %s",
        ("Other", sender_id),
        commit=True
    )

    auto_description = "AUTO-FILLED ISSUE DESCRIPTION:\n\n" + "\n\n".join(non_empty_captions)

    user_info = query_database("SELECT id, property_id FROM users WHERE whatsapp_number = %s", (sender_id,))
    if not user_info:
        send_whatsapp_message(sender_id, "❌ Error creating ticket. Please try again.")
        return

    user_id = user_info[0]["id"]
    property = user_info[0]["property_id"]

    create_ticket_with_media(sender_id, user_id, "Other", property, auto_description)

    with user_timers_lock:
        if sender_id in user_timers:
            del user_timers[sender_id]


    
    
    
def create_ticket_with_media(sender_id, user_id, category, property, description):
    ticket_check = query_database(
        "SELECT created_at FROM tickets WHERE user_id = %s ORDER BY created_at DESC LIMIT 1",
        (user_id,)
    )
    if ticket_check and (datetime.now() - ticket_check[0]["created_at"]).total_seconds() < 60:
        executor.submit(send_whatsapp_message, sender_id, "🛑 You've recently created a ticket. Please wait a minute before creating another.")
        return

    ticket_id = insert_ticket_and_get_id(user_id, description, category, property)

    with media_buffer_lock:
        media_list = media_buffer.get(sender_id, []).copy()
        if not media_list:
            logging.warning(f"No media found for sender {sender_id} during ticket creation.")
        else:
            for entry in media_list:
                save_ticket_media(ticket_id, entry["media_type"], entry["media_path"])
                logging.info(f"📁 Linked {entry['media_type']} to ticket #{ticket_id}")
            del media_buffer[sender_id]

    query_database(
        "UPDATE users SET last_action = NULL, temp_category = NULL WHERE whatsapp_number = %s",
        (sender_id,), commit=True
    )

    executor.submit(send_whatsapp_message, sender_id,
        f"✅ Your ticket #{ticket_id} has been created under *{category}* with {len(media_list)} attachment(s). Our team will get back to you soon!")

    with user_timers_lock:
        if sender_id in upload_state:
            if upload_state[sender_id]["timer"]:
                upload_state[sender_id]["timer"].cancel()
            del upload_state[sender_id]




def handle_done_command(sender_id):
    user_data = query_database("SELECT temp_category FROM users WHERE whatsapp_number = %s", (sender_id,))

    with media_buffer_lock:
        attachments = media_buffer.get(sender_id, [])

    logging.info(f"Processing /done for {sender_id}: {len(attachments)} attachments found")

    # Cancel any existing upload timer
    with user_timers_lock:
        if sender_id in upload_state and upload_state[sender_id]["timer"]:
            upload_state[sender_id]["timer"].cancel()
            upload_state[sender_id]["timer"] = None
            logging.info(f"Cancelled upload timer for {sender_id}")

    # Handle case with no attachments
    if not attachments:
        executor.submit(send_whatsapp_message, sender_id, "📎 You have not uploaded any attachments yet. You can still proceed by describing the issue, or upload files now.")
    else:
        executor.submit(send_whatsapp_message, sender_id, f"📎 You've uploaded {len(attachments)} file(s). Please describe your issue to proceed.")

    # Check category and update user state
    if user_data and user_data[0]["temp_category"]:
        query_database("UPDATE users SET last_action = 'awaiting_issue_description' WHERE whatsapp_number = %s", (sender_id,), commit=True)
        if not attachments:
            executor.submit(send_whatsapp_message, sender_id, "✏️ Great! Please describe your issue.")
    else:
        query_database("UPDATE users SET last_action = 'awaiting_category' WHERE whatsapp_number = %s", (sender_id,), commit=True)
        executor.submit(send_whatsapp_message, sender_id, "⚠️ Please select a category first.")
        executor.submit(send_category_prompt, sender_id)

    
def handle_clear_attachments(sender_id):
    with media_buffer_lock:
        if sender_id in media_buffer:
            count = len(media_buffer[sender_id])
            del media_buffer[sender_id]
            executor.submit(send_whatsapp_message, sender_id, f"🗑️ Cleared {count} pending attachment(s).")
        else:
            executor.submit(send_whatsapp_message, sender_id, "📎 You have no pending attachments.")
        
        
def handle_category_selection(sender_id: str, message_text: str):
    category_name = get_category_name(message_text)
    if category_name:
        query_database(
            "UPDATE users SET last_action = 'awaiting_issue_description', temp_category = %s WHERE whatsapp_number = %s",
            (category_name, sender_id), commit=True
        )
        with user_timers_lock:
            if sender_id in user_timers:
                del user_timers[sender_id]
                logging.info(f"Cancelled category selection timer for {sender_id}")
        send_whatsapp_message(sender_id, "Please describe your issue or upload a supporting file.")
    else:
        send_whatsapp_message(sender_id, "⚠️ Invalid selection. Please reply with 1️⃣, 2️⃣, 3️⃣, or 4️⃣.")
        send_category_prompt(sender_id)
        
        

def handle_ticket_creation(sender_id, message_text, property_id):
    user_info = query_database("SELECT id, temp_category FROM users WHERE whatsapp_number = %s", (sender_id,))
    if not user_info:
        send_whatsapp_message(sender_id, "❌ Error creating ticket. Please try again.")
        return

    user_id = user_info[0]["id"]
    category = user_info[0]["temp_category"]

    ticket_check = query_database("""
        SELECT created_at FROM tickets
        WHERE user_id = %s
        ORDER BY created_at DESC
        LIMIT 1
    """, (user_id,))

    if ticket_check:
        last_created = ticket_check[0]["created_at"]
        if (datetime.now() - last_created).total_seconds() < 300:  # 5-minute cooldown
            logging.info(f"🛑 Ticket already created recently for user {sender_id}. Skipping.")
            send_whatsapp_message(sender_id, "🛑 You recently created a ticket. Please wait before creating another.")
            return

    description = message_text.strip()
    if not description:
        with media_buffer_lock:
            media_list = media_buffer.get(sender_id, [])
            captions = [entry["caption"] for entry in media_list if entry["caption"]]
            if captions:
                description = "AUTO-FILLED ISSUE DESCRIPTION:\n\n" + "\n\n".join(captions)
            else:
                send_whatsapp_message(sender_id, "✏️ Please describe your issue.")
                return

    ticket_id = insert_ticket_and_get_id(user_id, description, category, property_id)

    with media_buffer_lock:
        media_list = media_buffer.get(sender_id, []).copy()
        now = time.time()
        recent_media = [entry for entry in media_list if now - entry["timestamp"] < 300]  # 5-minute window
        if not recent_media:
            logging.warning(f"No recent media found for sender {sender_id} during ticket creation.")
        for entry in recent_media:
            save_ticket_media(ticket_id, entry["media_type"], entry["media_path"])
            logging.info(f"📁 Linked {entry['media_type']} to ticket #{ticket_id}")
        media_buffer[sender_id] = [entry for entry in media_list if entry not in recent_media]
        if not media_buffer[sender_id]:
            del media_buffer[sender_id]

    query_database(
        "UPDATE users SET last_action = NULL, temp_category = NULL WHERE whatsapp_number = %s",
        (sender_id,), commit=True
    )

    send_whatsapp_message(sender_id, f"✅ Your ticket #{ticket_id} has been created under *{category}* with {len(recent_media)} attachment(s). Our team will get back to you soon!")

    with user_timers_lock:
        if sender_id in user_timers:
            del user_timers[sender_id]
            
            
def register_user(sender_id, user_info):
    try:
        name = user_info.get("name")
        property_id = user_info.get("property_id")
        unit_number = user_info.get("unit_number")
        logging.info(f"Attempting to register user {sender_id} with info: {user_info}")
        with get_db_connection1().connect() as conn:
            # Check if user is already registered
            existing_user = conn.execute(
                text("SELECT id FROM users WHERE whatsapp_number = :phone"),
                {"phone": sender_id}
            ).fetchone()
            if existing_user:
                logging.info(f"User {sender_id} already registered, skipping insert.")
                with terms_pending_lock:
                    if sender_id in terms_pending_users:
                        del terms_pending_users[sender_id]
                return
            # Insert or update user
            query = text("""
                INSERT INTO users (whatsapp_number, name, property_id, unit_number, last_action, created_at)
                VALUES (:phone, :name, :property_id, :unit_number, 'main_menu', NOW())
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    property_id = VALUES(property_id),
                    unit_number = VALUES(unit_number),
                    last_action = 'main_menu'
            """)
            conn.execute(query, {
                "phone": sender_id,
                "name": name,
                "property_id": property_id,
                "unit_number": unit_number
            })
            conn.commit()
        with terms_pending_lock:
            if sender_id in terms_pending_users:
                del terms_pending_users[sender_id]
        logging.info(f"✅ Registered user {sender_id} with name={name}, property_id={property_id}, unit_number={unit_number}")
    except Exception as e:
        logging.error(f"❌ Failed to register user {sender_id}: {e}", exc_info=True)
        send_whatsapp_message(sender_id, f"⚠️ Registration failed. Please contact support. error code: {e}")


def handle_accept(sender_id):
    with accept_lock:
        logging.info(f"Processing accept for {sender_id}")
        # Log the current state of temp_opt_in_data
        logging.info(f"Current temp_opt_in_data: {temp_opt_in_data}")
        # Send immediate "please wait" message
        send_whatsapp_message(sender_id, "⏳ We're getting things sorted, this may take a minute or two...")

        # Cancel any existing retry timer
        if sender_id in accept_retry_state:
            retry_info = accept_retry_state.pop(sender_id)
            if retry_info and retry_info["timer"]:
                retry_info["timer"].cancel()
                logging.info(f"🛑 Cancelled existing retry timer for {sender_id} due to new Accept input.")

        # Check if user is already registered
        if is_registered_user(sender_id):
            logging.info(f"User {sender_id} already registered, skipping accept.")
            with terms_pending_lock:
                terms_pending_users.pop(sender_id, None)
            send_whatsapp_message(sender_id, "🎉 You are already registered!")
            return

        def try_register(attempt):
            with accept_lock:
                logging.info(f"🔁 Attempt {attempt} to register {sender_id}")
                # Log temp_opt_in_data during retry attempt
                logging.info(f"temp_opt_in_data during attempt {attempt}: {temp_opt_in_data}")
                if is_registered_user(sender_id):
                    logging.info(f"User {sender_id} already registered during retry, skipping.")
                    with terms_pending_lock:
                        terms_pending_users.pop(sender_id, None)
                    accept_retry_state.pop(sender_id, None)
                    send_whatsapp_message(sender_id, "🎉 You are already registered!")
                    return

                if sender_id in temp_opt_in_data:
                    user_info = temp_opt_in_data.pop(sender_id)
                    try:
                        register_user(sender_id, user_info)
                        accept_retry_state.pop(sender_id, None)
                        send_whatsapp_message(sender_id, "🎉 You've been registered successfully!")
                    except Exception as e:
                        logging.error(f"❌ Registration failed on attempt {attempt} for {sender_id}: {e}")
                        if attempt < 3:
                            timer = Timer(15, try_register, args=[attempt + 1])
                            accept_retry_state[sender_id] = {"attempt": attempt + 1, "timer": timer}
                            timer.start()
                        else:
                            accept_retry_state.pop(sender_id, None)
                            send_whatsapp_message(sender_id, "⚠️ Registration failed after multiple attempts. Please try again or contact support.")
                    return

                if attempt < 3:
                    timer = Timer(15, try_register, args=[attempt + 1])
                    accept_retry_state[sender_id] = {"attempt": attempt + 1, "timer": timer}
                    timer.start()
                else:
                    accept_retry_state.pop(sender_id, None)
                    with terms_pending_lock:
                        terms_pending_users.pop(sender_id, None)
                    send_whatsapp_message(sender_id, "⚠️ We couldn't find your registration details. Please try again or contact support.")

        # Start the first attempt after a 15-second delay
        timer = Timer(15, try_register, args=[1])
        accept_retry_state[sender_id] = {"attempt": 1, "timer": timer}
        timer.start()


def extract_message_info(message):
    """
    Extracts message ID, sender ID, and message text (or caption) from a WhatsApp message.

    Args:
        message (dict): The incoming WhatsApp message object.

    Returns:
        tuple: (message_id, sender_id, message_text)
    """
    message_id = message.get("id")
    sender_id = message["from"]
    message_text = ""

    if "text" in message:
        # Text message
        message_text = message.get("text", {}).get("body", "").strip()
    elif message.get("type") in ["image", "video", "document"]:
        # Media message with optional caption
        media_type = message["type"]
        message_text = message[media_type].get("caption", "").strip()

    return message_id, sender_id, message_text


def manage_upload_timer(sender_id):
    with user_timers_lock:
        if sender_id in upload_state and upload_state[sender_id]["timer"]:
            upload_state[sender_id]["timer"].cancel()
        def send_prompt():
            with user_timers_lock:
                if sender_id in upload_state and upload_state[sender_id]["media_count"] > 1:
                    send_done_upload_prompt(sender_id)
                    upload_state[sender_id]["timer"] = None
        with media_buffer_lock:
            media_count = len(media_buffer.get(sender_id, []))
        if media_count > 1:
            t = Timer(10, send_prompt)
            upload_state[sender_id] = upload_state.get(sender_id, {"media_count": media_count, "last_upload_time": time.time()})
            upload_state[sender_id]["timer"] = t
            t.start()




def process_webhook(data):
    """Processes incoming WhatsApp messages from Meta Webhook."""
    purge_expired_items()
    logging.info(f"Processing webhook data:\n{json.dumps(data, indent=2)}")
    if "entry" not in data:
        logging.warning("No 'entry' found in webhook data.")
        return
    for entry in data["entry"]:
        for change in entry.get("changes", []):
            value = change.get("value", {})
            if "statuses" in value:
                logging.info(f"Ignoring status update for message ID {value['statuses'][0]['id']}")
                continue
            for message in value.get("messages", []):
                message_id, sender_id, message_text = extract_message_info(message)
                logging.info(f"Message from {sender_id} ({message_id}): {message_text}")
                logging.debug(f"terms_pending_users: {terms_pending_users}, temp_opt_in_data: {temp_opt_in_data}")
                # Handle button
                if "interactive" in message and "button_reply" in message["interactive"]:
                    handle_button_reply(message, sender_id)
                    continue
                normalized = message_text.strip().lower()
                # Handle TOS acceptance
                if normalized in ["accept", "reject"]:
                    with terms_pending_lock:
                        if normalized == "reject":
                            temp_opt_in_data.pop(sender_id, None)
                            terms_pending_users.pop(sender_id, None)
                            executor.submit(send_whatsapp_message, sender_id, "❌ You must accept the Terms to use this service.")
                            continue
                        executor.submit(handle_accept, sender_id)
                    continue
                # Validate message
                if not is_valid_message(sender_id, message_id, message_text):
                    logging.info(f"Invalid/duplicate message for {sender_id}")
                    continue
                # Handle media
                media_type = message.get("type")
                if media_type in ["document", "image", "video"]:
                    media_id = message[media_type]["id"]
                    base_filename = message[media_type].get("filename", f"{media_id}.{media_type[:3]}")
                    name, ext = os.path.splitext(base_filename)
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"{name}_{timestamp}{ext}"
                    download_result = download_media(media_id, filename)
                    if "success" in download_result:
                        with media_buffer_lock:
                            media_buffer.setdefault(sender_id, []).append({
                                "media_type": media_type,
                                "media_path": download_result["path"],
                                "caption": message_text.strip() if message_text else None,
                                "timestamp": time.time()
                            })
                        media_count = len(media_buffer[sender_id])
                        user_status = query_database(
                            "SELECT last_action, temp_category FROM users WHERE whatsapp_number = %s",
                            (sender_id,)
                        )
                        if not user_status or user_status[0]["last_action"] != "awaiting_issue_description":
                            if user_status and not user_status[0]["temp_category"]:
                                send_whatsapp_message(sender_id, "✅ File received! Please select a category.")
                                send_category_prompt(sender_id)
                            else:
                                query_database(
                                    "UPDATE users SET last_action = 'awaiting_issue_description' WHERE whatsapp_number = %s",
                                    (sender_id,), commit=True
                                )
                                send_whatsapp_message(sender_id, f"✅ File received! You’ve uploaded {media_count} file(s). Describe your issue or reply /done.")
                            manage_upload_timer(sender_id)
                        else:
                            send_whatsapp_message(sender_id, f"✅ {media_type.title()} received! You’ve uploaded {media_count} file(s). Describe your issue or reply /done.")
                        continue
                    else:
                        logging.error(f"Media download failed: {download_result}")
                        send_whatsapp_message(sender_id, f"❌ Failed to upload {media_type}. Try again.")
                    continue
                # Command handling
                if normalized == "/clear_attachments":
                    handle_clear_attachments(sender_id)
                    continue
                if normalized == "/done":
                    handle_done_command(sender_id)
                    continue
                if normalized == "/list_uploads":
                    handle_list_uploads(sender_id)
                    continue
                if normalized.startswith("/remove_upload"):
                    parts = normalized.split()
                    if len(parts) == 2:
                        handle_remove_upload(sender_id, parts[1])
                    else:
                        send_whatsapp_message(sender_id, "⚠️ Provide upload number (e.g., /remove_upload 1)")
                    continue
                # Final fallback based on user status
                user_status = query_database("SELECT last_action, temp_category FROM users WHERE whatsapp_number = %s", (sender_id,))
                user_info = query_database("SELECT property_id FROM users WHERE whatsapp_number = %s", (sender_id,))
                if not user_status or not user_info:
                    send_whatsapp_message(sender_id, "⚠️ You are not registered. Please contact support.")
                    continue
                last_action = user_status[0]["last_action"]
                property_id = user_info[0]["property_id"]
                if last_action == "awaiting_category":
                    handle_category_selection(sender_id, message_text)
                elif last_action == "awaiting_issue_description":
                    handle_ticket_creation(sender_id, message_text, property_id)
                elif normalized in ["hi", "hello", "help", "menu"]:
                    send_whatsapp_buttons(sender_id)