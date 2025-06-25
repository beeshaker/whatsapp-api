import os
import json
import time
import requests
import mysql.connector
import logging
import threading
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from conn1 import get_db_connection1, save_ticket_media, insert_ticket_and_get_id, mark_user_accepted_via_temp_table
from sqlalchemy.sql import text
from threading import Timer
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import re
import logging
from logging.handlers import RotatingFileHandler
import os


#image uploading finally works  

# Create logs directory if not exists
os.makedirs("logs", exist_ok=True)

# Log file path
log_file = "logs/app.log"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=5),  # Rotate at 5MB, keep 5 files
        logging.StreamHandler()  # Also log to console
    ]
)

# Initialize Flask app and executor
app = Flask(__name__)
executor = ThreadPoolExecutor(max_workers=10)


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
    time.sleep(300)  # Wait 5 minutes

    user_info = query_database("SELECT last_action FROM users WHERE whatsapp_number = %s", (to,))
    if user_info and user_info[0]["last_action"] != "awaiting_category":
        logging.info(f"Skipping reset for {to}: last_action={user_info[0]['last_action']}")
        with user_timers_lock:
            user_timers.pop(to, None)
        return

    logging.info(f"⏳ Resetting category selection for {to} due to timeout.")
    query_database(
        "UPDATE users SET last_action = NULL, temp_category = NULL WHERE whatsapp_number = %s",
        (to,), commit=True
    )

    with user_timers_lock:
        user_timers.pop(to, None)

    send_whatsapp_message(to, "⏳ You took too long to choose a category. Please tap '📝 Create Ticket' to start again.")



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
    executor.submit(process_webhook, data)  # <- correct: submit the function + argument


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
    logging.info(f"Media buffer download media: {media_buffer}")
    meta_url = f"https://graph.facebook.com/v22.0/{media_id}"
    headers = {"Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}"}
    media_response = requests.get(meta_url, headers=headers)
    if media_response.status_code != 200:
        logging.info(f"Downloading media_id={media_id}, filename={filename}")
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


                
def purge_expired_items():
    now = time.time()

    # 🧹 Purge expired media
    with media_buffer_lock:
        for wa_id, media_list in list(media_buffer.items()):
            fresh_media = []
            for entry in media_list:
                age = now - entry["timestamp"]
                logging.info(f"🕒 Media age for {wa_id}: {age:.2f}s ({entry['media_type']})")
                if age < MEDIA_TTL_SECONDS:
                    fresh_media.append(entry)

            if fresh_media:
                media_buffer[wa_id] = fresh_media
                logging.info(f"✅ Retained {len(fresh_media)} media items for {wa_id}")
            else:
                del media_buffer[wa_id]
                logging.info(f"🗑️ All media expired for {wa_id}")
                send_whatsapp_message(wa_id, "⏳ Your uploaded files have expired. Please start again.")

    # 🧹 Purge expired terms
    with terms_pending_lock:
        expired = [uid for uid, ts in terms_pending_users.items() if now - ts > 1800]
        for uid in expired:
            logging.info(f"🗑️ Purging expired terms prompt for {uid}")
            del terms_pending_users[uid]
            if uid in temp_opt_in_data:
                del temp_opt_in_data[uid]
            send_whatsapp_message(uid, "⏳ Your session to accept Terms expired. Please try again.")




        
        


        
        


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
    """
    Handles media upload during ticket creation.
    Stores uploads in the temp_ticket_media table instead of in-memory buffer.
    Enforces correct flow: only accept media after category selection.
    """
    from conn1 import save_temp_media_to_db

    user_status = query_database("SELECT last_action FROM users WHERE whatsapp_number = %s", (sender_id,))
    if not user_status:
        logging.warning(f"❌ Unregistered user tried to upload media: {sender_id}")
        send_whatsapp_message(sender_id, "⚠️ You're not registered. Please register first.")
        return

    last_action = user_status[0]["last_action"]
    if last_action not in ["awaiting_category", "awaiting_issue_description"]:
        logging.info(f"⚠️ Invalid last_action '{last_action}' for {sender_id} — forcing category prompt.")
        query_database("UPDATE users SET last_action = 'awaiting_category' WHERE whatsapp_number = %s", (sender_id,), commit=True)
        send_whatsapp_message(sender_id, "⚠️ Please start by selecting a category first.")
        send_category_prompt(sender_id)
        return

    download_result = download_media(media_id, filename)
    if "success" not in download_result:
        logging.error(f"❌ Download failed for {sender_id}: {download_result}")
        send_whatsapp_message(sender_id, f"❌ Failed to upload {media_type}. Please try again.")
        return

    caption = message_text.strip() if message_text else "No Caption"
    save_temp_media_to_db(sender_id, media_type, download_result["path"], caption)

    # Count uploads
    count_result = query_database("SELECT COUNT(*) AS count FROM temp_ticket_media WHERE sender_id = %s", (sender_id,))
    media_count = count_result[0]["count"] if count_result else 0
    logging.info(f"📦 Media added for {sender_id}. Total in DB: {media_count}")

    # Set state to awaiting description
    query_database("UPDATE users SET last_action = 'awaiting_issue_description' WHERE whatsapp_number = %s", (sender_id,), commit=True)

    # Acknowledge upload
    send_whatsapp_message(sender_id, f"✅ {media_type.capitalize()} received! You've uploaded {media_count} file(s). Use /done when ready.")

    # Start reminder only once per user
    with user_timers_lock:
        state = upload_state.get(sender_id, {"media_count": 0, "last_upload_time": 0, "timer": None})
        state["media_count"] = media_count
        state["last_upload_time"] = time.time()

        if state["timer"] is None:
            def prompt_reminder():
                time.sleep(120)
                status = query_database("SELECT last_action FROM users WHERE whatsapp_number = %s", (sender_id,))
                if status and status[0]["last_action"] == "awaiting_issue_description":
                    send_whatsapp_message(sender_id, "⏳ Reminder: please describe your issue or use /done.")

            reminder_thread = threading.Thread(target=prompt_reminder, daemon=True)
            reminder_thread.start()
            state["timer"] = reminder_thread

        upload_state[sender_id] = state



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
                #del media_buffer[sender_id]
                pass
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
        # Submit and wait for completion
        future = executor.submit(process_media_upload, media_id, filename, sender_id, media_type, None)
        future.result()  # Synchronize
        return True
    return False



def handle_list_uploads(sender_id):
    media_list = query_database("""
        SELECT media_type, caption FROM temp_ticket_media
        WHERE sender_id = %s
        ORDER BY uploaded_at
    """, (sender_id,))
    
    if not media_list:
        send_whatsapp_message(sender_id, "📎 You have no pending uploads.")
        return

    message = f"📎 Your pending uploads ({len(media_list)}):\n"
    for i, entry in enumerate(media_list, 1):
        caption = entry["caption"] or "No caption"
        message += f"{i}. {entry['media_type'].capitalize()}: {caption[:30]}...\n"

    message += "\nReply /remove_upload <number> to delete a specific file or /clear_attachments to clear all."
    send_whatsapp_message(sender_id, message)



def handle_remove_upload(sender_id, upload_index):
    try:
        index = int(upload_index) - 1
        media_list = query_database("""
            SELECT id, media_type FROM temp_ticket_media
            WHERE sender_id = %s
            ORDER BY uploaded_at
        """, (sender_id,))
        
        if 0 <= index < len(media_list):
            removed = media_list[index]
            query_database("DELETE FROM temp_ticket_media WHERE id = %s", (removed["id"],), commit=True)
            executor.submit(send_whatsapp_message, sender_id, f"🗑️ Removed {removed['media_type'].capitalize()} from your uploads.")
        else:
            executor.submit(send_whatsapp_message, sender_id, "⚠️ Invalid upload number.")
    except ValueError:
        executor.submit(send_whatsapp_message, sender_id, "⚠️ Please provide a valid upload number (e.g., /remove_upload 1).")


        
        


        
        
def send_done_upload_prompt(sender_id):
    media_count_result = query_database("SELECT COUNT(*) AS count FROM temp_ticket_media WHERE sender_id = %s", (sender_id,))
    media_count = media_count_result[0]["count"] if media_count_result else 0

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


def handle_done_command(sender_id):
    user_data = query_database("SELECT temp_category FROM users WHERE whatsapp_number = %s", (sender_id,))
    media_count = query_database("SELECT COUNT(*) as count FROM temp_ticket_media WHERE sender_id = %s", (sender_id,))
    count = media_count[0]["count"] if media_count else 0

    logging.info(f"Processing /done for {sender_id}: {count} attachments found")

    with user_timers_lock:
        if sender_id in upload_state and upload_state[sender_id].get("timer"):
            upload_state[sender_id]["timer"].cancel()
            upload_state[sender_id]["timer"] = None
            logging.info(f"Cancelled upload timer for {sender_id}")

    if count == 0:
        executor.submit(send_whatsapp_message, sender_id, "📎 You have not uploaded any attachments yet. You can still proceed by describing the issue, or upload files now.")
    else:
        executor.submit(send_whatsapp_message, sender_id, f"📎 You've uploaded {count} file(s). Please describe your issue to proceed.")

    if user_data and user_data[0]["temp_category"]:
        query_database("UPDATE users SET last_action = 'awaiting_issue_description' WHERE whatsapp_number = %s", (sender_id,), commit=True)
        if count == 0:
            executor.submit(send_whatsapp_message, sender_id, "✏️ Great! Please describe your issue.")
    else:
        query_database("UPDATE users SET last_action = 'awaiting_category' WHERE whatsapp_number = %s", (sender_id,), commit=True)
        executor.submit(send_whatsapp_message, sender_id, "⚠️ Please select a category first.")
        executor.submit(send_category_prompt, sender_id)



    
def handle_clear_attachments(sender_id):
    with media_buffer_lock:
        if sender_id in media_buffer:
            count = len(media_buffer[sender_id])
            #del media_buffer[sender_id]
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
        send_whatsapp_message(sender_id, "✏️ Please describe your issue.\n📎 If you wish to upload a file, please do so before describing your issue.\n⏳ Note: File uploads may take a while to process.")
    else:
        send_whatsapp_message(sender_id, "⚠️ Invalid selection. Please reply with 1️⃣, 2️⃣, 3️⃣, or 4️⃣.")
        send_category_prompt(sender_id)
    
        
        



        
def mark_user_accepted(whatsapp_number):
    engine = get_db_connection1()
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id FROM users WHERE whatsapp_number = :num
        """), {"num": whatsapp_number}).fetchone()
        if not result:
            raise Exception("User not found in DB")

        conn.execute(text("""
            UPDATE users SET terms_accepted = 1, terms_accepted_at = NOW()
            WHERE whatsapp_number = :num
        """), {"num": whatsapp_number})
        conn.commit()


def handle_accept(sender_id):
    with accept_lock:
        logging.info(f"Processing accept for {sender_id}")
        send_whatsapp_message(sender_id, "⏳ We're getting things sorted, this may take a minute or two...")

        # Cancel existing retry if needed
        if sender_id in accept_retry_state:
            retry_info = accept_retry_state.pop(sender_id)
            if retry_info and retry_info["timer"]:
                retry_info["timer"].cancel()
                logging.info(f"🛑 Cancelled existing retry timer for {sender_id}.")

        # Check if already registered
        if is_registered_user(sender_id):
            with terms_pending_lock:
                terms_pending_users.pop(sender_id, None)
            send_whatsapp_message(sender_id, "🎉 You are already registered!")
            return

        def try_register(attempt):
            with accept_lock:
                logging.info(f"🔁 Attempt {attempt} to mark accepted for {sender_id}")
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
                        send_whatsapp_message(sender_id, "⚠️ We couldn't finalize your registration. Please try again or contact support.")

        # Start first try after short delay
        timer = Timer(1, try_register, args=[1])
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
                count_result = query_database("SELECT COUNT(*) AS count FROM temp_ticket_media WHERE sender_id = %s", (sender_id,))
                count = count_result[0]["count"] if count_result else 0
                if count > 1:
                    send_done_upload_prompt(sender_id)
                    upload_state[sender_id]["timer"] = None

        count_result = query_database("SELECT COUNT(*) AS count FROM temp_ticket_media WHERE sender_id = %s", (sender_id,))
        count = count_result[0]["count"] if count_result else 0
        if count > 1:
            t = Timer(10, send_prompt)
            upload_state[sender_id] = upload_state.get(sender_id, {"media_count": count, "last_upload_time": time.time()})
            upload_state[sender_id]["timer"] = t
            t.start()



def process_webhook(data):
    """Processes incoming WhatsApp messages from Meta Webhook."""
    
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

                # Handle button replies
                if "interactive" in message and "button_reply" in message["interactive"]:
                    handle_button_reply(message, sender_id)
                    continue

                # Normalize message text
                normalized = message_text.strip().lower() if message_text else ""

                # Handle TOS acceptance
                if normalized in ["accept", "reject"]:
                    with terms_pending_lock:
                        if normalized == "reject":
                            temp_opt_in_data.pop(sender_id, None)
                            terms_pending_users.pop(sender_id, None)
                            executor.submit(send_whatsapp_message, sender_id, "❌ You must accept the Terms to use this service.")
                        else:
                            executor.submit(handle_accept, sender_id)
                    continue

                # Handle /start command
                if normalized == "/start":
                    # Reset user state in DB
                    query_database(
                        "UPDATE users SET last_action = NULL, temp_category = NULL WHERE whatsapp_number = %s",
                        (sender_id,), commit=True
                    )

                    # Clear temp media from database
                    query_database(
                        "DELETE FROM temp_ticket_media WHERE sender_id = %s",
                        (sender_id,), commit=True
                    )

                    # Clear in-memory state
                    with user_timers_lock:
                        user_timers.pop(sender_id, None)
                        upload_state.pop(sender_id, None)

                    with media_buffer_lock:
                        media_buffer.pop(sender_id, None)

                    with terms_pending_lock:
                        terms_pending_users.pop(sender_id, None)
                        temp_opt_in_data.pop(sender_id, None)

                    send_whatsapp_message(sender_id, "🔄 We've reset your session. Tap '📝 Create Ticket' to begin.")
                    send_whatsapp_buttons(sender_id)
                    continue


                # Deduplicate
                if not is_valid_message(sender_id, message_id, message_text):
                    logging.info(f"Invalid/duplicate message for {sender_id}")
                    continue

                # Handle media uploads via cleaner handler
                if handle_media_upload(message, sender_id, message_text):
                    continue

                # Handle commands
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
                if normalized == "/cancel":
                    query_database(
                        "UPDATE users SET last_action = NULL, temp_category = NULL WHERE whatsapp_number = %s",
                        (sender_id,), commit=True
                    )
                    with user_timers_lock:
                        user_timers.pop(sender_id, None)
                        upload_state.pop(sender_id, None)
                    send_whatsapp_message(sender_id, "❌ Your current session has been cancelled. Type /start to begin a new ticket.")
                    continue


                # Fallback: handle based on user flow status
                user_status = query_database("SELECT last_action, temp_category FROM users WHERE whatsapp_number = %s", (sender_id,))
                user_info = query_database("SELECT property_id FROM users WHERE whatsapp_number = %s", (sender_id,))
                if not user_status or not user_info:
                    send_whatsapp_message(sender_id, "⚠️ You are not registered. Please contact support.")
                    continue

                last_action = user_status[0]["last_action"]
                property_id = user_info[0]["property_id"]

                if last_action == "awaiting_category":
                    # Reset the timer for category selection
                    with user_timers_lock:
                        user_timers[sender_id] = datetime.now()
                        logging.info(f"🔁 Category timer reset for {sender_id}")
                    
                    # Start/reset expiry thread if needed
                    threading.Thread(target=reset_category_selection, args=(sender_id,), daemon=True).start()

                    handle_category_selection(sender_id, message_text)

                elif last_action == "awaiting_issue_description":
                    send_whatsapp_message(sender_id, "✏️ Please describe your issue.\n📎 If you wish to upload a file, please do so before describing your issue.\n⏳ Note: File uploads may take a while to process.")

                elif normalized in ["hi", "hello", "help", "menu"]:
                    send_whatsapp_buttons(sender_id)

                else:
                    send_whatsapp_message(
                        sender_id,
                        "🤖 I didn’t understand that message.\n\nIf you need help:\n- Tap a button below\n- Or type /start to begin again"
                    )
                    send_whatsapp_buttons(sender_id)

    purge_expired_items()