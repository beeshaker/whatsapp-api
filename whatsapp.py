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


executor = ThreadPoolExecutor(max_workers=10)




# Load environment variables
load_dotenv()

# Meta WhatsApp API Credentials
WHATSAPP_ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
WHATSAPP_PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")


# MySQL Database Connection
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
pending_confirmations = {}  # key: phone_number, value: Timer object
# Initialize Flask app
app = Flask(__name__)
logging.basicConfig(level=logging.WARNING)


# Add at the top of your script:
media_buffer_lock = threading.Lock()
user_timers_lock = threading.Lock()
upload_prompt_timers_lock = threading.Lock()
pending_confirmations_lock = threading.Lock()


# In-memory storage to track processed messages
processed_message_ids = set()
last_messages = {}  # { sender_id: (message_text, timestamp) }
user_timers = {}
media_buffer = {}  # global store for media before ticket creation
upload_prompt_timers = {}  # key: sender_id, value: Timer







from flask import Flask, request, jsonify
import os
import requests

app = Flask(__name__)

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


# Function to connect to MySQL and execute queries

def query_database(query, params=(), commit=False):
    engine = get_db_connection1()
    try:
        logging.debug(f"Running query: {query} | params: {params} | type: {type(params)}")
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            if commit:
                return True
            return [dict(row) for row in result]
    except Exception as e:
        logging.error(f"DB Error: {e}")
        return None
    
    
def send_category_prompt(to):
    """Asks the user to select a category for the ticket."""
    message = "Please select a category:\n1️⃣ Accounts\n2️⃣ Maintenance\n3️⃣ Security\n4️⃣ Other\n\nReply with the number."
    executor.submit(send_whatsapp_message, to, message)


    # Safely record the timestamp
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
                del user_timers[to]  # Safe to delete while lock is held
            else:
                return  # Exit early if not expired
        else:
            return  # Exit early if already cleared

    # Actions that don’t require the lock (outside the lock)
    logging.info(f"⏳ Resetting category selection for {to} due to timeout.")
    logging.warning(f"📌 Params being passed line 154: {to} | type: {type(to)}")
    query_database("UPDATE users SET last_action = NULL WHERE whatsapp_number = %s", (to,), commit=True)
    send_whatsapp_message(to, "⏳ Your category selection request has expired. Please start again by selecting '📝 Create Ticket'.")





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
    logging.warning(f"📌 Params being passed line 178: {message_id} | type: {type(message_id)}")
    result = query_database(query, (message_id,))
    return bool(result)

def mark_message_as_processed(message_id):
    """Mark a message as processed (in-memory & database)."""
    processed_message_ids.add(message_id)  # ✅ Immediate in-memory tracking
    query = "INSERT IGNORE INTO processed_messages (id) VALUES (%s)"
    logging.warning(f"📌 Params being passed line 186: {message_id} | type: {type(message_id)}")
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
    """Checks if the WhatsApp number is registered as a user or admin."""
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
    logging.warning(f"📌 Params being passed line 292: {to} | type: {type(to)}")
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

    data = request.get_json()
    logging.info(f"Incoming webhook data: {json.dumps(data, indent=2)}")

    # Offload full processing to background thread
    executor.submit(process_webhook_async, data)

    return jsonify({"status": "received"}), 200


def process_webhook_async(data):
    """Handles incoming WhatsApp messages in background."""
    try:
        purge_expired_media()
        logging.info(f"Processing webhook data asynchronously: {json.dumps(data, indent=2)}")
        
        if "entry" in data:
            for entry in data["entry"]:
                for change in entry.get("changes", []):
                    if "statuses" in change["value"]:
                        logging.info(f"Status update received for message ID {change['value']['statuses'][0]['id']}. Ignoring.")
                        continue
                    if "messages" in change["value"]:
                        for message in change["value"]["messages"]:
                            # Step 1: Handle message basics
                            message_id, sender_id, message_text = extract_message_info(message)
                            # Step 2: Check if we should process this message
                            if not is_valid_message(sender_id, message_id, message_text):
                                continue
                            # Step 3: Handle media uploads
                            if handle_media_upload(message, sender_id, message_text):
                                continue  # Already handled
                            # Step 4: Handle button replies
                            if "interactive" in message and "button_reply" in message["interactive"]:
                                handle_button_reply(message, sender_id)
                                continue
                            # Step 5: Handle text commands
                            if message_text.lower() == "/clear_attachments":
                                handle_clear_attachments(sender_id)
                                continue
                            # Step 6: Main menu or help
                            if message_text.lower() in ["hi", "hello", "help", "menu"]:
                                send_whatsapp_buttons(sender_id)
                                continue
                            # Step 7: Category selection
                            logging.warning(f"📌 Params being passed line 362: {sender_id} | type: {type(sender_id)}")
                            user_status = query_database("SELECT last_action FROM users WHERE whatsapp_number = %s", (sender_id,))
                            logging.warning(f"📌 Params being passed line 364: {sender_id} | type: {type(sender_id)}")
                            property_data = query_database("SELECT property_id FROM users WHERE whatsapp_number = %s", (sender_id,))
                            property = property_data[0]["property_id"] if property_data else None
                            if user_status and user_status[0]["last_action"] == "awaiting_category":
                                handle_category_selection(sender_id, message_text)
                                continue
                            # Step 8: Ticket creation
                            if user_status and user_status[0]["last_action"] == "awaiting_issue_description":
                                handle_ticket_creation(sender_id, message_text, property)
                                continue
    except Exception as e:
        logging.error(f"Error in async webhook processing: {e}", exc_info=True)


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
    url = f"https://graph.facebook.com/v17.0/{WHATSAPP_PHONE_NUMBER_ID}/messages"
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
    """Downloads WhatsApp media and saves it to a timestamped file in /tmp."""
    # Step 1: Get the media URL
    meta_url = f"https://graph.facebook.com/v22.0/{media_id}"
    headers = {"Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}"}
    media_response = requests.get(meta_url, headers=headers)

    if media_response.status_code != 200:
        return {"error": "Failed to fetch media URL", "details": media_response.text}

    media_url = media_response.json().get("url")
    if not media_url:
        return {"error": "Media URL not found"}

    # Step 2: Download the file
    media_file_response = requests.get(media_url, headers=headers)
    if media_file_response.status_code != 200:
        return {"error": "Failed to download media", "details": media_file_response.text}

    # Step 3: Build a unique timestamped filename
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

def purge_expired_media(ttl_seconds=300):
    now = time.time()
    expired_keys = []

    with media_buffer_lock:
        for wa_id, media_list in list(media_buffer.items()):
            fresh_media = [entry for entry in media_list if now - entry["timestamp"] < ttl_seconds]
            if fresh_media:
                media_buffer[wa_id] = fresh_media
            else:
                expired_keys.append(wa_id)

        for wa_id in expired_keys:
            del media_buffer[wa_id]

        
        
def flush_user_media_after_ticket(sender_id, ticket_id, delay=30):
    """Flushes any media uploaded shortly after ticket creation."""
    time.sleep(delay)

    with media_buffer_lock:
        media_list = media_buffer.pop(sender_id, [])

    for entry in media_list:
        media = entry["media"]
        save_ticket_media(ticket_id, media["media_type"], media["media_path"])
        logging.info(f"📁 (Post-ticket) Linked {media['media_type']} to ticket #{ticket_id}")

        
        
        
def start_fallback_timer(phone_number):
    def fallback_action():
        send_whatsapp_message(phone_number, "⌛ No response received.")
        with pending_confirmations_lock:
            pending_confirmations.pop(phone_number, None)

    with pending_confirmations_lock:
        if phone_number in pending_confirmations:
            pending_confirmations[phone_number].cancel()

        t = Timer(180, fallback_action)
        pending_confirmations[phone_number] = t
        t.start()


def send_caption_confirmation(phone_number, captions, access_token, phone_number_id):
    url = f"https://graph.facebook.com/v19.0/{phone_number_id}/messages"
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
    # Ignore unregistered users
    if not is_registered_user(sender_id):
        logging.info(f"Blocked unregistered user: {sender_id}")
        send_whatsapp_message(sender_id, "You are not registered. Please register first.")
        return False

    # Skip duplicate/rapid messages
    if is_message_processed(message_id) or not should_process_message(sender_id, message_text):
        logging.info(f"⚠️ Skipping duplicate message {message_id}")
        return False

    mark_message_as_processed(message_id)
    return True


def process_media_upload(media_id, filename, sender_id, media_type, message_text):
    download_result = download_media(media_id, filename)

    if "success" in download_result:
        with media_buffer_lock:
            if sender_id not in media_buffer:
                media_buffer[sender_id] = []
            media_buffer[sender_id].append({
                "media": {
                    "media_type": media_type,
                    "media_path": download_result["path"],
                    "caption": message_text.strip() if message_text else None
                },
                "timestamp": time.time()
            })

        logging.info(f"📎 Saved {media_type}: {download_result['path']}")

        with upload_prompt_timers_lock:
            if sender_id in upload_prompt_timers:
                upload_prompt_timers[sender_id].cancel()

            def delayed_prompt():
                with media_buffer_lock:
                    media_count = len(media_buffer.get(sender_id, []))

                logging.info(f"🕒 Sending 'done uploading?' prompt for {sender_id} with {media_count} file(s)")
                executor.submit(send_done_upload_prompt, sender_id, media_count)
                start_fallback_timer(sender_id)

                with upload_prompt_timers_lock:
                    upload_prompt_timers.pop(sender_id, None)

            t = Timer(5, delayed_prompt)
            upload_prompt_timers[sender_id] = t
            t.start()

    else:
        logging.error(f"❌ Failed to save {media_type}: {download_result}")


def handle_media_upload(message, sender_id, message_text):
    media_type = message.get("type")
    if media_type in ["document", "image", "video"]:
        media_id = message[media_type]["id"]
        base_filename = message[media_type].get("filename", f"{media_id}.{media_type[:3]}")
        name, ext = os.path.splitext(base_filename)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{name}_{timestamp}{ext}"

        # 🔁 Offload media download and buffer insertion to background
        executor.submit(
            process_media_upload,
            media_id,
            filename,
            sender_id,
            media_type,
            message_text
        )

        return True

    return False





def handle_button_reply(message, sender_id):
    button_id = message["interactive"]["button_reply"]["id"]

    # 🔐 Cancel any fallback timeout
    with pending_confirmations_lock:
        if sender_id in pending_confirmations:
            pending_confirmations[sender_id].cancel()
            del pending_confirmations[sender_id]

    if button_id == "create_ticket":
        logging.warning(f"📌 Params being passed line 652: {sender_id} | type: {type(sender_id)}")
        query_database("UPDATE users SET last_action = 'awaiting_category' WHERE whatsapp_number = %s", (sender_id,), commit=True)
        send_category_prompt(sender_id)

    elif button_id == "check_ticket":
        send_whatsapp_tickets(sender_id)

    elif button_id == "upload_done":
        logging.warning(f"📌 Params being passed line 660: {sender_id} | type: {type(sender_id)}")
        user_data = query_database("SELECT temp_category FROM users WHERE whatsapp_number = %s", (sender_id,))
        
        if user_data and user_data[0]["temp_category"]:
            logging.warning(f"📌 Params being passed line 664: {sender_id} | type: {type(sender_id)}")
            query_database(
                "UPDATE users SET last_action = 'awaiting_issue_description' WHERE whatsapp_number = %s",
                (sender_id,), commit=True
            )
            send_whatsapp_message(sender_id, "✏️ Great! Please describe your issue.")
        else:
            logging.warning(f"📌 Params being passed line 671: {sender_id} | type: {type(sender_id)}")
            query_database(
                "UPDATE users SET last_action = 'awaiting_category' WHERE whatsapp_number = %s",
                (sender_id,), commit=True
            )
            send_category_prompt(sender_id)

    elif button_id == "upload_not_done":
        send_whatsapp_message(sender_id, "👍 Okay, send the remaining file(s) when you're ready.")

        
        
        
def send_done_upload_prompt(phone_number, media_count):
    url = f"https://graph.facebook.com/v19.0/{WHATSAPP_PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    body_text = f"📎 You've sent *{media_count}* file(s).\nAre you done uploading attachments?"

    payload = {
        "messaging_product": "whatsapp",
        "to": phone_number,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {
                "text": body_text
            },
            "action": {
                "buttons": [
                    {
                        "type": "reply",
                        "reply": {
                            "id": "upload_done",
                            "title": "✅ Yes – Submit"
                        }
                    },
                    {
                        "type": "reply",
                        "reply": {
                            "id": "upload_not_done",
                            "title": "❌ No – More to Add"
                        }
                    }
                ]
            }
        }
    }

    response = requests.post(url, headers=headers, json=payload)
    return response.json()


        
        
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
    logging.warning(f"📌 Params being passed line 742: {sender_id} | type: {type(sender_id)}")
    query_database(
        "UPDATE users SET last_action = 'awaiting_issue_description', temp_category = %s WHERE whatsapp_number = %s",
        ("Other", sender_id),
        commit=True
    )

    auto_description = "AUTO-FILLED ISSUE DESCRIPTION:\n\n" + "\n\n".join(non_empty_captions)
    logging.warning(f"📌 Params being passed line 750: {sender_id} | type: {type(sender_id)}")
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
    logging.warning(f"📌 Params being passed line 770: {user_id} | type: {type(user_id)}")
    ticket_check = query_database("""
        SELECT created_at FROM tickets
        WHERE user_id = %s
        ORDER BY created_at DESC
        LIMIT 1
    """, (user_id,))

    if ticket_check:
        last_created = ticket_check[0]["created_at"]
        if (datetime.now() - last_created).total_seconds() < 60:
            logging.info(f"🛑 Ticket already created recently for user {sender_id}. Skipping.")
            return

    # Insert the new ticket
    ticket_id = insert_ticket_and_get_id(user_id, description, category, property)

    # Safely copy media buffer
    with media_buffer_lock:
        media_list = media_buffer.get(sender_id, []).copy()

    # Save each media file to DB
    for entry in media_list:
        media = entry["media"]
        save_ticket_media(ticket_id, media["media_type"], media["media_path"])
        logging.info(f"📁 Linked {media['media_type']} to ticket #{ticket_id}")

    # Clear media buffer
    with media_buffer_lock:
        if sender_id in media_buffer:
            del media_buffer[sender_id]

    # Clear user timer
    with user_timers_lock:
        if sender_id in user_timers:
            del user_timers[sender_id]

    # Clear user state
    logging.warning(f"📌 Params being passed line 770: {sender_id} | type: {type(sender_id)}")
    query_database(
        "UPDATE users SET last_action = NULL, temp_category = NULL WHERE whatsapp_number = %s",
        (sender_id,), commit=True
    )

    # Send confirmation in background
    executor.submit(
        send_whatsapp_message,
        sender_id,
        f"✅ Your ticket has been created under the *{category}* category. Our team will get back to you soon!"
    )

    # 🔁 Flush any additional media uploaded shortly after ticket creation
    threading.Thread(
        target=flush_user_media_after_ticket,
        args=(sender_id, ticket_id, 30),
        daemon=True
    ).start()





    
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
        logging.warning(f"📌 Params being passed line 847: {sender_id} | type: {type(sender_id)}")
        query_database(
            "UPDATE users SET last_action = 'awaiting_issue_description', temp_category = %s WHERE whatsapp_number = %s",
            (category_name, sender_id),
            commit=True
        )
        send_whatsapp_message(sender_id, "Please describe your issue, or upload a supporting file.")
        if sender_id in user_timers:
            del user_timers[sender_id]
    else:
        send_whatsapp_message(sender_id, "⚠️ Invalid selection. Please reply with 1️⃣, 2️⃣, 3️⃣ or 4️⃣.")
        send_category_prompt(sender_id)
        
        
def handle_ticket_creation(sender_id, message_text, property):
    logging.warning(f"📌 Params being passed line 862: {sender_id} | type: {type(sender_id)}")
    user_info = query_database("SELECT id, temp_category FROM users WHERE whatsapp_number = %s", (sender_id,))
    if not user_info:
        send_whatsapp_message(sender_id, "❌ Error creating ticket. Please try again.")
        return

    user_id = user_info[0]["id"]
    category = user_info[0]["temp_category"]
    logging.warning(f"📌 Params being passed line 870: {sender_id} | type: {type(sender_id)}")
    ticket_check = query_database("""
        SELECT created_at FROM tickets
        WHERE user_id = %s
        ORDER BY created_at DESC
        LIMIT 1
    """, (user_id,))

    if ticket_check:
        last_created = ticket_check[0]["created_at"]
        if (datetime.now() - last_created).total_seconds() < 60:
            logging.info(f"🛑 Ticket already created recently for user {sender_id}. Skipping.")
            return

    if not message_text:
        with media_buffer_lock:
            if sender_id in media_buffer:
                captions = [
                    entry["media"].get("caption")
                    for entry in media_buffer[sender_id]
                    if entry["media"].get("caption")
                ]
            else:
                captions = []

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



def process_webhook(data):
    """Handles incoming WhatsApp messages."""
    purge_expired_media()
    logging.info(f"Processing webhook data: {json.dumps(data, indent=2)}")

    if "entry" in data:
        for entry in data["entry"]:
            for change in entry.get("changes", []):
                if "statuses" in change["value"]:
                    logging.info(f"Status update received for message ID {change['value']['statuses'][0]['id']}. Ignoring.")
                    continue

                if "messages" in change["value"]:
                    for message in change["value"]["messages"]:
                        # Step 1: Handle message basics
                        message_id, sender_id, message_text = extract_message_info(message)

                        # Step 2: Check if we should process this message
                        if not is_valid_message(sender_id, message_id, message_text):
                            continue

                        # Step 3: Handle media uploads
                        if handle_media_upload(message, sender_id, message_text):
                            continue  # Already handled

                        # Step 4: Handle button replies
                        if "interactive" in message and "button_reply" in message["interactive"]:
                            handle_button_reply(message, sender_id)
                            continue

                        # Step 5: Handle text commands
                        if message_text.lower() == "/clear_attachments":
                            handle_clear_attachments(sender_id)
                            continue

                        # Step 6: Main menu or help
                        if message_text.lower() in ["hi", "hello", "help", "menu"]:
                            send_whatsapp_buttons(sender_id)
                            continue

                        # Step 7: Category selection
                        logging.warning(f"📌 Params being passed line 977: {sender_id} | type: {type(sender_id)}")
                        user_status = query_database("SELECT last_action FROM users WHERE whatsapp_number = %s", (sender_id,))
                        logging.warning(f"📌 Params being passed line 979: {sender_id} | type: {type(sender_id)}")
                        property = query_database("SELECT property_id FROM users WHERE whatsapp_number = %s", (sender_id,))[0]["property_id"]

                        if user_status and user_status[0]["last_action"] == "awaiting_category":
                            handle_category_selection(sender_id, message_text)
                            continue

                        # Step 8: Ticket creation
                        if user_status and user_status[0]["last_action"] == "awaiting_issue_description":
                            handle_ticket_creation(sender_id, message_text, property)
                            continue