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
import threading
from datetime import datetime

# Dictionary to track category selection timeouts


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

# Initialize Flask app
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# In-memory storage to track processed messages
processed_message_ids = set()
last_messages = {}  # { sender_id: (message_text, timestamp) }
user_timers = {}
media_buffer = {}  # global store for media before ticket creation



def opt_in_user(whatsapp_number):
    """Adds the recipient number to the WhatsApp allowed list."""
    url = f"https://graph.facebook.com/v22.0/{WHATSAPP_PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}",
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

    response = requests.post(url, headers=headers, json=payload)
    print("Status code ", response.status_code)
    print(WHATSAPP_PHONE_NUMBER_ID)
    
    if response.status_code == 200:
        print("worked")
        return True, "User opted in successfully!"
    else:
        print(f"Error: {response.json()}")
        return False, f"Error: {response.json()}"

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
    send_whatsapp_message(to, message)

    # Set timeout to reset user status if they don't respond within 5 minutes
    user_timers[to] = datetime.now()
    threading.Thread(target=reset_category_selection, args=(to,)).start()
    
def reset_category_selection(to):
    """Resets the category selection if the user takes more than 5 minutes to respond."""
    time.sleep(300)  # Wait for 5 minutes
    last_attempt_time = user_timers.get(to)
    
    if last_attempt_time:
        elapsed_time = (datetime.now() - last_attempt_time).total_seconds()
        if elapsed_time >= 300:  # If still waiting after 5 minutes
            logging.info(f"⏳ Resetting category selection for {to} due to timeout.")
            query_database("UPDATE users SET last_action = NULL WHERE whatsapp_number = %s", (to,), commit=True)
            send_whatsapp_message(to, "⏳ Your category selection request has expired. Please start again by selecting '📝 Create Ticket'.")
            del user_timers[to]  # Remove from trackin



def get_category_name(category_number):
    categories = {
        "1": "Accounts",
        "2": "Maintenance",
        "3": "Security",
        "4": "Other"  # ✅ Added new option
    }
    return categories.get(category_number, None)


# Prevent duplicate message processing
def is_message_processed(message_id, timestamp_iso):
    msg_time = datetime.fromisoformat(timestamp_iso.replace('Z', '+00:00'))
    now = datetime.utcnow()
    age = (now - msg_time).total_seconds()
    if age > 300:
        return True
    if message_id in processed_message_ids:
        return True
    result = query_database("SELECT id FROM processed_messages WHERE id = %s", (message_id,))
    if result:
        processed_message_ids[message_id] = msg_time
        return True
    return False

def mark_message_as_processed(message_id, timestamp_iso):
    msg_time = datetime.fromisoformat(timestamp_iso.replace('Z', '+00:00'))
    processed_message_ids[message_id] = msg_time
    query_database("INSERT IGNORE INTO processed_messages (id) VALUES (%s)", (message_id,), commit=True)

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
        """Checks if the WhatsApp number is registered in the database."""
        engine = get_db_connection1()
        with engine.connect() as conn:
            query = text("SELECT id FROM users WHERE whatsapp_number = :whatsapp_number")
            result = conn.execute(query, {"whatsapp_number": whatsapp_number}).fetchone()
        return result is not None


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
    
    send_whatsapp_message(to, message) 
    


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
    process_webhook(data)

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
            result = send_template_message(to, template_name, template_parameters)
        else:
            result = send_whatsapp_message(to, message)

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





def process_webhook(data):
    """Handles incoming WhatsApp messages."""
    logging.info(f"Processing webhook data: {json.dumps(data, indent=2)}")
    if "entry" in data:
        for entry in data["entry"]:
            for change in entry.get("changes", []):
                if "statuses" in change["value"]:
                    logging.info(f"Status update received for message ID {change['value']['statuses'][0]['id']}. Ignoring.")
                    continue

                if "messages" in change["value"]:
                    for message in change["value"]["messages"]:
                        message_id = message.get("id")
                        sender_id = message["from"]
                        message_text =""
                        if "text" in message:
                            message_text = message.get("text", {}).get("body", "").strip()
                        elif message.get("type") in ["image", "video", "document"]:
                            message_text = message[message["type"]].get("caption", "").strip()

                        # ✅ Handle media uploads (document, image, video)
                        media_type = message.get("type")
                        if media_type in ["document", "image", "video"]:
                            media_id = message[media_type]["id"]
                            
                            # Build base filename
                            if media_type == "document":
                                base_filename = message[media_type].get("filename", f"{media_id}.pdf")
                            elif media_type == "image":
                                base_filename = f"{media_id}.jpg"
                            elif media_type == "video":
                                base_filename = f"{media_id}.mp4"

                            # Add timestamp to filename
                            
                            name, ext = os.path.splitext(base_filename)
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            filename = f"{name}_{timestamp}{ext}"

                            download_result = download_media(media_id, filename)

                            if "success" in download_result:
                                media_buffer[sender_id] = {
                                    "media_type": media_type,
                                    "media_path": download_result["path"]
                                }
                                logging.info(f"📎 Saved {media_type}: {download_result['path']}")
                            else:
                                logging.error(f"❌ Failed to save {media_type}: {download_result}")

                        if not is_registered_user(sender_id):
                            logging.info(f"Blocked unregistered user: {sender_id}")
                            send_whatsapp_message(sender_id, "You are not registered. Please register first.")
                            continue
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        if is_message_processed(message_id, timestamp) or not should_process_message(sender_id, message_text):
                            logging.info(f"⚠️ Skipping duplicate message {message_id}")
                            continue

                        mark_message_as_processed(message_id, timestamp)

                        # ✅ Handle button replies
                        if "interactive" in message and "button_reply" in message["interactive"]:
                            button_id = message["interactive"]["button_reply"]["id"]

                            if button_id == "create_ticket":
                                query_database("UPDATE users SET last_action = 'awaiting_category' WHERE whatsapp_number = %s", (sender_id,), commit=True)
                                send_category_prompt(sender_id)
                                continue
                            elif button_id == "check_ticket":
                                send_whatsapp_tickets(sender_id)
                                continue

                        # ✅ Handle category selection
                        user_status = query_database("SELECT last_action FROM users WHERE whatsapp_number = %s", (sender_id,))
                        property = query_database("SELECT property FROM users WHERE whatsapp_number = %s", (sender_id,))[0]["property"]
                        assigned_admin = query_database("SELECT id FROM admin_users WHERE property = %s", (property,))[0]["id"]

                        if user_status and user_status[0]["last_action"] == "awaiting_category":
                            category_name = get_category_name(message_text)
                            if category_name:
                                query_database(
                                    "UPDATE users SET last_action = 'awaiting_issue_description', temp_category = %s WHERE whatsapp_number = %s",
                                    (category_name, sender_id),
                                    commit=True,
                                )
                                send_whatsapp_message(sender_id, "Please describe your issue, or upload a supporting file.")
                                del user_timers[sender_id]
                            else:
                                send_whatsapp_message(sender_id, "⚠️ Invalid selection. Please reply with 1️⃣, 2️⃣, 3️⃣ or 4️⃣.")
                                send_category_prompt(sender_id)
                            continue

                        # ✅ Handle issue description + media
                        if user_status and user_status[0]["last_action"] == "awaiting_issue_description":
                            user_info = query_database("SELECT id, temp_category FROM users WHERE whatsapp_number = %s", (sender_id,))
                            if user_info:
                                user_id = user_info[0]["id"]
                                category = user_info[0]["temp_category"]

                                # Use provided text or fallback to "No description"
                                description = message_text if message_text else "No description provided"

                                ticket_id = insert_ticket_and_get_id(user_id, description, category, property, assigned_admin)

                                # ✅ Attach media if any
                                media = media_buffer.pop(sender_id, None)
                                if media:
                                    save_ticket_media(ticket_id, media["media_type"], media["media_path"])
                                    logging.info(f"📁 Linked {media['media_type']} to ticket #{ticket_id}")

                                query_database("UPDATE users SET last_action = NULL, temp_category = NULL WHERE whatsapp_number = %s", (sender_id,), commit=True)
                                send_whatsapp_message(sender_id, f"✅ Your ticket has been created under the *{category}* category. Our team will get back to you soon!")
                                if sender_id in user_timers:
                                    del user_timers[sender_id]
                            else:
                                send_whatsapp_message(sender_id, "❌ Error creating ticket. Please try again.")
                            continue

                        if message_text.lower() in ["hi", "hello", "help", "menu"]:
                            send_whatsapp_buttons(sender_id)



if __name__ == "__main__":
    app.run(port=5000, debug=False)  # ⚠️ Disabled debug mode to prevent duplicate processing