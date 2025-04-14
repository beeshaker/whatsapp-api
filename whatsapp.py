import os
import json
import time
import requests
import mysql.connector
import logging
import threading
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from conn1 import get_db_connection1
from sqlalchemy.sql import text
import threading
import datetime

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
            "language": {"code": "en_US"},
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
    message = "Please select a category:\n1️⃣ Accounts\n2️⃣ Maintenance\n3️⃣ Security\nReply with the number."
    send_whatsapp_message(to, message)

    # Set timeout to reset user status if they don't respond within 5 minutes
    user_timers[to] = datetime.datetime.now()
    threading.Thread(target=reset_category_selection, args=(to,)).start()
    
def reset_category_selection(to):
    """Resets the category selection if the user takes more than 5 minutes to respond."""
    time.sleep(300)  # Wait for 5 minutes
    last_attempt_time = user_timers.get(to)
    
    if last_attempt_time:
        elapsed_time = (datetime.datetime.now() - last_attempt_time).total_seconds()
        if elapsed_time >= 300:  # If still waiting after 5 minutes
            logging.info(f"⏳ Resetting category selection for {to} due to timeout.")
            query_database("UPDATE users SET last_action = NULL WHERE whatsapp_number = %s", (to,), commit=True)
            send_whatsapp_message(to, "⏳ Your category selection request has expired. Please start again by selecting '📝 Create Ticket'.")
            del user_timers[to]  # Remove from trackin



def get_category_name(category_number):
    """Returns the category name based on the user's selection."""
    categories = {"1": "Accounts", "2": "Maintenance", "3": "Security"}
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
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json()
    logging.info(f"Incoming webhook data: {json.dumps(data, indent=2)}")
    # Immediately return 200 OK to prevent WhatsApp retries
    response = jsonify({"status": "received"})
    threading.Thread(target=process_webhook, args=(data,)).start()
    return response, 200




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
                        message_text = message.get("text", {}).get("body", "").strip()

                        if not is_registered_user(sender_id):
                            logging.info(f"Blocked unregistered user: {sender_id}")
                            send_whatsapp_message(sender_id, "You are not registered. Please register first.")
                            continue

                        # ✅ Prevent duplicate messages
                        if is_message_processed(message_id) or not should_process_message(sender_id, message_text):
                            logging.info(f"⚠️ Skipping duplicate message {message_id}")
                            continue

                        # ✅ Mark message as processed
                        mark_message_as_processed(message_id)

                        # ✅ Handle button replies
                        if "interactive" in message and "button_reply" in message["interactive"]:
                            button_id = message["interactive"]["button_reply"]["id"]

                            if button_id == "create_ticket":
                                # Step 1: Ask user to choose a category
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
                                # Store selected category and ask for issue description
                                query_database(
                                    "UPDATE users SET last_action = 'awaiting_issue_description', temp_category = %s WHERE whatsapp_number = %s",
                                    (category_name, sender_id),
                                    commit=True,
                                )
                                send_whatsapp_message(sender_id, "Please describe your issue, and we will create a ticket for you.")
                                del user_timers[sender_id]  # Remove timeout tracking
                            else:
                                send_whatsapp_message(sender_id, "⚠️ Invalid selection. Please reply with 1️⃣, 2️⃣, or 3️⃣.")
                                send_category_prompt(sender_id)  # Reprompt if invalid input
                            continue

                        # ✅ Handle text-based ticket creation with category
                        if user_status and user_status[0]["last_action"] == "awaiting_issue_description":
                            user_info = query_database("SELECT id, temp_category FROM users WHERE whatsapp_number = %s", (sender_id,))
                            if user_info:
                                user_id = user_info[0]["id"]
                                category = user_info[0]["temp_category"]

                                query_database(
                                    "INSERT INTO tickets (user_id, issue_description, status, created_at, category, property, assigned_admin) VALUES (%s, %s, 'Open', NOW(), %s, %s, %s)",
                                    (user_id, message_text, category, property, assigned_admin),
                                    commit=True,
                                )
                                query_database("UPDATE users SET last_action = NULL, temp_category = NULL WHERE whatsapp_number = %s", (sender_id,), commit=True)
                                send_whatsapp_message(sender_id, f"✅ Your ticket has been created under the *{category}* category. Our team will get back to you soon!")
                            else:
                                send_whatsapp_message(sender_id, "❌ Error creating ticket. Please try again.")
                            continue  

                        # ✅ Handle common messages
                        if message_text.lower() in ["hi", "hello", "help", "menu"]:
                            send_whatsapp_buttons(sender_id)


if __name__ == "__main__":
    app.run(port=5000, debug=True)  # ⚠️ Disabled debug mode to prevent duplicate processing
