from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from sqlalchemy.sql import text
import logging


load_dotenv()

# Database Connection using SQLAlchemy
DB_URI = f"mysql+mysqlconnector://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
#DB_URI = f"mysql+mysqlconnector://{st.secrets.DB_USER}:{st.secrets.DB_PASSWORD}@{st.secrets.DB_HOST}/{st.secrets.DB_NAME}"  # Using Streamlit secrets('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
def get_db_connection1():
    return create_engine(DB_URI)




import logging

def save_ticket_media(ticket_id, media_type, file_path):
    """
    Saves media content (as blob) linked to a ticket in the database.
    Logs errors if file cannot be read or if database insertion fails.
    """
    try:
        with open(file_path, "rb") as f:
            binary_content = f.read()
    except Exception as e:
        logging.error(f"❌ Failed to read media file {file_path}: {e}")
        return

    query = """
        INSERT INTO ticket_media (ticket_id, media_type, media_path, media_blob)
        VALUES (:ticket_id, :media_type, :media_path, :media_blob)
    """

    try:
        engine = get_db_connection1()
        with engine.connect() as conn:
            conn.execute(text(query), {
                "ticket_id": ticket_id,
                "media_type": media_type,
                "media_path": file_path,
                "media_blob": binary_content
            })
            conn.commit()
        logging.info(f"✅ Media saved for ticket #{ticket_id}: {media_type} -> {file_path}")
    except Exception as e:
        logging.error(f"❌ Failed to insert media into database for ticket {ticket_id}: {e}")


        
        
def insert_ticket_and_get_id(user_id, description, category, property):
    """Inserts a new ticket and returns the auto-incremented ticket ID."""
    engine = get_db_connection1()
    insert_query = text("""
        INSERT INTO tickets 
        (user_id, issue_description, status, created_at, category, property_id, assigned_admin)
        VALUES (:user_id, :description, 'Open', NOW(), :category, :property, :assigned_admin)
    """)
    select_query = text("SELECT LAST_INSERT_ID() AS id")

    with engine.connect() as conn:
        conn.execute(insert_query, {
            "user_id": user_id,
            "description": description,
            "category": category,
            "property": property,
            "assigned_admin": 6
        })
        result = conn.execute(select_query).fetchone()
        conn.commit()
        return result[0]



def mark_user_accepted_via_temp_table(whatsapp_number):
    """
    Moves a user from temp_opt_in_users to users table, and marks terms as accepted.
    If the user already exists, updates their terms_accepted status and timestamp.
    """

    engine = get_db_connection1()
    with engine.connect() as conn:
        try:
            # Step 1: Fetch user info from temporary table
            user = conn.execute(text("""
                SELECT name, property_id, unit_number FROM temp_opt_in_users
                WHERE whatsapp_number = :num
            """), {"num": whatsapp_number}).fetchone()

            if not user:
                raise Exception(f"User {whatsapp_number} not found in temp_opt_in_users")

            name, property_id, unit_number = user[0], user[1], user[2]

            # Step 2: Insert or update the user in the main users table
            conn.execute(text("""
                INSERT INTO users (name, whatsapp_number, property_id, unit_number, terms_accepted, terms_accepted_at)
                VALUES (:name, :whatsapp_number, :property_id, :unit_number, 1, NOW())
                ON DUPLICATE KEY UPDATE terms_accepted = 1, terms_accepted_at = NOW()
            """), {
                "name": name,
                "whatsapp_number": whatsapp_number,
                "property_id": property_id,
                "unit_number": unit_number
            })

            # Step 3: Delete from the temp table
            conn.execute(text("DELETE FROM temp_opt_in_users WHERE whatsapp_number = :num"), {"num": whatsapp_number})

            # Step 4: Commit the transaction
            conn.commit()

            logging.info(f"✅ Successfully registered and marked terms accepted for user {whatsapp_number}")

        except Exception as e:
            logging.error(f"❌ Error while registering user {whatsapp_number}: {e}")
            raise
        
        
def save_temp_media_to_db(sender_id, media_type, media_path, caption):
    """
    Saves a media reference temporarily in the database before ticket creation.
    """
    query = text("""
        INSERT INTO temp_ticket_media (sender_id, media_type, media_path, caption)
        VALUES (:sender_id, :media_type, :media_path, :caption)
    """)
    try:
        engine = get_db_connection1()
        with engine.connect() as conn:
            conn.execute(query, {
                "sender_id": sender_id,
                "media_type": media_type,
                "media_path": media_path,
                "caption": caption
            })
            conn.commit()
            logging.info(f"✅ Temp media saved for {sender_id}: {media_type} -> {media_path}")
    except Exception as e:
        logging.error(f"❌ Failed to insert temp media for {sender_id}: {e}")

