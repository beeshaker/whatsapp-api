# conn1.py (FULL UPDATED)
# - Uses NAIVE Kenya time for MySQL (prevents tz-aware datetime insert errors)
# - Uses engine.begin() for proper commit/rollback
# - Has a create_engine pool config (optional but recommended)
# - Improves logging with exc_info=True
# - Keeps your existing function names/signatures the same so your webhook code doesn’t break

from sqlalchemy import create_engine
import os
import logging
from dotenv import load_dotenv
from sqlalchemy.sql import text
from datetime import datetime
from zoneinfo import ZoneInfo

load_dotenv()

# -----------------------------------------------------------------------------
# Timezone: Kenya (Africa/Nairobi)
# -----------------------------------------------------------------------------
KENYA_TZ = ZoneInfo("Africa/Nairobi")

def kenya_now() -> datetime:
    """Timezone-aware Kenya time (good for logs/UI, NOT for MySQL DATETIME inserts)."""
    return datetime.now(KENYA_TZ)

def kenya_now_db() -> datetime:
    """
    Naive Kenya time for DB inserts.
    MySQL drivers often reject tz-aware datetimes for DATETIME columns.
    """
    return datetime.now(KENYA_TZ).replace(tzinfo=None)

# -----------------------------------------------------------------------------
# Database Connection using SQLAlchemy
# -----------------------------------------------------------------------------
DB_URI = (
    f"mysql+mysqlconnector://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
)

# Optional: create one engine (reused) instead of creating a new one per call
import threading
_ENGINE = None
_ENGINE_LOCK = threading.Lock()

def get_db_connection1():
    global _ENGINE
    if _ENGINE is None:
        with _ENGINE_LOCK:
            if _ENGINE is None:
                _ENGINE = create_engine(
                    DB_URI,
                    pool_pre_ping=True,
                    pool_recycle=1800,
                )
    return _ENGINE

# -----------------------------------------------------------------------------
# Media: Save final ticket media (blob)
# -----------------------------------------------------------------------------
def save_ticket_media(ticket_id, media_type, file_path):
    """
    Saves media content (as blob) linked to a ticket in the database.
    Returns True/False so caller can log failures cleanly.
    """
    try:
        with open(file_path, "rb") as f:
            binary_content = f.read()
    except Exception as e:
        logging.error(f"❌ Failed to read media file {file_path}: {e}", exc_info=True)
        return False

    # NOTE: If your ticket_media table does NOT have created_at or media_path, remove them here.
    query = text("""
        INSERT INTO ticket_media (ticket_id, media_type, media_path, media_blob, created_at)
        VALUES (:ticket_id, :media_type, :media_path, :media_blob, :created_at)
    """)

    try:
        engine = get_db_connection1()
        with engine.begin() as conn:  # ✅ commit/rollback handled automatically
            conn.execute(query, {
                "ticket_id": ticket_id,
                "media_type": media_type,
                "media_path": file_path,
                "media_blob": binary_content,
                "created_at": kenya_now_db(),  # ✅ naive Kenya time
            })

        logging.info(f"✅ Media saved for ticket #{ticket_id}: {media_type} -> {file_path}")
        return True
    except Exception as e:
        logging.error(f"❌ Failed to insert media into DB for ticket {ticket_id}: {e}", exc_info=True)
        return False

# -----------------------------------------------------------------------------
# Tickets: Insert and assign to property supervisor
# -----------------------------------------------------------------------------
def insert_ticket_and_get_id(user_id, description, category, property_id):
    """
    Inserts a new ticket and returns the auto-incremented ticket ID.
    Assigns the ticket to the property's supervisor_id (property manager).
    Falls back to admin_id=6 if supervisor_id is NULL.
    Uses naive Kenya time (MySQL-friendly).
    """
    engine = get_db_connection1()

    get_supervisor = text("""
        SELECT supervisor_id
        FROM properties
        WHERE id = :property_id
        LIMIT 1
    """)

    insert_query = text("""
        INSERT INTO tickets
        (user_id, issue_description, status, created_at, category, property_id, assigned_admin)
        VALUES (:user_id, :description, 'Open', :created_at, :category, :property_id, :assigned_admin)
    """)

    select_query = text("SELECT LAST_INSERT_ID() AS id")

    try:
        with engine.begin() as conn:
            row = conn.execute(get_supervisor, {"property_id": property_id}).fetchone()
            supervisor_id = row[0] if row else None
            assigned_admin = supervisor_id if supervisor_id else 6

            conn.execute(insert_query, {
                "user_id": user_id,
                "description": description,
                "category": category,
                "property_id": property_id,
                "assigned_admin": assigned_admin,
                "created_at": kenya_now_db(),  # ✅ naive Kenya time
            })

            result = conn.execute(select_query).fetchone()
            return int(result[0]) if result else None
    except Exception as e:
        logging.error(f"❌ Failed to insert ticket for user_id={user_id}: {e}", exc_info=True)
        raise

# -----------------------------------------------------------------------------
# Users: Accept terms via temp table -> main users table
# -----------------------------------------------------------------------------
def mark_user_accepted_via_temp_table(whatsapp_number):
    """
    Moves a user from temp_opt_in_users to users table, and marks terms as accepted.
    If the user already exists, updates their terms_accepted status and timestamp.
    Uses naive Kenya time (MySQL-friendly).
    """
    engine = get_db_connection1()

    try:
        with engine.begin() as conn:
            user = conn.execute(text("""
                SELECT name, property_id, unit_number
                FROM temp_opt_in_users
                WHERE whatsapp_number = :num
            """), {"num": whatsapp_number}).fetchone()

            if not user:
                raise Exception(f"User {whatsapp_number} not found in temp_opt_in_users")

            name, property_id, unit_number = user[0], user[1], user[2]
            now_ke = kenya_now_db()  # ✅ naive Kenya time

            conn.execute(text("""
                INSERT INTO users
                    (name, whatsapp_number, property_id, unit_number, terms_accepted, terms_accepted_at)
                VALUES
                    (:name, :whatsapp_number, :property_id, :unit_number, 1, :terms_accepted_at)
                ON DUPLICATE KEY UPDATE
                    terms_accepted = 1,
                    terms_accepted_at = :terms_accepted_at
            """), {
                "name": name,
                "whatsapp_number": whatsapp_number,
                "property_id": property_id,
                "unit_number": unit_number,
                "terms_accepted_at": now_ke,
            })

            conn.execute(
                text("DELETE FROM temp_opt_in_users WHERE whatsapp_number = :num"),
                {"num": whatsapp_number},
            )

        logging.info(f"✅ Successfully registered and marked terms accepted for user {whatsapp_number}")
    except Exception as e:
        logging.error(f"❌ Error while registering user {whatsapp_number}: {e}", exc_info=True)
        raise

# -----------------------------------------------------------------------------
# Media: Save temp media references before ticket creation
# -----------------------------------------------------------------------------
def save_temp_media_to_db(sender_id, media_type, media_path, caption):
    """
    Saves a media reference temporarily in the database before ticket creation.
    Uses naive Kenya time (MySQL-friendly).
    """
    # NOTE: If your temp_ticket_media table does NOT have uploaded_at, remove it here.
    query = text("""
        INSERT INTO temp_ticket_media (sender_id, media_type, media_path, caption, uploaded_at)
        VALUES (:sender_id, :media_type, :media_path, :caption, :uploaded_at)
    """)

    try:
        engine = get_db_connection1()
        with engine.begin() as conn:
            conn.execute(query, {
                "sender_id": sender_id,
                "media_type": media_type,
                "media_path": media_path,
                "caption": caption,
                "uploaded_at": kenya_now_db(),  # ✅ naive Kenya time
            })

        logging.info(f"✅ Temp media saved for {sender_id}: {media_type} -> {media_path}")
        return True
    except Exception as e:
        logging.error(f"❌ Failed to insert temp media for {sender_id}: {e}", exc_info=True)
        return False
