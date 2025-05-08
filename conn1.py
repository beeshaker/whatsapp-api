from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from sqlalchemy.sql import text


load_dotenv()

# Database Connection using SQLAlchemy
DB_URI = f"mysql+mysqlconnector://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
#DB_URI = f"mysql+mysqlconnector://{st.secrets.DB_USER}:{st.secrets.DB_PASSWORD}@{st.secrets.DB_HOST}/{st.secrets.DB_NAME}"  # Using Streamlit secrets('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
def get_db_connection1():
    return create_engine(DB_URI)




def save_ticket_media(ticket_id, media_type, file_path):
    """Saves media content (as blob) linked to a ticket in the database."""
    engine = get_db_connection1()
    query = """
        INSERT INTO ticket_media (ticket_id, media_type, media_path, media_blob)
        VALUES (:ticket_id, :media_type, :media_path, :media_blob)
    """

    with open(file_path, "rb") as f:
        binary_content = f.read()

    with engine.connect() as conn:
        conn.execute(text(query), {
            "ticket_id": ticket_id,
            "media_type": media_type,
            "media_path": file_path,
            "media_blob": binary_content
        })
        conn.commit()

        
        
def insert_ticket_and_get_id(user_id, description, category, property, assigned_admin):
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
            "assigned_admin": assigned_admin
        })
        result = conn.execute(select_query).fetchone()
        conn.commit()
        return result[0]
