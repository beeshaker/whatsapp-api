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
    """Saves a media file linked to a ticket in the ticket_media table."""
    engine = get_db_connection1()
    query = """
        INSERT INTO ticket_media (ticket_id, media_type, media_path)
        VALUES (:ticket_id, :media_type, :media_path)
    """
    with engine.connect() as conn:
        conn.execute(text(query), {
            "ticket_id": ticket_id,
            "media_type": media_type,
            "media_path": file_path
        })
        conn.commit()