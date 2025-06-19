# cleanup_script.py

from conn1 import get_db_connection1
from sqlalchemy.sql import text
import os
import logging
from datetime import datetime

# Optional: Setup basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def clean_up_uploaded_files():
    logging.info(f"🧹 Starting media cleanup: {datetime.now()}")

    try:
        engine = get_db_connection1()
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT media_path FROM ticket_media
                WHERE media_blob IS NOT NULL AND media_path IS NOT NULL
            """))
            file_paths = [row['media_path'] for row in result]

        deleted = 0
        missing = 0

        for path in file_paths:
            try:
                if os.path.exists(path):
                    os.remove(path)
                    deleted += 1
                    logging.info(f"🗑️ Deleted: {path}")
                else:
                    missing += 1
                    logging.warning(f"⚠️ File not found: {path}")
            except Exception as e:
                logging.error(f"❌ Error deleting {path}: {e}")

        logging.info(f"✅ Cleanup complete — Deleted: {deleted}, Missing: {missing}")

    except Exception as e:
        logging.error(f"❌ Cleanup failed: {e}")


if __name__ == "__main__":
    clean_up_uploaded_files()
