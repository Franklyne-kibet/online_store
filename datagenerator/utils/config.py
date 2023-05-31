import os

from utils.db import DBConnection

def get_database_creds() -> DBConnection:
    return DBConnection(
        user=os.getenv('DB_USER',''),
        password=os.getenv('DB_PASSWORD',''),
        db=os.getenv('DB', ''),
        host=os.getenv('DB_HOST',''),
        port=int(os.getenv('DB_PORT', 5432)),
    )