import os
from dotenv import load_dotenv
import psycopg2


load_dotenv()

POSTGRES_DB_USER  = os.getenv("POSTGRES_DB_USER")
POSTGRES_DB_HOST = os.getenv("POSTGRES_DB_HOST")
POSTGRES_DB_PASSWORD = os.getenv("POSTGRES_DB_PASSWORD")
POSTGRES_DB_PORT = os.getenv("POSTGRES_DB_PORT")
POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME")

# --------------------------------------------------
def connect_postgres():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_DB_HOST,
            database=POSTGRES_DB_NAME,
            user=POSTGRES_DB_USER,
            password=POSTGRES_DB_PASSWORD,
            port=POSTGRES_DB_PORT
        )
        return conn
    except Exception as e:
        print(f"[DB 연결 실패] {e}")
        raise e