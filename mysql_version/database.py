import pymysql
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import sys
import requests
from datetime import datetime
import glob
import logging

load_dotenv()
LOGS_FOLDER = os.getenv("LOGS_FOLDER")
DATA_FOLDER = os.getenv("DATA_FOLDER")

API_KEY = os.getenv("API_KEY")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOGS_FOLDER, "fetch_data.log")),
        logging.StreamHandler(sys.stdout)
    ]
)


load_dotenv()
db_user  = os.getenv("DB_USER")
db_host = os.getenv("DB_HOST")
db_password = os.getenv("DB_PASSWORD")
port = os.getenv("PORT")
db_name = os.getenv("DB_NAME")
testdb_name = os.getenv("TESTDB_NAME")


def test_connect_pymysql():
    if not all([db_user, db_host, db_password, port, db_name]):
        raise ValueError("One or more environment variables are missing.")
    
    try:
        conn = pymysql.connect(
            host=db_host, 
            db_user=db_user, 
            password=db_password,
            port=int(port),
            database=testdb_name,
            charset='utf8' # utf8: 한글
            )
    except pymysql.Error as e:
        raise e
        
    print('testDB 연결완료, 연결제공')

    return conn


def connect_pymysql():
    if not all([db_user, db_host, db_password, port, db_name]):
        raise ValueError("One or more environment variables are missing.")
    
    try:
        conn = pymysql.connect(
            host=db_host, 
            user=db_user, 
            password=db_password,
            port=int(port),
            database=db_name,
            charset='utf8' # utf8: 한글
            )
    except pymysql.Error as e:
        raise e
        
    print('transitDB 연결완료, 연결제공')

    return conn


def connect_sqlalchemy():
    if not all([db_user, db_host, db_password, port, db_name]):
        raise ValueError("One or more environment variables are missing.")
    
    connection_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{port}/{db_name}?charset=utf8"
    engine = create_engine(connection_string)
    print('transitDB 연결완료, 엔진제공')

    return engine


try:
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_connect_pymysql()
    elif len(sys.argv) > 1 and sys.argv[1] == "pymysql":
        connect_pymysql()
    elif len(sys.argv) > 1 and sys.argv[1] == "sqlalchemy":
        connect_sqlalchemy()
    else:
        pass
except Exception as e:
    logging.critical(f"Unhandled exception: {e}", exc_info=True)
    raise e
