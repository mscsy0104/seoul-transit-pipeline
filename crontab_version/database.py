import pymysql
from sqlalchemy import create_engine
import sys
import os
from dotenv import load_dotenv


load_dotenv()
user  = os.getenv("USER")
db_host = os.getenv("DB_HOST")
password = os.getenv("PASSWORD")
port = os.getenv("PORT")
db_name = os.getenv("DB_NAME")


def connect_pymysql():
    if not all([user, db_host, password, port, db_name]):
        raise ValueError("One or more environment variables are missing.")
    
    try:
        conn = pymysql.connect(
            host=db_host, 
            user=user, 
            password=password,
            port=int(port),
            database=db_name,
            charset='utf8' # utf8: 한글
            )
    except pymysql.Error as e:
        raise e
        
    cur = conn.cursor()
    print('연결완료, 커서제공')

    return cur

def connect_sqlalchemy():
    if not all([user, db_host, password, port, db_name]):
        raise ValueError("One or more environment variables are missing.")
    
    connection_string = f"mysql+pymysql://{user}:{password}@{db_host}:{port}/{db_name}?charset=utf8"
    engine = create_engine(connection_string)
    print('연결완료, 엔진제공')

    return engine