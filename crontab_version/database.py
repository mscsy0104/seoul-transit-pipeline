import pymysql
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv


load_dotenv()
user  = os.getenv("USER")
db_host = os.getenv("DB_HOST")
password = os.getenv("PASSWORD")
port = os.getenv("PORT")
db_name = os.getenv("DB_NAME")
testdb_name = os.getenv("TESTDB_NAME")


def test_connect_pymysql():
    if not all([user, db_host, password, port, db_name]):
        raise ValueError("One or more environment variables are missing.")
    
    try:
        conn = pymysql.connect(
            host=db_host, 
            user=user, 
            password=password,
            port=int(port),
            database=testdb_name,
            charset='utf8' # utf8: 한글
            )
    except pymysql.Error as e:
        raise e
        
    print('testDB 연결완료, 연결제공')

    return conn


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
        
    print('transitDB 연결완료, 연결제공')

    return conn


def connect_sqlalchemy():
    if not all([user, db_host, password, port, db_name]):
        raise ValueError("One or more environment variables are missing.")
    
    connection_string = f"mysql+pymysql://{user}:{password}@{db_host}:{port}/{db_name}?charset=utf8"
    engine = create_engine(connection_string)
    print('transitDB 연결완료, 엔진제공')

    return engine