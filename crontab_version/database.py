import pymysql
import sys
import os
from dotenv import load_dotenv

load_dotenv()
user  = os.getenv("USER")
db_host = os.getenv("DB_HOST")
password = os.getenv("PASSWORD")
port = os.getenv("PORT")
db_name = os.getenv("DB_NAME")

try:
    conn = pymysql.connect(
        host=db_host, 
        user=user, 
        password=password,
        port=int(port),
        database=db_name
        )
except pymysql.Error as e:
    raise e
    
    
    
cur = conn.cursor()


