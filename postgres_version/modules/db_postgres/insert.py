from datetime import datetime
import os
from dotenv import load_dotenv

from postgres_version.modules.db_postgres.connect import connect_postgres
from postgres_version.modules.db_postgres.queries import DatabaseQueries

load_dotenv()

SCHEMA_NAME = os.getenv("POSTGRES_SCHEMA_NAME", "transit_schema")
TABLE_NAME = os.getenv("POSTGRES_TABLE_NAME", "seoul_transit_patterns")


def insert_data_to_transit_db(data):
    try:
        conn = connect_postgres()
        cursor = conn.cursor()

        print("데이터 삽입 중...")
        created_at = datetime.now()
        query = DatabaseQueries.insert_data_to_table(SCHEMA_NAME, TABLE_NAME)
        data = data + (created_at,)
        cursor.execute(query, data)
        conn.commit()
        print("데이터 삽입 완료")  
    except Exception as e:
        conn.rollback()
        print(f"애플리케이션 실행 중 오류 발생: {e}")
        raise e
    