import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

from postgres_version.db.connect import connect_postgres
from postgres_version.db.queries import DatabaseQueries


load_dotenv()

SCHEMA_NAME = os.getenv("SCHEMA_NAME", "transit_schema")
TABLE_NAME = os.getenv("TABLE_NAME", "seoul_transit_pattern")

def insert_bulk_to_transit_db(data):
    try:
        conn = connect_postgres()
        cursor = conn.cursor()
        query = DatabaseQueries.insert_batch_data_to_table(SCHEMA_NAME, TABLE_NAME)
        created_at = datetime.now()
        # 각 행에 created_at을 추가
        data_with_created_at = [row + (created_at,) for row in data]
        execute_values(cursor, query, data_with_created_at)
        conn.commit()
        print(f"데이터 벌크 삽입 완료: {len(data)}개 행")
    except (Exception, psycopg2.Error) as e:
        conn.rollback()
        print(f"PostgreSQL 벌크 삽입 오류: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
            print("연결이 닫혔습니다.")
    


def insert_bulk_incremental_to_transit_db(data):
    try:
        conn = connect_postgres()
        cursor = conn.cursor()
        query = DatabaseQueries.insert_batch_data_to_table(SCHEMA_NAME, TABLE_NAME)
        # data에 이미 created_at이 포함되어 있음
        execute_values(cursor, query, data)
        conn.commit()
        print(f"증분 데이터 벌크 삽입 완료: {len(data)}개 행")
    except (Exception, psycopg2.Error) as e:
        conn.rollback()
        print(f"PostgreSQL 벌크 삽입 오류: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
            print("연결이 닫혔습니다.")