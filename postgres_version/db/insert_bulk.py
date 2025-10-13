import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

from postgres_version.db.connect import connect_postgres
from postgres_version.db.queries import DatabaseQueries


load_dotenv()

SCHEMA_NAME = os.getenv("SCHEMA_NAME", "transit_schema")
TABLE_NAME = os.getenv("TABLE_NAME", "seoul_transit_pattern")


def insert_bulk_to_transit_db(data):
    try:
        conn = connect_postgres()
        cursor = conn.cursor()

        print("데이터 벌크 삽입 중...")
        query = DatabaseQueries.insert_batch_data_to_table(SCHEMA_NAME, TABLE_NAME)
        execute_values(cursor, query, data)
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