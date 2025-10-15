
from postgres_version.db.connect import connect_postgres
from psycopg2 import sql
from datetime import datetime
import os
from dotenv import load_dotenv
from datetime import date

from postgres_version.db.queries import DatabaseQueries

load_dotenv()

SCHEMA_NAME = os.getenv("SCHEMA_NAME", "transit_schema")
TABLE_NAME = os.getenv("TABLE_NAME", "seoul_transit_pattern")

data = (
        date.today(),           # create_date
        "테스트",     # purpose_pattern
        9999,                   # total_people
        8000,                   # general_people
        500,                    # kid_people
        800,                    # youth_people
        200,                    # elder_people
        100                     # disabled_people
    )
if __name__ == "__main__":
    try:
        conn = connect_postgres()
        cursor = conn.cursor()

        print("테스트 데이터 삽입 중...")
        created_at = datetime.now()
        query = DatabaseQueries.insert_data_to_table(SCHEMA_NAME, TABLE_NAME)
        data = data + (created_at,)
        cursor.execute(query, data)
        conn.commit()
        print("테스트 데이터 삽입 완료")

        # 이제 쿼리 실행
        print(f"\n=== '{SCHEMA_NAME}.{TABLE_NAME}' 테이블 조회 ===")
        
        created_at = datetime.now()
        cursor.execute(f"SELECT * from {SCHEMA_NAME}.{TABLE_NAME} where created_at = %s", (created_at,))
        results = cursor.fetchall()
        print(f"Query results: {results}")        

        conn.close()

    except Exception as e:
        print(f"애플리케이션 실행 중 오류 발생: {e}")
        raise e
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("연결이 닫혔습니다.")
    

  
