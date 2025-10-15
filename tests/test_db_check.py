from postgres_version.db.connect import connect_postgres
from postgres_version.db.queries import DatabaseQueries, DatabaseCheckQueries
from psycopg2 import sql
from pprint import pprint
# DB_NAME = "transit_db"
SCHEMA_NAME = "transit_schema"
TABLE_NAME = "seoul_transit_pattern"


if __name__ == "__main__":
    
    try:
        conn = connect_postgres()

        cursor = conn.cursor()
        print("=== 모든 DNS 파라미터 ===")
        print(cursor.connection.get_dsn_parameters())
        # 출력: {'user': 'sychoi', 'channel_binding': 'prefer', 'dbname': 'transit_db', 'host': 'localhost', 'port': '5432', 'options': '', 'sslmode': 'prefer', 'sslcompression': '0', 'sslcertmode': 'allow', 'sslsni': '1', 'ssl_min_protocol_version': 'TLSv1.2', 'gssencmode': 'prefer', 'krbsrvname': 'postgres', 'gssdelegation': '0', 'target_session_attrs': 'any', 'load_balance_hosts': 'disable'}
        print("-"*100)

        # 먼저 스키마와 테이블이 존재하는지 확인
        print("=== 모든 스키마 목록 ===")
        cursor.execute(DatabaseCheckQueries.get_all_schemas())
        schemas = cursor.fetchall()
        for schema in schemas:
            print(f"  {schema[0]}")
        # 출력:
        #     public
        #     transit_schema
        
        print(f"\n=== 스키마 '{SCHEMA_NAME}' 확인 ===")
        schema_query, schema_params = DatabaseCheckQueries.check_schema_exists(SCHEMA_NAME)
        cursor.execute(schema_query, schema_params)
        schema_exists = cursor.fetchone()
        print(f"Schema '{SCHEMA_NAME}' exists: {schema_exists is not None}")
        # 출력: Schema 'transit_schema' exists: True
        
        print(f"\n=== 테이블 '{SCHEMA_NAME}.{TABLE_NAME}' 확인 ===")
        table_query, table_params = DatabaseCheckQueries.check_table_exists(SCHEMA_NAME, TABLE_NAME)
        cursor.execute(table_query, table_params)
        table_exists = cursor.fetchone()
        print(f"Table '{SCHEMA_NAME}.{TABLE_NAME}' exists: {table_exists is not None}")
        # 출력: Table 'transit_schema.seoul_transit_pattern' exists: True
        
        print("\n=== 사용 가능한 테이블 목록 ===")
        cursor.execute(DatabaseCheckQueries.get_all_tables())
        tables = cursor.fetchall()
        if tables:
            for table in tables:
                print(f"  {table[0]}.{table[1]}")
        else:
            print("  사용 가능한 테이블이 없습니다.")
        # 출력: transit_schema.seoul_transit_pattern
        print("-"*100)

        # psycopg2.sql.Identifier를 사용한 안전한 쿼리 생성
        query = DatabaseCheckQueries.select_from_table_with_sql_identifier(SCHEMA_NAME, TABLE_NAME)
        print(f"Executing query: {query.as_string(conn)}")
        # Executing query: SELECT * FROM "transit_schema"."seoul_transit_pattern"
        print("-"*100)
        
        # 이제 쿼리 실행
        print(f"\n=== '{SCHEMA_NAME}.{TABLE_NAME}' 테이블 조회 ===")
        cursor.execute(DatabaseQueries.select_from_table(SCHEMA_NAME, TABLE_NAME, limit=10))
        results = cursor.fetchall()
        print(f"Query results ({len(results)} rows):")
        for row in results:
            print(row)
        # 출력: Query results (1 rows):
        #      (1, datetime.date(2025, 10, 13), '테스트', 1000, 800, 50, 80, 60, 10, datetime.date(2025, 10, 13), datetime.date(2025, 10, 13))

        conn.close()        

    except Exception as e:
        print(f"애플리케이션 실행 중 오류 발생: {e}")
        raise e
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("연결이 닫혔습니다.")
    

  