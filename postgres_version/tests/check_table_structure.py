"""
테이블 구조를 확인하는 스크립트
"""
from postgres_version.db.connect import connect_postgres

SCHEMA_NAME = "transit_schema"
TABLE_NAME = "seoul_transit_pattern"

if __name__ == "__main__":
    try:
        conn = connect_postgres()
        cursor = conn.cursor()
        
        print("=== 테이블 구조 확인 ===")
        cursor.execute(f"""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_schema = '{SCHEMA_NAME}' 
            AND table_name = '{TABLE_NAME}'
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        print(f"테이블 '{SCHEMA_NAME}.{TABLE_NAME}' 구조:")
        for col in columns:
            print(f"  {col[0]}: {col[1]} (nullable: {col[2]}, default: {col[3]})")
        
        conn.close()
        
    except Exception as e:
        print(f"오류 발생: {e}")
        raise e
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("연결이 닫혔습니다.")
