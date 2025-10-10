from select import connect_postgres


if __name__ == "__main__":
    
    try:
        conn = connect_postgres()

        cursor = conn.cursor()

        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"[DB 연결 성공] PostgreSQL version: {version[0]}")

    except Exception as e:
        print(f"애플리케이션 실행 중 오류 발생: {e}", exc_info=True)
        raise e
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("연결이 닫혔습니다.")
    