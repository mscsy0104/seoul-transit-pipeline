from postgres_version.db.connect import connect_postgres

if __name__ == "__main__":
    try:
        conn = connect_postgres()
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()
            print(f"[DB 연결 성공] PostgreSQL version: {version[0]}")
    except Exception as e:
        print(f"[DB 연결 실패] {e}")