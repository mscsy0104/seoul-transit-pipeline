
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

from postgres_version.modules.db_postgres.insert_bulk import insert_bulk_incremental_to_transit_db
from postgres_version.modules.db_postgres.connect import connect_postgres

load_dotenv()

DATA_PARSED_DIR = os.getenv("DATA_PARSED_DIR")
DB_KIND = os.getenv("DB_KIND")

CHUNK_SIZE = 500


def process_row(row):
    """CSV 행을 데이터베이스에 맞게 변환"""
    # 날짜를 정수로 변환 (YYYYMMDD 형식 그대로 사용)
    date_str = str(row['CRTR_DD'])
    if len(date_str) == 8:
        create_date = int(date_str)
    else:
        create_date = int(datetime.now().strftime("%Y%m%d"))
    # 빈 값을 0으로 변환
    def safe_int(value):
        if pd.isna(value) or value == '' or value is None:
            return 0
        try:
            return int(value)
        except (ValueError, TypeError):
            return 0
    created_at = datetime.now()
    return (
        create_date,                      # create_date (bigint)
        str(row['PRPS_PTRN']),            # purpose_pattern
        safe_int(row['TNOPE']),           # total_people
        safe_int(row['TNOPE_GNRL']),      # general_people
        safe_int(row['TNOPE_KID']),       # kid_people
        safe_int(row['TNOPE_YOUT']),      # youth_people
        safe_int(row['TNOPE_ELDR']),      # elder_people
        safe_int(row['TNOPE_PWDBS']),     # disabled_people
        created_at                        # created_at
    )


def upload_incremental_data_from_df(df):
    """
    DataFrame에서 데이터를 읽어서 데이터베이스에 업로드합니다.
    연결을 한 번 열고 모든 청크를 처리한 후 닫습니다.
    """
    conn = None
    try:
        # 연결을 한 번만 열기
        conn = connect_postgres()
        print("데이터베이스 연결을 열었습니다.")
        
        for start in range(0, len(df), CHUNK_SIZE):
            end = start + CHUNK_SIZE
            chunk = df.iloc[start:end]
            data_to_insert_chunk = [process_row(row) for _, row in chunk.iterrows()]
            
            try:
                # 기존 연결을 재사용
                insert_bulk_incremental_to_transit_db(data_to_insert_chunk, conn)
                print(f"[{start} ~ {end-1}] 청크 ({len(data_to_insert_chunk)}개 행) 삽입 완료")
                
            except Exception as e:
                print(f"[{start} ~ {end-1}] 청크 삽입 실패: {e}")
                # 오류 발생 시 롤백
                conn.rollback()
                raise
                
    except Exception as e:
        print(f"업로드 중 오류 발생: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        # 모든 작업이 끝난 후 연결 닫기
        if conn:
            conn.close()
            print("데이터베이스 연결을 닫았습니다.")