from postgres_version.db.insert import insert_bulk_to_transit_db
import pandas as pd
from datetime import datetime


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
    
    return (
        create_date,                      # create_date (bigint)
        str(row['PRPS_PTRN']),            # purpose_pattern
        safe_int(row['TNOPE']),           # total_people
        safe_int(row['TNOPE_GNRL']),      # general_people
        safe_int(row['TNOPE_KID']),       # kid_people
        safe_int(row['TNOPE_YOUT']),      # youth_people
        safe_int(row['TNOPE_ELDR']),      # elder_people
        safe_int(row['TNOPE_PWDBS'])      # disabled_people
    )

# ---------------------------------------
# example_path = "/Users/sychoi/projects/seoul-transit-pipeline/data_parsed/postgres/example.csv"
# df = pd.read_csv(example_path)

# print(f"CSV 데이터 로드 완료: {len(df)}개 행")
# print("첫 5행 데이터:")
# print(df.head())
# # 테스트로 처음 5개 행만 처리
# print("\n처리할 데이터 샘플:")
# for i, (_, row) in enumerate(df.head(5).iterrows()):
#     processed_data = process_row(row)
#     print(f"행 {i+1}: {processed_data}")

# print("\n데이터 삽입 시작...")
# for i, (_, row) in enumerate(df.head(5).iterrows()):
#     try:
#         processed_data = process_row(row)
#         insert_data_to_transit_db(processed_data)
#         print(f"행 {i+1} 삽입 완료")
#     except Exception as e:
#         print(f"행 {i+1} 삽입 실패: {e}")

# print("데이터 삽입 완료!")

from pprint import pprint
import os
import glob
from dotenv import load_dotenv

load_dotenv()

DATA_PARSED_DIR = os.getenv("DATA_PARSED_DIR")
DB_KIND = os.getenv("DB_KIND")
CHUNK_SIZE = 500

today = datetime.now().strftime("%Y%m%d")
files = glob.glob(os.path.join(DATA_PARSED_DIR, DB_KIND, "*.csv"))

files_to_upload = []
for file in files:
    if os.path.basename(file) == "example.csv":
        continue
    
    # 빈 파일 건너뛰기
    if os.path.getsize(file) <= 1:
        print(f"빈 파일 건너뛰기: {os.path.basename(file)}")
        continue

    if today in file:
        files_to_upload.append(file)

print("-"*100)
print()
print(f"files_to_upload: {len(files_to_upload)}")
pprint([os.path.basename(f) for f in files_to_upload])
print()
print("-"*100)

for file in files_to_upload:
    try:
        df = pd.read_csv(file)
        # print(f"df 행 수:{len(df)}")
        # print(df.head())
    except pd.errors.EmptyDataError:
        print(f"빈 데이터 파일 건너뛰기: {os.path.basename(file)}")
        # 폴더에서 눈에 보이진 않지만 없는 파일이 존재: ksccPatternStation_30001_31000_31.csv
        continue


    for start in range(0, len(df), CHUNK_SIZE):
        end = start + CHUNK_SIZE
        chunk = df.iloc[start:end]
        data_to_insert_chunk = [process_row(row) for _, row in chunk.iterrows()]
    
        pprint(data_to_insert_chunk)
        try:
            insert_bulk_to_transit_db(data_to_insert_chunk)
            print(f"[{start} ~ {end-1}] 청크 ({len(data_to_insert_chunk)}개 행) 삽입 완료")
            
        except Exception as e:
            print(f"[{start} ~ {end-1}] 청크 삽입 실패: {e}")