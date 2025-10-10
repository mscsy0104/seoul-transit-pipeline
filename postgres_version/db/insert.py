import os
import sys
import glob
import pandas as pd
import argparse
from dotenv import load_dotenv
import logging
from concurrent.futures import ThreadPoolExecutor

from database import *
import csv
from datetime import datetime


load_dotenv()
LOGS_FOLDER = os.getenv("LOGS_FOLDER")
DATA_FOLDER = os.getenv("DATA_FOLDER")
PARSED_FOLDER = os.getenv("PARSED_FOLDER")
TEST_CSV_PATH = os.getenv("TEST_CSV_PATH")

# constants
CHUNK_SIZE = 1000
THREADS = 4

def read_sql(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()

insert_sql = read_sql("./sql/insert_to_transit_patterns.sql")


def parse_int(value):
    try:
        return int(value) if value else None
    except (ValueError, TypeError):
        print(f"[WARN] 정수 변환 실패: '{value}' → NULL 처리")
        return None
    

def parse_date(value):
    try:
        return datetime.strptime(value.strip(), "%Y%m%d").date() if value else None
    except Exception as e:
        print(f"[WARN] 날짜 변환 실패: '{value}' → NULL 처리 ({e})")
        return None


def test_insert_csv_to_db():
    conn = connect_pymysql()
    try:
        csv_path = os.path.join(PARSED_FOLDER, TEST_CSV_PATH)
        with conn.cursor() as cur, open(csv_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)

            data = []
            for row in reader:
                data.append((
                    parse_date(row.get("CRTR_DD", "")),
                    row.get("PRPS_PTRN") or None,
                    parse_int(row.get("TNOPE", "")),
                    parse_int(row.get("TNOPE_GNRL", "")),
                    parse_int(row.get("TNOPE_KID", "")),
                    parse_int(row.get("TNOPE_YOUT", "")),
                    parse_int(row.get("TNOPE_ELDR", "")),
                    parse_int(row.get("TNOPE_PWDBS", "")),
                    TEST_CSV_PATH
                ))
            
            cur.executemany(insert_sql, data)         
            conn.commit()
    finally:
        conn.close()


def insert_chunk(data_chunk):
    conn = connect_pymysql()
    try:
        with conn.cursor() as cur:            
            cur.executemany(insert_sql, data_chunk)         
            conn.commit()
    finally:
        cur.close()
        conn.close()


def insert_datum_with_threadpool(csv_file):
    df = pd.read_csv(csv_file, dtype=str).fillna('')
    records = []

    for _, row in df.iterrows():
        records.append((
            parse_date(row.get("CRTR_DD")),
            row.get("PRPS_PTRN") or None,
            parse_int(row.get("TNOPE")),
            parse_int(row.get("TNOPE_GNRL")),
            parse_int(row.get("TNOPE_KID")),
            parse_int(row.get("TNOPE_YOUT")),
            parse_int(row.get("TNOPE_ELDR")),
            parse_int(row.get("TNOPE_PWDBS")),
            TEST_CSV_PATH
        ))

    chunks = [
        records[i:i + CHUNK_SIZE] for i in range(0, len(records), CHUNK_SIZE)
    ]

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        executor.map(insert_chunk, chunks)



def insert_data_with_threadpool():
    csv_files = glob.glob(os.path.join(PARSED_FOLDER, "*.csv"))
    print(f"총 {len(csv_files)}개의 파일 발견")
    logging.info(f"insert_data_with_threadpool: {len(csv_files)} files to process")

    for file in csv_files:
        logging.info(f"Parsing CSV file: {file}")
        try:
            insert_datum_with_threadpool(file)
        except Exception as e:
            logging.error(f"Error processing {file}: {e}", exc_info=True)


def main():
    parser = argparse.ArgumentParser(description="CSV 파일 DB 테이블 적재 모드 선택")
    parser.add_argument("mode", choices=[ "individual", "single", "test"], nargs="?", default="single")
    args = parser.parse_args()

    try:
        if args.mode == "individual":
            logging.info(f"Mode: individual")
            insert_data_with_threadpool()

        elif args.mode == "single":
            logging.info(f"Mode: single")
            csv_path = input("DB에 적재할 csv 파일경로를 입력하세요: ").strip()
            insert_datum_with_threadpool(csv_path)

        elif args.mode == "test":
            logging.info(f"Mode: test")
            test_insert_csv_to_db()

        else:
            logging.info("No valid mode specified. Exiting.")
    except Exception as e:
        logging.critical(f"Unhandled exception: {e}", exc_info=True)
        raise e


if __name__ == "__main__":
    main()