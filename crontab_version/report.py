import os
from glob import glob

from dotenv import load_dotenv
import pymysql
from sqlalchemy import create_engine
import logging
import sys

from database import connect_pymysql

load_dotenv()
LOGS_FOLDER = os.getenv("LOGS_FOLDER")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOGS_FOLDER, "report_data.log")),
        logging.StreamHandler(sys.stdout)
    ]
)

data_dir = '/Users/sychoi/projects/seoul-trainsits-pipeline/crontab_version/data'
xml_files = sorted(glob(os.path.join(data_dir, '*.xml')), key=os.path.getmtime, reverse=True)

print(f"XML 파일 개수: {len(xml_files)}")
print("최근 5개 파일명:")
for fname in xml_files[:5]:
    print(os.path.basename(fname))


parsed_dir = '/Users/sychoi/projects/seoul-trainsits-pipeline/crontab_version/parsed'
csv_files = sorted(glob(os.path.join(parsed_dir, '*.csv')), key=os.path.getmtime, reverse=True)

print(f"CSV 파일 개수: {len(csv_files)}")
print("최근 5개 파일명:")



conn = connect_pymysql()
cursor = conn.cursor()

# 테이블 이름을 지정하세요 (예: 'your_table_name')
table_name = 'transit_patterns'

# 레코드 개수 출력
cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
count = cursor.fetchone()[0]
print(f"테이블 레코드 개수: {count}")

# 최근 5개 데이터의 file_path 출력
cursor.execute(f"SELECT DISTINCT file_path FROM {table_name} ORDER BY id DESC LIMIT 5")
rows = cursor.fetchall()
print("최근 데이터 file_path:")
for row in rows:
    print(row[0])

cursor.close()
conn.close()
