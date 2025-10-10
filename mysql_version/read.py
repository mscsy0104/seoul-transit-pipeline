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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOGS_FOLDER, "read_table_data.log")),
        logging.StreamHandler(sys.stdout)
    ]
)

# constants
CHUNK_SIZE = 1000
THREADS = 4


'''
1. xml 파일 체크
2. csv 파일 체크
3. table 데이터 체크
'''

def read_sql(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()

read_sql = read_sql("./sql/read_transit.sql")

def list_xml_files():
    # 리스트, 최하단에 개수
    pass

def list_csv_files():
    # 리스트, 최하단에 개수
    pass

def list_latest_xml_files():
    # 리스트 7개, 최하단에 개수
    pass

def list_latest_csv_files():
    # 리스트 7개, 최하단에 개수
    pass




def main():
    pass

if __name__ == "__main__":
    main()