import os
import sys
import requests
from dotenv import load_dotenv
import re
from datetime import datetime
import glob
import logging
import dotenv

load_dotenv()
LOGS_FOLDER = os.getenv("LOGS_FOLDER")
DATA_FOLDER = os.getenv("DATA_FOLDER")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOGS_FOLDER, "fetch_data.log")),
        logging.StreamHandler(sys.stdout)
    ]
)

load_dotenv()

API_KEY = os.getenv("API_KEY")


def ensure_directory_exists(filename):
    dirname = os.path.dirname(filename)

    if not os.path.exists(dirname):
        os.makedirs(dirname)
        logging.info(f"Created directory: {dirname}")


def fetch_bulk_data(total_cnt):
    logging.info(f"Starting bulk data fetch for total count: {total_cnt}")
    xml_data = []

    start = 1
    end = int(total_cnt)

    diff = end - start + 1
    step = 1000 if diff >= 1000 else diff

    for i in range(start, end + 1, step):
        start_idx = i
        end_idx = i + step - 1 if i + step - 1 < end else end
        logging.info(f"Fetching data from {start_idx} to {end_idx}")

        url = f'http://openapi.seoul.go.kr:8088/{API_KEY}/xml/ksccPatternStation/{start_idx}/{end_idx}'

        try:
            res = requests.get(url)
            res.raise_for_status()
            data = (start_idx, end_idx, res.text)
            xml_data.append(data)
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data from {start_idx} to {end_idx}: {e}")
            continue

    for idx, data in enumerate(xml_data):
        start_idx, end_idx, res_text = data
        filename = os.path.join(DATA_FOLDER, 'ksccPatternStation_{start_idx}_{end_idx}_{idx + 1}.xml')
        ensure_directory_exists(filename)
        with open(filename, "w", encoding="utf-8") as f:
            f.write(res_text)
            logging.info(f"Saved data to {filename}")


def test_fetch_data():
    logging.info("Running test fetch data")
    
    start_idx = 1
    end_idx = 1
    url = f'http://openapi.seoul.go.kr:8088/{API_KEY}/xml/ksccPatternStation/{start_idx}/{end_idx}'
    
    try:
        res = requests.get(url)
        text = res.text
        cnt = int(re.search(r"<list_total_count>(\d+)</list_total_count>", text).group(1))
        name = re.search(r'^\s*<\?xml.*?\?>\s*<([a-zA-Z0-9_]+)>', text).group(1)
        logging.info(f"Test fetch successful. Name: {name}, Total count: {cnt}")
    except Exception as e:
        logging.error(f"Error during test fetch: {e}")
        raise e
    

def fetch_total_count():
    logging.info("Fetching total count")
    start_idx = 1
    end_idx = 1
    url = f'http://openapi.seoul.go.kr:8088/{API_KEY}/xml/ksccPatternStation/{start_idx}/{end_idx}'
    
    try:
        res = requests.get(url)
        text = res.text
        cnt = int(re.search(r"<list_total_count>(\d+)</list_total_count>", text).group(1))
        logging.info(f"Total count fetched: {cnt}")
        return cnt
    except Exception as e:
        logging.error(f"Error fetching total count: {e}")
        raise e
    

def fetch_incremental_data(total_cnt):
    logging.info(f"Starting incremental data fetch for total count: {total_cnt}")

    list_of_files = glob.glob(os.path.join(os.getcwd(), 'data/*'))
    if list_of_files:
        latest_file = max(list_of_files, key=os.path.getctime)
        logging.info(f"Most recent file: {latest_file}")
    else:
        logging.warning("No files found in the ./data directory.")
        return
    
    start = int(latest_file.split('_')[3]) + 1
    end = total_cnt

    diff = end - start + 1
    step = 1000 if diff >= 1000 else diff

    xml_data = []

    for i in range(start, end + 1, step):
        start_idx = i
        end_idx = i + step - 1 if i + step - 1 < end else end
        logging.info(f"Fetching data from {start_idx} to {end_idx}")

        url = f'http://openapi.seoul.go.kr:8088/{API_KEY}/xml/ksccPatternStation/{start_idx}/{end_idx}'

        try:
            res = requests.get(url)
            res.raise_for_status()
            data = (start_idx, end_idx, res.text)
            xml_data.append(data)
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data from {start_idx} to {end_idx}: {e}")
            continue

    for data in xml_data:
        today = datetime.now().strftime("%Y%m%d%H%M%S")
        start_idx, end_idx, res_text = data
        filename = os.path.join(DATA_FOLDER, f'ksccPatternStation_{start_idx}_{end_idx}_{today}.xml')
        ensure_directory_exists(filename)
        with open(filename, "w", encoding="utf-8") as f:
            f.write(res_text)
            logging.info(f"Saved incremental data to {filename}")


try:
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_fetch_data()
    elif len(sys.argv) > 1 and sys.argv[1] == "bulk":
        fetch_bulk_data(fetch_total_count())
    else:
        fetch_incremental_data(fetch_total_count())
except Exception as e:
    logging.critical(f"Unhandled exception: {e}", exc_info=True)
    raise e
