import os
import sys
import requests
from dotenv import load_dotenv
import re
from datetime import datetime
import glob

from utils import measure_time, ensure_dir_exists


load_dotenv()
LOGS = os.getenv("LOGS")
DATA_DIR = os.getenv("DATA_DIR")
DB_KIND = "postgres"

API_KEY = os.getenv("API_KEY")

count_pattern = r"<list_total_count>(\d+)</list_total_count>"
name_pattern = r'^\s*<\?xml.*?\?>\s*<([a-zA-Z0-9_]+)>'




def test_fetch_data():
    print("Running test fetch data")
    
    start_idx = 1
    end_idx = 1
    url = f'http://openapi.seoul.go.kr:8088/{API_KEY}/xml/ksccPatternStation/{start_idx}/{end_idx}'
    
    try:
        res = requests.get(url)
        text = res.text
        cnt = int(re.search(count_pattern, text).group(1))
        name = re.search(name_pattern, text).group(1)
        print(f"Test fetch successful. Name: {name}, Total count: {cnt}")
    except Exception as e:
        print(f"Error during test fetch: {e}")
        raise e
    

def fetch_total_count():
    print("Fetching total count")
    start_idx = 1
    end_idx = 1
    url = f'http://openapi.seoul.go.kr:8088/{API_KEY}/xml/ksccPatternStation/{start_idx}/{end_idx}'
    
    try:
        res = requests.get(url)
        text = res.text
        cnt = int(re.search(count_pattern, text).group(1))
        print(f"Total count fetched: {cnt}")
        return cnt
    except Exception as e:
        print(f"Error fetching total count: {e}")
        raise e


@measure_time
def fetch_bulk_data(total_cnt):
    print(f"Starting bulk data fetch for total count: {total_cnt}")
    xml_data = []

    start = 1
    end = int(total_cnt)

    diff = end - start + 1
    step = 1000 if diff >= 1000 else diff

    for i in range(start, end + 1, step):
        start_idx = i
        end_idx = i + step - 1 if i + step - 1 < end else end
        print(f"Fetching data from {start_idx} to {end_idx}")

        # ex. http://openapi.seoul.go.kr:8088/(인증키)/xml/ksccPatternStation/1/5/
        url = f'http://openapi.seoul.go.kr:8088/{API_KEY}/xml/ksccPatternStation/{start_idx}/{end_idx}'

        try:
            res = requests.get(url)
            res.raise_for_status()
            data = (start_idx, end_idx, res.text)
            xml_data.append(data)
        except requests.exceptions.RequestException as e:
            # error
            print(f"Error fetching data from {start_idx} to {end_idx}: {e}")
            continue

    for idx, data in enumerate(xml_data):
        start_idx, end_idx, res_text = data
        filename = os.path.join(DATA_DIR, DB_KIND, f'ksccPatternStation_{start_idx}_{end_idx}_{idx + 1}.xml')
        ensure_dir_exists(filename)
        with open(filename, "w", encoding="utf-8") as f:
            f.write(res_text)
            print(f"Saved data to {filename}")



def fetch_incremental_data(total_cnt):
    print(f"Starting incremental data fetch for total count: {total_cnt}")

    list_of_files = glob.glob(os.path.join(DATA_DIR, '/*'))
    print(f"list_of_files: {list_of_files}")
    # list_of_files = glob.glob(os.path.join(os.getcwd(), 'data/*'))
    if list_of_files:
        latest_file = max(list_of_files, key=os.path.getctime)
        print(f"Most recent file: {latest_file}")
    else:
        # warning
        print("No files found in the data directory. Check the env file.")
        return
    
    start = int(latest_file.split('_')[3]) + 1
    end = total_cnt

    diff = end - start + 1
    step = 1000 if diff >= 1000 else diff

    xml_data = []

    for i in range(start, end + 1, step):
        start_idx = i
        end_idx = i + step - 1 if i + step - 1 < end else end
        print(f"Fetching data from {start_idx} to {end_idx}")

        url = f'http://openapi.seoul.go.kr:8088/{API_KEY}/xml/ksccPatternStation/{start_idx}/{end_idx}'

        try:
            res = requests.get(url)
            res.raise_for_status()
            data = (start_idx, end_idx, res.text)
            xml_data.append(data)
        except requests.exceptions.RequestException as e:
            # error
            print(f"Error fetching data from {start_idx} to {end_idx}: {e}")
            continue

    for data in xml_data:
        today = datetime.now().strftime("%Y%m%d%H%M%S")
        start_idx, end_idx, res_text = data
        filename = os.path.join(DATA_DIR, DB_KIND, f'ksccPatternStation_{start_idx}_{end_idx}_{today}.xml')
        ensure_dir_exists(filename)
        with open(filename, "w", encoding="utf-8") as f:
            f.write(res_text)
            print(f"Saved incremental data to {filename}")


try:
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_fetch_data()
    elif len(sys.argv) > 1 and sys.argv[1] == "bulk":
        fetch_bulk_data(fetch_total_count())
    elif len(sys.argv) > 1 and sys.argv[1] == "incremental":
        fetch_incremental_data(fetch_total_count())
    elif len(sys.argv) > 1 and sys.argv[1] == "count":
        fetch_total_count()
    else:
        pass
except Exception as e:
    # critical
    print(f"Unhandled exception: {e}", exc_info=True)
    raise e
