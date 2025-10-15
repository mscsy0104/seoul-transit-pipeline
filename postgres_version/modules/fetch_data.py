import os
import sys
import requests
from dotenv import load_dotenv
import re
from datetime import datetime
import glob
import pandas as pd

from utils import measure_time, ensure_dir_exists
from parse_data import parse_xml, process_xml_from_text, parse_xml_from_text


load_dotenv()
LOGS = os.getenv("LOGS")
DATA_DIR = os.getenv("DATA_DIR")
DATA_PARSED_DIR = os.getenv("DATA_PARSED_DIR")
DB_KIND = os.getenv("DB_KIND")

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

    today = datetime.now().strftime("%Y%m%d")
    for idx, data in enumerate(xml_data):
        start_idx, end_idx, res_text = data
        filename = os.path.join(DATA_DIR, DB_KIND, f'ksccPatternStation_{today}_{start_idx}_{end_idx}_{idx + 1}.xml')
        ensure_dir_exists(filename)
        with open(filename, "w", encoding="utf-8") as f:
            f.write(res_text)
            print(f"Saved data to {filename}")


@measure_time
def fetch_incremental_data(total_cnt):
    print(f"Starting incremental data fetch for total count: {total_cnt}")

    today = datetime.now().strftime("%Y%m%d")

    list_of_files = glob.glob(os.path.join(DATA_DIR, DB_KIND, '*.xml'))
    # print(f"list_of_files: {list_of_files}")
    if list_of_files:
        # 폴더 내 파일 필터링 1번째 조건: extract_date_int 최댓값, 2번째 조건: extract_number 최솟값
        def extract_date_int(file_path):
            try:
                return int(os.path.basename(file_path).split("_")[1])
            except (IndexError, ValueError):
                return -1
        def extract_number(file_path):
            try:
                return int(os.path.basename(file_path).split("_")[-1].split(".")[0])
            except (IndexError, ValueError):
                return -1

        # 1. extract_date_int가 최댓값인 것들만 필터
        max_date = max(map(extract_date_int, list_of_files))
        files_with_max_date = [f for f in list_of_files if extract_date_int(f) == max_date]
        # 2. 그중 extract_number가 최솟값인 파일 선택
        latest_file = min(files_with_max_date, key=extract_number)

        print(f"Most recent file: {latest_file}")
        
        parsed_latest_df = parse_xml(latest_file)
        standard_create_date = int(parsed_latest_df["CRTR_DD"].max())
        print(f"standard_create_date: {standard_create_date}")

        if 'PRPS_PTRN' in parsed_latest_df.columns:
            standard_purpose_pattern_col = parsed_latest_df["PRPS_PTRN"]        
    else:
        # warning
        print("No files found in the data directory. Check the env file.")
        print()
        print("-"*100)

        # 공공데이터 예시 데이터 API
        url = f'http://openapi.seoul.go.kr:8088/{API_KEY}/xml/ksccPatternStation/1/5'
        try:
            res = requests.get(url)
            res.raise_for_status()
            filename = os.path.join(DATA_DIR, DB_KIND, f'ksccPatternStation_incremental_example_{today}_0.xml')
            ensure_dir_exists(filename)
            with open(filename, "w", encoding="utf-8") as f:
                f.write(res.text)
                print(f"Saved incremental data to {filename}")
        except requests.exceptions.RequestException as e:
            # error
            print(f"Error fetching data from 1 to 5: {e}")
    
        return
    
    start = 1
    step = 500
    end = total_cnt

    df_li_to_download = []
    for start_idx in range(start, end + 1, step):
        end_idx = start_idx + step - 1 if start_idx + step - 1 < end else end
        # 실제 다운로드에는 start_idx, end_idx를 사용 (이후 코드와 호환)
        # 아래 코드 본문에서 사용할 변수를 덮어쓰기 위해 break로 반복 종료
        print(f"Fetching data from {start_idx} to {end_idx}")
        url = f'http://openapi.seoul.go.kr:8088/{API_KEY}/xml/ksccPatternStation/{start_idx}/{end_idx}'
        try:
            res = requests.get(url)
            res.raise_for_status()
            parsed_df = parse_xml_from_text(res.text, f"batch_{start_idx}_{end_idx}")
            print(f"Batch {start_idx + 1}: {len(parsed_df)} rows parsed")

            create_date_col = parsed_df["CRTR_DD"]
            max_create_date = create_date_col.astype(int).max()
            min_create_date = create_date_col.astype(int).min()
            if max_create_date < standard_create_date and min_create_date < standard_create_date:
                print(f"Stop fetching data from {start_idx} to {end_idx}")
                print(f"because of standard_create_date more than {max_create_date} and {min_create_date}")
                break

            df_li_to_download.append(parsed_df)
        except requests.exceptions.RequestException as e:
            # error
            print(f"Error fetching data from {start_idx} to {end_idx}: {e}")
            raise e

    print(f"final end idx: {end_idx}")
    
    # 모든 데이터를 하나의 DataFrame으로 합치기
    if df_li_to_download:
        print("Merging all data...")
        final_df = pd.concat(df_li_to_download, ignore_index=True)
        print(f"Total rows before filtering: {len(final_df)}")
        
        # print(final_df.head())
        # create_date 필터링 (CRTR_DD 컬럼 사용)
        if 'CRTR_DD' in final_df.columns:
            # create_date와 비교 (incremental의 경우)
            filter_date = str(standard_create_date)
            print(f"Filtering with max_create_date: {filter_date}")
            # if
            #     filter_date = str(standard_create_date)
            #     print(f"Filtering with max_create_date: {filter_date}")
            # else:
            #     # bulk의 경우 오늘 날짜 사용
            #     filter_date = datetime.now().strftime('%Y%m%d')
            #     print(f"Filtering with today's date: {filter_date}")
            
            # CRTR_DD가 필터 날짜보다 작은 row들 제거
            before_filter_count = len(final_df)
            final_df = final_df[final_df['CRTR_DD'].astype(str) >= filter_date]
            after_filter_count = len(final_df)

            print(f"Filtered out {before_filter_count - after_filter_count} rows with CRTR_DD < {filter_date}")
            print(f"Final rows after filtering: {after_filter_count}")
        else:
            print("Warning: CRTR_DD column not found, skipping date filtering")
        
        # 최종 데이터 출력
        print("\nFinal merged data sample:")
        print(final_df.head())
        
        final_start_idx = 1
        final_end_idx = end_idx
        filename = os.path.join(DATA_PARSED_DIR, DB_KIND, f'ksccPatternStation_{today}_{final_start_idx}_{final_end_idx}.csv')
        ensure_dir_exists(filename)
        with open(filename, "w", encoding="utf-8") as f:
            final_df.to_csv(f, index=False, encoding="utf-8")
            print(f"Saved incremental data to {filename}")
    else:
        print("No data to merge")
        return pd.DataFrame()


# ------------------------------------------------------------
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
    print(f"Unhandled exception: {e}")
    raise e
