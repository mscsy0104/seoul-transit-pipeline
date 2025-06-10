import os, sys
import requests
from dotenv import load_dotenv
import re
import json
from datetime import datetime
import glob

load_dotenv()

API_KEY = os.getenv("API_KEY")


def ensure_directory_exists(filename):
    dirname = os.path.dirname(filename)

    if not os.path.exists(dirname):
        os.makedirs(dirname)


def fetch_bulk_data(total_cnt):
    xml_data = []

    start = 1
    end = int(total_cnt)

    diff = end - start + 1
    if diff >= 1000:
        step = 1000
    else:
        step = diff

    for i in range(start, end + 1, step):
        start_idx = i
        end_idx = i + step
        print("-"*50)
        print(f"start index: {start_idx}")
        print(f"end index: {end_idx}")
        print(f"step: {step}")
        print("-"*50)

        url = 'http://openapi.seoul.go.kr:8088/{api_key}/xml/ksccPatternStation/{start_idx}/{end_idx}'.format(api_key=API_KEY, start_idx=start_idx, end_idx=end_idx)

        res = requests.get(url)
        try:
            res.raise_for_status()
            data = (start_idx, end_idx, res.text)
            xml_data.append(data)
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching data: {e}")
            continue
    
    for idx, data in enumerate(xml_data):
        start_idx, end_idx, res_text = data
        filename = f'./data/ksccPatternStation_{start_idx}_{end_idx}_{idx + 1}.xml'
        ensure_directory_exists(filename)
        with open(filename, "w", encoding="utf-8") as f:
            f.write(res_text)


def test_fetch_data():
    print('test 실행')
    
    start_idx = 1
    end_idx = 1
    url = 'http://openapi.seoul.go.kr:8088/{api_key}/xml/ksccPatternStation/{start_idx}/{end_idx}'.format(api_key=API_KEY, start_idx=start_idx, end_idx=end_idx)
    

    try:
        res = requests.get(url)
        text = res.text
        cnt = int(re.search(r"<list_total_count>(\d+)</list_total_count>", text).group(1))
        name = re.search(r'^\s*<\?xml.*?\?>\s*<([a-zA-Z0-9_]+)>', text).group(1)
        print(f"name: {name}")
        print(f"Total count extracted: {cnt}")
        print("-"*50)
        print("<< Test용 데이터 호출 1회 >>")
        print("-"*50)
        print(text)
    except Exception as e:
        raise e
    

def fetch_total_count():
    start_idx = 1
    end_idx = 1
    url = 'http://openapi.seoul.go.kr:8088/{api_key}/xml/ksccPatternStation/{start_idx}/{end_idx}'.format(api_key=API_KEY, start_idx=start_idx, end_idx=end_idx)
    

    try:
        res = requests.get(url)
        text = res.text
        cnt = int(re.search(r"<list_total_count>(\d+)</list_total_count>", text).group(1))
        return cnt
    except Exception as e:
        raise e
    

def fetch_incremental_data(total_cnt):
    print('incremental data 업데이트')


    # Get the most recent file in the ./data directory
    list_of_files = glob.glob('./data/*')
    if list_of_files:
        latest_file = max(list_of_files, key=os.path.getctime)
        print(f"Most recent file: {latest_file}")
    else:
        print("No files found in the ./data directory.")
        return
    
    print("-"*50)

    start = int(latest_file.split('_')[2])
    end = total_cnt

    diff = end - start + 1
    if diff >= 1000:
        step = 1000
    else:
        step = diff

    xml_data = []

    for i in range(start, end + 1, step):
        start_idx = i
        end_idx = i + step
        print("-"*50)
        print(f"start index: {start_idx}")
        print(f"end index: {end_idx}")
        print(f"step: {step}")
        print("-"*50)

        url = 'http://openapi.seoul.go.kr:8088/{api_key}/xml/ksccPatternStation/{start_idx}/{end_idx}'.format(api_key=API_KEY, start_idx=start_idx, end_idx=end_idx)

        res = requests.get(url)
        try:
            res.raise_for_status()
            data = (start_idx, end_idx, res.text)
            xml_data.append(data)
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching data: {e}")
            continue
    
    for data in xml_data:
        today = datetime.now().strftime("%Y%m%d")
        start_idx, end_idx, res_text = data
        filename = f'./data/ksccPatternStation_{start_idx}_{end_idx}_{today}.xml'
        ensure_directory_exists(filename)
        with open(filename, "w", encoding="utf-8") as f:
            f.write(res_text)

    

try:
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_fetch_data()
    elif len(sys.argv) > 1 and sys.argv[1] == "bulk":
        fetch_bulk_data(fetch_total_count())
    else:
        fetch_incremental_data(fetch_total_count())
except Exception as e:
    raise e


'''
주의 사항
반복문으로 인해 아래 코드 반복되니까
지워줘야지 가공 가능해짐.(파싱툴 특성상)

#
<?xml version="1.0" encoding="UTF-8"?>
<ksccPatternStation>
<list_total_count>254429</list_total_count>
<RESULT>
<CODE>INFO-000</CODE>
<MESSAGE>정상 처리되었습니다</MESSAGE>
</RESULT>

1000개씩 나눠 수집해야하니까
1000개별로 파일을 나누어 저장한 뒤(예시: ksccPatternStation_1_1000_datetime.xml, ksccPatternStation_1001_2001_datetime.xml')
각 파일에 대해 datetime 순서별로 파싱을 해서 DB에 저장하도록 한다.
'''