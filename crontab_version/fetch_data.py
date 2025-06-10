import os, sys
import requests
from dotenv import load_dotenv
import re
import json
from datetime import datetime

load_dotenv()

API_KEY = os.getenv("API_KEY")


def ensure_directory_exists(filename):
    dirname = os.path.dirname(filename)

    if not os.path.exists(dirname):
        os.makedirs(dirname)


def fetch_bulk_data(count):
    xml_data = []

    start = 1
    end = count
    step = 1000
    for i in range(start, end + 1, step):
        start_idx = i
        end_idx = i + 999
        print("-"*50)
        print(f"start index: {start_idx}")
        print(f"end index: {end_idx}")
        print(f"step: {step}")
        print("-"*50)

        url = 'http://openapi.seoul.go.kr:8088/{api_key}/xml/ksccPatternStation/{start_idx}/{end_idx}'.format(api_key=API_KEY, start_idx=start_idx, end_idx=end_idx)

        res = requests.get(url)
        if res.status_code == 200:
            xml_data.append(res.text)

    xml_data = ''.join(xml_data)

    filename = './data/ksccPatternStation.xml'
    ensure_directory_exists(filename)
    with open(filename, "w", encoding="utf-8") as f:
        f.write(xml_data)


def test_fetch_total_count():
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

        return cnt
    except Exception as e:
        raise e
    


    

def fetch_data():
    print('main 실행')
    
    start, end = get_start_end_index()
    if end == 1:
        return

    diff = end - start + 1
    if diff >= 1000:
        step = 1000
    else:
        step = diff

    xml_data = []

    for i in range(start, end + 1, step):
        start_idx = i
        end_idx = i + 999
        print("-"*50)
        print(f"start index: {start_idx}")
        print(f"end index: {end_idx}")
        print(f"step: {step}")
        print("-"*50)

        url = 'http://openapi.seoul.go.kr:8088/{api_key}/xml/ksccPatternStation/{start_idx}/{end_idx}'.format(api_key=API_KEY, start_idx=start_idx, end_idx=end_idx)

        res = requests.get(url)
        if res.status_code == 200:
            xml_data.append(res.text)

    xml_data = ''.join(xml_data)

    with open('./data/ksccPatternStation.xml', "w", encoding="utf-8") as f:
        f.write(xml_data)




# if __name__ == "__main__":
#     try:
#         test()
#         main()
#     except Exception as e:
#         print(e)
#         raise
        


    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test()
    else:
        fetch_data()



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
'''