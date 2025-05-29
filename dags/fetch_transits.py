from airflow import DAG
from airflow.decorators import task
from datetime import datetime

import os
import sys
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")


@task
def test_fetch_data():
    print('수집 test 실행')
    print('인덱스: 1')
    START_INDEX = 1
    END_INDEX = 1
    url = 'http://openapi.seoul.go.kr:8088/{api_key}/xml/ksccPatternStation/{start_idx}/{end_idx}'.format(api_key=API_KEY, start_idx=START_INDEX, end_idx=END_INDEX)
    
    res = requests.get(url)
    print(res.text)

@task
def fetch_data():
    print('main 실행')
    xml_data = ''
    for i in range(1, 254429, 1000): # 처음 샘플 수집해보니 list_total_count가 25449개임
        START_INDEX = i
        END_INDEX = i + 999

        url = 'http://openapi.seoul.go.kr:8088/{api_key}/xml/ksccPatternStation/{start_idx}/{end_idx}'.format(api_key=API_KEY, start_idx=START_INDEX, end_idx=END_INDEX)

        res = requests.get(url)
        if res.status_code == 200:
            xml_data += res.text


    with open('./data/ksccPatternStation.xml', "w", encoding="utf-8") as f:
        f.write(xml_data)