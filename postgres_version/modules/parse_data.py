import xml.etree.ElementTree as ET
import pandas as pd
import logging
import os
import sys
import glob
import json
from dotenv import load_dotenv
import re
from pprint import pprint
from io import StringIO

from utils import ensure_dir_exists

load_dotenv()

TEST_XML_FILE = os.getenv("TEST_XML_FILE")
DATA_PARSED_DIR = os.getenv("DATA_PARSED_DIR")
DATA_DIR = os.getenv("DATA_DIR")
DB_KIND = os.getenv("DB_KIND")
XML_CSV_MAP_BASENAME = "xml_csv_map.json"



def extract_number_from_filename(filename):
    m = re.search(r'_(\d+)\.xml$', filename)
    if m:
        return int(m.group(1))
    m = re.search(r'_(\d+)\.csv$', filename)
    if m:
        return int(m.group(1))
    return -1

def merge_xml(xml_text_list):

    doc_sting = """<?xml version="1.0" encoding="UTF-8"?>
<ksccPatternStation>
<list_total_count>294466</list_total_count>
<RESULT>
<CODE>INFO-000</CODE>
<MESSAGE>정상 처리되었습니다</MESSAGE>
</RESULT>
    """
    first_tree = ET.parse(xml_text_list[0])
    first_root = first_tree.getroot()

    merged_root = ET.Element(first_root.tag)
    for xml_text in xml_text_list:
        tree = ET.parse(StringIO(xml_text))
        root.append(tree.getroot())
    return ET.tostring(root, encoding="utf-8")




def parse_xml(file):
    tree = ET.parse(file)
    root = tree.getroot()

    records = []
    for item in root.findall(".//row"):
        record = {
            "CRTR_DD": item.findtext("CRTR_DD"),
            "PRPS_PTRN": item.findtext("PRPS_PTRN"),
            "TNOPE": item.findtext("TNOPE"),
            "TNOPE_GNRL": item.findtext("TNOPE_GNRL"),
            "TNOPE_KID": item.findtext("TNOPE_KID"),
            "TNOPE_YOUT": item.findtext("TNOPE_YOUT"),
            "TNOPE_ELDR": item.findtext("TNOPE_ELDR"),
            "TNOPE_PWDBS": item.findtext("TNOPE_PWDBS")
        }
        records.append(record)

    try:
        df = pd.DataFrame(records)
        print(f"첫 번째 row: \n{df.head(1)}")
        print("-"*50)
        print(f"DataFrame 완성: {file}")
        print("-"*50)
        return df
    except Exception as e:
        print(f"DataFrame 생성 중 오류 발생: {e}")
        raise e


def parse_xml_from_text(xml_text, source_name="xml_text"):
    """
    res.text 형태의 XML 문자열을 파싱하는 함수
    
    Args:
        xml_text (str): XML 문자열 (예: res.text)
        source_name (str): 소스 이름 (로깅용, 기본값: "xml_text")
    
    Returns:
        pd.DataFrame: 파싱된 데이터
    """
    try:
        # StringIO를 사용하여 문자열을 파일처럼 처리
        xml_file = StringIO(xml_text)
        tree = ET.parse(xml_file)
        root = tree.getroot()

        records = []
        for item in root.findall(".//row"):
            record = {
                "CRTR_DD": item.findtext("CRTR_DD"),
                "PRPS_PTRN": item.findtext("PRPS_PTRN"),
                "TNOPE": item.findtext("TNOPE"),
                "TNOPE_GNRL": item.findtext("TNOPE_GNRL"),
                "TNOPE_KID": item.findtext("TNOPE_KID"),
                "TNOPE_YOUT": item.findtext("TNOPE_YOUT"),
                "TNOPE_ELDR": item.findtext("TNOPE_ELDR"),
                "TNOPE_PWDBS": item.findtext("TNOPE_PWDBS")
            }
            records.append(record)

        df = pd.DataFrame(records)
        return df
        
    except ET.ParseError as e:
        print(f"XML 파싱 오류 발생 ({source_name}): {e}")
        raise e
    except Exception as e:
        print(f"DataFrame 생성 중 오류 발생 ({source_name}): {e}")
        raise e


def test_parse_and_save_xml():
    with open(TEST_XML_FILE, "r", encoding="utf-8") as f:
        df = parse_xml(f)
        print(df.head(5))

    filename = os.path.join(DATA_PARSED_DIR, DB_KIND, "example.csv")
    ensure_dir_exists(filename)
    with open(filename, "w", encoding="utf-8") as f:
        df.to_csv(f, index=False)


def process_every_xml_files():
    xml_files = glob.glob(os.path.join(DATA_DIR, DB_KIND, "*.xml"))
    print(f"총 {len(xml_files)}개의 파일 발견")

    xml_csv_map_path = os.path.join(DATA_DIR, DB_KIND, XML_CSV_MAP_BASENAME)
    ensure_dir_exists(xml_csv_map_path)

    mappings = {}
    
    for xml_file in xml_files:
        df = parse_xml(xml_file)
        csv_file = os.path.join(DATA_PARSED_DIR, DB_KIND, f"{os.path.basename(xml_file).split(".")[0]}.csv")
        ensure_dir_exists(csv_file)
        df.to_csv(csv_file, index=False)

        file_index = extract_number_from_filename(xml_file)
        mappings[str(file_index)] = {
            "xml_file": os.path.basename(xml_file),
            "csv_file": os.path.basename(csv_file)
        }

    with open(xml_csv_map_path, "w", encoding="utf-8") as f:
        json.dump(mappings, f, ensure_ascii=False, indent=4)


def process_single_xml_file(xml_file):
    df = parse_xml(xml_file)
    csv_file = os.path.join(DATA_PARSED_DIR, DB_KIND, f"{os.path.basename(xml_file).split('.')[0]}.csv")
    ensure_dir_exists(csv_file)
    df.to_csv(csv_file, index=False)

    xml_csv_map_path = os.path.join(DATA_DIR, DB_KIND, XML_CSV_MAP_BASENAME)
    ensure_dir_exists(xml_csv_map_path)
    try:
        # 기존 JSON 파일 읽기
        if os.path.exists(xml_csv_map_path) and os.path.getsize(xml_csv_map_path) > 0:
            with open(xml_csv_map_path, "r", encoding="utf-8") as f:
                mappings = json.load(f)
        else:
            mappings = {}
        
        # 새로운 데이터 추가
        file_index = extract_number_from_filename(xml_file)
        mappings[str(file_index)] = {
            "xml_file": os.path.basename(xml_file),
            "csv_file": os.path.basename(csv_file)
        }
        
        # 전체 배열을 다시 JSON 파일로 저장
        with open(xml_csv_map_path, "w", encoding="utf-8") as f:
            json.dump(mappings, f, ensure_ascii=False, indent=4)
            
    except Exception as e:
        print(f"Failed to write xml_csv_map.json: {e}")
        raise e


def process_xml_from_text(xml_text, source_name="xml_text", file_index=None):
    """
    res.text 형태의 XML을 받아서 파싱하고 CSV로 저장하는 함수
    
    Args:
        xml_text (str): XML 문자열 (예: res.text)
        source_name (str): 소스 이름 (기본값: "xml_text")
        file_index (int): 파일 인덱스 (기본값: None, 자동 생성)
    
    Returns:
        str: 생성된 CSV 파일 경로
    """
    df = parse_xml_from_text(xml_text, source_name)
    
    # 파일 인덱스가 없으면 타임스탬프 기반으로 생성
    if file_index is None:
        import time
        file_index = int(time.time())
    
    csv_file = os.path.join(DATA_PARSED_DIR, DB_KIND, f"{source_name}_{file_index}.csv")
    ensure_dir_exists(csv_file)
    df.to_csv(csv_file, index=False)

    xml_csv_map_path = os.path.join(DATA_DIR, DB_KIND, XML_CSV_MAP_BASENAME)
    ensure_dir_exists(xml_csv_map_path)
    
    try:
        # 기존 JSON 파일 읽기
        if os.path.exists(xml_csv_map_path) and os.path.getsize(xml_csv_map_path) > 0:
            with open(xml_csv_map_path, "r", encoding="utf-8") as f:
                mappings = json.load(f)
        else:
            mappings = {}
        
        # 새로운 데이터 추가
        mappings[str(file_index)] = {
            "xml_file": f"{source_name}.xml",
            "csv_file": os.path.basename(csv_file)
        }
        
        # 전체 배열을 다시 JSON 파일로 저장
        with open(xml_csv_map_path, "w", encoding="utf-8") as f:
            json.dump(mappings, f, ensure_ascii=False, indent=4)
            
        print(f"XML 텍스트 처리 완료: {csv_file}")
        return csv_file
            
    except Exception as e:
        print(f"Failed to write xml_csv_map.json: {e}")
        raise e
   


def map_xml_file_to_csv_file():
    xml_files = glob.glob(os.path.join(DATA_DIR, DB_KIND, "*.xml"))
    csv_files = glob.glob(os.path.join(DATA_PARSED_DIR, DB_KIND, "*.csv"))

    xml_files = sorted(xml_files, key=lambda f: extract_number_from_filename(f))
    csv_files = sorted(csv_files, key=lambda f: extract_number_from_filename(f))

    # pprint(xml_files)

    xml_csv_map_path = os.path.join(DATA_DIR, DB_KIND, XML_CSV_MAP_BASENAME)
    ensure_dir_exists(xml_csv_map_path)

    # Create a list to store all mappings
    mappings = {}
    
    for csv_file in csv_files:
        for xml_file in xml_files:
            if os.path.basename(xml_file).split(".")[0] == os.path.basename(csv_file).split(".")[0]:
                file_index = extract_number_from_filename(xml_file)
                mappings[str(file_index)] = {
                    "xml_file": os.path.basename(xml_file),
                    "csv_file": os.path.basename(csv_file)
                }
                break  # Found matching file, no need to continue inner loop

    # Write as a proper JSON array
    with open(xml_csv_map_path, "w", encoding="utf-8") as f:
        json.dump(mappings, f, ensure_ascii=False, indent=4)


# ------------------------------------------------------------
try:
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_parse_and_save_xml()
    elif len(sys.argv) > 1 and sys.argv[1] == "every":
        process_every_xml_files()
    elif len(sys.argv) > 1 and sys.argv[1] == "single":
        xml_file = sys.argv[2]
        process_single_xml_file(xml_file)
    else:
        pass
except Exception as e:
    logging.critical(f"Unhandled exception: {e}", exc_info=True)
    raise e