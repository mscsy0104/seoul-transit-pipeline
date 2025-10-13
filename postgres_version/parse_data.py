import xml.etree.ElementTree as ET
import pandas as pd
import logging
import os
import sys
import glob
from dotenv import load_dotenv

from utils import ensure_dir_exists

load_dotenv()

TEST_XML_FILE = os.getenv("TEST_XML_FILE")
DATA_PARSED_DIR = os.getenv("DATA_PARSED_DIR")
DATA_DIR = os.getenv("DATA_DIR")
DB_KIND = "postgres"

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


def test_parse_and_save_xml():
    with open(TEST_XML_FILE, "r", encoding="utf-8") as f:
        df = parse_xml(f)
        print(df.head(5))

    filename = os.path.join(DATA_PARSED_DIR, DB_KIND, "example.csv")
    ensure_dir_exists(filename)
    with open(filename, "w", encoding="utf-8") as f:
        df.to_csv(f, index=False)


def process_xml_files():

    xml_files = glob.glob(os.path.join(DATA_DIR, DB_KIND, "*.xml"))
    print(f"총 {len(xml_files)}개의 파일 발견")

    for file in xml_files:
        df = parse_xml(file)
        filename = os.path.join(DATA_PARSED_DIR, DB_KIND, f"{os.path.basename(file).split(".")[0]}.csv")
        ensure_dir_exists(filename)
        df.to_csv(filename, index=False)


try:
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_parse_and_save_xml()
    elif len(sys.argv) > 1 and sys.argv[1] == "process":
        process_xml_files()
    else:
        pass
except Exception as e:
    logging.critical(f"Unhandled exception: {e}", exc_info=True)
    raise e