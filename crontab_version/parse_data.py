import xml.etree.ElementTree as ET
import pandas as pd
import logging
import os
import sys

# 로그 설정
LOGS_FOLDER = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOGS_FOLDER, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOGS_FOLDER, "parse_xml_data.log")),
        logging.StreamHandler(sys.stdout)
    ]
)

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
        logging.info(f"첫 번째 row: \n{df.head(1)}")
        logging.info("-"*50)
        logging.info(f"DataFrame 완성: {file}")
        logging.info("-"*50)
        return df
    except Exception as e:
        logging.error(f"DataFrame 생성 중 오류 발생: {e}")
        raise e