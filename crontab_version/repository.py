import os
import glob
import xml.etree.ElementTree as ET
import pandas as pd
import dotenv

from database import *
from parse_data import parse_xml

load_dotenv()
DATA_FOLDER = os.getenv("DATA_FOLDER")
PARSED_FOLDER = os.getenv("PARSED_FOLDER")

# 프로젝트 경로 내에 적재된 파일 개수 확인
xml_files = glob.glob(os.path.join(DATA_FOLDER, "*.xml"))
print(f"총 {len(xml_files)}개의 파일 발견")


def concat_and_save_parsed_xml_data(xml_files):
    dfs = []
    for file in xml_files:
        df = parse_xml(file)
        dfs.append(df)

    final_df = pd.concat(dfs, ignore_index=True).sort_values(by="CRTR_DD", ascending=True)
    print("-"*50)
    print(final_df.head(1))

    try:
        final_df.to_csv(
            os.path.join(PARSED_FOLDER, "fetch_data3.csv"), index=False
        )
    except Exception as e:
        raise e
    

def save_parsed_xml_data(xml_files):
    for file in xml_files:
        df = parse_xml(file)
    
        try:
            filename = os.path.basename(file)
            filename_without_ext = os.path.splitext(filename)[0]
            df.to_csv(
                os.path.join(PARSED_FOLDER, f"{filename_without_ext}.csv"), index=False
            )
        except Exception as e:
            raise e


