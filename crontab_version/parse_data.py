import os
import glob
import xml.etree.ElementTree as ET
import pandas as pd
from sqlalchemy import create_engine


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
        print(df.head(1))
        print("-"*50)
        print(f"DataFrame 완성: {file}")
        print("-"*50)

        return df
    except Exception as e:
        raise e
    


# xml_folder = '/Users/sychoi/projects/seoul-trainsits-pipeline/crontab_version/data'
# xml_files = glob.glob(os.path.join(xml_folder, "*.xml"))

# print(f"총 {len(xml_files)}개의 파일 발견")

# file = xml_files[0]
# tree = ET.parse(file)
# root = tree.getroot()

# records = []
# for item in root.findall(".//row"):
#     record = {
#         "CRTR_DD": item.findtext("CRTR_DD"),
#         "PRPS_PTRN": item.findtext("PRPS_PTRN"),
#         "TNOPE": item.findtext("TNOPE"),
#         "TNOPE_GNRL": item.findtext("TNOPE_GNRL"),
#         "TNOPE_KID": item.findtext("TNOPE_KID"),
#         "TNOPE_YOUT": item.findtext("TNOPE_YOUT"),
#         "TNOPE_ELDR": item.findtext("TNOPE_ELDR"),
#         "TNOPE_PWDBS": item.findtext("TNOPE_PWDBS")
#     }
    
#     records.append(record)

#     try:
#         df = pd.DataFrame(records)
#         print(df.head(3))
#         print(f"DataFrame 완성: {file}")
#     except Exception as e:
#         raise e
