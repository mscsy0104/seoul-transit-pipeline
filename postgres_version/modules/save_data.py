from datetime import datetime

import os
from dotenv import load_dotenv

from utils import ensure_dir_exists

load_dotenv()
LOGS = os.getenv("LOGS")
DATA_DIR = os.getenv("DATA_DIR")
DATA_PARSED_DIR = os.getenv("DATA_PARSED_DIR")
DB_KIND = os.getenv("DB_KIND")

API_KEY = os.getenv("API_KEY")


def save_data_to_xml(xml_list):
    today = datetime.now().strftime("%Y%m%d")
    for idx, data in enumerate(xml_list):
        start_idx, end_idx, res_text = data
        filename = os.path.join(
            DATA_DIR, DB_KIND, f'ksccPatternStation_{today}_{start_idx}_{end_idx}_{idx + 1}.xml'
            )
        ensure_dir_exists(filename)
        with open(filename, "w", encoding="utf-8") as f:
            f.write(res_text)
            print(f"Saved data to {filename}")


def save_df_to_csv(df):
    today = datetime.now().strftime("%Y%m%d")
    min_create_date = df['CRTR_DD'].astype(int).min()
    max_create_date = df['CRTR_DD'].astype(int).max()
    filename = os.path.join(
        DATA_PARSED_DIR, DB_KIND, f'ksccPatternStation_{today}_{min_create_date}_{max_create_date}.csv')
    ensure_dir_exists(filename)
    df.to_csv(filename, index=False, encoding='utf-8')
    print(f"Saved data to {filename}")
