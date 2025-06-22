import os
import sys
import glob
import pandas as pd
import argparse
from dotenv import load_dotenv
import logging

from database import *
from parse_data import parse_xml


load_dotenv()
LOGS_FOLDER = os.getenv("LOGS_FOLDER")
DATA_FOLDER = os.getenv("DATA_FOLDER")
PARSED_FOLDER = os.getenv("PARSED_FOLDER")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOGS_FOLDER, "save_csv_data.log")),
        logging.StreamHandler(sys.stdout)
    ]
)


def concat_and_save_parsed_data(xml_files):
    logging.info(f"concat_and_save_parsed_data: {len(xml_files)} files to process")
    dfs = []
    for file in xml_files:
        logging.info(f"Parsing XML file: {file}")
        df = parse_xml(file)
        dfs.append(df)

    final_df = pd.concat(dfs, ignore_index=True).sort_values(by="CRTR_DD", ascending=True)
    logging.info(f"Concatenated DataFrame shape: {final_df.shape}")
    print("-"*50)
    print(final_df.head(1))

    try:
        output_path = os.path.join(PARSED_FOLDER, "fetch_total_data.csv")
        final_df.to_csv(output_path, index=False)
        logging.info(f"Saved concatenated data to {output_path}")
    except Exception as e:
        logging.error(f"Failed to save concatenated data: {e}", exc_info=True)
        raise e


def save_parsed_individual_data(xml_files):
    logging.info(f"save_parsed_individual_data: {len(xml_files)} files to process")
    for file in xml_files:
        logging.info(f"Parsing XML file: {file}")
        df = parse_xml(file)
        try:
            filename = os.path.basename(file)
            filename_without_ext = os.path.splitext(filename)[0]
            output_path = os.path.join(PARSED_FOLDER, f"{filename_without_ext}.csv")
            df.to_csv(output_path, index=False)
            logging.info(f"Saved parsed data to {output_path}")
        except Exception as e:
            logging.error(f"Failed to save parsed data for {file}: {e}", exc_info=True)
            raise e


def save_parsed_datum(xml_file):
    logging.info(f"save_parsed_datum: Parsing XML file: {xml_file}")
    df = parse_xml(xml_file)
    try: 
        filename = os.path.basename(xml_file)
        filename_without_ext = os.path.splitext(filename)[0]
        output_path = os.path.join(PARSED_FOLDER, f"{filename_without_ext}.csv")
        df.to_csv(output_path, index=False)
        logging.info(f"Saved parsed data to {output_path}")
    except Exception as e:
        logging.error(f"Failed to save parsed data for {xml_file}: {e}", exc_info=True)
        raise e
    


def main():
    parser = argparse.ArgumentParser(description="XML → CSV 파싱 모드 선택")
    parser.add_argument("mode", choices=["bulk", "individual", "single"], nargs="?", default="single")
    args = parser.parse_args()

    try:
        if args.mode == "bulk":
            xml_files = glob.glob(os.path.join(DATA_FOLDER, "*.xml"))
            logging.info(f"Mode: bulk, Found {len(xml_files)} files")
            print(f"총 {len(xml_files)}개의 파일 발견")
            concat_and_save_parsed_data(xml_files)

        elif args.mode == "individual":
            xml_files = glob.glob(os.path.join(DATA_FOLDER, "*.xml"))
            logging.info(f"Mode: individual, Found {len(xml_files)} files")
            print(f"총 {len(xml_files)}개의 파일 발견")
            save_parsed_individual_data(xml_files)

        elif args.mode == "single":
            xml_file = input("csv로 저장할 xml 파일경로를 입력하세요: ").strip()
            logging.info(f"Mode: single, File: {xml_file}")
            save_parsed_datum(xml_file)

    except Exception as e:
        logging.critical(f"Unhandled exception: {e}", exc_info=True)
        raise e

if __name__ == "__main__":
    main()

