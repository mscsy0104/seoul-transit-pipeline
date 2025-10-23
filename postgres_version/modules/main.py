from fetch_data import fetch_incremental_data, fetch_total_count
from save_data import save_df_to_csv
from upload_incremental_data import upload_incremental_data_from_df




def main():
    total_cnt = fetch_total_count()
    df = fetch_incremental_data(total_cnt)

    save_df_to_csv(df)
    upload_incremental_data_from_df(df)
if __name__ == "__main__":
    main()