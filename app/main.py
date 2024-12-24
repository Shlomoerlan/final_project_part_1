from app.service.csv_service_psql import merged_df
from app_insert_into_db.service.inital_db_from_df_service import insert_all_data

if __name__ == "__main__":
    try:
        insert_all_data(merged_df)
    except Exception as e:
        print(f"Error occurred: {e}")
