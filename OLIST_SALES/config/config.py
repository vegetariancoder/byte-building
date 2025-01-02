from datetime import date
from pyspark.sql import DataFrame

def read_json_file(path):
    return spark.read.json(path)

def get_environment_info():
    storage_account = "olistlearning"
    landing_container = "olist-landing"
    raw_container = "olist-raw"
    return storage_account,landing_container,raw_container

def get_today_date():
    date_value = date.today()
    return date_value

def check_for_duplicates(df: DataFrame, column_list: list, df_name: str):
    for col_name in column_list:
        duplicates = df.groupBy(col_name).count().filter("count > 1")
        if duplicates.count() > 0:
            raise Exception(f"Duplicate values found in column '{col_name}' of DataFrame '{df_name}'.")

