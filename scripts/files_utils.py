import csv
from io import StringIO
import pandas as pd
from datetime import date, timedelta
from sqlalchemy import create_engine 
from dotenv import load_dotenv
from connect_api import response  
import os 

load_dotenv()

current_date = date.today()
previous_date = current_date - timedelta(days=1)
previous_date_str = previous_date.strftime("%Y-%m-%d")

def get_parquet_path(filename_prefix):
    parquet_folder = os.environ.get("PARQUET_FOLDER")
    parquet_filename = f'{parquet_folder}/{previous_date_str}_{filename_prefix}_data.parquet'
    return parquet_filename

def get_csv_path(filename_prefix):
    csv_folder = os.environ.get("CSV_FOLDER")
    csv_filename = f'{csv_folder}/{previous_date_str}_{filename_prefix}_data.csv'
    return csv_filename

def create_parquet(response, filename_prefix):
    parquet_filename = get_parquet_path(filename_prefix)
    with open(parquet_filename, 'wb') as file:
        file.write(response.content)   
    print(f'{filename_prefix.capitalize()} data written to parquet')

def parquet_to_csv(filename_prefix): 
    parquet_filename = get_parquet_path(filename_prefix)
    csv_filename = get_csv_path(filename_prefix)
    df = pd.read_parquet(parquet_filename, engine='fastparquet')
    df.to_csv(csv_filename, index=False)
    print(f'{filename_prefix.capitalize()} CSV file created')

def create_csv(data=False, records=False, filename_prefix=""):  
    csv_filename = get_csv_path(filename_prefix)
    if records:
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.DictWriter(csv_file, fieldnames=records[0].keys())
            csv_writer.writeheader()
            csv_writer.writerows(records)
    else:
        df = pd.read_csv(StringIO(data)) if "costs" in filename_prefix else pd.read_json(StringIO(data))
        df.to_csv(csv_filename, index=False)
    print(f'{filename_prefix.capitalize()} CSV file created')

def write_to_database(filename_prefix):  # to_database=True arg could be done instead of def write_to_database
    csv_filename = get_csv_path(filename_prefix)
    db_params = os.environ.get('DB_URL')
    engine = create_engine(db_params)
    df = pd.read_csv(csv_filename)
    df.to_sql(f"{filename_prefix}_mart", engine, if_exists='append', index=False)
    print(f'{filename_prefix.capitalize()} data has been written to DB')