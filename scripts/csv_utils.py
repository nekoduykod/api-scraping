import csv
from io import StringIO
import pandas as pd
from datetime import date, timedelta
from sqlalchemy import create_engine 
from dotenv import load_dotenv
from api_connection import response  
import os 

load_dotenv()

current_date = date.today()
previous_date = current_date - timedelta(days=1)
previous_date_str = previous_date.strftime("%Y-%m-%d")

def create_csv(data, filename_prefix, format="csv", records=None, to_database=False):
    csv_folder = os.environ.get("CSV_FOLDER")
    parquet_folder = os.environ.get("PARQUET_FOLDER")
    csv_filename = f'{csv_folder}/{previous_date_str}_{filename_prefix}_data.csv'

    if format == "parquet":
        # Writing parquet content to file
        parquet_filename = f'{parquet_folder}/{previous_date_str}_{filename_prefix}_data.parquet'
        with open(parquet_filename, 'wb') as file:
            file.write(response.content)   
        print(f'{filename_prefix.capitalize()} data written to parquet')
    else:
        # Cases 1, 2, and 4: Reading data and writing to CSV
        if records is not None:
            # Case 1: Writing records to CSV
            with open(csv_filename, 'w', newline='', encoding='utf-8') as csv_file:
                csv_writer = csv.DictWriter(csv_file, fieldnames=records[0].keys())
                csv_writer.writeheader()
                csv_writer.writerows(records)
        else:
            # Cases 2 and 4: Reading data using pandas and writing to CSV
            df = pd.read_csv(StringIO(data)) if "costs" in filename_prefix else pd.read_json(StringIO(data))
            df.to_csv(csv_filename, index=False)

        print(f'{filename_prefix.capitalize()} CSV file created')

        if to_database:
            db_params = os.environ.get('DB_URL')
            engine = create_engine(db_params)
            df = pd.read_csv(csv_filename)
            df.to_sql(f"{filename_prefix}_mart", engine, if_exists='append', index=False)
            print(f'{filename_prefix.capitalize()} data has been written to DB')
        
    # Returning the DataFrame for potential further processing
    # return df