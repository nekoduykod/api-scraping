import os
from dotenv import load_dotenv
from datetime import date, timedelta 
import requests
import pandas as pd
from sqlalchemy import create_engine
 
load_dotenv()

current_date = date.today()
previous_date = current_date - timedelta(days=2)
previous_date_str = previous_date.strftime("%Y-%m-%d")

url = os.environ.get("API_URL")
url = url + "/orders"
auth_header = os.environ.get("MY_AUTH_HEADER")
headers = {"Authorization": auth_header}
orders_params = {"date": previous_date_str}

response = requests.get(url, params=orders_params, headers=headers)

parquet_filename = f'{previous_date_str}_orders_data.parquet'
with open(parquet_filename, 'wb') as file:
    file.write(response.content) 
print('Data written to parquet')

csv_filename = f'{previous_date_str}_orders_data.csv'
df = pd.read_parquet(parquet_filename, engine='fastparquet')
df.to_csv(csv_filename, index=False)
print('Orders CSV file created')

db_params = os.environ.get('DB_URL')
engine = create_engine(db_params)
df = pd.read_csv(csv_filename)
df.to_sql("orders_mart", engine, if_exists='append', index=False)
print('Orders data`s been written to DB')