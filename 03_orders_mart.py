from datetime import date, timedelta 
import requests
import pandas as pd
from sqlalchemy import create_engine

current_date = date.today()
previous_date = current_date - timedelta(days=1)
previous_date_str = previous_date.strftime("%Y-%m-%d")

url = "https://us-central1-passion-fbe7a.cloudfunctions.net/dzn54vzyt5ga/orders"
headers = {"Authorization": " *** "}
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

engine = create_engine('postgresql://postgres:123@localhost:5432/api_db')
df = pd.read_csv(csv_filename)
df.to_sql("orders_mart", engine, if_exists='append', index=False)
print('New orders data`s been written to DB')