import requests
from datetime import date, timedelta 
import pandas as pd
from sqlalchemy import create_engine

current_date = date.today()
previous_dates = current_date - timedelta(days=2)
previous_dates_str = previous_dates.strftime("%Y-%m-%d")

url = "https://us-central1-passion-fbe7a.cloudfunctions.net/dzn54vzyt5ga/orders"
orders_params = {"date": previous_dates_str}

headers = {"Authorization": "***_***kFNs45dpTw3NhQ7***EEZzpal1uUWdkR0TXgGkY0SXfehCy-4rUZh81Hr3PaZckxyJp3VIcgBzk8qGEpZRMD8_KBJukbtVYkaobYX7jMv4f2TA0kbXkCADTM2yCJw=="}

response = requests.get(url, params=orders_params, headers=headers)
with open('orders_data.parquet', 'wb') as file:
    file.write(response.content) 
print('Data written to parquet')

df = pd.read_parquet('orders_data.parquet', engine='fastparquet')
df.to_csv('orders_data.csv', index=False)
print('Orders CSV file created')

df = pd.read_csv('orders_data.csv')
engine = create_engine('postgresql://postgres:123@localhost:5432/api_db')
df.to_sql("orders_mart", engine, if_exists='append', index=False)
print('Orders written to DB')



# TO DO self-desctuction of the csv file.
# TO DO cron

# duckdb.sql("COPY(SELECT * FROM 'D:\\PROJECTS\\api_data_marts\orders_data.parquet' TO 'D:\\PROJECTS\\api_data_marts\orders_data.csv' (HEADER, FORMAT 'csv'))")
 

