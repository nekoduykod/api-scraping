import os
from dotenv import load_dotenv
from datetime import date, timedelta
import requests
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
 
load_dotenv()

current_date = date.today()
previous_date = current_date - timedelta(days=1)
previous_date_str = previous_date.strftime("%Y-%m-%d")

url = os.environ.get("API_URL")
url = url + "/events"
auth_header = os.environ.get("MY_AUTH_HEADER")
headers = {"Authorization": auth_header}
events_params = {"date": previous_date_str}
 
response = requests.get(url, params=events_params, headers=headers)
data = response.text.replace('\\', '').replace('{"data":"', '').replace('}}]",', '}}]')
next_page_index = data.find('"next_page"')
data = data[:next_page_index]

csv_folder = os.environ.get("CSV_FOLDER")  
csv_filename = f'{csv_folder}\\{previous_date_str}_events_data.csv'
df = pd.read_json(StringIO(data))
df.to_csv(csv_filename, index=False)
print('Events written to CSV')

db_params = os.environ.get('DB_URL')
engine = create_engine(db_params)
df = pd.read_csv(csv_filename)
df.to_sql("events_mart", engine, if_exists='append', index=False)
print('Events written to DB') 