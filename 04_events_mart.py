from datetime import date, timedelta
import requests
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine

current_date = date.today()
previous_date = current_date - timedelta(days=1)
previous_date_str = previous_date.strftime("%Y-%m-%d")

url = "https://us-central1-passion-fbe7a.cloudfunctions.net/dzn54vzyt5ga/events"
headers = {"Authorization": " *** "}
events_params = {"date": previous_date_str}
 
response = requests.get(url, params=events_params, headers=headers)
data = response.text.replace('\\', '').replace('{"data":"', '').replace('}}]",', '}}]')
next_page_index = data.find('"next_page"')
data = data[:next_page_index]

csv_filename = f'{previous_date_str}_events_data.csv'
df = pd.read_json(StringIO(data))
df.to_csv(csv_filename, index=False)
print('Events written to CSV')

engine = create_engine('postgresql://postgres:123@localhost:5432/api_db')
df = pd.read_csv(csv_filename)
df.to_sql("events_mart", engine, if_exists='append', index=False)
print('Events written to DB')

# Efforts to investigate a pagination process. Check draft_events_pagination.py  