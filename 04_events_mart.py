import requests
from datetime import date, timedelta
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine

current_date = date.today()
previous_dates = current_date - timedelta(days=3)
previous_dates_str = previous_dates.strftime("%Y-%m-%d")

url = "https://us-central1-passion-fbe7a.cloudfunctions.net/dzn54vzyt5ga/events"
headers = {"Authorization": "***Q7ysIwYIn8EEZzpal1***0SXfehCy-4rUZh81Hr3PaZckxyJp3VIcgBzk8qGEpZRMD8_KBJukbtVYkaobYX7jMv4f2TA0kbXkCADTM2yCJw=="}
events_params = {"date": previous_dates_str}
 
response = requests.get(url, params=events_params, headers=headers)
data = response.text.replace('\\', '').replace('{"data":"', '').replace('}}]",', '}}]')

next_page_index = data.find('"next_page"')
data = data[:next_page_index]
# print(data)

df = pd.read_json(StringIO(data))
df.to_csv('events_data.csv', index=False)
print('Events written to CSV')

df = pd.read_csv('events_data.csv')
engine = create_engine('postgresql://postgres:123@localhost:5432/api_db')
df.to_sql("events_mart", engine, if_exists='append', index=False)
print('Events written to DB')


''' Just efforts to investigate a pagination process. Check draft_events_pagination.py '''
# from urllib.parse import quote
# next_page_token = data.get('next_page')
# while next_page_token in data:
#     response = requests.get(url, params=events_params, headers=headers)
#     next_page_url = f"{url}?next_page={quote(next_page_token)}"
#     next_response = requests.get(next_page_url, headers=headers)
#     next_data = next_response.json()
#     print(next_data)
# else:
#     print("No more pages available.")


# if 'next_page' in data:
#     next_page_url = data['next_page']
#     events_params = {"next_page": next_page_url}
#     next_page_response = requests.get(url, params=events_params, headers=headers)
#     next_page_data = next_page_response.json()
#     print(next_page_data)