import requests
from datetime import date, timedelta
from sqlalchemy import create_engine
import pandas as pd
from io import StringIO

current_date = date.today()
previous_dates = current_date - timedelta(days=2)
previous_dates_str = previous_dates.strftime("%Y-%m-%d")

url = "https://us-central1-passion-fbe7a.cloudfunctions.net/dzn54vzyt5ga/costs"
headers = {"Authorization": "***_0cRdtmCdCu9XFkFNs45dpTw3NhQ7ysIwYIn8EEZzpal1uUWdkR0TXgGkY0SXfehCy-4rUZh81Hr3PaZckxyJp3VIcgBzk8qGEpZRMD8_KBJukbtVYkaobYX7jMv4f2TA0kbXkCADTM2yCJw=="}

costs_params = {
    "date": previous_dates_str, 
    "dimensions": "location,channel,medium,campaign,keyword,ad_content,ad_group,landing_page"
}
 
response = requests.get(url, params=costs_params, headers=headers)
data = response.text.replace('\t', ',').replace('/home', 'home').replace('/sale', 'sale')\
                    .replace('/signup', 'signup').replace('/new-arrivals', 'new-arrivals').replace('/products', 'products')
# print(data)

df = pd.read_csv(StringIO(data))
engine = create_engine('postgresql://postgres:123@localhost:5432/api_db')
df.to_sql('costs_mart', engine, if_exists='append', index=False)
print(f'Costs loaded to DB')

df = pd.read_csv(StringIO(data))
df.to_csv('costs_data.csv', index=False)
print('Costs CSV file created') 
 