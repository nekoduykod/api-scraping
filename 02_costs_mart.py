from datetime import date, timedelta
import requests
from sqlalchemy import create_engine
import pandas as pd
from io import StringIO

current_date = date.today()
previous_date = current_date - timedelta(days=1)
previous_date_str = previous_date.strftime("%Y-%m-%d")

url = "https://us-central1-passion-fbe7a.cloudfunctions.net/dzn54vzyt5ga/costs"
headers = {"Authorization": " *** "}
costs_params = {"date": previous_date_str, 
                "dimensions": "location,channel,medium,campaign,keyword,ad_content,ad_group,landing_page"}
 
response = requests.get(url, params=costs_params, headers=headers)
data = response.text.replace('\t', ',').replace('/home', 'home').replace('/sale', 'sale')\
                    .replace('/signup', 'signup').replace('/new-arrivals', 'new-arrivals').replace('/products', 'products')

engine = create_engine('postgresql://postgres:123@localhost:5432/api_db')
df = pd.read_csv(StringIO(data))
df.to_sql('costs_mart', engine, if_exists='append', index=False)
print(f'Costs loaded to DB')

csv_filename = f'{previous_date_str}_costs_data.csv'
df = pd.read_csv(StringIO(data))
df.to_csv(csv_filename, index=False)
print('Costs CSV file created') 