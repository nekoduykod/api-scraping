import os
from dotenv import load_dotenv
from datetime import date, timedelta
import requests
from sqlalchemy import create_engine
import pandas as pd
from io import StringIO
 
load_dotenv()

current_date = date.today()
previous_date = current_date - timedelta(days=1)
previous_date_str = previous_date.strftime("%Y-%m-%d")

url = os.environ.get("API_URL")
url = url + "/costs"
auth_header = os.environ.get("MY_AUTH_HEADER")
headers = {"Authorization": auth_header}
costs_params = {"date": previous_date_str, 
                "dimensions": "location,channel,medium,campaign,keyword,ad_content,ad_group,landing_page"}
 
response = requests.get(url, params=costs_params, headers=headers)
data = response.text.replace('\t', ',').replace('/home', 'home').replace('/sale', 'sale')\
                    .replace('/signup', 'signup').replace('/new-arrivals', 'new-arrivals').replace('/products', 'products')

db_params = os.environ.get('DB_URL')
engine = create_engine(db_params)
df = pd.read_csv(StringIO(data))
df.to_sql('costs_mart', engine, if_exists='append', index=False)
print(f'Costs data loaded to DB')

csv_folder = os.environ.get("CSV_FOLDER")  
csv_filename = f'{csv_folder}\\{previous_date_str}_costs_data.csv'
df = pd.read_csv(StringIO(data))
df.to_csv(csv_filename, index=False)
print('Costs CSV file created') 