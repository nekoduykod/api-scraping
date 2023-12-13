import os
from dotenv import load_dotenv
from datetime import date, timedelta
import requests
from sqlalchemy import create_engine, text
import json
import csv
import pandas as pd
from io import StringIO

load_dotenv()

current_date = date.today()
previous_date = current_date - timedelta(days=3)
previous_date_str = previous_date.strftime("%Y-%m-%d")

url = os.environ.get("API_URL")
auth_header = os.environ.get("MY_AUTH_HEADER")
headers = {"Authorization": auth_header}

db_params = os.environ.get("DB_URL")
engine = create_engine(db_params)   # error - sqlalchemy.exc.ResourceClosedError: This Connection is closed

with engine.connect() as conn:
   create_table_query = text('''
       CREATE TABLE IF NOT EXISTS installs_mart (
           install_time VARCHAR(255), marketing_id VARCHAR(255), channel VARCHAR(255), medium VARCHAR(255),
           campaign VARCHAR(255), keyword VARCHAR(255), ad_content VARCHAR(255), ad_group VARCHAR(255),
           landing_page VARCHAR(255), sex VARCHAR(255), alpha_2 VARCHAR(255), alpha_3 VARCHAR(255),
           flag VARCHAR(255), name VARCHAR(255), numeric VARCHAR(255), official_name VARCHAR(255),

           CONSTRAINT unique_install_marketing_id UNIQUE (install_time, marketing_id)
       );   
   ''')
   conn.execute(create_table_query)
   conn.commit() 

# installs data
url = url + "/installs"
installs_params = {"date": previous_date_str}

response = requests.get(url, params=installs_params, headers=headers)
data = response.text.replace('\\', '').replace('}]"}', '}]').replace('/', '')
prefix = '{"count":'      
count_start = data.find(prefix)
count_end = data.find(',"records":"') + len(',"records":"') 
characters_to_strip = count_end - count_start
data = data[characters_to_strip:]

records = json.loads(data)
for record in records:
   params = {
       'install_time': record['install_time'], 'marketing_id': record['marketing_id'], 
       'channel': record['channel'], 'medium': record['medium'],  
       'campaign': record['campaign'], 'keyword': record['keyword'], 
       'ad_content': record['ad_content'], 'ad_group': record['ad_group'],
       'landing_page': record['landing_page'], 'sex': record['sex'], 
       'alpha_2': record['alpha_2'], 'alpha_3': record['alpha_3'],
       'flag': record['flag'], 'name': record['name'], 
       'numeric': record['numeric'], 'official_name': record['official_name']
   }
   insert_query = text(''' 
   INSERT INTO installs_mart VALUES (
       :install_time, :marketing_id, :channel, :medium,  
       :campaign, :keyword, :ad_content, :ad_group,
       :landing_page, :sex, :alpha_2, :alpha_3,
       :flag, :name, :numeric, :official_name
   )
   ON CONFLICT (install_time, marketing_id) DO NOTHING;
   ''')
   conn.execute(insert_query, params) 
   conn.commit()  
print('Installs data loaded to DB')

csv_filename = f'{previous_date_str}_installs_data.csv'
with open(csv_filename, 'w', newline='', encoding='utf-8') as csv_file:
   csv_writer = csv.DictWriter(csv_file, fieldnames=records[0].keys())
   csv_writer.writeheader()
   csv_writer.writerows(records)
print('Installs CSV file created')

# previous_date = current_date - timedelta(days=1)
# previous_date_str = previous_date.strftime("%Y-%m-%d")

# costs data
url = url + "/costs"
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

csv_filename = f'{previous_date_str}_costs_data.csv'
df = pd.read_csv(StringIO(data))
df.to_csv(csv_filename, index=False)
print('Costs CSV file created') 

# previous_date = current_date - timedelta(days=2)
# previous_date_str = previous_date.strftime("%Y-%m-%d")

# orders data
url = url + "/orders"
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

# previous_date = current_date - timedelta(days=1)
# previous_date_str = previous_date.strftime("%Y-%m-%d")

# events data
url = url + "/events"
events_params = {"date": previous_date_str}

response = requests.get(url, params=events_params, headers=headers)
data = response.text.replace('\\', '').replace('{"data":"', '').replace('}}]",', '}}]')
next_page_index = data.find('"next_page"')
data = data[:next_page_index]

csv_filename = f'{previous_date_str}_events_data.csv'
df = pd.read_json(StringIO(data))
df.to_csv(csv_filename, index=False)
print('Events written to CSV')

db_params = os.environ.get('DB_URL')
engine = create_engine(db_params)
df = pd.read_csv(csv_filename)
df.to_sql("events_mart", engine, if_exists='append', index=False)
print('Events written to DB')