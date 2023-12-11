import os
from dotenv import load_dotenv
from datetime import date, timedelta 
import requests
from sqlalchemy import create_engine, text
import pandas as pd
import json
import csv
 
load_dotenv()

current_date = date.today()
previous_date = current_date - timedelta(days=1)
previous_date_str = previous_date.strftime("%Y-%m-%d")

url = os.environ.get("API_URL")

installs_url = url + "/installs"
costs_url = url + "/costs"
orders_url = url + "/orders"
events_url = url + "/events"

auth_header = os.environ.get("MY_AUTH_HEADER")
headers = {"Authorization": auth_header}

installs_params = {"date": previous_date_str}
costs_params = {"date": previous_date_str, "dimensions": "location,channel,medium,campaign,keyword,ad_content,ad_group,landing_page"}
orders_params = {"date": previous_date_str}
events_params = {"date": previous_date_str}

db_params = os.environ.get('DB_URL')
engine = create_engine(db_params)

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