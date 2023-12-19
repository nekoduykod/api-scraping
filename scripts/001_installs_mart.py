from dotenv import load_dotenv
from connect_api import APIConnector
from files_utils import create_csv, previous_date_str
from sqlalchemy import create_engine, text
import json
import os

installs_params = {"date": previous_date_str}

api_connector = APIConnector()
response = api_connector.connect_api('installs', installs_params)
data = api_connector.parse_installs(response.text)

records = json.loads(data)  

create_csv(records=True, filename_prefix="installs")

db_params = os.environ.get("DB_URL")
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
         
print('Installs data has been loaded to DB') 