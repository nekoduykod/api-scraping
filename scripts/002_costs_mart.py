from dotenv import load_dotenv
from connect_api import APIConnector
from files_utils import create_csv, previous_date_str
from sqlalchemy import create_engine
import pandas as pd
from io import StringIO
import os
 
costs_params = {"date": previous_date_str, 
                "dimensions": "location,channel,medium,campaign,keyword,ad_content,ad_group,landing_page"}
 
api_connector = APIConnector()
response = api_connector.connect_api('costs', costs_params)
data = api_connector.parse_costs(response.text) 
                
db_params = os.environ.get('DB_URL')
engine = create_engine(db_params)
df = pd.read_csv(StringIO(data))
df.to_sql('costs_mart', engine, if_exists='append', index=False)
print(f'Costs data loaded to DB')
             
create_csv(data=True, filename_prefix="costs")

# write_to_database(filename_prefix="costs")  <-- instead of 5-line snippet