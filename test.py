import requests
from datetime import date, timedelta
import pandas as pd
from urllib.parse import quote
import psycopg2
from psycopg2 import sql, extras
from io import StringIO
from sqlalchemy import create_engine

current_date = date.today()
previous_dates = current_date - timedelta(days=3)
previous_dates_str = previous_dates.strftime("%Y-%m-%d")

url = "https://us-central1-passion-fbe7a.cloudfunctions.net/dzn54vzyt5ga/events"
headers = {"Authorization": "gAAAAABlcEioZ8CObY_0cRdtmCdCu9XFkFNs45dpTw3NhQ7ysIwYIn8EEZzpal1uUWdkR0TXgGkY0SXfehCy-4rUZh81Hr3PaZckxyJp3VIcgBzk8qGEpZRMD8_KBJukbtVYkaobYX7jMv4f2TA0kbXkCADTM2yCJw=="}

events_params = {"date": previous_dates_str}

response = requests.get(url, params=events_params, headers=headers)
data = response.text.replace('\\', '').replace('{"data":"', '').replace('}}]",', '}}]')   
next_page_index = data.find('"next_page"')
data = data[:next_page_index]  
# print(data)

df = pd.read_csv(StringIO(data))
df.to_csv('events_data.csv', index=False)
engine = create_engine('postgresql://postgres:123@localhost:5432/api_db')
df.to_sql('events_mart', engine, if_exists='append', index=False)
print(f'Events loaded to PostgreSQL')

 
print('Events loaded to CSV file') 
 


 
# df = pd.read_json(StringIO(data))

# db_params = {
#     'dbname': 'api_db',
#     'user': 'postgres',
#     'password': '123',
#     'host': 'localhost',
#     'port': '5432'
# }

# # Establish a connection to the PostgreSQL database
# conn = psycopg2.connect(**db_params)
# cur = conn.cursor()

# # Create a table if it doesn't exist
# table_name = 'events_mart'
# create_table_query = '''
#     CREATE TABLE IF NOT EXISTS {} (
#         user_id VARCHAR,
#         event_time TIMESTAMP,
#         alpha_2 VARCHAR,
#         alpha_3 VARCHAR,
#         flag VARCHAR,
#         name VARCHAR,
#         numeric INTEGER,
#         official_name VARCHAR,
#         os VARCHAR,
#         brand VARCHAR,
#         model VARCHAR,
#         model_number VARCHAR,
#         specification VARCHAR,
#         event_type VARCHAR,
#         location VARCHAR,
#         user_action_detail VARCHAR,
#         session_number VARCHAR,
#         localization_id VARCHAR,
#         ga_session_id VARCHAR,
#         value FLOAT,
#         state FLOAT,
#         engagement_time_msec FLOAT,
#         current_progress FLOAT,
#         event_origin VARCHAR,
#         place FLOAT,
#         selection FLOAT,
#         analytics_storage VARCHAR,
#         browser VARCHAR,
#         install_store VARCHAR,
#         user_params JSONB
#     )
# '''.format(sql.Identifier(table_name))

# cur.execute(create_table_query)

# # Insert data into the table using psycopg2.extras.Json for JSONB
# df_columns = list(df.columns)
# table_columns = ', '.join(map(sql.Identifier, df_columns))
# insert_query = '''
#     INSERT INTO {} ({}) VALUES %s
# '''.format(sql.Identifier(table_name), sql.SQL(', ').join(map(sql.Identifier, df_columns)))

# data_values = [tuple(row) for row in df.itertuples(index=False, name=None)]
# psycopg2.extras.execute_values(cur, insert_query, data_values, template='(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)')
 
# # Commit the changes and close the connection
# conn.commit()
# conn.close()



# start_index = data.find('"user_params":') + len('"user_params":')
# while start_index != -1:
#     end_index = data.find('}"', start_index)
#     user_params_str = data[start_index:end_index]
#     # Replace quotes within the "user_params" string
#     user_params_str = user_params_str.replace('"{', '{').replace('}"', '}')
#     # Update data with modified "user_params" string
#     data = data[:start_index] + user_params_str + data[end_index:]
#     # Find the next occurrence
#     start_index = data.find('"user_params":', end_index + len(user_params_str))